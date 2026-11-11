package archiveprojection

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/processlock"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/sqlite"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/usage"
)

const projectionTable = "usage_monitoring_event_projection_v1"

type Result struct {
	Repaired int     `json:"repaired"`
	EventIDs []int64 `json:"event_ids"`
}

type missingEvent struct {
	ID          int64
	Hash        string
	RunID       string
	Sequence    int64
	TimestampMS int64
}

type segmentKey struct {
	RunID    string
	Sequence int64
}

type recordEnvelope struct {
	EventID   int64  `json:"_cpamp_archive_event_id"`
	EventHash string `json:"event_hash"`
}

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) (runErr error) {
	flags := flag.NewFlagSet("repair-archive-projection", flag.ContinueOnError)
	flags.SetOutput(stderr)
	dbPath := flags.String("db-path", "", "path to the existing CPA Manager Plus usage.sqlite database")
	archiveDir := flags.String("archive-dir", "", "path to the usage archive directory")
	flags.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "Usage: cpa-manager-plus repair-archive-projection [--db-path PATH] [--archive-dir PATH]")
		_, _ = fmt.Fprintln(stderr, "Stop Manager Server before running this offline command.")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("repair-archive-projection does not accept positional arguments")
	}
	path := strings.TrimSpace(*dbPath)
	directory := strings.TrimSpace(*archiveDir)
	if path == "" || directory == "" {
		cfg, err := config.LoadWithoutCreatingDefault()
		if err != nil {
			return fmt.Errorf("load config without creating defaults: %w", err)
		}
		path = firstNonEmpty(path, cfg.DBPath)
		directory = firstNonEmpty(directory, cfg.UsageArchiveDir)
	}
	path, err := sqlite.ResolveMaintenancePath(path)
	if err != nil {
		return err
	}
	databaseLock, err := processlock.Acquire(path)
	if err != nil {
		if errors.Is(err, processlock.ErrLocked) {
			return fmt.Errorf("repair-archive-projection requires exclusive database ownership; stop Manager Server and retry: %w", err)
		}
		return fmt.Errorf("acquire offline repair-archive-projection database lock: %w", err)
	}
	defer func() {
		if err := databaseLock.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("release offline repair-archive-projection database lock: %w", err))
		}
	}()
	result, err := Repair(ctx, databaseLock.DatabasePath(), directory)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func Repair(ctx context.Context, dbPath, archiveDir string) (Result, error) {
	db, err := sqlite.Open(dbPath)
	if err != nil {
		return Result{}, err
	}
	defer db.Close()
	missing, err := loadMissing(ctx, db)
	if err != nil {
		return Result{}, err
	}
	if len(missing) == 0 {
		return Result{EventIDs: []int64{}}, nil
	}
	events, err := readArchivedEvents(ctx, db, archiveDir, missing)
	if err != nil {
		return Result{}, err
	}
	rows, err := buildProjectionRows(ctx, events)
	if err != nil {
		return Result{}, err
	}
	return insertProjectionRows(ctx, db, missing, rows)
}

func firstNonEmpty(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func loadMissing(ctx context.Context, db interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}) ([]missingEvent, error) {
	rows, err := db.QueryContext(ctx, `select r.raw_event_id, r.event_hash, r.run_id, r.segment_sequence, r.timestamp_ms
		from usage_archive_event_refs r
		left join `+projectionTable+` p on p.event_id = r.raw_event_id
		where r.raw_deleted_at_ms is not null and p.event_id is null
		order by r.raw_event_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	missing := []missingEvent{}
	for rows.Next() {
		var event missingEvent
		if err := rows.Scan(&event.ID, &event.Hash, &event.RunID, &event.Sequence, &event.TimestampMS); err != nil {
			return nil, err
		}
		missing = append(missing, event)
	}
	return missing, rows.Err()
}

func readArchivedEvents(ctx context.Context, db *sql.DB, archiveDir string, missing []missingEvent) ([]usage.Event, error) {
	wanted := map[segmentKey]map[int64]missingEvent{}
	for _, event := range missing {
		key := segmentKey{RunID: event.RunID, Sequence: event.Sequence}
		if wanted[key] == nil {
			wanted[key] = map[int64]missingEvent{}
		}
		wanted[key][event.ID] = event
	}
	found := map[int64]usage.Event{}
	for key, events := range wanted {
		segmentEvents, err := readSegment(ctx, db, archiveDir, key, events)
		if err != nil {
			return nil, err
		}
		for id, event := range segmentEvents {
			found[id] = event
		}
	}
	ordered := make([]usage.Event, 0, len(missing))
	for _, event := range missing {
		restored, ok := found[event.ID]
		if !ok {
			return nil, fmt.Errorf("archived event %d is not in segment %s/%d", event.ID, event.RunID, event.Sequence)
		}
		ordered = append(ordered, restored)
	}
	return ordered, nil
}

func readSegment(ctx context.Context, db *sql.DB, archiveDir string, key segmentKey, events map[int64]missingEvent) (map[int64]usage.Event, error) {
	var fileName, contentSHA256, status string
	err := db.QueryRowContext(ctx, `select file_name, content_sha256, status from usage_archive_segments
		where run_id = ? and sequence = ?`, key.RunID, key.Sequence).Scan(&fileName, &contentSHA256, &status)
	if err != nil {
		return nil, fmt.Errorf("load archive segment %s/%d: %w", key.RunID, key.Sequence, err)
	}
	if status != "verified" {
		return nil, fmt.Errorf("archive segment %s/%d is %s, not verified", key.RunID, key.Sequence, status)
	}
	root, err := filepath.Abs(archiveDir)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(root, filepath.FromSlash(fileName))
	relative, err := filepath.Rel(root, path)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return nil, fmt.Errorf("archive segment %s/%d path leaves the archive directory", key.RunID, key.Sequence)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(content)
	if hex.EncodeToString(digest[:]) != contentSHA256 {
		return nil, fmt.Errorf("archive segment %s/%d checksum does not match", key.RunID, key.Sequence)
	}
	reader, err := gzip.NewReader(bytes.NewReader(content))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64*1024), usage.MaxJSONLRecordBytes+1)
	restored := map[int64]usage.Event{}
	for scanner.Scan() {
		payload := scanner.Bytes()
		var envelope recordEnvelope
		if err := json.Unmarshal(payload, &envelope); err != nil {
			return nil, fmt.Errorf("decode archive segment %s/%d record: %w", key.RunID, key.Sequence, err)
		}
		wanted, ok := events[envelope.EventID]
		if !ok {
			continue
		}
		parsed, err := usage.ParseImportPayload(payload)
		if err != nil {
			return nil, fmt.Errorf("parse archived event %d: %w", envelope.EventID, err)
		}
		if parsed.Failed != 0 || parsed.Unsupported != 0 || len(parsed.Events) != 1 {
			return nil, fmt.Errorf("archived event %d is not restorable", envelope.EventID)
		}
		event := parsed.Events[0]
		if event.EventHash != wanted.Hash || event.TimestampMS != wanted.TimestampMS {
			return nil, fmt.Errorf("archived event %d does not match its archive reference", envelope.EventID)
		}
		restored[envelope.EventID] = event
	}
	return restored, scanner.Err()
}

func buildProjectionRows(ctx context.Context, events []usage.Event) (map[string]map[string]any, error) {
	directory, err := os.MkdirTemp("", "cpamp-archive-projection-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(directory)
	recoveryPath := filepath.Join(directory, "usage.sqlite")
	recovery, err := store.Open(recoveryPath)
	if err != nil {
		return nil, err
	}
	inserted, err := recovery.InsertEvents(ctx, events)
	if err != nil {
		_ = recovery.Close()
		return nil, err
	}
	if inserted.Inserted != len(events) {
		_ = recovery.Close()
		return nil, fmt.Errorf("recovery store inserted %d of %d archived events", inserted.Inserted, len(events))
	}
	for range 100 {
		caughtUp, err := recovery.CatchUpUsageMonitoringProjection(ctx, 1000, time.Now().UnixMilli())
		if err != nil {
			_ = recovery.Close()
			return nil, err
		}
		if caughtUp.Processed == 0 && !caughtUp.ContinueSoon && !caughtUp.Pending {
			break
		}
	}
	if err := recovery.Close(); err != nil {
		return nil, err
	}
	db, err := sqlite.Open(recoveryPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, `select e.event_hash as archive_event_hash, p.*
		from `+projectionTable+` p join usage_events e on e.id = p.event_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	projected := map[string]map[string]any{}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for index := range values {
			pointers[index] = &values[index]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		row := map[string]any{}
		for index, column := range columns[1:] {
			row[column] = values[index+1]
		}
		projected[fmt.Sprint(values[0])] = row
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(projected) != len(events) {
		return nil, fmt.Errorf("recovery store projected %d of %d archived events", len(projected), len(events))
	}
	return projected, nil
}

func insertProjectionRows(ctx context.Context, db *sql.DB, missing []missingEvent, rows map[string]map[string]any) (Result, error) {
	columns, err := tableColumns(ctx, db)
	if err != nil {
		return Result{}, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Result{}, err
	}
	defer tx.Rollback()
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(columns)), ", ")
	statement := fmt.Sprintf("insert into %s (%s) values (%s)", projectionTable, strings.Join(columns, ", "), placeholders)
	result := Result{EventIDs: []int64{}}
	for _, event := range missing {
		row, ok := rows[event.Hash]
		if !ok {
			return Result{}, fmt.Errorf("no projection row was built for archived event %d", event.ID)
		}
		if fmt.Sprint(row["timestamp_ms"]) != fmt.Sprint(event.TimestampMS) {
			return Result{}, fmt.Errorf("projection row for archived event %d has another timestamp", event.ID)
		}
		values := make([]any, len(columns))
		for index, column := range columns {
			value, present := row[column]
			if !present {
				return Result{}, fmt.Errorf("recovery projection has no column %s", column)
			}
			values[index] = value
		}
		values[indexOf(columns, "event_id")] = event.ID
		if _, err := tx.ExecContext(ctx, statement, values...); err != nil {
			return Result{}, fmt.Errorf("insert projection row for archived event %d: %w", event.ID, err)
		}
		result.EventIDs = append(result.EventIDs, event.ID)
	}
	remaining, err := loadMissing(ctx, tx)
	if err != nil {
		return Result{}, err
	}
	if len(remaining) != 0 {
		return Result{}, fmt.Errorf("%d archived events still have no projection row", len(remaining))
	}
	if err := tx.Commit(); err != nil {
		return Result{}, err
	}
	result.Repaired = len(result.EventIDs)
	return result, nil
}

func tableColumns(ctx context.Context, db *sql.DB) ([]string, error) {
	rows, err := db.QueryContext(ctx, "select name from pragma_table_info('"+projectionTable+"') order by cid")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, rows.Err()
}

func indexOf(values []string, wanted string) int {
	for index, value := range values {
		if value == wanted {
			return index
		}
	}
	return -1
}

func IsHelp(err error) bool {
	return errors.Is(err, flag.ErrHelp)
}
