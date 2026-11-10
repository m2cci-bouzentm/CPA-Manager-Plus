import { useEffect, useState, type CSSProperties } from 'react';
import { useTranslation } from 'react-i18next';
import { Modal } from '@/components/ui/Modal';
import { logsApi } from '@/services/api/logs';

type RequestPayloadModalProps = {
  requestId: string | null;
  onClose: () => void;
};

type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

type LogSection = { title: string; value: JsonValue };

type PayloadState =
  | { status: 'loading' }
  | { status: 'ready'; sections: LogSection[] }
  | { status: 'missing' };

type JsonNodeProps = {
  name?: string;
  value: JsonValue;
};

const SECTION_HEADER = /^=== (.+) ===$/;

const monoStyle: CSSProperties = {
  fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
  fontSize: 12,
  lineHeight: 1.6,
};

const keyStyle: CSSProperties = { opacity: 0.65 };

const sectionStyle: CSSProperties = {
  border: '1px solid rgba(128, 128, 128, 0.3)',
  borderRadius: 6,
  padding: '6px 10px',
  marginBottom: 8,
};

const summaryStyle: CSSProperties = { cursor: 'pointer', fontWeight: 600 };

function parseJsonOrText(text: string): JsonValue {
  try {
    return JSON.parse(text) as JsonValue;
  } catch {
    return text;
  }
}

function parseBody(text: string): JsonValue {
  const trimmed = text.trim();
  const json = parseJsonOrText(trimmed);
  if (typeof json !== 'string') return json;

  const events = trimmed
    .split('\n')
    .filter((line) => line.startsWith('data:'))
    .map((line) => line.slice(5).trim())
    .filter((line) => line !== '' && line !== '[DONE]');
  if (events.length === 0) return trimmed;
  return events.map(parseJsonOrText);
}

function parseKeyValueLines(text: string): Record<string, JsonValue> {
  const result: Record<string, JsonValue> = {};
  text.split('\n').forEach((line) => {
    const separator = line.indexOf(':');
    if (separator <= 0) return;
    result[line.slice(0, separator).trim()] = line.slice(separator + 1).trim();
  });
  return result;
}

function parseExchange(text: string): JsonValue {
  const [head, body] = text.split(/\nBody:\n/);
  const [meta, headers] = head.split(/\nHeaders:\n|^Headers:\n/m);
  const result: Record<string, JsonValue> = parseKeyValueLines(meta);
  if (headers !== undefined) result.Headers = parseKeyValueLines(headers);
  if (body !== undefined) result.Body = parseBody(body);
  return result;
}

function parseSectionValue(title: string, text: string): JsonValue {
  if (title === 'REQUEST BODY' || title === 'RESPONSE') return parseBody(text);
  if (title === 'HEADERS') return parseKeyValueLines(text);
  return parseExchange(text);
}

function parseRequestLog(raw: string): LogSection[] {
  const sections: { title: string; lines: string[] }[] = [];
  raw.split('\n').forEach((line) => {
    const header = SECTION_HEADER.exec(line);
    if (header) {
      sections.push({ title: header[1], lines: [] });
      return;
    }
    sections[sections.length - 1]?.lines.push(line);
  });
  const parsed = sections.map(({ title, lines }) => ({
    title,
    value: parseSectionValue(title, lines.join('\n')),
  }));
  return mergeClientRequest(parsed);
}

function mergeClientRequest(sections: LogSection[]): LogSection[] {
  const clientTitles = ['REQUEST INFO', 'HEADERS', 'REQUEST BODY'];
  const client = sections.filter((section) => clientTitles.includes(section.title));
  const others = sections.filter((section) => !clientTitles.includes(section.title));
  const request: Record<string, JsonValue> = {};
  client.forEach((section) => {
    if (section.title === 'REQUEST INFO') Object.assign(request, section.value);
    if (section.title === 'HEADERS') request.Headers = section.value;
    if (section.title === 'REQUEST BODY') request.Body = section.value;
  });
  return [{ title: 'REQUEST', value: request }, ...others];
}

async function readPayload(requestId: string): Promise<PayloadState> {
  try {
    const response = await logsApi.downloadRequestLogById(requestId);
    const raw = await (response.data as Blob).text();
    return { status: 'ready', sections: parseRequestLog(raw) };
  } catch {
    return { status: 'missing' };
  }
}

function jsonEntries(value: JsonValue[] | { [key: string]: JsonValue }): [string, JsonValue][] {
  if (Array.isArray(value)) return value.map((item, index) => [String(index), item]);
  return Object.entries(value);
}

function JsonNode({ name, value }: JsonNodeProps) {
  if (value === null || typeof value !== 'object') {
    return (
      <div style={{ marginLeft: 14, whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
        {name !== undefined && <span style={keyStyle}>{name}: </span>}
        <span>{typeof value === 'string' ? `"${value}"` : String(value)}</span>
      </div>
    );
  }

  const entries = jsonEntries(value);
  const size = Array.isArray(value) ? `[${entries.length}]` : `{${entries.length}}`;
  return (
    <details open style={{ marginLeft: 14 }}>
      <summary style={{ cursor: 'pointer' }}>
        {name !== undefined && <span style={keyStyle}>{name} </span>}
        <span style={keyStyle}>{size}</span>
      </summary>
      {entries.map(([key, item]) => (
        <JsonNode key={key} name={key} value={item} />
      ))}
    </details>
  );
}

function SectionContent({ value }: { value: JsonValue }) {
  if (value === null || typeof value !== 'object') return <JsonNode value={value} />;
  return (
    <>
      {jsonEntries(value).map(([key, item]) => (
        <JsonNode key={key} name={key} value={item} />
      ))}
    </>
  );
}

export function RequestPayloadModal({ requestId, onClose }: RequestPayloadModalProps) {
  const [payload, setPayload] = useState<PayloadState>({ status: 'loading' });

  const { t } = useTranslation();

  useEffect(() => {
    if (!requestId) return;
    let isCurrent = true;
    setPayload({ status: 'loading' });
    readPayload(requestId).then((result) => {
      if (isCurrent) setPayload(result);
    });
    return () => {
      isCurrent = false;
    };
  }, [requestId]);

  return (
    <Modal
      open={requestId !== null}
      title={`${t('usage_analytics.payload_title')} ${requestId ?? ''}`}
      onClose={onClose}
      width="min(1100px, 95vw)"
    >
      {payload.status === 'loading' && <p>{t('usage_analytics.payload_loading')}</p>}
      {payload.status === 'missing' && <p>{t('usage_analytics.payload_missing')}</p>}
      {payload.status === 'ready' && (
        <div style={{ ...monoStyle, maxHeight: '70vh', overflow: 'auto' }}>
          {payload.sections.map((section, index) => (
            <details open key={`${index}-${section.title}`} style={sectionStyle}>
              <summary style={summaryStyle}>{section.title}</summary>
              <SectionContent value={section.value} />
            </details>
          ))}
        </div>
      )}
    </Modal>
  );
}
