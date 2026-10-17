import { Buffer } from 'node:buffer';
import { parseJson } from './json.ts';
import type { FileSourceInput, Json, ParsedRecord, ReaderLimits, SourceBundle, SourceLocation } from '../types.ts';
import { consumeBytes, utf8Decoder } from './bytes.ts';
import { fail } from './errors.ts';
import { limitsFor, RecordCollector, readerPaginationPolicy, safeValue, textId } from './records.ts';

/** JSON arrays are visited per element; an object/scalar is one input. NDJSON uses physical rows. */
export async function readFileSource(input: FileSourceInput, limits?: ReaderLimits): Promise<SourceBundle> {
  const policy = readerPaginationPolicy(input.paginationCursorPolicy);
  const bounded = limitsFor(limits);
  textId(input.name); safeValue(input.name, [], undefined, policy);
  if (input.format !== 'json' && input.format !== 'ndjson') fail('INVALID_SOURCE_FORMAT');
  if (typeof input.parse !== 'function') fail('INVALID_SOURCE_PARSER');
  const collector = new RecordCollector(input.namespace, bounded, [], policy);
  const location = { artifact: input.name };
  const decoder = utf8Decoder(location);
  const parse = (value: Json, index: number, at: SourceLocation): void => {
    const encoded = safeValue(value, [], at, policy);
    if (Buffer.byteLength(encoded) > bounded.maxRecordBytes) fail('SOURCE_RECORD_BYTE_LIMIT', at);
    let result: ParsedRecord | ParsedRecord[];
    try { result = input.parse(value, index); } catch { fail('SOURCE_PARSER_FAILED', at); }
    const records = Array.isArray(result) ? result : [result];
    if (!records.length) fail('SOURCE_OBJECT_DROPPED', at);
    collector.add(records, at, '');
  };
  let pending = '', row = 0, index = 0;
  const line = (text: string): void => {
    row++;
    const at = { ...location, row };
    if (!text.trim()) return;
    if (Buffer.byteLength(text) > bounded.maxRecordBytes) fail('SOURCE_RECORD_BYTE_LIMIT', at);
    parse(parseJson(text, at), index++, at);
  };
  const decoded = (text: string): void => {
    pending += text;
    if (input.format === 'ndjson') {
      let start = 0, end: number;
      while ((end = pending.indexOf('\n', start)) >= 0) { line(pending.slice(start, end)); start = end + 1; }
      pending = pending.slice(start);
      if (Buffer.byteLength(pending) > bounded.maxRecordBytes) fail('SOURCE_RECORD_BYTE_LIMIT', { ...location, row: row + 1 });
    }
  };
  const artifact = await consumeBytes(input.bytes, { used: 0, max: bounded.maxBytes }, location, chunk => decoded(decoder.decode(chunk)));
  decoded(decoder.decode(undefined, true));
  if (input.format === 'ndjson') { if (pending.length) line(pending); }
  else {
    const root = parseJson(pending, location);
    if (Array.isArray(root)) root.forEach((value, position) => parse(value, position, { ...location, row: position + 1, pointer: `/${position}` }));
    else parse(root, 0, { ...location, row: 1, pointer: '' });
  }
  collector.bundle.artifacts.push({ name: input.name, ...artifact });
  for (const record of collector.bundle.records) record.artifactHash = artifact.sha256;
  return collector.bundle;
}
