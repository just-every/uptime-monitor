import { dictionaryEncode, dictionaryDecode } from './plan-dictionary.ts';
import { deflateRawSync, inflateRawSync, brotliCompressSync, brotliDecompressSync, constants } from 'node:zlib';
import { Buffer } from 'node:buffer';
import type { ImportPlan, Json } from '../types.ts';
import type { D1PlanCodec } from './types.ts';
import { planFingerprint } from '../plan/index.ts';
import { canonicalHash, canonicalSize, canonicalChunks, cloneJson } from '../provenance/canonical-stream.ts';
import { canonicalJson, sha256 } from '../provenance/identity.ts';

function validateRoundTrip(plan: ImportPlan, payload: Json, codec: D1PlanCodec): void {
  const roundtrip = codec.decode(cloneJson(payload));
  if (roundtrip.fingerprint !== plan.fingerprint || planFingerprint(roundtrip) !== plan.fingerprint) throw new Error('IMPORT_PLAN_CODEC_MISMATCH');
}

export function compressedPlan(plan: ImportPlan, codec?: D1PlanCodec): string {
  const expandedSize = canonicalSize(plan), payload = codec ? codec.encode(plan) : plan as unknown as Json;
  if (codec) validateRoundTrip(plan, payload, codec);
  const payloadSize = codec ? canonicalSize(payload) : undefined;
  const body = codec ? JSON.stringify(dictionaryEncode(payload)) : [...canonicalChunks(plan)].join('');
  const compressed = codec ? brotliCompressSync(body,{params:{[constants.BROTLI_PARAM_QUALITY]:4,[constants.BROTLI_PARAM_LGWIN]:20}}) : deflateRawSync(body);
  return canonicalJson({ d1Plan: codec ? 3 : 2, ...(codec ? {codec:codec.id,dictionary:2,compression:'brotli',fingerprint:plan.fingerprint,expandedSize,payloadSize} : {}), size: Buffer.byteLength(body), sha256: sha256(body), data: Buffer.from(compressed.buffer,compressed.byteOffset,compressed.byteLength).toString('base64') });
}
export function expandCompressedPlan(body: string, codec?: D1PlanCodec): ImportPlan {
  return expandCompressedEnvelope(body, JSON.parse(body), codec);
}
/** Custody loading already parses the envelope to distinguish storage formats.
 * Reuse that object, still comparing its complete canonical bytes with body. */
export function expandCompressedEnvelope(body: string, value: any, codec?: D1PlanCodec): ImportPlan {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('STORED_PLAN_CHANGED');
  if (canonicalHash(value) !== sha256(body)) throw new Error('STORED_PLAN_CHANGED');
  if (![2,3].includes(value.d1Plan) || !Number.isSafeInteger(value.size) || value.size < 0) throw new Error('STORED_PLAN_CHANGED');
  const keys = value.d1Plan === 3 ? ['codec','compression','d1Plan','data','dictionary','expandedSize','fingerprint','sha256','size'] : ['d1Plan','data','sha256','size'];
  if (value.d1Plan === 3 && value.dictionary === 2) keys.push('payloadSize');
  if (canonicalJson(Object.keys(value).sort()) !== canonicalJson(keys.sort()) || value.d1Plan === 3 && (![1,2].includes(value.dictionary) || value.compression !== 'brotli')) throw new Error('STORED_PLAN_CHANGED');
  if (value.compression !== undefined && value.compression !== 'brotli') throw new Error('STORED_PLAN_CHANGED');
  if (value.d1Plan === 3 && (!Number.isSafeInteger(value.expandedSize) || value.expandedSize < 0)) throw new Error('STORED_PLAN_CHANGED');
  if (value.d1Plan === 3 && value.dictionary === 2 && (!Number.isSafeInteger(value.payloadSize) || value.payloadSize < 0)) throw new Error('STORED_PLAN_CHANGED');
  const payload = decodePayload(value, codec);
  const payloadHash = value.d1Plan === 3 ? canonicalHash(payload) : undefined;
  const plan = (value.d1Plan === 3 ? codec!.decode(payload) : payload) as ImportPlan;
  if (value.d1Plan === 3 && canonicalHash(codec!.encode(plan)) !== payloadHash) throw new Error('STORED_PLAN_CHANGED');
  if (value.d1Plan === 3 && plan.fingerprint !== value.fingerprint) throw new Error('STORED_PLAN_CHANGED');
  if (value.d1Plan === 3 && canonicalSize(plan) !== value.expandedSize) throw new Error('STORED_PLAN_CHANGED');
  if (planFingerprint(plan) !== plan.fingerprint) throw new Error('STORED_PLAN_CHANGED');
  return plan;
}
/** Release compressed bytes and the dictionary tree before native decoding. */
function decodePayload(value: any, codec?: D1PlanCodec): Json {
  // workerd accounts Brotli decoder workspace against maxOutputLength too.
  // Keep that allowance fixed and still require the exact declared output.
  const input = Buffer.from(value.data, 'base64');
  const result: any = (value.compression === 'brotli' ? brotliDecompressSync : inflateRawSync)(input, { info: true, maxOutputLength: value.size + (value.compression === 'brotli' ? 2 * 1024 * 1024 : 0) });
  if (input.toString('base64') !== value.data || result.engine.bytesWritten !== input.length) throw new Error('STORED_PLAN_CHANGED');
  const decoded: Buffer = result.buffer;
  if (decoded.length !== value.size || sha256(decoded) !== value.sha256) throw new Error('STORED_PLAN_CHANGED');
  if (value.d1Plan === 3 && (!codec || codec.id !== value.codec)) throw new Error('IMPORT_PLAN_CODEC_REQUIRED');
  const packed = JSON.parse(Buffer.from(decoded.buffer,decoded.byteOffset,decoded.byteLength).toString('utf8'));
  if (canonicalHash(packed) !== value.sha256) throw new Error('STORED_PLAN_CHANGED');
  if (value.d1Plan === 3 && (value.dictionary === 2 ? packed?.version !== 2 : packed?.version !== undefined)) throw new Error('STORED_PLAN_CHANGED');
  const payload = value.d1Plan === 3 ? dictionaryDecode(packed, value.dictionary === 2 ? value.payloadSize : undefined) : packed;
  if (value.d1Plan === 3 && value.dictionary === 2 && canonicalSize(payload) !== value.payloadSize) throw new Error('STORED_PLAN_CHANGED');
  return payload;
}
