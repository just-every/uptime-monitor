import { configureClaim } from '../../../owner-claim/verify.mjs';
import type { OwnerClaimConfig } from '../../../owner-claim/verify.mjs';

export type DoorAction = 'uploads' | 'plan' | 'apply' | 'status' | 'reconcile' | 'undo';

export interface DoorGrant {
  v: 1;
  iss: string;
  aud: string;
  installationId: string;
  email: string;
  jti: string;
  iat: number;
  exp: number;
  action: DoorAction;
  namespace: string;
  maxBytes: number;
  maxChunks: number;
  maxRecords: number;
  fingerprint?: string;
  approvalRecordId?: string;
  approvedAt?: string;
  catchup?: 'bounded';
}

export class DoorError extends Error {
  readonly code: string;
  readonly status: number;
  readonly details?: Record<string, unknown>;

  constructor(code: string, status = 403, details?: Record<string, unknown>) {
    super(code);
    this.name = 'DoorError';
    this.code = code;
    this.status = status;
    this.details = details;
  }
}

function fail(): never { throw new DoorError('invalid_grant'); }
const object = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === 'object' && !Array.isArray(value);
const requiredKeys = ['v', 'iss', 'aud', 'installationId', 'email', 'jti', 'iat', 'exp', 'action', 'namespace', 'maxBytes', 'maxChunks', 'maxRecords'];
const optionalKeys = ['fingerprint', 'approvalRecordId', 'approvedAt', 'catchup'];
const actions: readonly string[] = ['uploads', 'plan', 'apply', 'status', 'reconcile', 'undo'];

function decode(segment: string): Uint8Array<ArrayBuffer> {
  if (!/^[A-Za-z0-9_-]+$/.test(segment)) fail();
  try {
    const raw = atob(segment.replaceAll('-', '+').replaceAll('_', '/'));
    if (btoa(raw).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/, '') !== segment) fail();
    return Uint8Array.from(raw, c => c.charCodeAt(0));
  } catch { return fail(); }
}

function json(segment: string): Record<string, unknown> {
  try {
    const text = new TextDecoder('utf-8', { fatal: true }).decode(decode(segment));
    const value: unknown = JSON.parse(text);
    if (!object(value)) fail();
    // Walk whole string tokens so colons or escaped quotes inside values cannot
    // masquerade as members. Compare decoded keys, including escaped aliases.
    const keys = new Set<string>();
    for (let i = 0; i < text.length; i++) {
      if (text[i] !== '"') continue;
      const start = i++;
      while (i < text.length && text[i] !== '"') {
        if (text[i] === '\\') i++;
        i++;
      }
      const end = i + 1;
      let next = end;
      while (next < text.length && /\s/.test(text[next])) next++;
      if (text[next] !== ':') continue;
      const key = JSON.parse(text.slice(start, end)) as string;
      if (keys.has(key)) fail();
      keys.add(key);
    }
    return value;
  } catch { return fail(); }
}

function quota(value: unknown, max: number): boolean {
  return typeof value === 'number' && Number.isSafeInteger(value) && value > 0 && value <= max;
}

function approved(value: Record<string, unknown>): boolean {
  if (typeof value.approvalRecordId !== 'string' || !value.approvalRecordId.trim() || value.approvalRecordId.length > 256
    || typeof value.approvedAt !== 'string') return false;
  const date = new Date(value.approvedAt);
  return Number.isFinite(date.getTime()) && date.toISOString() === value.approvedAt;
}

function validateGrant(value: Record<string, unknown>, config: Readonly<OwnerClaimConfig>, now: number): Readonly<DoorGrant> {
  if (requiredKeys.some(key => !Object.hasOwn(value, key))
    || Object.keys(value).some(key => !requiredKeys.includes(key) && !optionalKeys.includes(key))
    || value.v !== 1 || value.iss !== config.issuer || value.aud !== config.appOrigin
    || value.installationId !== config.installationId || value.email !== config.expectedEmail
    || typeof value.jti !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value.jti)
    || typeof value.iat !== 'number' || typeof value.exp !== 'number'
    || !Number.isSafeInteger(value.iat) || !Number.isSafeInteger(value.exp) || value.iat < 0
    || value.iat - now > 30 || value.exp <= now || value.exp <= value.iat || value.exp - value.iat > 300
    || typeof value.action !== 'string' || !actions.includes(value.action)
    || typeof value.namespace !== 'string' || !value.namespace || value.namespace !== value.namespace.trim()
    || value.namespace.length > 256 || /[\x00-\x1f\x7f-\x9f]/.test(value.namespace)
    || !quota(value.maxBytes, 300_000_000) || !quota(value.maxChunks, 1024) || !quota(value.maxRecords, 1_000_000)) fail();

  const catchup = Object.hasOwn(value, 'catchup');
  if (catchup && (value.catchup !== 'bounded' || (value.action !== 'plan' && value.action !== 'apply'))) fail();
  if (catchup && !approved(value)) fail();
  if (value.action === 'apply') {
    if (typeof value.fingerprint !== 'string' || !/^[0-9a-f]{64}$/.test(value.fingerprint) || !approved(value)) fail();
  } else {
    if (Object.hasOwn(value, 'fingerprint')) fail();
    const hasApproval = Object.hasOwn(value, 'approvalRecordId') || Object.hasOwn(value, 'approvedAt');
    if (hasApproval && (value.action !== 'plan' || !catchup || !approved(value))) fail();
  }
  return Object.freeze(value) as unknown as Readonly<DoorGrant>;
}

export async function verifyImportGrant(
  token: string,
  input: OwnerClaimConfig & { now?: number },
): Promise<{ grant: Readonly<DoorGrant>; kid: string }> {
  const configKeys = ['issuer', 'installationId', 'appOrigin', 'expectedEmail', 'publicKeys', 'now'];
  if (!object(input) || Reflect.ownKeys(input).some(key => typeof key !== 'string' || !configKeys.includes(key))) throw new DoorError('invalid_configuration', 503);
  let config: Readonly<OwnerClaimConfig>;
  try { config = configureClaim(input); }
  catch { throw new DoorError('invalid_configuration', 503); }
  const now = input.now === undefined ? Math.floor(Date.now() / 1000) : input.now;
  if (!Number.isSafeInteger(now) || now < 0) throw new DoorError('invalid_configuration', 503);
  if (typeof token !== 'string' || token.length > 8192) fail();
  const parts = token.split('.');
  if (parts.length !== 3) fail();
  const header = json(parts[0]);
  if (Object.keys(header).length !== 3 || !['alg', 'typ', 'kid'].every(key => Object.hasOwn(header, key))
    || header.alg !== 'ES256' || header.typ !== 'rer-import-grant+jwt'
    || typeof header.kid !== 'string' || !Object.hasOwn(config.publicKeys, header.kid)) fail();
  const kid = header.kid as string;
  const signature = decode(parts[2]);
  if (signature.length !== 64) fail();
  try {
    const key = await crypto.subtle.importKey('jwk', config.publicKeys[kid], { name: 'ECDSA', namedCurve: 'P-256' }, false, ['verify']);
    const message = new TextEncoder().encode(`${parts[0]}.${parts[1]}`);
    if (!await crypto.subtle.verify({ name: 'ECDSA', hash: 'SHA-256' }, key, signature, message)) fail();
  } catch { fail(); }
  return { grant: validateGrant(json(parts[1]), config, now), kid };
}
