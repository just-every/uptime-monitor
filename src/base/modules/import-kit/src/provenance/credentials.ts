import type { ImportPlan, PaginationCursorPolicy } from '../types.ts';

const MAX_ENCODING_DEPTH = 32;

/** Trusted adapter selection, never inferred from response metadata. */
export function paginationPolicy(value: unknown): PaginationCursorPolicy | undefined {
  if (value !== undefined && value !== 'page-token-v1') throw new Error('INVALID_PAGINATION_CURSOR_POLICY');
  return value;
}

/** Inspect every encoding layer without losing malformed or Latin-1 bytes. */
function* credentialTextLayers(value: string): Generator<string> {
  let decoded = value, depth = 0;
  yield decoded;
  while (/%[\da-f]{2}/i.test(decoded)) {
    if (++depth > MAX_ENCODING_DEPTH) throw new Error('CREDENTIAL_ENCODING_DEPTH_LIMIT');
    // Decode individual bytes so an unrelated malformed escape cannot hide a key
    // or credential. UTF-8 interpretations are matched against known secrets below.
    const next = decoded.replace(/%([\da-f]{2})/gi, (_, hex: string) => String.fromCharCode(parseInt(hex, 16)));
    if (next === decoded) break;
    decoded = next;
    yield decoded;
  }
}

/** Decode nested percent escapes for inspection only; never rewrite retained bytes or requests. */
export function decodedCredentialText(value: string): string {
  let decoded = value;
  for (const layer of credentialTextLayers(value)) decoded = layer;
  return decoded;
}

/** Match only provided credentials, in literal or encoded byte form at each layer. */
export function reflectedCredentialMatcher(secrets: readonly string[]): (value: string) => boolean {
  if (!secrets.length) return () => false;
  const escape = (text: string) => text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const bytePattern = (byte: number) => {
    const hex = byte.toString(16).padStart(2, '0').replace(/[a-f]/g, digit => `[${digit}${digit.toUpperCase()}]`);
    return `(?:${escape(String.fromCharCode(byte))}|%(?:25){0,${MAX_ENCODING_DEPTH - 1}}${hex})`;
  };
  const encoder = new TextEncoder();
  const patterns = secrets.map(secret => new RegExp([...secret].map(character => {
    const codePoint = character.codePointAt(0)!;
    const literal = codePoint <= 255 ? bytePattern(codePoint) : escape(character);
    return codePoint < 128 ? literal : `(?:${literal}|${Array.from(encoder.encode(character), bytePattern).join('')})`;
  }).join('')));
  return value => {
    // Per-byte alternatives preserve literal '%' in credentials while another part
    // is escaped, and allow mixed literal/Latin-1/UTF-8 representations.
    for (const layer of credentialTextLayers(value)) if (patterns.some(pattern => pattern.test(layer))) return true;
    return false;
  };
}

export function credentialQueryKey(rawKey: string, policy?: PaginationCursorPolicy): boolean {
  // The sole exception is the exact wire spelling, not encoded/case aliases.
  if (policy === 'page-token-v1' && rawKey === 'page_token') return false;
  return /token|secret|key|auth|signature|credential|password|cookie/i.test(decodedCredentialText(rawKey));
}

/** Inspection-only structure; opaque values are checked separately, never reparsed as outer query syntax. */
function credentialUrlStructure(value: string, policy?: PaginationCursorPolicy): string {
  if (policy !== 'page-token-v1') return value;
  const queryStart = value.indexOf('?'), fragmentStart = value.indexOf('#');
  if (queryStart < 0 || (fragmentStart >= 0 && fragmentStart < queryStart)) return value;
  const queryEnd = fragmentStart < 0 ? value.length : fragmentStart;
  const pairs = value.slice(queryStart + 1, queryEnd).split('&');
  // Split before decoding so only an exact wire key owns an opaque value.
  const structure = pairs.filter(pair => pair.split('=')[0] !== 'page_token').join('&');
  return value.slice(0, queryStart + 1) + structure + value.slice(queryEnd);
}

export function assertNoCredentials(value: unknown, policy?: PaginationCursorPolicy): void {
  paginationPolicy(policy);
  const visit = (item: unknown): void => {
    if (Array.isArray(item)) { for (const child of item) visit(child); return; }
    if (item && typeof item === 'object') {
      for (const [key, child] of Object.entries(item)) {
        if (key === 'secret' && typeof child === 'boolean') continue;
        if (/^(?:access[_-]?token|refresh[_-]?token|token|authorization|cookie|password|api[_-]?key|secret)$/i.test(decodedCredentialText(key))) throw new Error('CREDENTIAL_IN_DURABLE_DATA');
        visit(child);
      }
    } else if (typeof item === 'string') {
      // Relative pagination links receive the same check as absolute links.
      const decoded = decodedCredentialText(item);
      if (!/^(?:https?:\/\/|\/|\?)/i.test(decoded)) return;
      const url = new URL(item, 'https://credential-check.invalid');
      const structure = decodedCredentialText(credentialUrlStructure(item, policy).split('#')[0]);
      const decodedUrl = new URL(structure, 'https://credential-check.invalid');
      if (url.username || url.password || decodedUrl.username || decodedUrl.password) throw new Error('CREDENTIAL_IN_DURABLE_URL');
      for (const pair of url.search.slice(1).split('&')) {
        if (credentialQueryKey(pair.split('=')[0], policy)) throw new Error('CREDENTIAL_IN_DURABLE_URL');
      }
      for (const child of url.searchParams.values()) visit(child);
      // Encoded delimiters outside opaque values must not hide credential keys.
      // Any key introduced by decoding has no exact-wire cursor exemption.
      // A decoded fragment delimiter must not truncate these query checks either.
      const queryStart = structure.indexOf('?');
      for (const pair of (queryStart < 0 ? '' : structure.slice(queryStart + 1)).split('&')) {
        const key = pair.split('=')[0];
        if (credentialQueryKey(key)) throw new Error('CREDENTIAL_IN_DURABLE_URL');
      }
    }
  };
  visit(value);
}

/** Only source custody carries the opt-in; mapped target effects stay strict. */
export function assertPlanCredentials(plan: ImportPlan): void {
  const policy = paginationPolicy(plan.source.paginationCursorPolicy);
  for (const [key, value] of Object.entries(plan)) {
    if (key === 'source') assertNoCredentials({ [key]: value }, policy);
    else if (key === 'entries') for (const entry of plan.entries) {
      for (const [field, item] of Object.entries(entry)) assertNoCredentials({ [field]: item }, field === 'source' ? policy : undefined);
    }
    else assertNoCredentials({ [key]: value });
  }
}
