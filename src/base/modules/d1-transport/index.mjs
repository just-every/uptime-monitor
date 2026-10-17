const wide = /[^\u0000-\u00ff]/;
const encoder = new TextEncoder();
const bytes = text => encoder.encode(text).byteLength;
const escaped = text => text.replace(/[^\u0000-\u00ff]/g, c => '\\u' + c.charCodeAt(0).toString(16).padStart(4, '0'));
export const RPC_LIMIT = 32 * 1024 * 1024;

// SQLite parameter tokens, excluding quoted strings/identifiers and comments.
// Keep explicit indexes and repeated names: bind() addresses SQLite slots.
function parameters(sql) {
  const tokens = [], names = new Map(); let slot = 0;
  const pattern = /'(?:[^']|'')*'|"(?:[^"]|"")*"|`(?:[^`]|``)*`|\[[^\]]*\]|--[^\n]*(?:\n|$)|\/\*[\s\S]*?\*\/|\?\d*|[:@$][\w\u0080-\uffff]+(?:::[\w\u0080-\uffff]+)*(?:\([^)]*\))?/g;
  for (const match of sql.matchAll(pattern)) {
    const token = match[0];
    if (!'?:@$'.includes(token[0])) continue;
    let index;
    if (token[0] === '?') index = token.length > 1 ? Number(token.slice(1)) : slot + 1;
    else { if (!names.has(token)) names.set(token, slot + 1); index = names.get(token); }
    slot = Math.max(slot, index);
    tokens.push({ start: match.index, end: match.index + token.length, index: index - 1 });
  }
  return tokens;
}

/** Keep wide bound text out of D1's single batch JSON string. JSON containers
 * passed directly to json_each can be escaped in place; other text is quoted
 * and decoded in SQLite. Stored values and transaction boundaries are unchanged.
 * Values whose encoding exceeds D1's bind ceiling remain native; rpcBytes then
 * accounts for their effect on the ENTIRE call, never just that parameter. */
export function wireSql(statement) {
  const { sql, params = [] } = statement;
  if (!params.some(value => typeof value === 'string' && wide.test(value))) return { sql, params };
  const tokens = parameters(sql), values = [...params], replacements = new Set();
  for (let index = 0; index < params.length; index++) {
    const value = params[index];
    if (typeof value !== 'string' || !wide.test(value)) continue;
    const uses = tokens.filter(token => token.index === index);
    if (!uses.length) continue;
    const json = uses.every(token => /\bjson_each\s*\(\s*$/i.test(sql.slice(0, token.start)));
    const encoded = escaped(json ? value : JSON.stringify(value));
    if (bytes(encoded) > 2_000_000) continue;
    values[index] = encoded;
    if (!json) replacements.add(index);
  }
  let query = '', position = 0;
  for (const token of tokens) {
    query += sql.slice(position, token.start);
    const original = sql.slice(token.start, token.end);
    query += replacements.has(token.index) ? `json_extract(${original},'$')` : original;
    position = token.end;
  }
  const result = { sql: query + sql.slice(position), params: values };
  // Encoding is a transport representation, never permission to exceed D1's
  // SQL budget. Each encoded value was checked against its bind ceiling above.
  // Native text remains an admissible choice;
  // the complete call below still accounts for its two-byte representation.
  if (bytes(result.sql) > 100_000) return { sql, params };
  return result;
}

/** A conservative size of D1's JSON request plus RPC envelope. A single wide
 * character makes workerd serialize every UTF-16 code unit in the call at two
 * bytes. UTF-8 also matters independently for provider payload limits. */
export function rpcSize(statements) {
  let utf8 = 2, units = 2, hasWide = false;
  const add = text => { utf8 += bytes(text); units += text.length; hasWide ||= wide.test(text); };
  for (const statement of statements) {
    add('{"sql":' + JSON.stringify(statement.sql) + ',"params":[');
    for (const value of statement.params ?? []) {
      if (value instanceof ArrayBuffer || ArrayBuffer.isView(value)) {
        // D1 sends binary bindings as numeric arrays. Four bytes per element is
        // conservative and avoids constructing a second large byte array.
        const size = 3 + value.byteLength * 4; utf8 += size; units += size;
      } else add(JSON.stringify(value) + ',');
    }
    add(']},');
  }
  return { utf8, units, hasWide };
}
export const rpcSizeBytes = ({ utf8, units, hasWide }) => 4096 + Math.max(utf8, units * (hasWide ? 2 : 1));
export const rpcBytes = statements => rpcSizeBytes(rpcSize(statements));

export function wireBatch(statements) {
  const encoded = statements.map(wireSql);
  // Dense CJK may be smaller as native two-byte text than as six-byte escapes.
  // Choose for the whole call: one native wide value affects every statement.
  return rpcBytes(statements) < rpcBytes(encoded) ? statements : encoded;
}

/** Adapt only D1's prepare/batch boundary; native statements supplied by a
 * caller remain usable (e.g. an erasure batch shared with product mutations). */
export function wireDatabase(db) {
  const statements = new WeakMap();
  const prepare = (sql, params = []) => {
    const original = { sql, params }, query = wireBatch([original])[0];
    const native = params.length ? db.prepare(query.sql).bind(...query.params) : db.prepare(sql);
    const wrapped = new Proxy(native, { get(target, key) {
      if (key === 'bind') return (...values) => prepare(sql, values);
      const value = target[key];
      if (['all', 'first', 'run', 'raw'].includes(key) && typeof value === 'function') return (...args) => {
        if (rpcBytes([query]) > RPC_LIMIT) throw new Error('D1_RPC_TRANSPORT_LIMIT');
        return value.apply(target, args);
      };
      return typeof value === 'function' ? value.bind(target) : value;
    } });
    statements.set(wrapped, { native, query, original });
    return wrapped;
  };
  return new Proxy(db, { get(target, key) {
    if (key === 'prepare') return prepare;
    if (key === 'batch') return members => {
      const known = members.map(member => statements.get(member));
      const queries = wireBatch(known.filter(Boolean).map(member => member.original));
      if (rpcBytes(queries) > RPC_LIMIT) throw new Error('D1_RPC_TRANSPORT_LIMIT');
      let index = 0;
      return db.batch(members.map((member, i) => {
        if (!known[i]) return member;
        const query = queries[index++];
        return query.params?.length ? db.prepare(query.sql).bind(...query.params) : db.prepare(query.sql);
      }));
    };
    const value = target[key];
    return typeof value === 'function' ? value.bind(target) : value;
  } });
}
