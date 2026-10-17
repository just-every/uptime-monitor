import type { Json, SourceLocation } from '../types.ts';
import { fail } from './errors.ts';

/** JSON.parse validates syntax; this pass rejects fields its last-key-wins policy would discard. */
export function parseJson(text: string, location: SourceLocation): Json {
  let value: Json;
  try { value = JSON.parse(text) as Json; } catch { fail('MALFORMED_JSON', location); }
  const stack: ({ keys: Set<string>; expectingKey: boolean } | null)[] = [];
  for (let index = 0; index < text.length; index++) {
    const character = text[index];
    if (character === '{') stack.push({ keys: new Set(), expectingKey: true });
    else if (character === '[') stack.push(null);
    else if (character === '}' || character === ']') stack.pop();
    else if (character === ':' || character === ',') {
      const object = stack.at(-1);
      if (object) object.expectingKey = character === ',';
    } else if (character === '"') {
      const start = index++;
      while (text[index] !== '"') { if (text[index] === '\\') index++; index++; }
      const object = stack.at(-1);
      if (object?.expectingKey) {
        const key = JSON.parse(text.slice(start, index + 1)) as string;
        if (object.keys.has(key)) fail('DUPLICATE_JSON_KEY', location);
        object.keys.add(key);
      }
    }
  }
  return value;
}
