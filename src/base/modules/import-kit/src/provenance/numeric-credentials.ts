import { reflectedCredentialMatcher } from './credentials.ts';

const JSON_NUMBER = /^(-?)(0|[1-9]\d*)(?:\.(\d+))?(?:[eE]([+-]?\d+))?$/;

/** Exact decimal identity, without rounding the coefficient or expanding exponents. */
function decimalIdentity(text: string, maxExponentDigits = Infinity): string | undefined {
  const parts = JSON_NUMBER.exec(text);
  if (!parts) return;
  const fraction = parts[3] ?? '', exponent = parts[4] ?? '0';
  const digits = (parts[2] + fraction).replace(/^0+/, '');
  if (!digits) return '0';
  // A response exponent larger than any credential exponent plus its possible
  // decimal-point adjustment cannot match. Avoid parsing an unbounded BigInt.
  if (exponent.replace(/^[+-]?0*/, '').length > maxExponentDigits) return;
  let end = digits.length;
  while (digits[end - 1] === '0') end--;
  const coefficient = digits.slice(0, end);
  const scale = BigInt(exponent) + BigInt(digits.length - coefficient.length - fraction.length);
  return `${parts[1]}${coefficient}e${scale}`;
}

/** Inspect received numeric text as well as values recoverable by JSON consumers. */
export function reflectedNumberCredentialMatcher(
  secrets: readonly string[], reflectsText = reflectedCredentialMatcher(secrets),
): (lexeme: string) => boolean {
  if (!secrets.length) return () => false;
  const identities = new Set(secrets.map(secret => decimalIdentity(secret)).filter(value => value !== undefined));
  const exponentDigits = Math.max(0, ...Array.from(identities, value => value.split('e')[1]?.replace('-', '').length ?? 1));
  return lexeme => {
    if (reflectsText(lexeme)) return true;
    const number = Number(lexeme);
    if (Number.isFinite(number) && reflectsText(String(number))) return true;
    if (!identities.size) return false;
    // Normalization adjusts an exponent by at most the numeric text's length.
    // One extra digit bounds the sum with the largest credential exponent.
    const limit = Math.max(exponentDigits, String(lexeme.length).length) + 1;
    const identity = decimalIdentity(lexeme, limit);
    return identity !== undefined && identities.has(identity);
  };
}
