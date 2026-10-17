import { Buffer } from 'node:buffer';
/** SHA-256 (FIPS 180-4) whose running state can be saved and restored, so one
 * digest can span several requests. Node's hash cannot export its state. The
 * saved state is the eight chaining words, the unprocessed tail (< 64 bytes)
 * and the total length: it reveals no more of the input than that tail. */
const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2]);
const INITIAL = [0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19];

export interface Sha256State { h: number[]; tail: string; length: number }

export class ResumableSha256 {
  private h = new Uint32Array(INITIAL);
  private block = new Uint8Array(64);
  private used = 0;
  private length = 0;
  private w = new Uint32Array(64);

  static from(state: Sha256State): ResumableSha256 {
    const tail = Buffer.from(state.tail, 'hex');
    if (!Array.isArray(state.h) || state.h.length !== 8 || !state.h.every(word => Number.isInteger(word) && word >= 0 && word <= 0xffffffff)
      || tail.length >= 64 || tail.toString('hex') !== state.tail || !Number.isSafeInteger(state.length) || state.length % 64 !== tail.length) throw new Error('IMPORT_HASH_STATE_INVALID');
    const hash = new ResumableSha256();
    hash.h.set(state.h); hash.block.set(tail); hash.used = tail.length; hash.length = state.length;
    return hash;
  }

  state(): Sha256State {
    return { h: [...this.h], tail: Buffer.from(this.block.subarray(0, this.used)).toString('hex'), length: this.length };
  }

  update(bytes: Uint8Array): this {
    let offset = 0;
    this.length += bytes.length;
    if (this.used) {
      const take = Math.min(64 - this.used, bytes.length);
      this.block.set(bytes.subarray(0, take), this.used); this.used += take; offset = take;
      if (this.used < 64) return this;
      this.compress(this.block, 0); this.used = 0;
    }
    for (; offset + 64 <= bytes.length; offset += 64) this.compress(bytes, offset);
    if (offset < bytes.length) { this.block.set(bytes.subarray(offset), 0); this.used = bytes.length - offset; }
    return this;
  }

  digest(encoding: 'hex'): string {
    if (encoding !== 'hex') throw new Error('IMPORT_HASH_ENCODING');
    const bits = this.length * 8, padding = new Uint8Array((this.used < 56 ? 56 : 120) - this.used + 8);
    padding[0] = 0x80;
    const view = new DataView(padding.buffer);
    view.setUint32(padding.length - 8, Math.floor(bits / 0x100000000)); view.setUint32(padding.length - 4, bits >>> 0);
    const length = this.length;
    this.update(padding); this.length = length;
    return Buffer.from(new Uint8Array(this.h.buffer.slice(0))).swap32().toString('hex');
  }

  private compress(bytes: Uint8Array, offset: number): void {
    const w = this.w, h = this.h;
    for (let i = 0; i < 16; i++) { const j = offset + i * 4; w[i] = (bytes[j] << 24) | (bytes[j + 1] << 16) | (bytes[j + 2] << 8) | bytes[j + 3]; }
    for (let i = 16; i < 64; i++) {
      const a = w[i - 15], b = w[i - 2];
      w[i] = (((a >>> 7) | (a << 25)) ^ ((a >>> 18) | (a << 14)) ^ (a >>> 3)) + w[i - 7] + (((b >>> 17) | (b << 15)) ^ ((b >>> 19) | (b << 13)) ^ (b >>> 10)) + w[i - 16];
    }
    let a = h[0], b = h[1], c = h[2], d = h[3], e = h[4], f = h[5], g = h[6], k = h[7];
    for (let i = 0; i < 64; i++) {
      const t1 = (k + (((e >>> 6) | (e << 26)) ^ ((e >>> 11) | (e << 21)) ^ ((e >>> 25) | (e << 7))) + ((e & f) ^ (~e & g)) + K[i] + w[i]) | 0;
      const t2 = ((((a >>> 2) | (a << 30)) ^ ((a >>> 13) | (a << 19)) ^ ((a >>> 22) | (a << 10))) + ((a & b) ^ (a & c) ^ (b & c))) | 0;
      k = g; g = f; f = e; e = (d + t1) | 0; d = c; c = b; b = a; a = (t1 + t2) | 0;
    }
    h[0] += a; h[1] += b; h[2] += c; h[3] += d; h[4] += e; h[5] += f; h[6] += g; h[7] += k;
  }
}
