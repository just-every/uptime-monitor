import { Buffer } from 'node:buffer';
/** Retain exact bytes in fixed blocks; one-byte stream chunks cannot create an unbounded object list. */
export class OriginalBytes {
  private readonly blocks: Buffer[] = [];
  private current: Buffer | undefined;
  private offset = 0;
  private size = 0;
  append(bytes: Uint8Array): void {
    let inputOffset = 0;
    while (inputOffset < bytes.byteLength) {
      let current = this.current;
      if (!current || this.offset === current.byteLength) {
        // The explicit type keeps this narrowing sound in projects whose global Buffer is overridden
        // by Worker typings (their `declare const Buffer: any` makes allocUnsafe return any).
        const fresh: Buffer = Buffer.allocUnsafe(64 * 1024);
        this.current = fresh;
        this.blocks.push(fresh);
        this.offset = 0;
        current = fresh;
      }
      const length = Math.min(bytes.byteLength - inputOffset, current.byteLength - this.offset);
      current.set(bytes.subarray(inputOffset, inputOffset + length), this.offset);
      this.offset += length; inputOffset += length; this.size += length;
    }
  }
  finish(): { status: 'available'; base64: string } {
    // Limit the final view: unused bytes in allocUnsafe blocks must never be exposed.
    const filled = this.blocks.map((block, index) => index === this.blocks.length - 1 ? block.subarray(0, this.offset) : block);
    return { status: 'available', base64: Buffer.concat(filled, this.size).toString('base64') };
  }
}
