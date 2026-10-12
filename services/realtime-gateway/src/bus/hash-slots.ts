const SLOT_COUNT = 16_384;

// The hash slot of a key or channel: CRC16 (XMODEM) modulo 16384, with the
// {hash tag} rule, as in the cluster specification. Auction channels carry no
// braces, so the whole name is hashed.
export function slotOf(name: string): number {
  const bytes = Buffer.from(hashedPart(name));
  let crc = 0;
  for (const byte of bytes) {
    crc ^= byte << 8;
    for (let bit = 0; bit < 8; bit += 1) {
      const topBitSet = (crc & 0x8000) !== 0;
      crc = (topBitSet ? (crc << 1) ^ 0x1021 : crc << 1) & 0xffff;
    }
  }
  return crc % SLOT_COUNT;
}

// Only the text between the first "{" and the next "}" is hashed, when that
// text is not empty.
function hashedPart(name: string): string {
  const open = name.indexOf("{");
  if (open === -1) {
    return name;
  }
  const close = name.indexOf("}", open + 1);
  if (close === -1 || close === open + 1) {
    return name;
  }
  return name.slice(open + 1, close);
}
