// Money is a decimal string of minor units end to end ("1050" is 10.50 for the
// two-decimal currencies the backend accepts: EUR, GBP, USD). Nothing here
// converts to a number, so large amounts cannot lose precision.

const MINOR_DIGITS = 2;

// "1050" -> "10.50". Returns the input unchanged when it is not digits.
export function formatMinor(amountMinor: string): string {
  if (!/^\d+$/.test(amountMinor)) {
    return amountMinor;
  }
  const padded = amountMinor.padStart(MINOR_DIGITS + 1, "0");
  const whole = padded.slice(0, -MINOR_DIGITS).replace(/^0+(?=\d)/, "");
  const fraction = padded.slice(-MINOR_DIGITS);
  return `${whole}.${fraction}`;
}

export function formatMoney(amountMinor: string, currencyCode: string): string {
  return `${formatMinor(amountMinor)} ${currencyCode}`;
}

// "10.5" -> "1050", "10" -> "1000". Returns undefined for anything that is not
// a positive amount with at most two decimals.
export function parseMajorToMinor(input: string): string | undefined {
  const match = /^(\d+)(?:\.(\d{1,2}))?$/.exec(input.trim());
  if (match === null) {
    return undefined;
  }
  const whole = match[1] ?? "0";
  const fraction = (match[2] ?? "").padEnd(MINOR_DIGITS, "0");
  const minor = `${whole}${fraction}`.replace(/^0+(?=\d)/, "");
  if (minor === "0") {
    return undefined;
  }
  return minor;
}

// The lowest bid the form can suggest: the current price plus the increment,
// or the starting price when nobody has bid.
export function addMinor(a: string, b: string): string {
  return (BigInt(a) + BigInt(b)).toString();
}
