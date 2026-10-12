// Shared by every feature that crosses PostgreSQL bigint IDs and minor-unit
// monetary amounts over the JSON boundary as decimal strings.

const POSTGRES_BIGINT_MAX = 9_223_372_036_854_775_807n;

export function parseMinorUnits(value: string, allowZero: boolean): string | undefined {
  if (!/^(0|[1-9][0-9]*)$/.test(value)) {
    return undefined;
  }
  const amount = BigInt(value);
  if (amount > POSTGRES_BIGINT_MAX || (!allowZero && amount === 0n)) {
    return undefined;
  }
  return amount.toString();
}

export function isPostgresBigintId(value: string): boolean {
  if (!/^[1-9][0-9]{0,18}$/.test(value)) {
    return false;
  }
  return BigInt(value) <= POSTGRES_BIGINT_MAX;
}
