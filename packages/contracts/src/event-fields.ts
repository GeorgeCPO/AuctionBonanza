// Schema pieces every event contract shares.

export const EVENT_ID_PATTERN =
  "^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$";

// Identifiers, versions and money amounts cross the wire as decimal strings.
export const POSITIVE_DECIMAL_PATTERN = "^[1-9][0-9]*$";

export const OCCURRED_AT_PATTERN =
  "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\\.[0-9]{1,6})?Z$";

export function isValidOccurredAt(value: string): boolean {
  const year = Number(value.slice(0, 4));
  const month = Number(value.slice(5, 7));
  const day = Number(value.slice(8, 10));
  const hour = Number(value.slice(11, 13));
  const minute = Number(value.slice(14, 16));
  const second = Number(value.slice(17, 19));

  if (hour > 23 || minute > 59 || second > 59) {
    return false;
  }

  const daysInMonth =
    month === 2
      ? isLeapYear(year)
        ? 29
        : 28
      : month === 4 || month === 6 || month === 9 || month === 11
        ? 30
        : month >= 1 && month <= 12
          ? 31
          : 0;
  return day >= 1 && day <= daysInMonth;
}

function isLeapYear(year: number): boolean {
  return year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
}
