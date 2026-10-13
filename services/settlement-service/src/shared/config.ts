import { Type } from "@sinclair/typebox";

// Pieces of configuration that other services spell the same way, copied
// rather than shared because services keep no common runtime code.

export function hostPortList() {
  return Type.Array(Type.String({ pattern: "^[A-Za-z0-9.-]+:[0-9]{1,5}$" }), {
    minItems: 1,
    maxItems: 32,
  });
}

export const nodeEnvSchema = Type.Union([
  Type.Literal("development"),
  Type.Literal("test"),
  Type.Literal("production"),
]);

export const logLevelSchema = Type.Union([
  Type.Literal("fatal"),
  Type.Literal("error"),
  Type.Literal("warn"),
  Type.Literal("info"),
  Type.Literal("debug"),
  Type.Literal("trace"),
]);

// "a:1, b:2" -> ["a:1", "b:2"]; an empty value is an empty list.
export function splitList(value: string): string[] {
  return value === "" ? [] : value.split(",").map((item) => item.trim());
}

export function hasDuplicates(list: readonly string[]): boolean {
  return new Set(list).size !== list.length;
}
