// The Vitest configuration refuses to start when a required variable is
// missing, so reaching the throw below means this file was run under some
// other configuration.
export function requireEnvironment(name: string): string {
  const value = process.env[name];
  if (value === undefined) {
    throw new Error(`${name} is not set; run these tests with \`pnpm test:e2e:local\``);
  }
  return value;
}
