import { describe, expect, it } from "vitest";

import { SerialByKey } from "../../src/shared/serial-by-key.js";

// A task the test finishes by hand, recording when it starts.
function controlledTask(log: string[], name: string) {
  const done = Promise.withResolvers<void>();
  return {
    task: async () => {
      log.push(`${name} started`);
      await done.promise;
      log.push(`${name} finished`);
      return name;
    },
    finish: () => done.resolve(),
    fail: () => done.reject(new Error(`${name} failed`)),
  };
}

const settle = () => new Promise((resolve) => setImmediate(resolve));

describe("SerialByKey", () => {
  it("starts a task only after the previous task with the same key finished", async () => {
    const log: string[] = [];
    const serial = new SerialByKey();
    const first = controlledTask(log, "first");
    const second = controlledTask(log, "second");

    const firstResult = serial.run("a", first.task);
    const secondResult = serial.run("a", second.task);
    await settle();
    expect(log).toEqual(["first started"]);

    first.finish();
    await firstResult;
    await settle();
    expect(log).toEqual(["first started", "first finished", "second started"]);

    second.finish();
    expect(await secondResult).toBe("second");
  });

  it("does not make tasks with different keys wait for each other", async () => {
    const log: string[] = [];
    const serial = new SerialByKey();
    const a = controlledTask(log, "a");
    const b = controlledTask(log, "b");

    void serial.run("a", a.task);
    void serial.run("b", b.task);
    await settle();

    expect(log).toEqual(["a started", "b started"]);
  });

  it("runs the next task after a failure, and reports each failure only to its own caller", async () => {
    const log: string[] = [];
    const serial = new SerialByKey();
    const first = controlledTask(log, "first");
    const second = controlledTask(log, "second");

    const firstResult = serial.run("a", first.task);
    const secondResult = serial.run("a", second.task);
    first.fail();
    await expect(firstResult).rejects.toThrow("first failed");
    second.finish();

    expect(await secondResult).toBe("second");
    expect(log).toEqual(["first started", "second started", "second finished"]);
  });

  it("forgets a key once its work is done, and starts again immediately", async () => {
    const serial = new SerialByKey();
    await serial.run("a", async () => "one");
    await settle();

    expect(await serial.run("a", async () => "two")).toBe("two");
    await settle();
    expect(serial.size).toBe(0);
  });
});
