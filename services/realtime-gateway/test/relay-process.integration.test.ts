import { execFile, spawn, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { resolve } from "node:path";
import { promisify } from "node:util";

import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { auctionChannel } from "../src/bus/bus.js";
import { bidAccepted, BusRecorder, KafkaTestTopics, waitFor } from "./support/relay-harness.js";

const brokers = process.env.TEST_KAFKA_BROKERS;
const valkeyNodes = process.env.TEST_VALKEY_NODES;
const serviceRoot = resolve(import.meta.dirname, "..");
// Compiled into a throwaway directory under node_modules, so dist/ is never
// overwritten and Node resolves the service's dependencies from it.
const buildCacheRoot = resolve(serviceRoot, "node_modules/.cache");
let entryPoint: string;

// Runs the built relay as its own OS process, exactly as it is deployed.
describe.skipIf(brokers === undefined || valkeyNodes === undefined)(
  "relay process",
  { timeout: 120_000 },
  () => {
    const partitions = 3;
    let buildDirectory: string | undefined;
    let topics: KafkaTestTopics;
    let bus: BusRecorder;

    function relayEnvironment(topic: string, groupId: string): Record<string, string> {
      return {
        LOG_LEVEL: "debug",
        KAFKA_BROKERS: brokers!,
        KAFKA_GROUP_ID: groupId,
        AUCTION_EVENTS_TOPIC: topic,
        VALKEY_NODES: valkeyNodes!,
        // The broker's minimum, so a killed relay's partitions move quickly.
        RELAY_SESSION_TIMEOUT_MS: "6000",
      };
    }

    beforeAll(async () => {
      await mkdir(buildCacheRoot, { recursive: true });
      buildDirectory = await mkdtemp(resolve(buildCacheRoot, "relay-test-"));
      await promisify(execFile)(
        resolve(serviceRoot, "node_modules/.bin/tsc"),
        ["-p", "tsconfig.build.json", "--outDir", buildDirectory],
        { cwd: serviceRoot },
      );
      entryPoint = resolve(buildDirectory, "relay.main.js");
      topics = new KafkaTestTopics(brokers!);
      await topics.connect();
      bus = new BusRecorder(valkeyNodes!.split(","));
      await bus.connect();
    });

    afterAll(async () => {
      await bus?.close();
      await topics?.disconnect();
      if (buildDirectory !== undefined) await rm(buildDirectory, { recursive: true, force: true });
    });

    it("refuses to start with invalid configuration", async () => {
      const relay = startRelayProcess({ KAFKA_BROKERS: brokers! });

      const [exitCode] = (await once(relay.process, "exit")) as [number | null];
      expect(exitCode).toBe(1);
      expect(relay.stderr()).toContain("Invalid relay configuration: /valkeyNodes");
    });

    it("republishes after a crash between publish and commit, then drains cleanly on SIGTERM", async () => {
      const topic = await topics.create(partitions);
      const groupId = topics.newGroup();
      const auctionId = "501";
      await bus.subscribe(auctionChannel(auctionId));
      await topics.produce(
        topic,
        [2, 3, 4].map((version) => bidAccepted(auctionId, version)),
      );

      // Nothing is committed before the kill: the first commit is a minute away.
      const crashing = startRelayProcess({
        ...relayEnvironment(topic, groupId),
        RELAY_COMMIT_INTERVAL_MS: "60000",
      });
      const crashed = once(crashing.process, "exit") as Promise<[number | null, string | null]>;
      await waitFor(() => bus.versionsOf(auctionId).length === 3, "the first delivery", 30_000);
      crashing.process.kill("SIGKILL");
      expect(await crashed).toEqual([null, "SIGKILL"]);

      // The next relay resumes from the last committed offset and publishes
      // the same events again: at-least-once, as the edges expect.
      const restarted = startRelayProcess(relayEnvironment(topic, groupId));
      const exited = once(restarted.process, "exit") as Promise<[number | null, string | null]>;
      await waitFor(
        () => bus.versionsOf(auctionId).length === 6,
        `the redelivery:\n${restarted.stdout()}`,
        60_000,
      );
      restarted.process.kill("SIGTERM");

      expect(await exited).toEqual([0, null]);
      expect(bus.versionsOf(auctionId)).toEqual(["2", "3", "4", "2", "3", "4"]);
      const messages = restarted
        .stdout()
        .split("\n")
        .filter((line) => line.startsWith("{"))
        .map((line) => (JSON.parse(line) as { msg: string }).msg);
      expect(messages).toContain("relay started");
      expect(messages).toContain("shutdown requested; finishing in-flight messages");
      expect(messages.at(-1)).toBe("relay stopped");
      // A clean shutdown commits what it forwarded.
      expect(await topics.committedOffsetsOfNonEmptyPartitions(groupId, topic)).toEqual(
        await topics.nonEmptyEndOffsets(topic),
      );
    });
  },
);

function startRelayProcess(environment: Record<string, string>): {
  process: ChildProcess;
  stdout: () => string;
  stderr: () => string;
} {
  const child = spawn(process.execPath, [entryPoint], {
    cwd: serviceRoot,
    // Only the explicit settings: nothing leaks in from a developer's shell.
    env: { PATH: process.env.PATH ?? "", NODE_ENV: "test", ...environment },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (chunk: Buffer) => (stdout += chunk.toString("utf8")));
  child.stderr?.on("data", (chunk: Buffer) => (stderr += chunk.toString("utf8")));
  return { process: child, stdout: () => stdout, stderr: () => stderr };
}
