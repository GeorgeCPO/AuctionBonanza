import { createClient } from "redis";

import type { ClusterShard, Primary, PrimaryConnection } from "./test-cluster.types.js";

const POLL_INTERVAL_MS = 2;
const WAIT_TIMEOUT_MS = 15_000;

// Controls the topology of the local Valkey topology cluster, the way an
// operator would: which primary owns a hash slot, and which node of a shard is
// its primary. Only tests that change the topology use that cluster, but they
// share it with each other, so a test must give back what it changed.
//
// `addresses` are the nodes that are primaries when the cluster is formed.
// Their replicas are found through CLUSTER SHARDS.
export class TestCluster {
  private readonly connections: PrimaryConnection[] = [];
  // Connections to nodes that were not given to the constructor (replicas),
  // opened on first use.
  private readonly replicaConnections: PrimaryConnection[] = [];

  constructor(private readonly addresses: readonly string[]) {}

  // The nodes given to the constructor, which are primaries unless a test
  // failed one over and has not restored it.
  get primaries(): readonly Primary[] {
    return this.connections.map(({ address, id }) => ({ address, id }));
  }

  async connect(): Promise<void> {
    for (const address of this.addresses) {
      const client = createClient({ url: `redis://${address}` });
      await client.connect();
      const id = (await client.sendCommand(["CLUSTER", "MYID"])) as string;
      this.connections.push({ address, id, client });
    }
  }

  async ownerOf(slot: number): Promise<Primary> {
    for (const shard of await this.shards()) {
      for (let index = 0; index + 1 < shard.slots.length; index += 2) {
        if (slot >= shard.slots[index]! && slot <= shard.slots[index + 1]!) {
          const primary = shard.nodes.find((node) => node.role === "master");
          return { id: primary!.id, address: `${primary!.endpoint}:${primary!.port}` };
        }
      }
    }
    throw new Error(`No primary owns slot ${slot}`);
  }

  // Moves a slot that holds no keys (only pub/sub channels) to another
  // primary, with the steps of a manual resharding. When it returns, every
  // primary agrees on the new owner.
  async moveSlot(slot: number, destination: Primary): Promise<void> {
    const owner = await this.ownerOf(slot);
    if (owner.id === destination.id) {
      return;
    }
    const source = await this.connectionOf(owner);
    const target = await this.connectionOf(destination);
    try {
      await this.setSlot(target, slot, "IMPORTING", source.id);
      await this.setSlot(source, slot, "MIGRATING", target.id);
      await this.setSlot(target, slot, "NODE", target.id);
      await this.setSlot(source, slot, "NODE", target.id);
      for (const other of this.connections) {
        if (other.id !== source.id && other.id !== target.id) {
          await this.setSlot(other, slot, "NODE", target.id);
        }
      }
    } catch (error) {
      await this.clearMigrationStates(slot);
      throw error;
    }
  }

  // The replica of a shard's primary, as the cluster knows it.
  async replicaOf(primary: Primary): Promise<Primary> {
    const shards = await this.shards();
    const shard = shards.find((candidate) =>
      candidate.nodes.some((node) => node.role === "master" && node.id === primary.id),
    );
    const replica = shard?.nodes.find((node) => node.role === "replica");
    if (replica === undefined || replica.endpoint === undefined || replica.port === undefined) {
      throw new Error(`Primary ${primary.id} has no replica`);
    }
    return { id: replica.id, address: `${replica.endpoint}:${replica.port}` };
  }

  // "master" or "slave", as the node itself reports it.
  async roleOf(node: Primary): Promise<string> {
    const reply = (await (await this.connectionOf(node)).client.sendCommand(["ROLE"])) as unknown[];
    return String(reply[0]);
  }

  // Fails the replica's shard over to it (CLUSTER FAILOVER) and resolves as
  // soon as it reports itself as a primary. The old primary learns of that
  // slightly later, which is the window in which an edge's subscription is
  // still on the old primary.
  async promote(replica: Primary): Promise<Primary> {
    const connection = await this.connectionOf(replica);
    // A failover needs a replica that is in sync with its primary.
    await waitUntil(async () => {
      const info = (await connection.client.sendCommand(["INFO", "replication"])) as string;
      return info.includes("master_link_status:up");
    }, `${replica.address} to sync with its primary`);
    await connection.client.sendCommand(["CLUSTER", "FAILOVER"]);
    await waitUntil(
      async () => (await this.roleOf(replica)) === "master",
      `${replica.address} to become a primary`,
    );
    return replica;
  }

  // SPUBLISH straight to one primary, without the cluster client's routing.
  // Resolves with the number of subscribers that primary delivered it to.
  async publishTo(primary: Primary, channel: string, message: string): Promise<number> {
    const connection = await this.connectionOf(primary);
    return Number(await connection.client.sendCommand(["SPUBLISH", channel, message]));
  }

  // How many clients this primary has subscribed to the channel.
  async subscribersOf(primary: Primary, channel: string): Promise<number> {
    const connection = await this.connectionOf(primary);
    const reply = (await connection.client.sendCommand([
      "PUBSUB",
      "SHARDNUMSUB",
      channel,
    ])) as unknown[];
    // [channel, count]
    return Number(reply[1]);
  }

  async close(): Promise<void> {
    for (const connection of [...this.connections, ...this.replicaConnections]) {
      await connection.client.close();
    }
    this.connections.length = 0;
    this.replicaConnections.length = 0;
  }

  // A connection to any node of the cluster, opened on first use for nodes
  // that were not primaries when the cluster was formed.
  private async connectionOf(node: Primary): Promise<PrimaryConnection> {
    const known = [...this.connections, ...this.replicaConnections].find(
      (candidate) => candidate.id === node.id,
    );
    if (known !== undefined) {
      return known;
    }
    const client = createClient({ url: `redis://${node.address}` });
    await client.connect();
    const connection = { address: node.address, id: node.id, client };
    this.replicaConnections.push(connection);
    return connection;
  }

  private async shards(): Promise<ClusterShard[]> {
    return (await this.connections[0]!.client.sendCommand(["CLUSTER", "SHARDS"])) as ClusterShard[];
  }

  private setSlot(connection: PrimaryConnection, slot: number, ...arguments_: string[]) {
    return connection.client.sendCommand(["CLUSTER", "SETSLOT", String(slot), ...arguments_]);
  }

  // A move that failed halfway leaves slots marked importing or migrating,
  // which would make later tests fail in confusing ways.
  private async clearMigrationStates(slot: number): Promise<void> {
    for (const connection of this.connections) {
      await this.setSlot(connection, slot, "STABLE").catch(() => {});
    }
  }
}

// Polls until the condition holds. The wait ends on the fact, not on a
// guessed delay; the timeout only turns a hang into a failure.
async function waitUntil(condition: () => Promise<boolean>, description: string): Promise<void> {
  const deadline = Date.now() + WAIT_TIMEOUT_MS;
  while (!(await condition())) {
    if (Date.now() > deadline) {
      throw new Error(`Timed out waiting for ${description}`);
    }
    await new Promise((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));
  }
}
