import { createClient } from "redis";

import type { ClusterShard, Primary, PrimaryConnection } from "./test-cluster.types.js";

// Controls which primary owns a hash slot in the local Valkey cluster, the
// way an operator resharding it would. The cluster is shared with every other
// test, so a test that moves a slot must give it back when it ends.
export class TestCluster {
  private readonly connections: PrimaryConnection[] = [];

  constructor(private readonly addresses: readonly string[]) {}

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
    const shards = (await this.connections[0]!.client.sendCommand([
      "CLUSTER",
      "SHARDS",
    ])) as ClusterShard[];
    for (const shard of shards) {
      for (let index = 0; index + 1 < shard.slots.length; index += 2) {
        if (slot >= shard.slots[index]! && slot <= shard.slots[index + 1]!) {
          const primary = shard.nodes.find((node) => node.role === "master");
          return this.connectionOf(primary!.id);
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
    const source = this.connectionOf(owner.id);
    const target = this.connectionOf(destination.id);
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

  // SPUBLISH straight to one primary, without the cluster client's routing.
  // Resolves with the number of subscribers that primary delivered it to.
  async publishTo(primary: Primary, channel: string, message: string): Promise<number> {
    const connection = this.connectionOf(primary.id);
    return Number(await connection.client.sendCommand(["SPUBLISH", channel, message]));
  }

  // How many clients this primary has subscribed to the channel.
  async subscribersOf(primary: Primary, channel: string): Promise<number> {
    const connection = this.connectionOf(primary.id);
    const reply = (await connection.client.sendCommand([
      "PUBSUB",
      "SHARDNUMSUB",
      channel,
    ])) as unknown[];
    // [channel, count]
    return Number(reply[1]);
  }

  async close(): Promise<void> {
    for (const connection of this.connections) {
      await connection.client.close();
    }
    this.connections.length = 0;
  }

  private connectionOf(id: string): PrimaryConnection {
    const connection = this.connections.find((candidate) => candidate.id === id);
    if (connection === undefined) {
      throw new Error(`Unknown primary ${id}`);
    }
    return connection;
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
