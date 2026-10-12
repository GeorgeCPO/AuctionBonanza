// Runs tasks that share a key one at a time, in the order they were
// requested. Tasks with different keys do not wait for each other.
//
// A task starts only after the previous task for its key has finished, and
// it starts whether that one succeeded or failed. Each caller gets the result
// (or the error) of its own task only.
export class SerialByKey {
  // For each key with work pending: a promise that resolves when the last
  // requested task has finished. It never rejects, so waiting on it is safe.
  private readonly lastFinished = new Map<string, Promise<void>>();

  // Keys with a task still running or waiting. Zero when idle.
  get size(): number {
    return this.lastFinished.size;
  }

  run<T>(key: string, task: () => Promise<T>): Promise<T> {
    const previousFinished = this.lastFinished.get(key) ?? Promise.resolve();
    const result = previousFinished.then(task);

    const finished = result.then(ignore, ignore);
    this.lastFinished.set(key, finished);
    void finished.then(() => {
      // Only the newest task clears the key. If another was requested while
      // this one ran, the entry now belongs to it.
      const newest = this.lastFinished.get(key);
      if (newest === finished) {
        this.lastFinished.delete(key);
      }
    });

    return result;
  }
}

function ignore(): void {}
