import type { LoroDoc } from "loro-crdt";

export class IndexedDBPersistence {
  syncedPromise: Promise<boolean>;

  constructor(name: string, doc: LoroDoc) {
    const { promise: syncedPromise, resolve: resolveSynced } =
      Promise.withResolvers<boolean>();
    this.syncedPromise = syncedPromise;
    // TODO: implement
    resolveSynced(false);
  }

  waitForSync(): Promise<boolean> {
    return this.syncedPromise;
  }

  destroy(): void {
    // TODO: implement
  }
}
