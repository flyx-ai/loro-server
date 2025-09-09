import { EphemeralStore, LoroDoc, VersionVector } from "loro-crdt";
import { IndexedDBPersistence } from "./indexeddb";

const WS_BIN_REQUEST_TYPE_CRDT = 1;
const WS_BIN_REQUEST_TYPE_SYNC = 2;

const WS_BIN_RESPONSE_TYPE_CRDT = 1;
const WS_BIN_RESPONSE_TYPE_VV = 2;

export class CRDTDoc {
  doc: LoroDoc;
  socket: WebSocket | null = null;
  socketOpen = false;
  indexedDBProvider: IndexedDBPersistence;
  indexedDBSynced = false;
  intervalID: number;
  onInit?: () => void;
  isFirstSyncDone = false;
  onFirstSync?: () => void;
  hasInit = false;
  lastVVRequest: number = 0;
  lastSyncRequest: number = 0;
  isDestroyed = false;
  documentID: string;
  endpoint: string;

  constructor(
    documentID: string,
    endpoint: string,
    onInit?: () => void,
    onFirstSync?: () => void,
    offline?: boolean,
  ) {
    this.doc = new LoroDoc();
    this.indexedDBProvider = new IndexedDBPersistence(
      "crdt-document-" + documentID,
      this.doc,
    );
    this.intervalID = setInterval(
      this.syncCRDTChanges.bind(this),
      1000,
    ) as unknown as number;
    this.onInit = onInit;
    this.onFirstSync = onFirstSync;
    this.documentID = documentID;
    this.endpoint = endpoint;

    this.indexedDBProvider.waitForSync().then((empty) => {
      if (!empty) {
        this.indexedDBSynced = true;
        if (!this.hasInit) {
          this.hasInit = true;
          this.onInit?.();
        }
        this.syncCRDTChanges();
      }
    });

    this.doc.subscribeLocalUpdates((update) => {
      this.sendBinarySocketMessage(WS_BIN_REQUEST_TYPE_CRDT, update);
    });

    if (!offline) {
      const initSocket = () => {
        this.socket = new WebSocket(
          endpoint + "/api/v1/document/" + documentID + "/ws",
        );
        this.socket.binaryType = "arraybuffer";
        this.socket.onmessage = this.socketMsgHandler.bind(this);
        this.socket.onclose = () => {
          if (this.isDestroyed) return;
          this.socketOpen = false;
          console.warn("Socket closed, attempting to reconnect in 1s...");
          setTimeout(initSocket, 1000);
        };
        this.socket.onerror = (err: unknown) => {
          console.error("Socket error:", err);
          this.socket?.close();
        };
        this.socket.onopen = () => {
          this.socketOpen = true;
          this.syncCRDTChanges();
        };
      };
      initSocket.bind(this)();
    }
  }

  sendBinarySocketMessage(messageType: number, data: Uint8Array): boolean {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return false;
    const newData = new Uint8Array(data.length + 1);
    newData[0] = messageType;
    newData.set(data, 1);
    this.socket.send(newData);
    return true;
  }

  sendTextSocketMessage(messageType: number, data: unknown): boolean {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return false;
    this.socket.send(JSON.stringify([messageType, data]));
    return true;
  }

  socketMsgHandler(event: MessageEvent) {
    if (event.data instanceof ArrayBuffer) {
      const rawData = new Uint8Array(event.data);
      const messageType = rawData[0];
      const data = rawData.slice(1);
      switch (messageType) {
        case WS_BIN_RESPONSE_TYPE_CRDT: {
          this.doc.import(data);
          if (!this.isFirstSyncDone) {
            this.isFirstSyncDone = true;
            this.onFirstSync?.();
          }
          break;
        }
        case WS_BIN_RESPONSE_TYPE_VV: {
          const now = Date.now();
          if (!this.indexedDBSynced || now - this.lastVVRequest < 1000) {
            break;
          } else {
            this.lastVVRequest = now;
          }
          const vv = VersionVector.decode(data);
          const localVV = this.doc.oplogVersion();
          const cmpResult = localVV.compare(vv);
          if (cmpResult !== undefined && cmpResult <= 0) {
            break;
          }
          const update = this.doc.export({ mode: "update", from: vv });
          this.sendBinarySocketMessage(WS_BIN_REQUEST_TYPE_CRDT, update);
          break;
        }
        default: {
          console.error("Unknown binary message type", data[0]);
        }
      }
    } else {
      console.error("Unknown message format: text");
    }
  }

  syncCRDTChanges() {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
    if (!this.indexedDBSynced) return;
    const now = Date.now();
    if (now - this.lastSyncRequest < 1000) {
      return;
    } else {
      this.lastSyncRequest = now;
    }
    const vv = this.doc.oplogVersion();
    const encodedVV = vv.encode();
    this.sendBinarySocketMessage(WS_BIN_REQUEST_TYPE_SYNC, encodedVV);
    if (!this.hasInit) {
      this.hasInit = true;
      this.onInit?.();
    }
  }

  // VERY DANGEROUS, REMOVES ALL DATA, USE WITH CAUTION
  async purge() {
    await fetch(
      this.endpoint + "/api/v1/document/" + this.documentID + "/purge",
      {
        method: "POST",
      },
    );
  }

  destroy() {
    this.isDestroyed = true;
    console.log("destroying table...");
    clearInterval(this.intervalID);
    this.indexedDBProvider.destroy();
    this.socket?.close();
  }
}

const WS_AWARENESS_BIN_REQUEST_TYPE_UPDATE = 1;
const WS_AWARENESS_BIN_REQUEST_TYPE_SYNC = 2;

const WS_AWARENESS_BIN_RESPONSE_TYPE_UPDATE = 1;
const WS_AWARENESS_BIN_RESPONSE_TYPE_SYNC = 2;

export class CRDTAwareness {
  store: EphemeralStore;
  socket: WebSocket | null = null;
  socketOpen = false;
  onInit?: () => void;
  hasInit = false;
  isDestroyed = false;
  awarenessID: string;
  endpoint: string;

  constructor(
    awarenessID: string,
    endpoint: string,
    onInit?: () => void,
    offline?: boolean,
  ) {
    this.store = new EphemeralStore();
    this.onInit = onInit;
    this.awarenessID = awarenessID;
    this.endpoint = endpoint;

    this.store.subscribeLocalUpdates((update) => {
      this.sendBinarySocketMessage(
        WS_AWARENESS_BIN_REQUEST_TYPE_UPDATE,
        update,
      );
    });

    if (!offline) {
      const initSocket = () => {
        this.socket = new WebSocket(
          endpoint + "/api/v1/awareness/" + awarenessID + "/ws",
        );
        this.socket.binaryType = "arraybuffer";
        this.socket.onmessage = this.socketMsgHandler.bind(this);
        this.socket.onclose = () => {
          if (this.isDestroyed) return;
          this.socketOpen = false;
          console.warn("Socket closed, attempting to reconnect in 1s...");
          setTimeout(initSocket, 1000);
        };
        this.socket.onerror = (err: unknown) => {
          console.error("Socket error:", err);
          this.socket?.close();
        };
        this.socket.onopen = () => {
          this.socketOpen = true;
        };
      };
      initSocket.bind(this)();
    }
  }

  sendBinarySocketMessage(messageType: number, data: Uint8Array): boolean {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return false;
    const newData = new Uint8Array(data.length + 1);
    newData[0] = messageType;
    newData.set(data, 1);
    this.socket.send(newData);
    return true;
  }

  sendTextSocketMessage(messageType: number, data: unknown): boolean {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return false;
    this.socket.send(JSON.stringify([messageType, data]));
    return true;
  }

  socketMsgHandler(event: MessageEvent) {
    if (event.data instanceof ArrayBuffer) {
      const rawData = new Uint8Array(event.data);
      const messageType = rawData[0];
      const data = rawData.slice(1);
      switch (messageType) {
        case WS_AWARENESS_BIN_RESPONSE_TYPE_UPDATE: {
          this.store.apply(data);
          break;
        }
        case WS_AWARENESS_BIN_RESPONSE_TYPE_SYNC: {
          const update = this.store.encodeAll();
          this.sendBinarySocketMessage(
            WS_AWARENESS_BIN_REQUEST_TYPE_SYNC,
            update,
          );
          break;
        }
        default: {
          console.error("Unknown binary message type", data[0]);
        }
      }
    } else {
      console.error("Unknown message format: text");
    }
  }

  destroy() {
    this.isDestroyed = true;
    console.log("destroying awareness...");
    this.socket?.close();
    this.store.destroy();
  }
}

export async function createDocument(endpoint: string, documentID: string) {
  await fetch(endpoint + "/api/v1/document/" + documentID, { method: "POST" });
}
