import { LoroDoc, VersionVector } from "loro-crdt";
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
  intervalID: NodeJS.Timeout;
  onInit?: () => void;
  hasInit = false;
  lastVVRequest: number = 0;
  lastSyncRequest: number = 0;

  constructor(
    documentID: string,
    endpoint: string,
    onInit?: () => void,
    offline?: boolean,
  ) {
    const self = this;
    this.doc = new LoroDoc();
    this.indexedDBProvider = new IndexedDBPersistence(
      "crdt-document-" + documentID,
      self.doc,
    );
    this.intervalID = setInterval(self.syncCRDTChanges.bind(self), 1000);
    this.onInit = onInit;

    self.indexedDBProvider.waitForSync().then((empty) => {
      if (!empty) {
        self.indexedDBSynced = true;
        if (!self.hasInit) {
          self.onInit?.();
          self.hasInit = true;
        }
        self.syncCRDTChanges();
      }
    });

    self.doc.subscribeLocalUpdates((update) => {
      this.sendBinarySocketMessage(WS_BIN_REQUEST_TYPE_CRDT, update);
    });

    if (!offline) {
      function initSocket() {
        self.socket = new WebSocket(endpoint);
        self.socket.binaryType = "arraybuffer";
        self.socket.onmessage = self.socketMsgHandler.bind(self);
        self.socket.onclose = function () {
          self.socketOpen = false;
          console.warn("Socket closed, attempting to reconnect in 1s...");
          setTimeout(initSocket, 1000);
        };
        self.socket.onerror = function (err) {
          console.error("Socket error:", err);
          self.socket?.close();
        };
        self.socket.onopen = () => {
          self.socketOpen = true;
          self.syncCRDTChanges();
        };
      }
      initSocket();
    }
  }

  sendBinarySocketMessage(messageType: number, data: Uint8Array): boolean {
    const self = this;
    if (!self.socket || self.socket.readyState !== WebSocket.OPEN) return false;
    const newData = new Uint8Array(data.length + 1);
    newData[0] = messageType;
    newData.set(data, 1);
    self.socket.send(newData);
    return true;
  }

  sendTextSocketMessage(messageType: number, data: unknown): boolean {
    const self = this;
    if (!self.socket || self.socket.readyState !== WebSocket.OPEN) return false;
    self.socket.send(JSON.stringify([messageType, data]));
    return true;
  }

  socketMsgHandler(event: MessageEvent) {
    const self = this;
    if (event.data instanceof ArrayBuffer) {
      const rawData = new Uint8Array(event.data);
      const messageType = rawData[0];
      const data = rawData.slice(1);
      switch (messageType) {
        case WS_BIN_RESPONSE_TYPE_CRDT: {
          self.doc.import(data);
          break;
        }
        case WS_BIN_RESPONSE_TYPE_VV: {
          const now = Date.now();
          if (!self.indexedDBSynced || now - self.lastVVRequest < 1000) {
            break;
          } else {
            self.lastVVRequest = now;
          }
          const vv = VersionVector.decode(data);
          const localVV = self.doc.oplogVersion();
          const cmpResult = localVV.compare(vv);
          if (cmpResult !== undefined && cmpResult <= 0) {
            break;
          }
          const update = self.doc.export({ mode: "update", from: vv });
          self.sendBinarySocketMessage(WS_BIN_REQUEST_TYPE_CRDT, update);
          break;
        }
        default: {
          console.error("Unknown binary message type", data[0]);
        }
      }
    } else if (typeof event.data === "string") {
      const msg = JSON.parse(event.data);
      switch (msg.t) {
        // case "initawareness": {
        //   const newMsg = msg as {
        //     a: string[];
        //     r: string[];
        //     s: Record<string, Record<string, Record<string, unknown>>>;
        //   };
        //   for (const key of Object.keys(newMsg.s))
        //     awareness[key] = newMsg.s[key];
        //   for (const key of newMsg.r) delete awareness[key];
        //   for (const key of Object.keys(awareness))
        //     if (!newMsg.s[key]) delete awareness[key];
        //   break;
        // }
        // case "awareness": {
        //   const newMsg = msg as {
        //     a: string[];
        //     r: string[];
        //     s: Record<string, Record<string, Record<string, unknown>>>;
        //   };
        //   for (const key of Object.keys(newMsg.s))
        //     awareness[key] = newMsg.s[key];
        //   for (const key of newMsg.r) delete awareness[key];
        //   break;
        // }
        default:
          console.error("Unknown string message type " + msg.t);
      }
    }
  }

  syncCRDTChanges() {
    const self = this;
    if (!self.socket || self.socket.readyState !== WebSocket.OPEN) return;
    if (!self.indexedDBSynced) return;
    const now = Date.now();
    if (now - self.lastSyncRequest < 1000) {
      return;
    } else {
      self.lastSyncRequest = now;
    }
    const vv = self.doc.oplogVersion();
    const encodedVV = vv.encode();
    self.sendBinarySocketMessage(WS_BIN_REQUEST_TYPE_SYNC, encodedVV);
    if (!self.hasInit) {
      self.onInit?.();
      self.hasInit = true;
    }
  }

  destroy() {
    console.log("destroying table...");
    clearInterval(this.intervalID);
    this.indexedDBProvider.destroy();
    this.socket?.close();
  }
}
