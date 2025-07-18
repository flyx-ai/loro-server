# WAL

The `WAL` is the persisted write ahead log that stores the crdt updates for lorogo. Updates are idempotent which is why this can guarantee "at least once delivery" using retries. 

The subject for the `WAL` is `loro.wal.{document_id}` where `{document_id}` is the identifier of the document being updated.

The client will always ensure that the server is up before sending updates, and if not it will wait for the server to be up before sending any messages. The mechanism for this is explained in the section below.

The client will send messages to the server in chunks:

Here are the headers that every message will contain:

+ `LORO_LOG_ID: {string}`: A unique identifier for the log message being sent. This will be used to correlate chunks of the same message.
+ `LORO_CHUNK_INDEX: {number}`: The index of the chunk in the sequence of chunks for the message. This will start from `0` and increment for each subsequent chunk.

Here are the headers that the final message will contain:

+ `LORO_FINAL: true`: To indicate that this is the last chunk of the message.
+ `LORO_DIGEST: {digest}`: The digest of the entire message for integrity verification.
+ `LORO_RESPONSE_INBOX: {string}`: The inbox to send the response to. The only accepted responses are with the strings `ACK` or `NACK`. The client sending the update will wait for this response and retry as needed.

There will be a short timeout when waiting for response after sending the last chunk (5 seconds by default). If the server does not respond within this timeout, the client will retry sending the whole message.

The server will process these chunks and reassemble the final message. If the number of chunks received does not match `LORO_CHUNK_COUNT` or if `LORO_CHUNK_DIGEST` does not match the digest of the reassembled message, the server will send a `NAK` response to the inbox specified in `LORO_RESPONSE_INBOX`. The client will then retry sending the entire message. Otherwise, the server will send an `ACK` response.

The response from the server will be sent through core NATS, but the actual `WAL` messages will be sent through NATS JetStream to ensure persistence.

# How to ensure server is up

NOTE: this section is only applicable when the clocks between the client and server and NATS servers are in sync. If they are not, weird things will happen.

NOTE: CAS = Compare And Swap. This is a NATS JetStream KV operation that allows you to atomically update a key only if it matches the expected value. This is used as a distributed lock to ensure only one server instance can exist at a time.

Using NATS JetStream KV, there will be bucket called `loro-document-status`. This will contain the status of each document being edited. The key will be the `document_id` and the value is described below.

The possible values for the `loro-document-status` bucket are:

+ `DOWN:{timestamp}`: This indicates that the server is down or has not been initialized.
+ `STARTING:{operation_id}:{retry_count}:{timestamp}`: This indicates that the server is in the process of starting up for a specific document. The `operation_id` is a nanoid, and the `retry_count` is the number of times the document has been attempted to start.
+ `UP:{server_id}:{timestamp}`: This indicates that the server is up and running for the document. The timestamp will be updated every 3 seconds to indicate that the server is still alive.
+ `ERROR:{trace_id}:{timestamp}`: This indicates that the server encountered an error while initializing the document for more than 3 attempts and has given up. The `trace_id` can be used to correlate logs for debugging purposes. This will also be used to indicate that the server cannot recover from this error and further attempts to initialize will fail until manual intervention.

By default, either the key does not exist or it has the status `DOWN`. The server will hopefully mark all documents it owns as `DOWN` when being gracefully shut down.

When a client tries to make a request, it will first retrieve the status to see if it is up and whether the timestamp is less than 5 seconds old. If so, it will send a NATS Core Request to `loro.ping.{document_id}`. The server will send an empty message back, but if there are no server instances, the request will throw the error `nats: no responders available for request` which will indicate to the client that the server is down.

In this case, the client will CAS the `loro-document-status` bucket for the `document_id` key to have the status `STARTING:{operation_id}:{retry_count}` with the current timestamp where `operation_id` is a nanoid. It will then make a request to a queue group with the subject `loro.init.{document_id}` with the body as the `operation_id`. By using a queue group, only one server can respond to the message. The server will then initialize the document and update the same key to a new timestamp every 3 seconds to indicate that it is still starting. The client will then watch the key for updates.

If a client (including the one that started the server) encounters that a document is `STARTING` for more than 5 seconds, it will generate another `operation_id` and retry the initialization process by sending another request to `loro.init.{document_id}`. The retry count will be incremented in the `loro-document-status` bucket. If the retry count exceeds 3, the server will mark itself as `ERROR:{trace_id}` and stop further attempts to initialize until manual intervention.

If the server successfully initializes, it will update the `loro-document-status` bucket to `UP:{server_id}:{timestamp}`. The client who were watching the key will then proceed with their operations on the server. The server will then watch the bucket for changes to its own key and if it sees the status change to `DOWN` or `ERROR`, it will change it back to `UP`. But if it changes to `STARTING` or `UP` with a different `server_id`, it will shutdown the document to allow for a new server to take over.

# Document Initialization

To initialize a document, the server will retrieve the document snapshot (if exists) and create a subscriber to the `WAL` which preloads every message starting from the last sequence number in the snapshot.

# Document Compaction

Every server that is running will run a compaction process on the `WAL` periodically (some time every day). This is to ensure that the size of the log does not grow indefinitely and to reclaim space.

The compacted document is stored in the NATS JetStream ObjectStore bucket `loro-document-snapshots`. The key for the object will be `{document_id}`.

When the compaction process is started, the server will store the last sequence number it contains in the `WAL` as the first sequence number. It will then restart the subscriber to start reading messages beginning from that sequence number, allowing for partial chunks to be invalidated since data from earlier chunks does not exist in memory anymore. It will then export a snapshot of the document with only 30 days of history and store it in the bucket. Once the snapshot is successfully stored, the server will purge the `WAL` entries that are older than the snapshot.

# What can the server do

+ Provide an API to update a document
+ Provide a way to sync documents
    + Client gets frontiers and tries to sync with server -> Server gets frontiers and send to client along with update -> Client applies updates and exports updates -> Server applies client updates
    + Whenever oplog cannot be used to synchronize, the client or server can request a version vector sync.
    + Accept updates from `WAL` and apply them to the document
+ Provide a way to sync awareness
