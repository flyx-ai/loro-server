use futures::{FutureExt, StreamExt};
use loro::ToJson;
use std::hash::Hasher;
use thiserror::Error;

type NatsGenericError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Error, Debug)]
pub enum LoroInitDocumentError {
    #[error("failed to create WAL consumer: {0}")]
    CreateConsumerError(async_nats::jetstream::stream::ConsumerError),
    #[error("failed to get batch messages from WAL: {0}")]
    GetBatchMessagesError(async_nats::jetstream::consumer::pull::BatchError),
    #[error("failed to get messages from WAL: {0}")]
    GetMessagesError(async_nats::jetstream::consumer::StreamError),
    #[error("malformed WAL message: {0}")]
    MalformedMessageError(String),
    #[error("loro import error: {0}")]
    LoroImportError(loro::LoroError),
    #[error("failed to acknowledge WAL message: {0}")]
    AckMessageError(NatsGenericError),
}

struct ChunkStoreEntry {
    chunks: std::collections::HashMap<usize, Vec<u8>>,
    hash: twox_hash::xxhash64::Hasher,
    hash_idx: usize,
    error_message: String,
    response_inbox: String,
    chunk_count: usize,
    digest: u64,
}

pub async fn init_loro_document(
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    wal_stream: std::sync::Arc<async_nats::jetstream::stream::Stream>,
    server_state: std::sync::Arc<tokio::sync::RwLock<super::initialize::LoroServerState>>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
) -> Result<(), LoroInitDocumentError> {
    tracing::debug!("initializing loro document: {}", document_id);

    let loro_doc = &server_state.read().await.document.clone();

    let consumer_id = nanoid::nanoid!();
    let consumer = std::sync::Arc::new(
        wal_stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                name: Some(format!("loro-wal-consumer-{}-{}", document_id, consumer_id)),
                description: Some(format!("Loro WAL consumer for document {}", document_id)),
                filter_subject: format!("loro.wal.{}", document_id),
                ..Default::default()
            })
            .await
            .map_err(LoroInitDocumentError::CreateConsumerError)?,
    );

    let mut chunk_store = std::collections::HashMap::<String, ChunkStoreEntry>::new();
    let mut message_queue = Vec::<Vec<u8>>::new();

    let mut messages = consumer
        .fetch()
        .max_messages(usize::MAX) // the entire document needs to fit in memory, so this is fine
        .messages()
        .await
        .map_err(LoroInitDocumentError::GetBatchMessagesError)?;

    while let Some(Ok(message)) = messages.next().await {
        if let Err(e) =
            process_message_wrapper(nc.clone(), &mut chunk_store, &mut message_queue, &message)
                .await
        {
            tracing::error!("Failed to process message: {:?}", e);
        }
        let seq = get_jetstream_msg_metadata(&message).sequence.stream;
        let mut state = server_state.write().await;
        if state.last_seq < seq {
            state.last_seq = seq;
        }
    }

    tracing::debug!("running import_batch on {} messages", message_queue.len());

    loro_doc
        .import_batch(message_queue.as_slice())
        .map_err(LoroInitDocumentError::LoroImportError)?;

    tracing::info!(
        "state of loro_doc after import_batch: {}",
        loro_doc.get_deep_value().to_json_value()
    );

    message_queue.clear();

    {
        let document_id = document_id.clone();
        let nc = nc.clone();
        let server_states = server_states.clone();
        tokio::spawn(async move {
            if let Err(e) =
                super::document_endpoints::init_document_endpoints(document_id, nc, server_states)
                    .await
            {
                tracing::error!("Failed to initialize document endpoints: {:?}", e);
            }
        });
    }

    {
        let document_id = document_id.clone();
        let server_state = server_state.clone();
        tokio::spawn(async move {
            if let Err(e) = process_messages(
                nc,
                document_id,
                server_state,
                consumer,
                chunk_store,
                message_queue,
                server_states,
            )
            .await
            {
                tracing::error!("Failed to process messages: {:?}", e);
            }
        });
    }

    tracing::debug!("setting server state to initialized");

    server_state.write().await.initialized = true;

    tracing::debug!("loro document initialized: {}", document_id.clone());

    Ok(())
}

async fn process_messages(
    nc: std::sync::Arc<async_nats::Client>,
    document_id: String,
    server_state: std::sync::Arc<tokio::sync::RwLock<super::initialize::LoroServerState>>,
    consumer: std::sync::Arc<
        async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
    >,
    chunk_store: std::collections::HashMap<String, ChunkStoreEntry>,
    message_queue: Vec<Vec<u8>>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
) -> Result<(), LoroInitDocumentError> {
    tracing::debug!("processing messages for document: {}", document_id);
    let loro_doc = &server_state.read().await.document.clone();
    let mut wal_stream_messages = consumer
        .messages()
        .await
        .map_err(LoroInitDocumentError::GetMessagesError)?;

    let mut chunk_store = chunk_store;
    let mut message_queue = message_queue;

    loop {
        if !server_states.exists(document_id.clone()).await {
            break;
        }

        let next_message = wal_stream_messages.next();
        let msg: Option<
            Result<
                async_nats::jetstream::Message,
                async_nats::jetstream::consumer::pull::MessagesError,
            >,
        >;

        if message_queue.len() > 0 {
            if let Some(v) = next_message.now_or_never() {
                msg = v;
            } else {
                tracing::debug!("running import_batch on {} messages", message_queue.len());

                loro_doc
                    .import_batch(message_queue.as_slice())
                    .map_err(LoroInitDocumentError::LoroImportError)?;
                message_queue.clear();

                tracing::info!(
                    "state of loro_doc after import_batch: {}",
                    loro_doc.get_deep_value().to_json_value()
                );

                continue;
            }
        } else {
            msg = next_message.await;
        }

        if let Some(Ok(msg)) = msg {
            if let Err(e) =
                process_message_wrapper(nc.clone(), &mut chunk_store, &mut message_queue, &msg)
                    .await
            {
                tracing::error!("Failed to process message: {:?}", e);
            }
            let seq = get_jetstream_msg_metadata(&msg).sequence.stream;
            let mut state = server_state.write().await;
            if state.last_seq < seq {
                state.last_seq = seq;
            }
        }
    }

    Ok(())
}

async fn process_message_wrapper(
    nc: std::sync::Arc<async_nats::Client>,
    chunk_store: &mut std::collections::HashMap<String, ChunkStoreEntry>,
    message_queue: &mut Vec<Vec<u8>>,
    message: &async_nats::jetstream::Message,
) -> Result<(), LoroInitDocumentError> {
    let message_timestamp: chrono::DateTime<chrono::Utc> =
        get_jetstream_msg_metadata(&message).timestamp;
    let do_send_response = chrono::Utc::now() - message_timestamp <= chrono::Duration::seconds(600);
    let res = process_message(chunk_store, message_queue, &message);
    match res {
        Ok(v) => {
            message
                .ack_with(async_nats::jetstream::AckKind::Ack)
                .await
                .map_err(LoroInitDocumentError::AckMessageError)?;
            if do_send_response {
                if let Some(response_inbox) = v {
                    let nc = nc.clone();
                    tokio::spawn(async move {
                        tracing::debug!("sending ACK to response inbox: {}", response_inbox);
                        if let Err(e) = nc.publish(response_inbox, "ACK".into()).await {
                            tracing::error!("Failed to publish ACK: {:?}", e);
                        }
                    });
                }
            }
            Ok(())
        }
        Err(e) => {
            message
                .ack_with(async_nats::jetstream::AckKind::Term)
                .await
                .map_err(LoroInitDocumentError::AckMessageError)?;
            if do_send_response {
                if let Some(response_inbox) = e.response_inbox {
                    let nc = nc.clone();
                    let msg = e.message.clone();
                    tokio::spawn(async move {
                        tracing::debug!(
                            "sending NAK to response inbox: {}, message: {}",
                            response_inbox,
                            msg
                        );
                        if let Err(e) = nc
                            .publish(response_inbox, format!("NAK:{}", msg.to_string()).into())
                            .await
                        {
                            tracing::error!("Failed to publish NAK: {:?}", e);
                        }
                    });
                }
            }
            Err(LoroInitDocumentError::MalformedMessageError(
                e.message.clone(),
            ))
        }
    }
}

#[derive(Error, Debug)]
struct MalformedMessageError {
    response_inbox: Option<String>,
    message: String,
}

impl std::fmt::Display for MalformedMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.response_inbox {
            Some(ref inbox) => write!(
                f,
                "Malformed message: {}. Response inbox: {}",
                self.message, inbox
            ),
            None => write!(f, "Malformed message: {}", self.message),
        }
    }
}

fn process_message(
    chunk_store: &mut std::collections::HashMap<String, ChunkStoreEntry>,
    message_queue: &mut Vec<Vec<u8>>,
    message: &async_nats::jetstream::Message,
) -> Result<Option<String>, MalformedMessageError> {
    let header = match message.headers {
        Some(ref header) => header,
        None => {
            return Err(MalformedMessageError {
                response_inbox: None,
                message: "Missing headers".to_string(),
            });
        }
    };
    let log_id = header
        .get("LORO_LOG_ID")
        .ok_or(MalformedMessageError {
            response_inbox: None,
            message: "Missing LORO_LOG_ID header".to_string(),
        })?
        .as_str();
    if !chunk_store.contains_key(log_id) {
        chunk_store.insert(
            log_id.to_string(),
            ChunkStoreEntry {
                chunks: std::collections::HashMap::new(),
                hash: twox_hash::XxHash64::with_seed(0),
                hash_idx: 0,
                error_message: String::new(),
                response_inbox: String::new(),
                chunk_count: 0,
                digest: 0,
            },
        );
    }
    let chunk_store_entry = chunk_store
        .get_mut(log_id)
        .expect("Chunk store entry not found when it should be");
    let chunk_index = header
        .get("LORO_CHUNK_INDEX")
        .ok_or(MalformedMessageError {
            response_inbox: None,
            message: "Missing LORO_CHUNK_INDEX header".to_string(),
        })
        .or_else(|e| {
            chunk_store_entry.error_message = e.message.clone();
            Err(e)
        })?
        .as_str()
        .parse::<usize>()
        .map_err(|_| MalformedMessageError {
            response_inbox: None,
            message: "Invalid LORO_CHUNK_INDEX header".to_string(),
        })
        .or_else(|e| {
            chunk_store_entry.error_message = e.message.clone();
            Err(e)
        })?;
    let loro_final = match header.get("LORO_FINAL") {
        Some(final_header) => final_header.as_str().len() > 0,
        None => false,
    };
    chunk_store_entry
        .chunks
        .insert(chunk_index, message.payload.to_vec());
    while let Some(chunk) = chunk_store_entry.chunks.get(&chunk_store_entry.hash_idx) {
        chunk_store_entry.hash.write(chunk.as_slice());
        chunk_store_entry.hash_idx += 1;
    }
    if loro_final {
        let response_inbox = header
            .get("LORO_RESPONSE_INBOX")
            .ok_or(MalformedMessageError {
                response_inbox: None,
                message: "Missing LORO_RESPONSE_INBOX header".to_string(),
            })
            .or_else(|e| {
                chunk_store_entry.error_message = e.message.clone();
                Err(e)
            })?
            .as_str();
        let digest = header
            .get("LORO_DIGEST")
            .ok_or(MalformedMessageError {
                response_inbox: Some(response_inbox.to_string()),
                message: "Missing LORO_CHUNK_DIGEST header".to_string(),
            })?
            .as_str()
            .parse::<u64>()
            .map_err(|_| MalformedMessageError {
                response_inbox: Some(response_inbox.to_string()),
                message: "Invalid LORO_CHUNK_DIGEST header".to_string(),
            })?;
        chunk_store_entry.response_inbox = response_inbox.to_string();
        chunk_store_entry.chunk_count = chunk_index + 1;
        chunk_store_entry.digest = digest;
    }
    if chunk_store_entry.chunk_count > 0
        && chunk_store_entry.chunk_count == chunk_store_entry.hash_idx
    {
        let sum = chunk_store_entry.hash.finish();
        if sum != chunk_store_entry.digest {
            return Err(MalformedMessageError {
                response_inbox: Some(chunk_store_entry.response_inbox.to_string()),
                message: format!(
                    "chunk digest differs from expected: calculated {} != given {}, possible error message: {}",
                    sum, chunk_store_entry.digest, chunk_store_entry.error_message
                ),
            });
        }
        // All chunks received and verified, now we can process the batch.
        let buffer_size = chunk_store_entry
            .chunks
            .iter()
            .map(|(_, v)| v.len())
            .sum::<usize>();
        message_queue.push(Vec::with_capacity(buffer_size));
        let buffer = message_queue.last_mut().unwrap();
        for i in 0..chunk_store_entry.chunk_count {
            let chunk = chunk_store_entry
                .chunks
                .remove(&i)
                .ok_or(MalformedMessageError {
                    response_inbox: Some(chunk_store_entry.response_inbox.to_string()),
                    message: format!("Missing chunk {}", i),
                })?;
            buffer.extend_from_slice(chunk.as_slice());
        }
        return Ok(Some(chunk_store_entry.response_inbox.to_string()));
    }

    Ok(None)
}

// SequencePair includes the consumer and stream sequence numbers for a
// message.
struct SequencePair {
    // Consumer is the consumer sequence number for message deliveries. This
    // is the total number of messages the consumer has seen (including
    // redeliveries).
    // consumer: u64,

    // Stream is the stream sequence number for a message.
    stream: u64,
}

struct JetstreamMsgMetadata {
    // Sequence is the sequence information for the message.
    sequence: SequencePair,

    // NumDelivered is the number of times this message was delivered to the
    // consumer.
    // num_delivered: u64,

    // NumPending is the number of messages that match the consumer's
    // filter, but have not been delivered yet.
    // num_pending: u64,

    // Timestamp is the time the message was originally stored on a stream.
    timestamp: chrono::DateTime<chrono::Utc>,
    // Stream is the stream name this message is stored on.
    // stream: String,

    // Consumer is the consumer name this message was delivered to.
    // consumer: String,

    // Domain is the domain this message was received on.
    // domain: String,
}

fn get_jetstream_msg_metadata(msg: &async_nats::jetstream::Message) -> JetstreamMsgMetadata {
    let tokens = msg.reply.as_ref().unwrap();
    let tokens = get_jetstream_tokens_from_string(tokens);

    // let domain = tokens[2].to_string();
    // let account_hash = tokens[3].to_string();
    // let stream = tokens[4].to_string();
    // let consumer = tokens[5].to_string();
    // let delivered = tokens[6].to_string();
    // let sseq = tokens[7].to_string();
    let cseq = tokens[8].to_string();
    let tm = tokens[9].to_string();
    // let pending = tokens[10].to_string();

    JetstreamMsgMetadata {
        // domain: if domain == "_" {
        //     "".to_string()
        // } else {
        //     domain
        // },
        // num_delivered: delivered.parse::<u64>().unwrap_or(0),
        // num_pending: pending.parse::<u64>().unwrap_or(0),
        timestamp: chrono::DateTime::<chrono::Utc>::from_timestamp_nanos(
            tm.parse::<i64>().unwrap_or(0),
        ),
        // stream,
        // consumer,
        sequence: SequencePair {
            // consumer: sseq.parse::<u64>().unwrap_or(0),
            stream: cseq.parse::<u64>().unwrap_or(0),
        },
    }
}

fn get_jetstream_tokens_from_string(s: &str) -> Vec<&str> {
    let mut tokens = s.split('.').collect::<Vec<_>>();

    // Newer server will include the domain name and account hash in the subject,
    // and a token at the end.
    //
    // Old subject was:
    // $JS.ACK.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>
    //
    // New subject would be:
    // $JS.ACK.<domain>.<account hash>.<stream>.<consumer>.<delivered>.<sseq>.<cseq>.<tm>.<pending>.<a token with a random value>
    //
    // v1 has 9 tokens, v2 has 12, but we must not be strict on the 12th since
    // it may be removed in the future. Also, the library has no use for it.
    // The point is that a v2 ACK subject is valid if it has at least 11 tokens.

    let tokens_len = tokens.len();
    // For v1 style, we insert 2 empty tokens (domain and hash) so that the
    // rest of the library references known fields at a constant location.
    if tokens_len == 9 {
        tokens.insert(2, "");
        tokens.insert(2, "");
    } else if tokens[2] == "_" {
        tokens[2] = ""
    }
    return tokens;
}
