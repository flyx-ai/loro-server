use bytes::Bytes;
use futures::{FutureExt, StreamExt};
use std::hash::Hasher;

const CHUNK_SIZE: usize = 128 * 1024; // 128 KiB

type NatsGenericError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(thiserror::Error, Debug)]
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
    #[error("failed to get message info: {0}")]
    GetMessageInfoError(NatsGenericError),
}

struct ChunkStoreEntry {
    chunks: std::collections::HashMap<usize, Vec<u8>>,
    hash: twox_hash::xxhash64::Hasher,
    hash_idx: usize,
    error_message: String,
    response_inbox: String,
    chunk_count: usize,
    digest: u64,
    new_message: bool,
}

fn save_last_seq(
    id: String,
    document_seq_kv: std::sync::Arc<async_nats::jetstream::kv::Store>,
    last_seq: i64,
) -> () {
    tokio::spawn(async move {
        let last_seq_val_temp = i64::to_le_bytes(last_seq);

        loop {
            let old_seq = document_seq_kv.entry(id.clone()).await;
            match old_seq {
                Ok(Some(v)) => {
                    let mut v_val = 0;
                    if v.value.len() == 8 {
                        v_val = i64::from_le_bytes(
                            (*v.value).try_into().expect("entry should be 8 bytes"),
                        );
                    }
                    if v_val < last_seq {
                        let last_seq_val = Bytes::copy_from_slice(&last_seq_val_temp);
                        let res = document_seq_kv
                            .update(id.clone(), last_seq_val, v.revision)
                            .await;
                        if let Err(e) = res {
                            if e.kind()
                                == async_nats::jetstream::kv::UpdateErrorKind::WrongLastRevision
                            {
                                continue;
                            }

                            tracing::error!("Failed to update last_seq entry: {:?}", e);
                            return;
                        }
                    } else {
                        return;
                    }
                }
                Ok(None) => {
                    let last_seq_val = Bytes::copy_from_slice(&last_seq_val_temp);
                    let res = document_seq_kv.create(id.clone(), last_seq_val).await;
                    if let Err(e) = res {
                        if e.kind() == async_nats::jetstream::kv::CreateErrorKind::AlreadyExists {
                            return;
                        }

                        tracing::error!("Failed to create last_seq entry: {:?}", e);
                        return;
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to get last_seq: {:?}", e);
                    return;
                }
            };
        }
    });
}

pub async fn init_loro_document(
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    js: std::sync::Arc<async_nats::jetstream::Context>,
    wal_stream: std::sync::Arc<async_nats::jetstream::stream::Stream>,
    server_state: std::sync::Arc<tokio::sync::RwLock<super::initialize::LoroServerState>>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
    document_seq_kv: std::sync::Arc<async_nats::jetstream::kv::Store>,
) -> Result<(), LoroInitDocumentError> {
    tracing::info!(
        "initializing loro document {} with last_seq: {}",
        document_id,
        server_state.read().await.last_seq
    );

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
    let mut old_message_queue = Vec::<Vec<u8>>::new();
    let mut message_queue = Vec::<Vec<u8>>::new();

    loop {
        let mut processed_messages = 0;
        let mut messages = consumer
            .fetch()
            .messages()
            .await
            .map_err(LoroInitDocumentError::GetBatchMessagesError)?;
        while let Some(Ok(message)) = messages.next().await {
            processed_messages += 1;
            if let Err(e) = process_message_wrapper(
                nc.clone(),
                &mut chunk_store,
                &mut old_message_queue,
                &mut message_queue,
                &message,
                server_state.read().await.last_seq,
            )
            .await
            {
                tracing::error!("Failed to process message: {:?}", e);
            }

            let message_info = message
                .info()
                .map_err(LoroInitDocumentError::GetMessageInfoError)?;
            let seq = message_info.stream_sequence as i64;
            let mut state = server_state.write().await;
            if state.last_seq < seq {
                state.last_seq = seq;
                save_last_seq(document_id.clone(), document_seq_kv.clone(), seq);
            }
        }
        if processed_messages == 0 {
            break;
        }
    }

    tracing::info!(
        "running initial import_batch on {} messages",
        old_message_queue.len()
    );

    loro_doc
        .import_batch(old_message_queue.as_slice())
        .map_err(LoroInitDocumentError::LoroImportError)?;

    old_message_queue.clear();

    {
        let document_id = document_id.clone();
        let nc = nc.clone();
        let js = js.clone();
        let server_states = server_states.clone();
        let document_seq_kv = document_seq_kv.clone();
        tokio::spawn(async move {
            if let Err(e) = super::document_endpoints::init_document_endpoints(
                document_id,
                nc,
                js,
                server_states,
                wal_stream,
                document_seq_kv,
            )
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
                js,
                document_id,
                server_state,
                consumer,
                chunk_store,
                old_message_queue,
                message_queue,
                server_states,
                document_seq_kv,
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
    js: std::sync::Arc<async_nats::jetstream::Context>,
    document_id: String,
    server_state: std::sync::Arc<tokio::sync::RwLock<super::initialize::LoroServerState>>,
    consumer: std::sync::Arc<
        async_nats::jetstream::consumer::Consumer<async_nats::jetstream::consumer::pull::Config>,
    >,
    chunk_store: std::collections::HashMap<String, ChunkStoreEntry>,
    old_message_queue: Vec<Vec<u8>>,
    message_queue: Vec<Vec<u8>>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
    document_seq_kv: std::sync::Arc<async_nats::jetstream::kv::Store>,
) -> Result<(), LoroInitDocumentError> {
    tracing::debug!("processing messages for document: {}", document_id);
    let loro_doc = &server_state.read().await.document.clone();
    let mut wal_stream_messages = consumer
        .messages()
        .await
        .map_err(LoroInitDocumentError::GetMessagesError)?;

    let mut chunk_store = chunk_store;
    let mut old_message_queue = old_message_queue;
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

                let old_vv = loro_doc.oplog_vv();
                loro_doc
                    .import_batch(message_queue.as_slice())
                    .map_err(LoroInitDocumentError::LoroImportError)?;
                let new_nc = nc.clone();
                let new_js = js.clone();
                let new_vv = loro_doc.oplog_vv();
                let new_loro_doc = loro_doc.clone();
                let new_document_id = document_id.clone();
                tokio::spawn(async move {
                    let json_update =
                        new_loro_doc.export_json_updates_without_peer_compression(&old_vv, &new_vv);
                    let document_id = new_document_id;
                    let res = super::json_update::export_json_update(
                        new_nc,
                        new_js,
                        document_id,
                        new_loro_doc,
                        &json_update,
                    )
                    .await;
                    if let Err(e) = res {
                        tracing::error!("Failed to export JSON update: {:?}", e);
                    }
                });
                message_queue.clear();

                continue;
            }
        } else {
            msg = next_message.await;
        }

        if let Some(Ok(msg)) = msg {
            if let Err(e) = process_message_wrapper(
                nc.clone(),
                &mut chunk_store,
                &mut old_message_queue,
                &mut message_queue,
                &msg,
                server_state.read().await.last_seq,
            )
            .await
            {
                tracing::error!("Failed to process message: {:?}", e);
            }
            if !old_message_queue.is_empty() {
                tracing::error!(
                    "old_message_queue is not empty after processing new message: {}",
                    old_message_queue.len()
                );
            }
            let message_info = msg
                .info()
                .map_err(LoroInitDocumentError::GetMessageInfoError)?;
            let seq = message_info.stream_sequence as i64;
            let mut state = server_state.write().await;
            if state.last_seq < seq {
                state.last_seq = seq;
                save_last_seq(document_id.clone(), document_seq_kv.clone(), seq);
            }
        }
    }

    Ok(())
}

async fn process_message_wrapper(
    nc: std::sync::Arc<async_nats::Client>,
    chunk_store: &mut std::collections::HashMap<String, ChunkStoreEntry>,
    old_message_queue: &mut Vec<Vec<u8>>,
    message_queue: &mut Vec<Vec<u8>>,
    message: &async_nats::jetstream::Message,
    last_seq: i64,
) -> Result<(), LoroInitDocumentError> {
    let message_info = message
        .info()
        .map_err(LoroInitDocumentError::GetMessageInfoError)?;
    let message_seq = message_info.stream_sequence as i64;
    let is_new_message = message_seq > last_seq;
    let res = process_message(
        chunk_store,
        old_message_queue,
        message_queue,
        &message,
        is_new_message,
    );
    match res {
        Ok(v) => {
            message
                .ack_with(async_nats::jetstream::AckKind::Ack)
                .await
                .map_err(LoroInitDocumentError::AckMessageError)?;
            if is_new_message {
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
            if is_new_message {
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

#[derive(thiserror::Error, Debug)]
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
    old_message_queue: &mut Vec<Vec<u8>>,
    message_queue: &mut Vec<Vec<u8>>,
    message: &async_nats::jetstream::Message,
    is_new_message: bool,
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
                new_message: is_new_message,
            },
        );
    }
    let chunk_store_entry = chunk_store
        .get_mut(log_id)
        .expect("Chunk store entry not found when it should be");
    if is_new_message && !chunk_store_entry.new_message {
        chunk_store_entry.new_message = true;
    }
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
        let queue = if chunk_store_entry.new_message {
            message_queue
        } else {
            old_message_queue
        };
        queue.push(Vec::with_capacity(buffer_size));
        let buffer = queue.last_mut().unwrap();
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

#[derive(thiserror::Error, Debug)]
pub enum SendLogError {
    #[error("failed to subscribe to response inbox: {0}")]
    SubscribeInboxError(async_nats::SubscribeError),
    #[error("timed out waiting for response")]
    TimeoutWaitingForResponse,
    #[error("nats response malformed: {0}")]
    NatsResponseMalformed(String),
    #[error("error processing message: {0}")]
    MessageProcessingError(String),
}

pub async fn send_log(
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    js: std::sync::Arc<async_nats::jetstream::Context>,
    body: &Vec<u8>,
) -> Result<(), SendLogError> {
    let log_id = nanoid::nanoid!();
    let mut current_size = 0;
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    let mut chunk_idx = 0;

    let response_inbox = format!("loro.response.{}", nanoid::nanoid!());
    let mut subscription = nc
        .subscribe(response_inbox.clone())
        .await
        .map_err(SendLogError::SubscribeInboxError)?;

    for chunk in body.chunks(CHUNK_SIZE) {
        hasher.write(&chunk);
        current_size += chunk.len();

        let document_id = document_id.clone();
        let mut headers = async_nats::HeaderMap::new();
        headers.append("LORO_LOG_ID", log_id.clone());
        headers.append("LORO_CHUNK_INDEX", chunk_idx.to_string());
        if current_size >= body.len() {
            headers.append("LORO_FINAL", "true".to_string());
            headers.append("LORO_DIGEST", hasher.finish().to_string());
            headers.append("LORO_RESPONSE_INBOX", response_inbox.clone());
        }
        chunk_idx += 1;
        let js = js.clone();
        let cloned_chunk = chunk.to_vec();
        tokio::spawn(async move {
            js.publish_with_headers(
                format!("loro.wal.{}", document_id),
                headers,
                bytes::Bytes::from_owner(cloned_chunk),
            )
            .await
            .map_err(|e| {
                tracing::error!("Failed to publish message: {}", e);
            })
        });
    }

    loop {
        let msg =
            tokio::time::timeout(std::time::Duration::from_secs(5), subscription.next()).await;
        let msg = match msg {
            Ok(msg) => msg.ok_or(SendLogError::NatsResponseMalformed("".to_string()))?,
            Err(_) => return Err(SendLogError::TimeoutWaitingForResponse),
        };
        let payload = String::from_utf8_lossy(&msg.payload);
        if payload == "ACK" {
            return Ok(());
        }

        match payload.strip_prefix("NACK:") {
            Some(_) => {
                return Err(SendLogError::MessageProcessingError(format!(
                    "Log has failed to process: {}",
                    payload
                )));
            }
            None => {
                return Err(SendLogError::NatsResponseMalformed(format!(
                    "Unknown response: {}",
                    payload
                )));
            }
        }
    }
}
