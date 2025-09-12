use futures::StreamExt;
use std::hash::Hasher;

const CHUNK_SIZE: usize = 128 * 1024; // 128 KiB

trait IgnoreRwLockPoison<T> {
    fn write_or_panic(&self) -> std::sync::RwLockWriteGuard<'_, T>;
    fn read_or_panic(&self) -> std::sync::RwLockReadGuard<'_, T>;
}

impl<T> IgnoreRwLockPoison<T> for std::sync::RwLock<T> {
    fn write_or_panic(&self) -> std::sync::RwLockWriteGuard<'_, T> {
        self.write().expect("RwLock poisoned")
    }

    fn read_or_panic(&self) -> std::sync::RwLockReadGuard<'_, T> {
        self.read().expect("RwLock poisoned")
    }
}

struct ChunkStoreEntry {
    chunks: std::sync::Arc<std::sync::RwLock<std::collections::HashMap<usize, Vec<u8>>>>,
    hash: twox_hash::xxhash64::Hasher,
    hash_idx: usize,
    chunk_count: usize,
    digest: u64,
    timeout_cancel_sender: Option<tokio::sync::oneshot::Sender<()>>,
}

#[derive(thiserror::Error, Debug)]
pub enum SendResponseError {
    #[error("failed to join tasks: {0}")]
    JoinError(tokio::task::JoinError),
    #[error("failed to publish response: {0}")]
    PublishError(async_nats::PublishError),
}

#[derive(thiserror::Error, Debug)]
pub enum ProcessMessageError {
    #[error("failed to process message: {0}")]
    MessageProcessingError(String),
}

pin_project_lite::pin_project! {
    pub struct CoreSubscriber {
        #[pin]
        inner: async_nats::Subscriber,
        nc: std::sync::Arc<async_nats::Client>,
        chunks: std::sync::Arc<std::sync::RwLock<std::collections::HashMap<String, std::sync::Arc<std::sync::RwLock<ChunkStoreEntry>>>>>,
    }
}

impl CoreSubscriber {
    pub async fn subscribe(
        nc: &std::sync::Arc<async_nats::Client>,
        subject: String,
    ) -> Result<Self, async_nats::SubscribeError> {
        let subscriber = nc.subscribe(subject).await?;
        Ok(CoreSubscriber {
            inner: subscriber,
            nc: nc.clone(),
            chunks: std::sync::Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        })
    }
}

async fn send_response(
    nc: &std::sync::Arc<async_nats::Client>,
    response_inbox: String,
    message_id: String,
    content: &[u8],
    status_code: u32,
) -> Result<(), SendResponseError> {
    let mut current_size = 0;
    let content_size = content.len();
    let mut joins: Vec<tokio::task::JoinHandle<Result<(), async_nats::PublishError>>> = Vec::new();
    for (idx, chunk) in content.chunks(CHUNK_SIZE).enumerate() {
        current_size += chunk.len();
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("X-Message-ID", message_id.clone());
        headers.insert("X-Chunk-Index", idx.to_string());
        headers.insert("X-Status-Code", status_code.to_string());
        if current_size == content_size {
            let mut hasher = twox_hash::XxHash64::with_seed(0);
            hasher.write(content);
            let digest = hasher.finish();
            headers.insert("X-Message-Digest", digest.to_string());
        }
        let nc = nc.clone();
        let response_inbox = response_inbox.clone();
        let chunk = bytes::Bytes::copy_from_slice(chunk);
        joins.push(tokio::spawn(async move {
            nc.publish_with_headers(response_inbox, headers, chunk)
                .await
        }));
    }
    if content.len() == 0 {
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("X-Message-ID", message_id.clone());
        headers.insert("X-Chunk-Index", "0".to_string());
        headers.insert("X-Status-Code", status_code.to_string());
        let mut hasher = twox_hash::XxHash64::with_seed(0);
        hasher.write(&[]);
        let digest = hasher.finish();
        headers.insert("X-Message-Digest", digest.to_string());
        let nc = nc.clone();
        let response_inbox = response_inbox.clone();
        joins.push(tokio::spawn(async move {
            nc.publish_with_headers(response_inbox, headers, bytes::Bytes::new())
                .await
        }));
    }
    let join_res = futures_util::future::join_all(joins).await;
    for res in join_res {
        match res {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                return Err(SendResponseError::PublishError(e));
            }
            Err(e) => {
                return Err(SendResponseError::JoinError(e));
            }
        }
    }

    Ok(())
}

fn process_message(
    nc: &std::sync::Arc<async_nats::Client>,
    chunks: std::sync::Arc<
        std::sync::RwLock<
            std::collections::HashMap<String, std::sync::Arc<std::sync::RwLock<ChunkStoreEntry>>>,
        >,
    >,
    msg: async_nats::Message,
) -> Result<Option<CoreMessage>, ProcessMessageError> {
    let Some(reply) = msg.reply else {
        return Err(ProcessMessageError::MessageProcessingError(
            "Missing reply field".to_string(),
        ));
    };
    let Some(headers) = msg.headers else {
        return Err(ProcessMessageError::MessageProcessingError(
            "Missing headers".to_string(),
        ));
    };
    let payload = msg.payload;
    let message_id = headers
        .get("X-Message-ID")
        .ok_or_else(|| {
            ProcessMessageError::MessageProcessingError("Missing message ID".to_string())
        })?
        .to_string();
    let chunk_index: usize = headers
        .get("X-Chunk-Index")
        .ok_or_else(|| {
            ProcessMessageError::MessageProcessingError("Missing chunk index".to_string())
        })?
        .to_string()
        .parse()
        .map_err(|e| {
            ProcessMessageError::MessageProcessingError(format!(
                "Invalid chunk index {}: {}",
                headers.get("X-Chunk-Index").unwrap(),
                e
            ))
        })?;
    let message_digest: Option<u64> = match headers.get("X-Message-Digest") {
        Some(v) => Some(v.to_string().parse().map_err(|e| {
            ProcessMessageError::MessageProcessingError(format!(
                "Invalid message digest {}: {}",
                v, e
            ))
        })?),
        None => None,
    };

    let entry = {
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let new_chunks = chunks.clone();
        let new_reply = reply.clone();
        let new_message_id = message_id.clone();
        let nc = nc.clone();
        tokio::spawn(async move {
            let res = tokio::time::timeout(std::time::Duration::from_secs(60), rx).await;
            if res.is_err() {
                // Timeout occurred, remove the entry
                new_chunks.write_or_panic().remove(&new_message_id.clone());
                let resp = send_response(
                    &nc,
                    new_reply.to_string(),
                    new_message_id.clone(),
                    b"{{\"message\": \"Timeout occurred: message not completed after 60 seconds\"}}",
                    500,
                ).await;
                if let Err(e) = resp {
                    tracing::error!(
                        "Failed to send timeout error response for message {}: {}",
                        new_message_id,
                        e
                    );
                }
            }
        });
        let mut chunks = chunks.write_or_panic();
        if !chunks.contains_key(&message_id) {
            chunks.insert(
                message_id.clone(),
                std::sync::Arc::new(std::sync::RwLock::new(ChunkStoreEntry {
                    chunks: std::sync::Arc::new(std::sync::RwLock::new(
                        std::collections::HashMap::new(),
                    )),
                    hash: twox_hash::XxHash64::with_seed(0),
                    hash_idx: 0,
                    chunk_count: 0,
                    digest: 0,
                    timeout_cancel_sender: Some(tx),
                })),
            );
        }
        let entry = chunks
            .get(&message_id)
            .expect("Chunk store entry not found when it should be")
            .clone();
        entry
    };
    entry
        .write_or_panic()
        .chunks
        .write_or_panic()
        .insert(chunk_index, payload.to_vec());
    {
        let mut entry = entry.write_or_panic();
        let chunks = entry.chunks.clone();
        let chunks = chunks.read_or_panic();
        while let Some(chunk) = chunks.get(&entry.hash_idx) {
            entry.hash.write(chunk.as_slice());
            entry.hash_idx += 1;
        }
    }

    if let Some(digest) = message_digest {
        entry.write_or_panic().digest = digest;
        entry.write_or_panic().chunk_count = chunk_index + 1;
    }

    let chunk_count = entry.read_or_panic().chunk_count;
    let hash_idx = entry.read_or_panic().hash_idx;
    if chunk_count > 0 && chunk_count == hash_idx {
        let message_digest = entry.read_or_panic().digest;
        let sum = entry.read_or_panic().hash.finish();
        if sum != message_digest {
            return Err(ProcessMessageError::MessageProcessingError(format!(
                "Message digest mismatch: expected {} != actual {}",
                message_digest, sum
            )));
        }
        let buffer_size = entry
            .read_or_panic()
            .chunks
            .read_or_panic()
            .iter()
            .map(|(_, v)| v.len())
            .sum::<usize>();
        let mut buffer: Vec<u8> = Vec::with_capacity(buffer_size);
        for i in 0..chunk_count {
            let chunk = entry
                .read_or_panic()
                .chunks
                .write_or_panic()
                .remove(&i)
                .ok_or(ProcessMessageError::MessageProcessingError(format!(
                    "Missing chunk {}",
                    i
                )))?;
            buffer.extend_from_slice(&chunk);
        }
        entry
            .write_or_panic()
            .timeout_cancel_sender
            .take()
            .expect("Timeout sender not found")
            .send(())
            .expect("Failed to send timeout cancel signal");
        chunks.write_or_panic().remove(&message_id);
        return Ok(Some(CoreMessage {
            responder: CoreMessageResponder {
                message_id: message_id.clone(),
                response_inbox: reply.to_string(),
            },
            payload: buffer,
        }));
    }

    Ok(None)
}

pub struct CoreMessage {
    pub responder: CoreMessageResponder,
    pub payload: Vec<u8>,
}

pub struct CoreMessageResponder {
    message_id: String,
    response_inbox: String,
}

impl CoreMessageResponder {
    pub async fn send_response(
        self: &Self,
        nc: &std::sync::Arc<async_nats::Client>,
        content: &[u8],
        status_code: u32,
    ) -> Result<(), SendResponseError> {
        send_response(
            nc,
            self.response_inbox.clone(),
            self.message_id.clone(),
            content,
            status_code,
        )
        .await
    }

    pub async fn send_response_string(
        self: &Self,
        nc: &std::sync::Arc<async_nats::Client>,
        content: &str,
        status_code: u32,
    ) -> Result<(), SendResponseError> {
        self.send_response(nc, content.as_bytes(), status_code)
            .await
    }
}

impl futures::Stream for CoreSubscriber {
    type Item = CoreMessage;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        let msg = futures_util::ready!(this.inner.poll_next(cx));
        let Some(msg) = msg else {
            return std::task::Poll::Ready(None);
        };

        let message_id = msg
            .headers
            .as_ref()
            .and_then(|h| h.get("X-Message-ID"))
            .map(|v| v.to_string())
            .unwrap_or_default();
        let reply = msg.reply.clone();

        let processed_message = process_message(this.nc, this.chunks.clone(), msg);

        match processed_message {
            Ok(Some(message)) => std::task::Poll::Ready(Some(message)),
            Ok(None) => std::task::Poll::Pending,
            Err(e) => {
                if let Some(reply) = reply {
                    let nc = this.nc.clone();
                    tokio::spawn(async move {
                        send_response(
                            &nc,
                            reply.to_string(),
                            message_id.clone(),
                            e.to_string().as_bytes(),
                            500,
                        )
                        .await
                        .map_err(|e| {
                            tracing::error!(
                                "{{\"message\": Failed to send error response for message {}: {}}}",
                                message_id,
                                e
                            );
                        })
                    });
                }
                std::task::Poll::Pending
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum MakeRequestError {
    #[error("failed to subscribe to inbox: {0}")]
    SubscribeError(async_nats::SubscribeError),
    #[error("failed to publish response: {0}")]
    PublishError(async_nats::PublishError),
    #[error("failed to join tasks: {0}")]
    JoinError(tokio::task::JoinError),
    #[error("failed to process message: {0}")]
    ProcessMessageError(String),
    #[error("timeout occurred: {0}")]
    TimeoutError(tokio::time::error::Elapsed),
    #[error("no responders")]
    NoResponders,
}

pub async fn send_message(
    nc: &std::sync::Arc<async_nats::Client>,
    subject: String,
    message_id: String,
    content: &[u8],
    response_inbox: Option<String>,
) -> Result<(), MakeRequestError> {
    let mut current_size = 0;
    let content_size = content.len();
    let mut joins: Vec<tokio::task::JoinHandle<Result<(), async_nats::PublishError>>> = Vec::new();
    for (idx, chunk) in content.chunks(CHUNK_SIZE).enumerate() {
        current_size += chunk.len();
        let mut headers = async_nats::HeaderMap::new();
        headers.insert("X-Message-ID", message_id.clone());
        headers.insert("X-Chunk-Index", idx.to_string());
        if current_size == content_size {
            let mut hasher = twox_hash::XxHash64::with_seed(0);
            hasher.write(content);
            let digest = hasher.finish();
            headers.insert("X-Message-Digest", digest.to_string());
        }
        let nc = nc.clone();
        let response_inbox = response_inbox.clone();
        let chunk = bytes::Bytes::copy_from_slice(chunk);
        let subject = subject.clone();
        joins.push(tokio::spawn(async move {
            if let Some(response_inbox) = response_inbox {
                nc.publish_with_reply_and_headers(subject, response_inbox, headers, chunk)
                    .await
            } else {
                nc.publish_with_headers(subject, headers, chunk).await
            }
        }));
    }
    let join_res = futures_util::future::join_all(joins).await;
    for res in join_res {
        match res {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                return Err(MakeRequestError::PublishError(e));
            }
            Err(e) => {
                return Err(MakeRequestError::JoinError(e));
            }
        }
    }

    Ok(())
}

pub struct CoreResponse {
    pub status_code: u32,
    pub payload: Vec<u8>,
}

pub async fn make_request(
    nc: &std::sync::Arc<async_nats::Client>,
    subject: String,
    content: &[u8],
    timeout: std::time::Duration,
) -> Result<CoreResponse, MakeRequestError> {
    let message_id = nanoid::nanoid!();
    let response_inbox = nc.new_inbox();
    let mut subscriber = nc
        .subscribe(response_inbox.clone())
        .await
        .map_err(MakeRequestError::SubscribeError)?;

    let response_listener: tokio::task::JoinHandle<Result<(Vec<u8>, u32), MakeRequestError>> =
        tokio::spawn(async move {
            let mut chunks: std::collections::HashMap<usize, Vec<u8>> =
                std::collections::HashMap::new();
            let mut hash = twox_hash::XxHash64::with_seed(0);
            let mut hash_idx = 0;
            let mut chunk_count = 0;
            let mut digest = 0;
            let mut status_code = 200;
            loop {
                let Some(msg) = subscriber.next().await else {
                    continue;
                };

                if let Some(status) = msg.status {
                    if status == async_nats::status::StatusCode::NO_RESPONDERS {
                        return Err(MakeRequestError::NoResponders);
                    }
                }

                let Some(headers) = msg.headers else {
                    return Err(MakeRequestError::ProcessMessageError(
                        "Missing headers".to_string(),
                    ));
                };
                let payload = msg.payload;
                let chunk_index: usize = headers
                    .get("X-Chunk-Index")
                    .ok_or_else(|| {
                        MakeRequestError::ProcessMessageError("Missing chunk index".to_string())
                    })?
                    .to_string()
                    .parse()
                    .map_err(|e| {
                        MakeRequestError::ProcessMessageError(format!(
                            "Invalid chunk index {}: {}",
                            headers.get("X-Chunk-Index").unwrap(),
                            e
                        ))
                    })?;
                let message_digest: Option<u64> = match headers.get("X-Message-Digest") {
                    Some(v) => Some(v.to_string().parse().map_err(|e| {
                        MakeRequestError::ProcessMessageError(format!(
                            "Invalid message digest {}: {}",
                            v, e
                        ))
                    })?),
                    None => None,
                };
                let status_code_str = headers
                    .get("X-Status-Code")
                    .ok_or_else(|| {
                        MakeRequestError::ProcessMessageError("Missing status code".to_string())
                    })?
                    .to_string();
                if !status_code_str.is_empty() {
                    status_code = status_code_str.parse().map_err(|e| {
                        MakeRequestError::ProcessMessageError(format!(
                            "Invalid status code {}: {}",
                            status_code_str, e
                        ))
                    })?;
                }

                chunks.insert(chunk_index, payload.to_vec());
                while let Some(chunk) = chunks.get(&hash_idx) {
                    hash.write(chunk.as_slice());
                    hash_idx += 1;
                }

                if let Some(message_digest) = message_digest {
                    digest = message_digest;
                    chunk_count = chunk_index + 1;
                }

                if chunk_count > 0 && chunk_count == hash_idx {
                    let sum = hash.finish();
                    if sum != digest {
                        return Err(MakeRequestError::ProcessMessageError(format!(
                            "Message digest mismatch: expected {} != actual {}",
                            digest, sum
                        )));
                    }
                    let buffer_size = chunks.iter().map(|(_, v)| v.len()).sum::<usize>();
                    let mut buffer: Vec<u8> = Vec::with_capacity(buffer_size);
                    for i in 0..chunk_count {
                        let chunk =
                            chunks
                                .remove(&i)
                                .ok_or(MakeRequestError::ProcessMessageError(format!(
                                    "Missing chunk {}",
                                    i
                                )))?;
                        buffer.extend_from_slice(&chunk);
                    }
                    return Ok((buffer, status_code));
                }
            }
        });

    send_message(nc, subject, message_id, content, Some(response_inbox)).await?;

    let timeout_result = tokio::time::timeout(timeout, response_listener).await;
    let (resp, code) = timeout_result
        .map_err(MakeRequestError::TimeoutError)?
        .map_err(MakeRequestError::JoinError)??;

    Ok(CoreResponse {
        status_code: code,
        payload: resp,
    })
}
