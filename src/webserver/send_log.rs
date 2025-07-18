use futures::StreamExt;
use http_body_util::BodyDataStream;
use std::hash::Hasher;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum SendLogError {
    #[error("body next error: {0}")]
    BodyNextError(hyper::Error),
    #[error("failed to subscribe to response inbox: {0}")]
    SubscribeInboxError(async_nats::SubscribeError),
    #[error("timed out waiting for response")]
    TimeoutWaitingForResponse,
    #[error("nats response malformed: {0}")]
    NatsResponseMalformed(String),
    #[error("error processing message: {0}")]
    MessageProcessingError(String),
}

const CHUNK_SIZE: usize = 128 * 1024; // 128 KiB

pub async fn send_log(
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    js: std::sync::Arc<async_nats::jetstream::Context>,
    document_status_kv: std::sync::Arc<async_nats::jetstream::kv::Store>,
    body: hyper::body::Incoming,
) -> Result<(), SendLogError> {
    let log_id = nanoid::nanoid!();
    let server_initialized = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    {
        let document_id = document_id.clone();
        let nc = nc.clone();
        let js = js.clone();
        let document_status_kv = document_status_kv.clone();
        let server_initialized = server_initialized.clone();
        tokio::spawn(async move {
            super::init_doc::ping_and_init_doc(document_id, nc, js, document_status_kv)
                .await
                .unwrap_or_else(|e| {
                    tracing::error!("Failed to ping and init document: {}", e);
                });
            server_initialized.store(true, std::sync::atomic::Ordering::Relaxed);
        });
    }

    let mut deque: std::collections::VecDeque<bytes::Bytes> = std::collections::VecDeque::new();
    let mut current_size = 0;
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    let mut chunk_idx = 0;
    let mut body = BodyDataStream::new(body);
    while let Some(frame) = body.next().await {
        let frame = frame.map_err(SendLogError::BodyNextError)?;
        hasher.write(&frame);
        current_size += frame.len();
        deque.push_back(frame);

        while current_size >= CHUNK_SIZE {
            let mut buffer: [u8; CHUNK_SIZE] = [0; CHUNK_SIZE];
            let mut buffer_ptr = 0;
            while buffer_ptr < CHUNK_SIZE {
                let remaining = CHUNK_SIZE - buffer_ptr;
                let mut data = deque.pop_front().unwrap();
                let data_len = data.len(); // safe to unwrap
                if remaining < data_len {
                    let leftover = data.split_off(remaining);
                    deque.push_front(leftover);
                }
                let copy_len = data.len();
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        data.as_ptr(),
                        buffer[buffer_ptr..].as_mut_ptr(),
                        copy_len,
                    );
                }
                buffer_ptr += copy_len;
                current_size -= copy_len;
            }
            let document_id = document_id.clone();
            let mut headers = async_nats::HeaderMap::new();
            headers.append("LORO_LOG_ID", log_id.clone());
            headers.append("LORO_CHUNK_INDEX", chunk_idx.to_string());
            chunk_idx += 1;
            let js = js.clone();
            tokio::spawn(async move {
                js.publish_with_headers(
                    format!("loro.wal.{}", document_id),
                    headers,
                    bytes::Bytes::from_owner(buffer),
                )
                .await
                .map_err(|e| {
                    tracing::error!("Failed to publish message: {}", e);
                })
            });
        }
    }

    let buffer_len = current_size;
    let mut buffer: Vec<u8> = vec![0; buffer_len];
    let mut buffer_ptr = 0;
    while buffer_ptr < buffer_len {
        let remaining = buffer_len - buffer_ptr;
        let mut data = deque.pop_front().unwrap();
        let data_len = data.len(); // safe to unwrap
        if remaining < data_len {
            let leftover = data.split_off(remaining);
            deque.push_front(leftover);
        }
        let copy_len = data.len();
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                buffer[buffer_ptr..].as_mut_ptr(),
                copy_len,
            );
        }
        buffer_ptr += copy_len;
        current_size -= copy_len;
    }
    let document_id = document_id.clone();
    let mut headers = async_nats::HeaderMap::new();
    headers.append("LORO_LOG_ID", log_id.clone());
    headers.append("LORO_CHUNK_INDEX", chunk_idx.to_string());
    headers.append("LORO_FINAL", "true".to_string());
    headers.append("LORO_DIGEST", hasher.finish().to_string());
    let response_inbox = format!("loro.response.{}", nanoid::nanoid!());
    headers.append("LORO_RESPONSE_INBOX", response_inbox.clone());

    let mut subscription = nc
        .subscribe(response_inbox)
        .await
        .map_err(SendLogError::SubscribeInboxError)?;

    tokio::spawn(async move {
        js.publish_with_headers(
            format!("loro.wal.{}", document_id),
            headers,
            bytes::Bytes::from_owner(buffer),
        )
        .await
        .map_err(|e| {
            tracing::error!("Failed to publish message: {}", e);
        })
    });

    loop {
        let msg =
            tokio::time::timeout(std::time::Duration::from_secs(5), subscription.next()).await;
        let msg = match msg {
            Ok(msg) => msg.ok_or(SendLogError::NatsResponseMalformed("".to_string()))?,
            Err(_) => {
                if server_initialized.load(std::sync::atomic::Ordering::Relaxed) {
                    continue;
                }
                return Err(SendLogError::TimeoutWaitingForResponse);
            }
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
