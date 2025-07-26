use std::{any::Any, hash::Hasher};

const CHUNK_SIZE: usize = 128 * 1024; // 128 KiB

#[derive(thiserror::Error, Debug)]
pub enum EncodeJSONUpdateError {
    #[error("serde serialize json error: {0}")]
    SerdeSerializeJSONError(serde_json::Error),
    #[error("failed to publish response: {0}")]
    PublishError(async_nats::jetstream::context::PublishError),
    #[error("failed to join tasks: {0}")]
    JoinError(tokio::task::JoinError),
}

#[derive(serde::Serialize)]
struct MappedJsonOp {
    path: String,
    #[serde(rename = "type")]
    typ: String,
    op: loro::JsonOpContent,
}

pub async fn export_json_update(
    nc: std::sync::Arc<async_nats::Client>,
    js: std::sync::Arc<async_nats::jetstream::Context>,
    document_id: String,
    document: std::sync::Arc<loro::LoroDoc>,
    json_update: &loro::JsonSchema,
) -> Result<(), EncodeJSONUpdateError> {
    let _ = nc;
    let json_update_id = nanoid::nanoid!();

    let changes = json_update.changes.clone();
    let ops: Vec<MappedJsonOp> = changes
        .iter()
        .map(|change| change.ops.clone())
        .flatten()
        .filter_map(|op| {
            let container = op.container;
            let content = op.content;
            let path = document.get_path_to_container(&container);
            path.map(|v| MappedJsonOp {
                path: v
                    .iter()
                    .map(|i| i.1.to_string())
                    .collect::<Vec<String>>()
                    .join("/"),
                typ: container.container_type().to_string(),
                op: content,
            })
        })
        .collect();

    let content =
        serde_json::to_vec(&ops).map_err(EncodeJSONUpdateError::SerdeSerializeJSONError)?;

    let mut joins: Vec<
        tokio::task::JoinHandle<
            Result<
                async_nats::jetstream::context::PublishAckFuture,
                async_nats::jetstream::context::PublishError,
            >,
        >,
    > = Vec::new();
    let mut hasher = twox_hash::XxHash64::with_seed(0);
    hasher.write(&content);
    let digest = hasher.finish();
    let num_chunks = (content.len() + CHUNK_SIZE - 1) / CHUNK_SIZE;
    for (idx, chunk) in content.chunks(CHUNK_SIZE).enumerate() {
        let mut headers = async_nats::HeaderMap::new();
        if idx == 0 {
            headers.insert("X-Document-ID", document_id.clone());
            headers.insert("X-Chunk-Count", num_chunks.to_string());
            headers.insert("X-Message-Digest", digest.to_string());
            headers.insert("X-JSON-Update-ID", json_update_id.clone());
        } else {
            headers.insert("X-Chunk-Index", idx.to_string());
        }
        let buffer = bytes::Bytes::copy_from_slice(chunk);
        let new_js = js.clone();
        let new_json_update_id = json_update_id.clone();
        joins.push(tokio::spawn(async move {
            if idx == 0 {
                new_js
                    .publish_with_headers("loro.json_update.init", headers, buffer)
                    .await
            } else {
                new_js
                    .publish_with_headers(
                        format!("loro.json_update.rest.{}", new_json_update_id),
                        headers,
                        buffer,
                    )
                    .await
            }
        }));
    }
    let join_res = futures::future::join_all(joins).await;
    for res in join_res {
        match res {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                return Err(EncodeJSONUpdateError::PublishError(e));
            }
            Err(e) => {
                return Err(EncodeJSONUpdateError::JoinError(e));
            }
        }
    }

    Ok(())
}
