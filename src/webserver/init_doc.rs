use futures::StreamExt;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum InitDocError {
    #[error("failed to send ping request")]
    PingRequestFailed(crate::transport::MakeRequestError),
    #[error("failed to get document entry")]
    DocumentEntryError(async_nats::jetstream::kv::EntryError),
    #[error("failed to create document status")]
    DocumentCreateStatusError(async_nats::jetstream::kv::CreateError),
    #[error("failed to update document status")]
    DocumentUpdateStatusError(async_nats::jetstream::kv::UpdateError),
    #[error("invalid document status: {0}")]
    InvalidDocumentStatus(String),
    #[error("document is in ERROR status")]
    DocumentInErrorStatus,
    #[error("failed to publish init message: {0}")]
    PublishInitMessageError(async_nats::PublishError),
    #[error("failed to watch kv: {0}")]
    WatchKvError(async_nats::jetstream::kv::WatchError),
    #[error("failed while watching kv: {0}")]
    WatchKvEntryError(async_nats::jetstream::kv::WatcherError),
    #[error("failed to put document status: {0}")]
    PutDocumentStatusError(async_nats::jetstream::kv::PutError),
    #[error("too many retries")]
    TooManyRetries,
}

pub async fn ping_and_init_doc(
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    _js: std::sync::Arc<async_nats::jetstream::Context>,
    document_status_kv: std::sync::Arc<async_nats::jetstream::kv::Store>,
) -> Result<(), InitDocError> {
    let mut first_run = true;
    let mut old_operation_ids: Vec<String> = vec![];
    for retry_count in 0..3 {
        let operation_id = nanoid::nanoid!();
        old_operation_ids.push(operation_id.clone());
        loop {
            let document_entry = document_status_kv
                .entry(document_id.clone())
                .await
                .map_err(InitDocError::DocumentEntryError)?;
            if let Some(entry) = document_entry {
                let document_status = String::from_utf8_lossy(&entry.value);
                let split_status = document_status.split(':').collect::<Vec<_>>();
                let document_status = split_status[0];
                match document_status {
                    "UP" => {
                        let timestamp = split_status[2];
                        let timestamp = super::super::time::to_timestamp(timestamp)
                            .map_err(|e| InitDocError::InvalidDocumentStatus(e.to_string()))?;
                        let now = chrono::Utc::now();
                        if now - timestamp <= chrono::Duration::seconds(5) {
                            if first_run {
                                first_run = false;
                                let resp = crate::transport::make_request(
                                    &nc,
                                    format!("loro.doc.ping.{}", document_id),
                                    "ping".as_bytes(),
                                    std::time::Duration::from_secs(5),
                                )
                                .await;
                                match resp {
                                    Ok(v) => {
                                        if String::from_utf8_lossy(&v.payload) == "pong" {
                                            return Ok(());
                                        }
                                    }
                                    Err(e) => {
                                        if !matches!(
                                            e,
                                            crate::transport::MakeRequestError::NoResponders
                                        ) {
                                            return Err(InitDocError::PingRequestFailed(e));
                                        }
                                    }
                                }
                            }
                        }
                    }
                    "DOWN" => {}
                    "STARTING" => {
                        if !old_operation_ids.contains(&split_status[1].to_string()) {
                            break;
                        }
                    }
                    "ERROR" => {
                        return Err(InitDocError::DocumentInErrorStatus);
                    }
                    _ => {
                        return Err(InitDocError::InvalidDocumentStatus(
                            document_status.to_string(),
                        ));
                    }
                }
                let update_res = document_status_kv
                    .update(
                        document_id.clone(),
                        format!(
                            "STARTING:{}:{}:{}",
                            operation_id,
                            retry_count,
                            super::super::time::from_now(),
                        )
                        .into(),
                        entry.revision,
                    )
                    .await;
                match update_res {
                    Ok(_) => {
                        nc.publish(
                            format!("loro.init.{}", document_id.clone()),
                            operation_id.into(),
                        )
                        .await
                        .map_err(InitDocError::PublishInitMessageError)?;
                        tracing::info!("Document {} is starting", document_id);
                        break;
                    }
                    Err(e) => {
                        if e.kind() != async_nats::jetstream::kv::UpdateErrorKind::WrongLastRevision
                        {
                            return Err(InitDocError::DocumentUpdateStatusError(e));
                        }
                        continue;
                    }
                }
            } else {
                let create_res = document_status_kv
                    .create(
                        document_id.clone(),
                        format!(
                            "STARTING:{}:{}:{}",
                            operation_id,
                            retry_count,
                            super::super::time::from_now(),
                        )
                        .into(),
                    )
                    .await;
                match create_res {
                    Ok(_) => {
                        nc.publish(
                            format!("loro.init.{}", document_id.clone()),
                            operation_id.into(),
                        )
                        .await
                        .map_err(InitDocError::PublishInitMessageError)?;
                        tracing::info!("Document {} is starting", document_id);
                        break;
                    }
                    Err(e) => {
                        if e.kind() != async_nats::jetstream::kv::CreateErrorKind::AlreadyExists {
                            return Err(InitDocError::DocumentCreateStatusError(e));
                        }
                        continue;
                    }
                }
            }
        }
        let mut entries = document_status_kv
            .watch(document_id.clone())
            .await
            .map_err(InitDocError::WatchKvError)?;
        loop {
            let res = tokio::time::timeout(std::time::Duration::from_secs(6), entries.next()).await;
            match res {
                Ok(v) => {
                    if let Some(entry) = v {
                        let entry = entry.map_err(InitDocError::WatchKvEntryError)?;
                        let document_status = String::from_utf8_lossy(&entry.value);
                        let split_status = document_status.split(':').collect::<Vec<_>>();
                        let document_status = split_status[0];
                        match document_status {
                            "UP" => {
                                return Ok(());
                            }
                            "DOWN" => break,
                            "STARTING" => {}
                            "ERROR" => {
                                return Err(InitDocError::DocumentInErrorStatus);
                            }
                            _ => {
                                return Err(InitDocError::InvalidDocumentStatus(
                                    document_status.to_string(),
                                ));
                            }
                        }
                    } else {
                        break;
                    }
                }
                Err(_) => {
                    tracing::info!("Timeout waiting for document {} to start", document_id);
                    break;
                }
            }
        }
    }

    document_status_kv
        .put(
            document_id.clone(),
            format!(
                "ERROR:{}:{}",
                (0..32).map(|_| "0").collect::<String>(),
                super::super::time::from_now(),
            )
            .into(),
        )
        .await
        .map_err(InitDocError::PutDocumentStatusError)?;

    Err(InitDocError::TooManyRetries)
}
