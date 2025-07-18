use futures::StreamExt;
use thiserror::Error;

type NatsGenericError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Error, Debug)]
pub enum LoroServerInitError {
    #[error("nats service endpoint start failed: {0}")]
    NatsServiceEndpointStartFailed(NatsGenericError),
    #[error("create document status kv failed: {0}")]
    CreateDocumentStatusKvFailed(async_nats::jetstream::context::CreateKeyValueError),
    #[error("create wal stream failed: {0}")]
    CreateWalStreamFailed(async_nats::jetstream::context::CreateStreamError),
}

pub struct LoroServerState {
    pub document: std::sync::Arc<loro::LoroDoc>,
    pub last_seq: u64,
    pub initialized: bool,
    operation_id: String,
}

pub struct LoroServerStates {
    states: tokio::sync::RwLock<
        std::collections::HashMap<String, std::sync::Arc<tokio::sync::RwLock<LoroServerState>>>,
    >,
}

impl LoroServerStates {
    fn new() -> Self {
        Self {
            states: tokio::sync::RwLock::new(std::collections::HashMap::new()),
        }
    }

    pub async fn exists(&self, id: String) -> bool {
        let states = self.states.read().await;
        states.contains_key(&id)
    }

    pub async fn get(
        &self,
        id: String,
    ) -> Option<std::sync::Arc<tokio::sync::RwLock<LoroServerState>>> {
        let states = self.states.read().await;
        states.get(&id).cloned()
    }

    async fn get_or_create(
        &self,
        id: String,
        operation_id: String,
    ) -> std::sync::Arc<tokio::sync::RwLock<LoroServerState>> {
        let mut states = self.states.write().await;
        if let Some(state) = states.get(&id) {
            state.write().await.operation_id = operation_id.clone();
            state.clone()
        } else {
            let new_state = std::sync::Arc::new(tokio::sync::RwLock::new(LoroServerState {
                document: std::sync::Arc::new(loro::LoroDoc::new()),
                last_seq: 0,
                operation_id: operation_id.clone(),
                initialized: false,
            }));
            states.insert(id.to_string(), new_state.clone());
            new_state
        }
    }

    async fn documents(&self) -> Vec<String> {
        let states = self.states.read().await;
        states.keys().cloned().collect()
    }

    async fn remove(&self, id: String) {
        let mut states = self.states.write().await;
        states.remove(&id);
    }
}

pub async fn init_loro_server(
    nc: std::sync::Arc<async_nats::Client>,
    js: std::sync::Arc<async_nats::jetstream::Context>,
    service: &async_nats::service::Service,
) -> Result<(), LoroServerInitError> {
    tracing::debug!("initializaing loro server");

    let loro_server_id = nanoid::nanoid!();
    tracing::debug!("loro server id: {}", loro_server_id);
    let loro_server_states = std::sync::Arc::new(LoroServerStates::new());
    let loro_service_group = service.group("loro");
    let document_status_kv = std::sync::Arc::new(
        js.create_key_value(async_nats::jetstream::kv::Config {
            bucket: "loro-document-status".to_string(),
            description: "Loro Document Status".to_string(),
            ..Default::default()
        })
        .await
        .map_err(LoroServerInitError::CreateDocumentStatusKvFailed)?,
    );
    let wal_stream = std::sync::Arc::new(
        js.create_stream(async_nats::jetstream::stream::Config {
            name: "loro-wal".to_string(),
            description: Some(
                "Stream for storing the WAL (Write Ahead Log) messages for all Lorogo documents."
                    .to_string(),
            ),
            subjects: vec!["loro.wal.>".to_string()],
            discard: async_nats::jetstream::stream::DiscardPolicy::New,
            storage: async_nats::jetstream::stream::StorageType::File,
            num_replicas: 1,
            ..Default::default()
        })
        .await
        .map_err(LoroServerInitError::CreateWalStreamFailed)?,
    );

    let mut document_check_interval = tokio::time::interval(std::time::Duration::from_secs(3));

    let init_endpoint = loro_service_group
        .endpoint("init.*")
        .await
        .map_err(LoroServerInitError::NatsServiceEndpointStartFailed)?;
    let init_endpoint_arc_mutex = std::sync::Arc::new(tokio::sync::Mutex::new(init_endpoint));
    tokio::spawn(async move {
        let mut init_endpoint = init_endpoint_arc_mutex.lock().await;
        loop {
            tokio::select! {
                _ = document_check_interval.tick() => {
                    tracing::debug!("document check interval tick");
                    let loro_server_id = loro_server_id.clone();
                    let loro_server_states = loro_server_states.clone();
                    let document_status_kv = document_status_kv.clone();
                    tokio::spawn(async move {
                        handle_document_check(loro_server_id, loro_server_states, document_status_kv).await.unwrap_or_else(|e| {
                            tracing::error!("Failed to handle document check: {:?}", e);
                        });
                    });
                }
                Some(msg) = init_endpoint.next() => {
                    tracing::debug!("init endpoint received message");
                    let nc = nc.clone();
                    let loro_server_id = loro_server_id.clone();
                    let loro_server_states = loro_server_states.clone();
                    let document_status_kv = document_status_kv.clone();
                    let wal_stream = wal_stream.clone();
                    tokio::spawn(async move {
                        handle_init(
                            nc,
                            loro_server_id,
                            loro_server_states,
                            document_status_kv,
                            wal_stream,
                            &msg,
                        )
                        .await
                        .unwrap_or_else(|e| {
                            tracing::error!("Failed to handle init: {:?}", e);
                        });
                    });
                }
            }
        }
    });

    Ok(())
}

#[derive(Error, Debug)]
pub enum LoroServerDocumentCheckError {
    #[error("failed to get document status: {0}")]
    GetDocumentStatusError(#[from] async_nats::jetstream::kv::EntryError),
    #[error("failed to parse document status {0}: {1}")]
    ParseDocumentStatusError(String, String),
    #[error("document does not exist: {0}")]
    DocumentNotFoundError(String),
}

async fn handle_document_check(
    loro_server_id: String,
    loro_server_states: std::sync::Arc<LoroServerStates>,
    document_status_kv: std::sync::Arc<async_nats::jetstream::kv::Store>,
) -> Result<(), LoroServerDocumentCheckError> {
    let documents = loro_server_states.documents().await;
    if documents.is_empty() {
        return Ok(());
    }

    for document_id in documents.iter() {
        let doc_id = document_id.clone();
        let loro_server_id = loro_server_id.clone();
        let loro_server_states = loro_server_states.clone();
        let document_status_kv = document_status_kv.clone();
        tokio::spawn(async move {
            check_document(
                loro_server_id,
                loro_server_states,
                document_status_kv,
                doc_id,
            )
            .await
            .unwrap_or_else(|e| {
                tracing::error!("Failed to check document: {:?}", e);
            });
        });
    }

    Ok(())
}

async fn check_document(
    loro_server_id: String,
    loro_server_states: std::sync::Arc<LoroServerStates>,
    document_status_kv: std::sync::Arc<async_nats::jetstream::kv::Store>,
    doc_id: String,
) -> Result<(), LoroServerDocumentCheckError> {
    tracing::debug!("checking document {}", doc_id);
    loop {
        let document_status = document_status_kv
            .entry(doc_id.clone())
            .await
            .map_err(LoroServerDocumentCheckError::GetDocumentStatusError)?;
        let Some(document_status) = document_status else {
            tracing::info!(
                "Document {} not found in kv store, removing it locally",
                doc_id
            );
            loro_server_states.remove(doc_id.clone()).await;
            return Ok(());
        };
        let document_revision = document_status.revision;
        let document_status = String::from_utf8_lossy(document_status.value.as_ref());
        tracing::debug!("Document {} status: {}", doc_id, document_status);
        let split_states = document_status.split(":").collect::<Vec<_>>();
        let document_state = split_states[0];
        let (document_initialized, document_operation_id) = {
            let server_state = loro_server_states.get(doc_id.clone()).await.ok_or(
                LoroServerDocumentCheckError::DocumentNotFoundError(doc_id.clone()),
            )?;
            let server_state = server_state.read().await;
            (server_state.initialized, server_state.operation_id.clone())
        };
        let new_document_state = match document_state {
            "STARTING" => {
                let operation_id = split_states[1];
                let retry_count = split_states[2];
                let timestamp = split_states[3];
                let timestamp = super::super::time::to_timestamp(timestamp).map_err(|e| {
                    LoroServerDocumentCheckError::ParseDocumentStatusError(
                        doc_id.clone(),
                        format!("cannot parse timestamp {}: {}", timestamp, e.to_string()),
                    )
                })?;
                if operation_id != document_operation_id {
                    tracing::info!(
                        "Document {} ({}) is STARTING but not owned by the operation ({}) on this server ({}), removing it locally",
                        doc_id,
                        operation_id,
                        document_operation_id,
                        loro_server_id
                    );
                    loro_server_states.remove(doc_id.clone()).await;
                    break;
                }
                if chrono::Utc::now() - timestamp > chrono::Duration::seconds(5) {
                    tracing::info!(
                        "Document {} is STARTING but has not been updated for 5 seconds, removing it locally",
                        doc_id
                    );
                    loro_server_states.remove(doc_id.clone()).await;
                    break;
                }
                if document_initialized {
                    format!("UP:{}:{}", loro_server_id, super::super::time::from_now())
                } else {
                    format!(
                        "STARTING:{}:{}:{}",
                        document_operation_id,
                        retry_count,
                        super::super::time::from_now()
                    )
                }
            }
            "UP" => {
                let server_id = split_states[1];
                let timestamp = split_states[2];
                let timestamp = super::super::time::to_timestamp(timestamp).map_err(|e| {
                    LoroServerDocumentCheckError::ParseDocumentStatusError(
                        doc_id.clone(),
                        format!("cannot parse timestamp {}: {}", timestamp, e.to_string()),
                    )
                })?;
                if server_id != loro_server_id {
                    tracing::info!(
                        "Document {} ({}) is UP but not owned by this server ({}), removing it locally",
                        doc_id,
                        server_id,
                        loro_server_id
                    );
                    loro_server_states.remove(doc_id.clone()).await;
                    break;
                }
                if chrono::Utc::now() - timestamp > chrono::Duration::seconds(5) {
                    tracing::info!(
                        "Document {} is UP but has not been updated for 5 seconds, removing it locally",
                        doc_id
                    );
                    loro_server_states.remove(doc_id.clone()).await;
                    break;
                }
                format!("UP:{}:{}", loro_server_id, super::super::time::from_now())
            }
            _ => {
                tracing::info!(
                    "Document {} with state {} is not in STARTING or UP state, removing it locally",
                    doc_id,
                    document_state
                );
                loro_server_states.remove(doc_id.clone()).await;
                break;
            }
        };
        tracing::debug!(
            "Document {} updating from {} to {}",
            doc_id,
            document_status,
            new_document_state.clone(),
        );
        let document_update_res = document_status_kv
            .update(doc_id.clone(), new_document_state.into(), document_revision)
            .await;
        match document_update_res {
            Ok(_) => break,
            Err(e) => {
                if e.kind() == async_nats::jetstream::kv::UpdateErrorKind::WrongLastRevision {
                    continue;
                } else {
                    tracing::error!(
                        "Failed to update document status kv for {}: {:?}",
                        doc_id,
                        e
                    );
                    break;
                }
            }
        }
    }
    Ok(())
}

#[derive(Error, Debug)]
pub enum LoroServerInitServiceError {
    #[error("failed to decode table id from subject: {0}")]
    DecodeTableIDError(String),
    #[error("failed to check document")]
    CheckDocumentError(#[from] LoroServerDocumentCheckError),
    #[error("failed to init document")]
    InitDocumentError(#[from] super::wal::LoroInitDocumentError),
}

async fn handle_init(
    nc: std::sync::Arc<async_nats::Client>,
    loro_server_id: String,
    loro_server_states: std::sync::Arc<LoroServerStates>,
    document_status_kv: std::sync::Arc<async_nats::jetstream::kv::Store>,
    wal_stream: std::sync::Arc<async_nats::jetstream::stream::Stream>,
    req: &async_nats::service::Request,
) -> Result<(), LoroServerInitServiceError> {
    let subject = req.message.subject.as_ref();
    let document_id = subject.strip_prefix("loro.init.").ok_or(
        LoroServerInitServiceError::DecodeTableIDError(subject.to_string()),
    )?;
    tracing::debug!("handling init for document {}", document_id);
    let document_id = document_id.to_string();
    let operation_id = String::from_utf8_lossy(req.message.payload.as_ref());
    let loro_server_state = loro_server_states
        .get_or_create(document_id.clone(), operation_id.to_string())
        .await;
    super::wal::init_loro_document(
        document_id.clone(),
        nc.clone(),
        wal_stream.clone(),
        loro_server_state,
        loro_server_states.clone(),
    )
    .await?;
    tracing::debug!(
        "manually calling check_document after updating document {}",
        document_id.clone()
    );
    check_document(
        loro_server_id.clone(),
        loro_server_states.clone(),
        document_status_kv.clone(),
        document_id,
    )
    .await?;
    Ok(())
}
