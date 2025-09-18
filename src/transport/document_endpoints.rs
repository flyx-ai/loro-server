use futures::StreamExt;

#[derive(thiserror::Error, Debug)]
pub enum InitDocumentEndpointsError {
    #[error("failed to subscribe to endpoint subject: {0}")]
    SubscribeSubjectError(async_nats::SubscribeError),
}

async fn subscribe_endpoint(
    nc: &std::sync::Arc<async_nats::Client>,
    endpoint_name: &str,
    document_id: &String,
) -> Result<super::core_transport::CoreSubscriber, InitDocumentEndpointsError> {
    super::core_transport::CoreSubscriber::subscribe(
        nc,
        format!("loro.doc.{}.{}", endpoint_name, document_id),
    )
    .await
    .map_err(InitDocumentEndpointsError::SubscribeSubjectError)
}

pub async fn init_document_endpoints(
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    js: std::sync::Arc<async_nats::jetstream::Context>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
    wal_stream: std::sync::Arc<async_nats::jetstream::stream::Stream>,
    document_seq_kv: std::sync::Arc<async_nats::jetstream::kv::Store>,
) -> Result<(), InitDocumentEndpointsError> {
    tracing::info!("Initializing document endpoints for {}", document_id);
    let mut ping_subscriber = subscribe_endpoint(&nc, "ping", &document_id).await?;
    let mut sync_down_subscriber = subscribe_endpoint(&nc, "sync_down", &document_id).await?;
    let mut version_vector_subscriber =
        subscribe_endpoint(&nc, "version_vector", &document_id).await?;
    let mut patch_subscriber = subscribe_endpoint(&nc, "patch", &document_id).await?;
    let mut get_subscriber = subscribe_endpoint(&nc, "get", &document_id).await?;
    let mut purge_subscriber = subscribe_endpoint(&nc, "purge", &document_id).await?;
    let mut fork_subscriber = subscribe_endpoint(&nc, "fork", &document_id).await?;

    loop {
        if !server_states.exists(document_id.clone()).await {
            return Ok(());
        }

        let endpoint = tokio::select! {
            Some(msg) = ping_subscriber.next() => (msg, Endpoint::Ping),
            Some(msg) = sync_down_subscriber.next() => (msg, Endpoint::SyncDown),
            Some(msg) = version_vector_subscriber.next() => (msg, Endpoint::VersionVector),
            Some(msg) = patch_subscriber.next() => (msg, Endpoint::Patch),
            Some(msg) = get_subscriber.next() => (msg, Endpoint::Get),
            Some(msg) = purge_subscriber.next() => (msg, Endpoint::Purge),
            Some(msg) = fork_subscriber.next() => (msg, Endpoint::Fork),
        };

        let new_document_id = document_id.clone();
        let new_nc = nc.clone();
        let new_js = js.clone();
        let new_server_states = server_states.clone();
        let new_wal_stream = wal_stream.clone();
        let new_document_seq_kv = document_seq_kv.clone();
        tokio::spawn(async move {
            process_message(
                endpoint.1,
                endpoint.0,
                new_document_id,
                new_nc,
                new_js,
                new_server_states,
                new_wal_stream,
                new_document_seq_kv,
            )
            .await
        });
    }
}

enum Endpoint {
    Ping,
    SyncDown,
    VersionVector,
    Patch,
    Get,
    Purge,
    Fork,
}

async fn process_message(
    endpoint: Endpoint,
    msg: super::core_transport::CoreMessage,
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    js: std::sync::Arc<async_nats::jetstream::Context>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
    wal_stream: std::sync::Arc<async_nats::jetstream::stream::Stream>,
    document_seq_kv: std::sync::Arc<async_nats::jetstream::kv::Store>,
) {
    match endpoint {
        Endpoint::Ping => process_ping(msg, document_id, nc, server_states).await,
        Endpoint::SyncDown => process_sync_down(msg, document_id, nc, server_states).await,
        Endpoint::VersionVector => {
            process_version_vector(msg, document_id, nc, server_states).await
        }
        Endpoint::Patch => process_patch(msg, document_id, nc, js, server_states).await,
        Endpoint::Get => process_get(msg, document_id, nc, server_states).await,
        Endpoint::Purge => {
            process_purge(
                msg,
                document_id,
                nc,
                server_states,
                wal_stream,
                document_seq_kv,
            )
            .await
        }
        Endpoint::Fork => process_fork(msg, document_id, nc, js, server_states).await,
    }
}

async fn ensure_document(
    msg: &super::core_transport::CoreMessage,
    document_id: String,
    nc: &std::sync::Arc<async_nats::Client>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
) -> Option<std::sync::Arc<loro::LoroDoc>> {
    let state = server_states.get(document_id.clone()).await;
    if let Some(state) = state {
        Some(state.read().await.document.clone())
    } else {
        let resp_msg = format!(
            "document {} does not exist on this server anymore",
            document_id
        );
        send_error(&msg.responder, nc, resp_msg.as_str()).await;
        None
    }
}

async fn send_error(
    responder: &super::core_transport::CoreMessageResponder,
    nc: &std::sync::Arc<async_nats::Client>,
    error_message: &str,
) {
    let resp = responder
        .send_response_string(
            &nc,
            serde_json::json!({"message": error_message})
                .to_string()
                .as_str(),
            500,
        )
        .await;
    if let Err(e) = resp {
        tracing::error!("Failed to publish error response: {}", e);
    }
}

async fn send_ok(
    responder: &super::core_transport::CoreMessageResponder,
    nc: &std::sync::Arc<async_nats::Client>,
    message: &[u8],
) {
    let resp = responder.send_response(&nc, message, 200).await;
    if let Err(e) = resp {
        tracing::error!("Failed to publish OK response: {}", e);
    }
}

async fn send_ok_string(
    responder: &super::core_transport::CoreMessageResponder,
    nc: &std::sync::Arc<async_nats::Client>,
    message: &str,
) {
    let resp = responder.send_response_string(&nc, message, 200).await;
    if let Err(e) = resp {
        tracing::error!("Failed to publish OK response: {}", e);
    }
}

async fn process_ping(
    msg: super::core_transport::CoreMessage,
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
) {
    tracing::info!("Processing ping for document {}", document_id);
    let exists = server_states.exists(document_id.clone()).await;
    send_ok_string(&msg.responder, &nc, if exists { "pong" } else { "not_up" }).await;
}

async fn process_sync_down(
    msg: super::core_transport::CoreMessage,
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
) {
    tracing::info!("Processing sync_down for document {}", document_id);
    let Some(doc) = ensure_document(&msg, document_id.clone(), &nc, server_states).await else {
        return;
    };
    let responder = msg.responder;

    let payload = msg.payload;
    let vv = loro::VersionVector::decode(payload.as_slice());
    let vv = match vv {
        Ok(vv) => vv,
        Err(e) => {
            return send_error(
                &responder,
                &nc,
                &format!("Failed to decode version vector: {}", e),
            )
            .await;
        }
    };
    let cmp_res = doc.oplog_vv().partial_cmp(&vv);
    if let Some(std::cmp::Ordering::Less) = cmp_res {
        let resp = responder.send_response(&nc, &[], 200).await;
        if let Err(e) = resp {
            tracing::error!(
                "Failed to publish empty sync_down response for document {}: {}",
                document_id,
                e
            );
        }
        return;
    }
    let diff = doc.export(loro::ExportMode::updates(&vv));
    let diff = match diff {
        Ok(diff) => diff,
        Err(e) => {
            return send_error(
                &responder,
                &nc,
                &format!("Failed to export document: {}", e),
            )
            .await;
        }
    };
    let resp = responder.send_response(&nc, &diff, 200).await;
    if let Err(e) = resp {
        tracing::error!(
            "Failed to publish sync_down response for document {}: {}",
            document_id,
            e
        );
    }
}

async fn process_version_vector(
    msg: super::core_transport::CoreMessage,
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
) {
    tracing::info!("Processing version_vector for document {}", document_id);
    let Some(doc) = ensure_document(&msg, document_id.clone(), &nc, server_states).await else {
        return;
    };
    let responder = msg.responder;

    let vv = doc.oplog_vv().encode();
    send_ok(&responder, &nc, &vv).await;
}

async fn process_patch(
    msg: super::core_transport::CoreMessage,
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    js: std::sync::Arc<async_nats::jetstream::Context>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
) {
    tracing::info!("Processing patch for document {}", document_id);
    let Some(doc) = ensure_document(&msg, document_id.clone(), &nc, server_states).await else {
        return;
    };
    let responder = msg.responder;

    let prev_vv = doc.oplog_vv().clone();

    let patch: Result<super::json_patch::Patch, serde_json::Error> =
        serde_json::from_slice(&msg.payload);
    let patch = match patch {
        Ok(patch) => patch,
        Err(e) => {
            send_error(&responder, &nc, &format!("failed to decode patch: {}", e)).await;
            return;
        }
    };

    // tracing::info!("Applying patch {:?} to document {}", patch, document_id);

    let result = super::json_patch::patch_loro_document(doc.clone(), patch);
    match result {
        Ok(_) => {
            let res = doc.export(loro::ExportMode::updates(&prev_vv));
            match res {
                Ok(update) => {
                    let doc_id_clone = document_id.clone();
                    let nc_clone = nc.clone();
                    let js_clone = js.clone();
                    let cloned_update = update.clone();
                    tokio::spawn(async move {
                        let res = super::wal::send_log(
                            doc_id_clone.clone(),
                            nc_clone,
                            js_clone,
                            &cloned_update,
                        )
                        .await;
                        if let Err(e) = res {
                            tracing::error!(
                                "Failed to send WAL log for document {}: {}",
                                doc_id_clone,
                                e
                            );
                        }
                    });
                    let doc_id_clone = document_id.clone();
                    let nc_clone = nc.clone();
                    tokio::spawn(async move {
                        let res = super::core_transport::send_message(
                            &nc_clone,
                            format!("crdt.{}.update", doc_id_clone.clone()),
                            nanoid::nanoid!(),
                            update.as_slice(),
                            None,
                        )
                        .await;
                        if let Err(e) = res {
                            tracing::error!(
                                "Failed to publish CRDT update for document {}: {}",
                                doc_id_clone,
                                e
                            );
                        }
                    });
                }
                Err(e) => {
                    send_error(
                        &responder,
                        &nc,
                        &format!("failed to export document after patch: {}", e),
                    )
                    .await;
                    return;
                }
            }

            send_ok(
                &responder,
                &nc,
                format!("{{\"status\": \"ok\"}}").as_bytes(),
            )
            .await;
        }
        Err(e) => {
            send_error(&responder, &nc, &format!("failed to apply patch: {}", e)).await;
        }
    }
}

async fn process_get(
    msg: super::core_transport::CoreMessage,
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
) {
    tracing::info!("Processing get for document {}", document_id);
    let Some(doc) = ensure_document(&msg, document_id.clone(), &nc, server_states).await else {
        return;
    };
    let responder = msg.responder;

    let paths: Result<Vec<String>, serde_json::Error> = serde_json::from_slice(&msg.payload);
    let paths = match paths {
        Ok(paths) => paths,
        Err(e) => {
            send_error(&responder, &nc, &format!("failed to decode paths: {}", e)).await;
            return;
        }
    };

    let get_results = paths
        .iter()
        .map(|path| {
            let res = doc.jsonpath(path);
            match res {
                Ok(value) => {
                    let values: Result<
                        Vec<super::serde::CRDTValue>,
                        super::serde::LoroToSerdeError,
                    > = value
                        .into_iter()
                        .map(|v| super::serde::loro_to_serde(v))
                        .collect();
                    let values = values.map(super::serde::CRDTValue::Array);
                    match values {
                        Ok(values) => values,
                        Err(e) => {
                            super::serde::CRDTValue::Map(std::collections::BTreeMap::from([(
                                String::from("🦜error"),
                                super::serde::CRDTValue::Value(serde_json::Value::String(
                                    e.to_string(),
                                )),
                            )]))
                        }
                    }
                }
                Err(e) => super::serde::CRDTValue::Map(std::collections::BTreeMap::from([(
                    String::from("🦜error"),
                    super::serde::CRDTValue::Value(serde_json::Value::String(e.to_string())),
                )])),
            }
        })
        .collect::<Vec<_>>();

    let res = serde_json::to_vec(&get_results);
    match res {
        Ok(res) => send_ok(&responder, &nc, &res).await,
        Err(e) => send_error(&responder, &nc, &format!("failed to encode results: {}", e)).await,
    };
}

async fn process_purge(
    msg: super::core_transport::CoreMessage,
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
    wal_stream: std::sync::Arc<async_nats::jetstream::stream::Stream>,
    document_seq_kv: std::sync::Arc<async_nats::jetstream::kv::Store>,
) {
    tracing::info!("Processing purge for document {}", document_id);
    let responder = msg.responder;

    tracing::info!("Purging document {}", document_id);

    server_states.clone().remove(document_id.clone()).await;

    document_seq_kv
        .delete(format!("loro.doc.{}", document_id))
        .await
        .map_err(|e| {
            tracing::error!("Failed to delete document seq from KV: {}", e);
            e
        })
        .ok();

    // TODO: purge snapshots when implemented
    let res = wal_stream
        .purge()
        .filter(format!("loro.wal.{}", document_id))
        .await;
    if let Err(e) = res {
        tracing::error!("Failed to purge WAL for document {}: {}", document_id, e);
        send_error(&responder, &nc, "failed to purge WAL").await;
        return;
    }

    tracing::info!("Purged WAL for document {}", document_id);

    send_ok_string(&responder, &nc, "{{\"status\": \"ok\"}}").await;
}

async fn process_fork(
    msg: super::core_transport::CoreMessage,
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    js: std::sync::Arc<async_nats::jetstream::Context>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
) {
    tracing::info!("Processing fork for document {}", document_id);
    let Some(doc) = ensure_document(&msg, document_id.clone(), &nc, server_states).await else {
        return;
    };
    let responder = msg.responder;

    let new_document_id = String::from_utf8_lossy(msg.payload.as_slice());

    if new_document_id.len() == 0 {
        send_error(&responder, &nc, "new document ID cannot be empty").await;
        return;
    }

    let new_doc = doc.fork();
    let snapshot = new_doc.export(loro::ExportMode::all_updates());

    match snapshot {
        Ok(snapshot) => {
            let res =
                super::wal::send_log(new_document_id.to_string(), nc.clone(), js, &snapshot).await;
            if let Err(e) = res {
                tracing::error!(
                    "Failed to send WAL log for document {}: {}",
                    new_document_id.to_string(),
                    e
                );
            }

            send_ok(
                &responder,
                &nc,
                format!("{{\"status\": \"ok\"}}").as_bytes(),
            )
            .await;
        }
        Err(e) => {
            send_error(&responder, &nc, &format!("failed to fork: {}", e)).await;
        }
    }
}
