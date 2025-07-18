use futures::StreamExt;
use thiserror::Error;

#[derive(Error, Debug)]
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
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
) -> Result<(), InitDocumentEndpointsError> {
    tracing::info!("Initializing document endpoints for {}", document_id);
    let mut ping_subscriber = subscribe_endpoint(&nc, "ping", &document_id).await?;
    let mut sync_down_subscriber = subscribe_endpoint(&nc, "sync_down", &document_id).await?;
    let mut version_vector_subscriber =
        subscribe_endpoint(&nc, "version_vector", &document_id).await?;

    loop {
        if !server_states.exists(document_id.clone()).await {
            return Ok(());
        }

        let endpoint = tokio::select! {
            Some(msg) = ping_subscriber.next() => (msg, Endpoint::Ping),
            Some(msg) = sync_down_subscriber.next() => (msg, Endpoint::SyncDown),
            Some(msg) = version_vector_subscriber.next() => (msg, Endpoint::VersionVector),
        };

        let new_document_id = document_id.clone();
        let new_nc = nc.clone();
        let new_server_states = server_states.clone();
        tokio::spawn(async move {
            process_message(
                endpoint.1,
                endpoint.0,
                new_document_id,
                new_nc,
                new_server_states,
            )
            .await
        });
    }
}

enum Endpoint {
    Ping,
    SyncDown,
    VersionVector,
}

async fn process_message(
    endpoint: Endpoint,
    msg: super::core_transport::CoreMessage,
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
) {
    match endpoint {
        Endpoint::Ping => process_ping(msg, document_id, nc, server_states).await,
        Endpoint::SyncDown => process_sync_down(msg, document_id, nc, server_states).await,
        Endpoint::VersionVector => {
            process_version_vector(msg, document_id, nc, server_states).await
        }
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
    let exists = server_states.exists(document_id.clone()).await;
    send_ok_string(&msg.responder, &nc, if exists { "pong" } else { "not_up" }).await;
}

async fn process_sync_down(
    msg: super::core_transport::CoreMessage,
    document_id: String,
    nc: std::sync::Arc<async_nats::Client>,
    server_states: std::sync::Arc<super::initialize::LoroServerStates>,
) {
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
    let Some(doc) = ensure_document(&msg, document_id.clone(), &nc, server_states).await else {
        return;
    };
    let responder = msg.responder;

    let vv = doc.oplog_vv().encode();
    send_ok(&responder, &nc, &vv).await;
}
