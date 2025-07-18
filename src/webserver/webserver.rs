use std::convert::Infallible;
use std::net::SocketAddr;
use thiserror::Error;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;

#[derive(Error, Debug)]
pub enum InitWebserverError {
    #[error("failed to create document status kv: {0}")]
    CreateDocumentStatusKvFailed(async_nats::jetstream::context::CreateKeyValueError),
    #[error("failed to bind to address: {0}")]
    BindError(std::io::Error),
    #[error("failed to accept connection: {0}")]
    AcceptError(std::io::Error),
}

pub async fn init_webserver(
    nc: std::sync::Arc<async_nats::Client>,
    js: std::sync::Arc<async_nats::jetstream::Context>,
) {
    let document_status_kv = std::sync::Arc::new(
        js.create_key_value(async_nats::jetstream::kv::Config {
            bucket: "loro-document-status".to_string(),
            description: "Loro Document Status".to_string(),
            ..Default::default()
        })
        .await
        .map_err(InitWebserverError::CreateDocumentStatusKvFailed)
        .unwrap(),
    );

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(&addr)
        .await
        .map_err(InitWebserverError::BindError)
        .unwrap();
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .map_err(InitWebserverError::AcceptError)
            .unwrap();
        let io = TokioIo::new(stream);
        let nc = nc.clone();
        let js = js.clone();
        let document_status_kv = document_status_kv.clone();
        tokio::task::spawn(async move {
            let service = service_fn(|req: Request<hyper::body::Incoming>| {
                let nc = nc.clone();
                let js = js.clone();
                let document_status_kv = document_status_kv.clone();
                async move {
                    let method = req.method().clone();
                    let req_uri = req.uri().clone();
                    let body = req.into_body();
                    let path = req_uri.path();
                    match (&method, path) {
                        (&Method::POST, path) if path.starts_with("/log/") => {
                            let document_id = path.strip_prefix("/log/").unwrap();
                            if let Err(e) = super::send_log::send_log(
                                document_id.to_string(),
                                nc.clone(),
                                js.clone(),
                                document_status_kv.clone(),
                                body,
                            )
                            .await
                            {
                                return Ok::<_, Infallible>(
                                    Response::builder()
                                        .status(500)
                                        .body(Full::new(Bytes::from(format!(
                                            "Failed to send log: {}",
                                            e
                                        ))))
                                        .unwrap(),
                                );
                            }
                            return Ok::<_, Infallible>(
                                Response::builder()
                                    .status(200)
                                    .body(Full::new(Bytes::from("{}")))
                                    .unwrap(),
                            );
                        }
                        _ => {
                            return Ok(Response::builder()
                                .status(404)
                                .body(Full::new(Bytes::from("Not Found")))
                                .unwrap());
                        }
                    }
                }
            });
            let http = http1::Builder::new().serve_connection(io, service).await;
            if let Err(e) = http {
                tracing::error!("Error serving connection: {}", e);
            }
        });
    }
}
