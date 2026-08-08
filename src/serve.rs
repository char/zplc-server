use std::{net::SocketAddr, path::Path};

use anyhow::Result;
use axum::{
    Router,
    extract::{Path as AxumPath, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use rusqlite::Connection;

use crate::db;

fn text(status: StatusCode, body: impl Into<String>) -> Response {
    (status, [(header::CONTENT_TYPE, "text/plain")], body.into()).into_response()
}

fn internal_error(error: impl Into<anyhow::Error>) -> Response {
    let error = error.into().context("serving request");
    tracing::error!(%error);
    text(StatusCode::INTERNAL_SERVER_ERROR, "Internal Server Error")
}

pub async fn run(path: &Path, listen: SocketAddr) -> Result<()> {
    let pool_size = tokio::runtime::Handle::current().metrics().num_workers();
    let state = db::open_pool(path, pool_size).await?;
    let app = Router::new()
        .route("/", get(index))
        .route("/latest", get(latest))
        .route("/{value}", get(resolve))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, pool_size, "serving zplc");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn index() -> Response {
    text(
        StatusCode::OK,
        [
            "github.com/char/zplc-server",
            "GET /<zplc> - resolve a zplc id to a did:plc: identifier",
            "GET /did:plc:<id> - resolve a did:plc: identifier to a zplc id",
        ]
        .join("\n"),
    )
}

async fn query<T>(
    pool: &db::Pool,
    operation: impl FnOnce(&Connection) -> Result<T> + Send + 'static,
) -> Result<T>
where
    T: Send + 'static,
{
    let conn = pool.get().await?;
    conn.interact(move |conn| operation(conn))
        .await
        .map_err(|error| anyhow::anyhow!("database interaction failed: {error}"))?
}

async fn latest(State(db): State<db::Pool>) -> Response {
    match query(&db, db::latest_created_at).await {
        Ok(value) => text(StatusCode::OK, value.unwrap_or_default()),
        Err(error) => internal_error(error),
    }
}

async fn resolve(State(db): State<db::Pool>, AxumPath(value): AxumPath<String>) -> Response {
    if value.starts_with("did:plc:") {
        match query(&db, move |conn| db::did_to_zplc(conn, &value)).await {
            Ok(Some(id)) => text(StatusCode::OK, id.to_string()),
            Ok(None) => text(StatusCode::NOT_FOUND, "zplc not found"),
            Err(error) => internal_error(error),
        }
    } else if value.bytes().all(|byte| byte.is_ascii_digit()) && !value.is_empty() {
        match value.parse::<i64>() {
            Ok(id) => match query(&db, move |conn| db::zplc_to_did(conn, id)).await {
                Ok(Some(did)) => text(StatusCode::OK, did),
                Ok(None) => text(StatusCode::NOT_FOUND, "did not found"),
                Err(error) => internal_error(error),
            },
            Err(_) => text(StatusCode::NOT_FOUND, "did not found"),
        }
    } else {
        text(StatusCode::NOT_FOUND, "Not Found")
    }
}
