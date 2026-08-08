use std::{path::Path, time::Duration};

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde::Deserialize;

use crate::db::{self, Entry};

const REQUEST_INTERVAL: Duration = Duration::from_millis(750);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExportEntry {
    seq: u64,
    did: String,
    created_at: String,
}

pub async fn run(path: &Path, base_url: &str, once: bool, poll_interval: u64) -> Result<()> {
    let client = reqwest::Client::builder()
        .user_agent("zplc-server/0.1 (github.com/char/zplc-server)")
        .timeout(Duration::from_secs(30))
        .build()?;
    let mut conn = db::open(path)?;
    let base_url = base_url.trim_end_matches('/');
    let mut first_request = true;

    loop {
        if !first_request {
            tokio::time::sleep(REQUEST_INTERVAL).await;
        }
        first_request = false;

        let after = db::latest_seq(&conn)?;
        let entries = fetch_page(&client, base_url, after).await?;
        if entries.is_empty() {
            if once {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(poll_interval)).await;
            continue;
        }

        let count = entries.len();
        let entries = entries
            .into_iter()
            .filter(|entry| entry.seq > after)
            .map(|entry| Entry {
                seq: entry.seq,
                did: entry.did,
                created_at: entry.created_at,
            })
            .collect::<Vec<_>>();
        db::ingest(&mut conn, &entries)?;

        let latest = db::latest_seq(&conn)?;
        tracing::info!(count = entries.len(), latest, "ingested PLC export page");
        if count < 1000 {
            if once {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_secs(poll_interval)).await;
        }
    }
}

async fn fetch_page(
    client: &reqwest::Client,
    base_url: &str,
    after: u64,
) -> Result<Vec<ExportEntry>> {
    let mut url = reqwest::Url::parse(&format!("{base_url}/export"))?;
    url.query_pairs_mut()
        .append_pair("after", &after.to_string())
        .append_pair("count", &1000.to_string());

    let mut backoff = Duration::from_secs(1);
    loop {
        match client.get(url.clone()).send().await {
            Ok(response) if response.status().is_success() => {
                let body = response.text().await.context("reading PLC export")?;
                return body
                    .lines()
                    .filter(|line| !line.is_empty())
                    .map(|line| serde_json::from_str(line).context("parsing PLC export entry"))
                    .collect();
            }
            Ok(response)
                if response.status() == StatusCode::TOO_MANY_REQUESTS
                    || response.status().is_server_error() =>
            {
                let status = response.status();
                let retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse().ok())
                    .map(Duration::from_secs)
                    .unwrap_or(backoff);
                tracing::warn!(%status, ?retry_after, "PLC export request failed; retrying");
                tokio::time::sleep(retry_after).await;
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                bail!("PLC export returned {status}: {body}");
            }
            Err(error) => {
                tracing::warn!(%error, ?backoff, "PLC export request failed; retrying");
                tokio::time::sleep(backoff).await;
            }
        }
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}
