use std::{io::BufReader, path::Path};

use anyhow::{Context, Result, bail, ensure};
use cid::Cid;
use futures_util::{StreamExt, stream};
use serde::{Deserialize, de::IgnoredAny};
use sha2::{Digest, Sha256};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;

use crate::db::{self, Entry};

const INSERT_BATCH_SIZE: usize = 20_000;

#[derive(Clone)]
struct Bundle {
    after: u64,
    count: Option<u64>,
    cid: String,
}

struct DownloadedBundle {
    bundle: Bundle,
    file: NamedTempFile,
}

#[derive(Deserialize)]
struct ListRecords {
    records: Vec<Record>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct Record {
    value: BundleRecord,
}

#[derive(Deserialize)]
struct BundleRecord {
    after: u64,
    count: Option<u64>,
    blob: Blob,
}

#[derive(Deserialize)]
struct Blob {
    #[serde(rename = "ref")]
    reference: Link,
}

#[derive(Deserialize)]
struct Link {
    #[serde(rename = "$link")]
    cid: String,
}

pub async fn run(
    path: &Path,
    repo: &str,
    pds_url: Option<&str>,
    plc_url: &str,
    download_concurrency: usize,
) -> Result<()> {
    ensure!(
        download_concurrency > 0,
        "download concurrency must be greater than zero"
    );

    let (actor, collection) = parse_repo(repo)?;
    ensure!(
        collection == "blue.cerulea.plox.bundle",
        "unsupported Plox collection {collection}"
    );

    let http = reqwest::Client::builder()
        .user_agent("zplc-server/0.1 (github.com/char/zplc-server)")
        .build()?;
    let did = if actor.starts_with("did:") {
        actor
    } else {
        resolve_handle(&http, &actor).await?
    };
    let pds_url = match pds_url {
        Some(url) => url.trim_end_matches('/').to_owned(),
        None => resolve_pds(&http, plc_url, &did).await?,
    };

    tracing::info!(%did, %pds_url, "discovering Plox bundles");
    let bundles = list_bundles(&http, &pds_url, &did).await?;
    let mut conn = db::open(path)?;
    let latest = db::latest_seq(&conn)?;
    let bundles = bundles_to_ingest(bundles, latest)?;
    if bundles.is_empty() {
        tracing::info!(latest, "no new Plox bundles");
        return Ok(());
    }

    tracing::info!(count = bundles.len(), latest, "downloading Plox bundles");
    let downloads = stream::iter(bundles.into_iter().map(|bundle| {
        let http = http.clone();
        let pds_url = pds_url.clone();
        let did = did.clone();
        async move { download_bundle(&http, &pds_url, &did, bundle).await }
    }))
    .buffered(download_concurrency);
    tokio::pin!(downloads);

    while let Some(download) = downloads.next().await {
        ingest_bundle(&mut conn, download?)?;
    }
    Ok(())
}

fn parse_repo(repo: &str) -> Result<(String, String)> {
    let repo = repo.strip_prefix("at://").unwrap_or(repo);
    let mut parts = repo.split('/');
    let actor = parts
        .next()
        .filter(|value| !value.is_empty())
        .context("missing Plox repo")?;
    let collection = parts
        .next()
        .unwrap_or("blue.cerulea.plox.bundle");
    ensure!(
        parts.next().is_none(),
        "expected an actor or collection AT URI"
    );
    Ok((actor.to_owned(), collection.to_owned()))
}

async fn resolve_handle(http: &reqwest::Client, handle: &str) -> Result<String> {
    #[derive(Deserialize)]
    struct Resolution {
        did: String,
    }

    let mut url =
        reqwest::Url::parse("https://public.api.bsky.app/xrpc/com.atproto.identity.resolveHandle")?;
    url.query_pairs_mut().append_pair("handle", handle);
    let response = http.get(url).send().await?.error_for_status()?;
    Ok(response.json::<Resolution>().await?.did)
}

async fn resolve_pds(http: &reqwest::Client, plc_url: &str, did: &str) -> Result<String> {
    #[derive(Deserialize)]
    struct DidDocument {
        service: Vec<Service>,
    }
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Service {
        id: String,
        service_endpoint: String,
    }

    let url = if did.starts_with("did:plc:") {
        format!("{}/{did}", plc_url.trim_end_matches('/'))
    } else if let Some(domain) = did.strip_prefix("did:web:") {
        format!("https://{domain}/.well-known/did.json")
    } else {
        bail!("unsupported DID method in Plox repo: {did}");
    };
    let document = http
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json::<DidDocument>()
        .await?;
    document
        .service
        .into_iter()
        .find(|service| service.id.ends_with("#atproto_pds"))
        .map(|service| service.service_endpoint.trim_end_matches('/').to_owned())
        .context("Plox repo DID has no #atproto_pds service")
}

async fn list_bundles(http: &reqwest::Client, pds_url: &str, did: &str) -> Result<Vec<Bundle>> {
    let mut cursor: Option<String> = None;
    let mut bundles = Vec::new();
    loop {
        let mut url = reqwest::Url::parse(&format!("{pds_url}/xrpc/com.atproto.repo.listRecords"))?;
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("repo", did)
                .append_pair("collection", "blue.cerulea.plox.bundle")
                .append_pair("limit", "100");
            if let Some(cursor) = &cursor {
                query.append_pair("cursor", cursor);
            }
        }
        let page = http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json::<ListRecords>()
            .await?;
        bundles.extend(page.records.into_iter().map(|record| Bundle {
            after: record.value.after,
            count: record.value.count,
            cid: record.value.blob.reference.cid,
        }));
        match page.cursor {
            Some(next) if !next.is_empty() => cursor = Some(next),
            _ => break,
        }
    }

    bundles.sort_unstable_by_key(|bundle| bundle.after);
    for pair in bundles.windows(2) {
        ensure!(
            pair[0].after != pair[1].after,
            "duplicate Plox bundle after={}",
            pair[0].after
        );
    }
    Ok(bundles)
}

fn bundles_to_ingest(bundles: Vec<Bundle>, latest: u64) -> Result<Vec<Bundle>> {
    let start = bundles.iter().position(|bundle| {
        bundle.after == latest
            || (bundle.after < latest
                && bundle
                    .count
                    .is_some_and(|count| latest < bundle.after + count))
    });

    if let Some(start) = start {
        return Ok(bundles.into_iter().skip(start).collect());
    }
    if let Some(next) = bundles.iter().find(|bundle| bundle.after > latest) {
        bail!(
            "Plox bundles have a gap after seq {latest}; next bundle starts after {}",
            next.after
        );
    }
    Ok(Vec::new())
}

async fn download_bundle(
    http: &reqwest::Client,
    pds_url: &str,
    did: &str,
    bundle: Bundle,
) -> Result<DownloadedBundle> {
    let mut url = reqwest::Url::parse(&format!("{pds_url}/xrpc/com.atproto.sync.getBlob"))?;
    url.query_pairs_mut()
        .append_pair("did", did)
        .append_pair("cid", &bundle.cid);

    tracing::info!(after = bundle.after, cid = %bundle.cid, "downloading Plox bundle");
    let mut response = http.get(url).send().await?.error_for_status()?;
    let file = NamedTempFile::new().context("creating temporary Plox bundle")?;
    let mut output = tokio::fs::File::from_std(file.reopen()?);
    let mut hash = Sha256::new();
    while let Some(chunk) = response.chunk().await? {
        hash.update(&chunk);
        output.write_all(&chunk).await?;
    }
    output.flush().await?;

    let expected: Cid = bundle.cid.parse().context("invalid Plox blob CID")?;
    ensure!(expected.codec() == 0x55, "Plox blob CID is not a raw CID");
    ensure!(
        expected.hash().code() == 0x12,
        "Plox blob CID does not use SHA-256"
    );
    ensure!(
        expected.hash().digest() == hash.finalize().as_slice(),
        "Plox blob CID mismatch"
    );

    Ok(DownloadedBundle { bundle, file })
}

fn ingest_bundle(conn: &mut rusqlite::Connection, download: DownloadedBundle) -> Result<()> {
    type BundleEntry = (u64, String, String, IgnoredAny);

    let after = download.bundle.after;
    let entries: Vec<BundleEntry> =
        serde_ipld_dagcbor::from_reader(BufReader::new(download.file.reopen()?))
            .with_context(|| format!("decoding Plox bundle after={after}"))?;
    if let Some(count) = download.bundle.count {
        ensure!(
            entries.len() as u64 == count,
            "Plox bundle after={after} declared {count} entries but contained {}",
            entries.len()
        );
    }

    let mut batch = Vec::with_capacity(INSERT_BATCH_SIZE);
    let mut latest = db::latest_seq(conn)?;
    for (expected, (seq, did, created_at, _)) in (after + 1..).zip(entries) {
        ensure!(
            seq == expected,
            "non-contiguous Plox bundle after={after}: expected {expected}, got {seq}"
        );
        if seq <= latest {
            continue;
        }
        batch.push(Entry {
            seq,
            did,
            created_at,
        });
        if batch.len() == INSERT_BATCH_SIZE {
            db::ingest(conn, &batch)?;
            latest = batch.last().unwrap().seq;
            batch.clear();
        }
    }
    if !batch.is_empty() {
        db::ingest(conn, &batch)?;
    }

    let latest = db::latest_seq(conn)?;
    tracing::info!(after, latest, "ingested Plox bundle");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_and_ingests_bundle() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let mut conn = db::open(&directory.path().join("ids.db"))?;
        let file = NamedTempFile::new()?;
        let encoded = serde_ipld_dagcbor::to_vec(&vec![
            (
                1_u64,
                "did:plc:a",
                "one",
                serde_json::json!({ "type": "create" }),
            ),
            (
                2_u64,
                "did:plc:b",
                "two",
                serde_json::json!({ "type": "create" }),
            ),
            (
                3_u64,
                "did:plc:a",
                "three",
                serde_json::json!({ "type": "plc_operation" }),
            ),
        ])?;
        std::fs::write(file.path(), encoded)?;

        ingest_bundle(
            &mut conn,
            DownloadedBundle {
                bundle: Bundle {
                    after: 0,
                    count: Some(3),
                    cid: String::new(),
                },
                file,
            },
        )?;

        assert_eq!(db::did_to_zplc(&conn, "did:plc:a")?, Some(1));
        assert_eq!(db::did_to_zplc(&conn, "did:plc:b")?, Some(2));
        assert_eq!(db::latest_seq(&conn)?, 3);
        Ok(())
    }

    #[test]
    fn resumes_inside_bundle() -> Result<()> {
        let bundles = vec![
            Bundle {
                after: 0,
                count: Some(10),
                cid: "a".into(),
            },
            Bundle {
                after: 10,
                count: Some(10),
                cid: "b".into(),
            },
        ];
        let selected = bundles_to_ingest(bundles, 5)?;
        assert_eq!(selected.len(), 2);
        assert_eq!(selected[0].after, 0);
        Ok(())
    }
}
