//! S3-like storage provider implementations
//!
//! Supports: OSS, COS, S3, KS3, OBS

use crate::uploader::chunk::{ChunkInfo, ChunkProgress};
use crate::uploader::session::UploadSession;
use anyhow::{Context, Result};
use bytes::Bytes;
use cloudreve_api::Client as CrClient;
use cloudreve_api::api::ExplorerApi;
use futures::Stream;
use reqwest::{Body, Client as HttpClient};
use std::io;
use std::sync::Arc;
use tracing::debug;

/// Upload chunk to S3/KS3 using generic stream
pub async fn upload_chunk_s3_generic<S>(
    http_client: &HttpClient,
    chunk: &ChunkInfo,
    stream: S,
    session: &UploadSession,
) -> Result<Option<String>>
where
    S: Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static,
{
    let url = session
        .upload_url_for_chunk(chunk.index)
        .with_context(|| format!("no upload URL for chunk {}", chunk.index))?;

    debug!(
        target: "uploader::s3",
        chunk = chunk.index,
        size = chunk.size,
        "Uploading chunk to S3 (streaming)"
    );

    // Create streaming body from the chunk stream
    let body = Body::wrap_stream(stream);

    let response = http_client
        .put(url)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", chunk.size)
        .body(body)
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .with_context(|| format!("failed to upload chunk {}", chunk.index))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = crate::uploader::providers::read_error_body(response).await;
        return Err(parse_s3_error(status.as_u16(), &body)
            .context(format!("chunk {} upload failed", chunk.index)));
    }

    // Extract ETag from response headers
    let etag = response
        .headers()
        .get("etag")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim_matches('"').to_string());

    Ok(etag)
}

/// Upload chunk to OSS with generic stream
pub async fn upload_chunk_oss_generic<S>(
    http_client: &HttpClient,
    chunk: &ChunkInfo,
    stream: S,
    session: &UploadSession,
) -> Result<Option<String>>
where
    S: Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static,
{
    // OSS uses the same mechanism as S3
    upload_chunk_s3_generic(http_client, chunk, stream, session).await
}

/// Upload chunk to COS with generic stream
pub async fn upload_chunk_cos_generic<S>(
    http_client: &HttpClient,
    chunk: &ChunkInfo,
    stream: S,
    session: &UploadSession,
) -> Result<Option<String>>
where
    S: Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static,
{
    // COS uses the same mechanism as S3
    upload_chunk_s3_generic(http_client, chunk, stream, session).await
}

/// Upload chunk to OBS with generic stream
pub async fn upload_chunk_obs_generic<S>(
    http_client: &HttpClient,
    chunk: &ChunkInfo,
    stream: S,
    session: &UploadSession,
) -> Result<Option<String>>
where
    S: Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static,
{
    // OBS uses the same mechanism as S3
    upload_chunk_s3_generic(http_client, chunk, stream, session).await
}

/// Complete multipart upload for OSS (uses x-oss-complete-all header)
pub async fn complete_upload_oss(http_client: &HttpClient, session: &UploadSession) -> Result<()> {
    let url = session.complete_url();

    debug!(
        target: "uploader::s3",
        "Completing OSS multipart upload"
    );

    let response = http_client
        .post(url)
        .header("Content-Type", "application/octet-stream")
        .header("Content-Length", "0")
        .header("x-oss-forbid-overwrite", "true")
        .header("x-oss-complete-all", "yes")
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("failed to complete OSS upload")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = crate::uploader::providers::read_error_body(response).await;
        return Err(parse_s3_error(status.as_u16(), &body).context("failed to complete OSS upload"));
    }

    Ok(())
}

/// Complete multipart upload for S3-like providers (S3, KS3, COS)
pub async fn complete_upload_s3like(
    http_client: &HttpClient,
    session: &UploadSession,
) -> Result<()> {
    let url = session.complete_url();
    let body = build_complete_multipart_xml(&session.chunk_progress);

    debug!(
        target: "uploader::s3",
        "Completing S3-like multipart upload"
    );

    let mut request = http_client
        .post(url)
        .header("Content-Type", "application/octet-stream")
        .body(body);

    // Add COS-specific header if needed
    if session.policy_type() == crate::uploader::providers::PolicyType::Cos {
        request = request.header("x-cos-forbid-overwrite", "true");
    }

    let response = request
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("failed to complete S3-like upload")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = crate::uploader::providers::read_error_body(response).await;
        return Err(
            parse_s3_error(status.as_u16(), &body).context("failed to complete S3-like upload")
        );
    }

    Ok(())
}

/// Complete multipart upload for OBS
pub async fn complete_upload_obs(http_client: &HttpClient, session: &UploadSession) -> Result<()> {
    let url = session.complete_url();
    let body = build_complete_multipart_xml(&session.chunk_progress);

    debug!(
        target: "uploader::s3",
        "Completing OBS multipart upload"
    );

    let response = http_client
        .post(url)
        .header("Content-Type", "application/octet-stream")
        .body(body)
        .send()
        .await
        .map_err(reqwest::Error::without_url)
        .context("failed to complete OBS upload")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = crate::uploader::providers::read_error_body(response).await;

        // OBS may return JSON or XML errors
        if body.starts_with('{') {
            #[derive(serde::Deserialize)]
            struct ObsError {
                message: String,
                code: String,
            }
            if let Ok(err) = serde_json::from_str::<ObsError>(&body) {
                return Err(crate::uploader::providers::provider_error(
                    "OBS",
                    status.as_u16(),
                    Some(&err.code),
                ));
            }
        }

        return Err(parse_s3_error(status.as_u16(), &body).context("failed to complete OBS upload"));
    }

    Ok(())
}

/// Send callback to Cloudreve after S3-like upload completion
pub async fn callback_s3like(
    cr_client: &Arc<CrClient>,
    session: &UploadSession,
    policy_type: &str,
) -> Result<()> {
    debug!(
        target: "uploader::s3",
        session_id = session.session_id(),
        policy_type,
        "Sending upload callback to Cloudreve"
    );

    cr_client
        .complete_s3_upload(policy_type, session.session_id(), session.callback_secret())
        .await
        .context("upload callback failed")?;

    Ok(())
}

/// Build XML body for CompleteMultipartUpload
fn build_complete_multipart_xml(chunks: &[ChunkProgress]) -> String {
    let mut xml = String::from("<CompleteMultipartUpload>");

    for chunk in chunks {
        if let Some(ref etag) = chunk.etag {
            xml.push_str("<Part>");
            xml.push_str(&format!("<PartNumber>{}</PartNumber>", chunk.index + 1));
            xml.push_str(&format!("<ETag>{}</ETag>", etag));
            xml.push_str("</Part>");
        }
    }

    xml.push_str("</CompleteMultipartUpload>");
    xml
}

/// Parse a bounded provider response without retaining its raw contents.
fn parse_s3_error(status: u16, body: &str) -> anyhow::Error {
    // Try to parse XML error
    if let Some(code) = extract_xml_element(body, "Code") {
        return crate::uploader::providers::provider_error("S3", status, Some(&code));
    }
    crate::uploader::providers::provider_error("S3", status, None)
}

/// Simple XML element extraction (for error parsing)
fn extract_xml_element(xml: &str, tag: &str) -> Option<String> {
    let open_tag = format!("<{}>", tag);
    let close_tag = format!("</{}>", tag);

    let start = xml.find(&open_tag)? + open_tag.len();
    let end = xml[start..].find(&close_tag)?;

    Some(xml[start..start + end].to_string())
}
