//! Storage provider implementations

mod local;
mod onedrive;
mod qiniu;
mod s3;
mod upyun;

use crate::uploader::chunk::ChunkInfo;
use crate::uploader::session::UploadSession;
use anyhow::Result;
use bytes::Bytes;
use cloudreve_api::Client as CrClient;
use cloudreve_api::models::explorer::PolicyType as ApiPolicyType;
use futures::Stream;
use futures::StreamExt;
use reqwest::Client as HttpClient;
use std::io;
use std::sync::Arc;
use std::time::Duration;

/// Bound diagnostic parsing even when a provider sends an enormous error page.
async fn read_error_body(response: reqwest::Response) -> String {
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(Ok(chunk)) = stream.next().await {
        let remaining = 4096usize.saturating_sub(bytes.len());
        bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
        if bytes.len() == 4096 {
            break;
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

#[derive(Debug)]
pub(crate) struct ProviderStatusError {
    provider: &'static str,
    status: u16,
    code: Option<String>,
}
impl std::fmt::Display for ProviderStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} HTTP {}", self.provider, self.status)?;
        if let Some(code) = &self.code {
            write!(f, " ({code})")?;
        }
        Ok(())
    }
}
impl std::error::Error for ProviderStatusError {}

pub(crate) fn safe_upload_diagnostic(error: &anyhow::Error) -> String {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<ProviderStatusError>())
        .map(ToString::to_string)
        .unwrap_or_else(|| "Upload request failed".to_string())
}

fn provider_error(provider: &'static str, status: u16, code: Option<&str>) -> anyhow::Error {
    let code = code
        .filter(|code| {
            code.len() <= 64
                && code
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
        })
        .map(str::to_string);
    ProviderStatusError {
        provider,
        status,
        code,
    }
    .into()
}

#[cfg(test)]
mod diagnostic_tests {
    use super::*;

    #[test]
    fn provider_diagnostics_never_include_signed_urls_or_bodies() {
        let signed = "https://storage.example/object?signature=SIGNED_SENTINEL";
        let error = provider_error("S3", 403, Some(signed));
        let diagnostic = safe_upload_diagnostic(&error);
        assert_eq!(diagnostic, "S3 HTTP 403");
        assert!(!format!("{error:?}").contains("SIGNED_SENTINEL"));
    }
}

/// Supported storage policy types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyType {
    /// Local storage (upload directly to Cloudreve)
    Local,
    /// Remote slave node
    Remote,
    /// Alibaba Cloud OSS
    Oss,
    /// Qiniu Cloud Storage
    Qiniu,
    /// Microsoft OneDrive
    OneDrive,
    /// Tencent Cloud COS
    Cos,
    /// Upyun
    Upyun,
    /// Amazon S3 (or S3-compatible)
    S3,
    /// Kingsoft Cloud KS3
    Ks3,
    /// Huawei Cloud OBS
    Obs,
}

impl PolicyType {
    /// Convert from API PolicyType
    pub fn from_api(api_type: &ApiPolicyType) -> Self {
        match api_type {
            ApiPolicyType::Local => PolicyType::Local,
            ApiPolicyType::Remote => PolicyType::Remote,
            ApiPolicyType::Oss => PolicyType::Oss,
            ApiPolicyType::Qiniu => PolicyType::Qiniu,
            ApiPolicyType::Onedrive => PolicyType::OneDrive,
            ApiPolicyType::Cos => PolicyType::Cos,
            ApiPolicyType::Upyun => PolicyType::Upyun,
            ApiPolicyType::S3 => PolicyType::S3,
            ApiPolicyType::Ks3 => PolicyType::Ks3,
            ApiPolicyType::Obs => PolicyType::Obs,
            ApiPolicyType::LoadBalance => PolicyType::Local, // Fallback
        }
    }

    /// Convert from string
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "local" => PolicyType::Local,
            "remote" => PolicyType::Remote,
            "oss" => PolicyType::Oss,
            "qiniu" => PolicyType::Qiniu,
            "onedrive" => PolicyType::OneDrive,
            "cos" => PolicyType::Cos,
            "upyun" => PolicyType::Upyun,
            "s3" => PolicyType::S3,
            "ks3" => PolicyType::Ks3,
            "obs" => PolicyType::Obs,
            _ => PolicyType::Local,
        }
    }

    /// Convert to string
    pub fn as_str(&self) -> &'static str {
        match self {
            PolicyType::Local => "local",
            PolicyType::Remote => "remote",
            PolicyType::Oss => "oss",
            PolicyType::Qiniu => "qiniu",
            PolicyType::OneDrive => "onedrive",
            PolicyType::Cos => "cos",
            PolicyType::Upyun => "upyun",
            PolicyType::S3 => "s3",
            PolicyType::Ks3 => "ks3",
            PolicyType::Obs => "obs",
        }
    }

    /// Check if this is an S3-like provider
    pub fn is_s3_like(&self) -> bool {
        matches!(
            self,
            PolicyType::Oss | PolicyType::Cos | PolicyType::S3 | PolicyType::Ks3 | PolicyType::Obs
        )
    }

    /// Check if this provider requires a callback after upload
    pub fn requires_callback(&self) -> bool {
        matches!(
            self,
            PolicyType::S3 | PolicyType::Ks3 | PolicyType::Cos | PolicyType::OneDrive
        )
    }

    /// Check if this provider uses per-chunk URLs
    pub fn uses_per_chunk_urls(&self) -> bool {
        self.is_s3_like()
    }
}

/// Upload a chunk to the appropriate provider using streaming with progress tracking
pub async fn upload_chunk_with_progress<S>(
    http_client: &HttpClient,
    cr_client: &Arc<CrClient>,
    policy_type: PolicyType,
    chunk: &ChunkInfo,
    stream: S,
    session: &UploadSession,
) -> Result<Option<String>>
where
    S: Stream<Item = Result<Bytes, io::Error>> + Send + Sync + Unpin + 'static,
{
    match policy_type {
        PolicyType::Local | PolicyType::Remote => {
            local::upload_chunk_generic(http_client, cr_client, chunk, stream, session).await
        }
        PolicyType::Oss => s3::upload_chunk_oss_generic(http_client, chunk, stream, session).await,
        PolicyType::Cos => s3::upload_chunk_cos_generic(http_client, chunk, stream, session).await,
        PolicyType::S3 | PolicyType::Ks3 => {
            s3::upload_chunk_s3_generic(http_client, chunk, stream, session).await
        }
        PolicyType::Obs => s3::upload_chunk_obs_generic(http_client, chunk, stream, session).await,
        PolicyType::OneDrive => {
            onedrive::upload_chunk_generic(http_client, chunk, stream, session).await
        }
        PolicyType::Qiniu => qiniu::upload_chunk_generic(http_client, chunk, stream, session).await,
        PolicyType::Upyun => upyun::upload_chunk_generic(http_client, chunk, stream, session).await,
    }
}

/// Complete the upload for the appropriate provider
pub async fn complete_upload(
    http_client: &HttpClient,
    cr_client: &Arc<CrClient>,
    session: &UploadSession,
) -> Result<()> {
    let policy_type = if session.is_relay() {
        PolicyType::Local
    } else {
        session.policy_type()
    };

    match policy_type {
        PolicyType::Local | PolicyType::Remote => {
            // Local/Remote uploads are completed automatically by Cloudreve
            Ok(())
        }
        PolicyType::Oss => s3::complete_upload_oss(http_client, session).await,
        PolicyType::Cos => {
            s3::complete_upload_s3like(http_client, session).await?;
            s3::callback_s3like(cr_client, session, "cos").await
        }
        PolicyType::S3 => {
            s3::complete_upload_s3like(http_client, session).await?;
            s3::callback_s3like(cr_client, session, "s3").await
        }
        PolicyType::Ks3 => {
            s3::complete_upload_s3like(http_client, session).await?;
            s3::callback_s3like(cr_client, session, "ks3").await
        }
        PolicyType::Obs => s3::complete_upload_obs(http_client, session).await,
        PolicyType::OneDrive => onedrive::complete_upload(cr_client, session).await,
        PolicyType::Qiniu => qiniu::complete_upload(http_client, session).await,
        PolicyType::Upyun => {
            // Sleep 10s for a callback
            tokio::time::sleep(Duration::from_secs(10)).await;
            Ok(())
        }
    }
}
