use super::providers::PolicyType;
use super::session::UploadSession;
use anyhow::{Context, Result};
use std::{
    collections::HashSet,
    fmt,
    sync::{Arc, RwLock},
};
use url::Url;

#[derive(Debug)]
pub struct StorageConsentRequired {
    pub origin: String,
}

impl fmt::Display for StorageConsentRequired {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Storage HTTP consent required for {}",
            self.origin
        )
    }
}
impl std::error::Error for StorageConsentRequired {}

pub type StorageApprovals = Arc<RwLock<HashSet<String>>>;

pub fn canonical_http_origin(raw: &str) -> Result<String> {
    let url = Url::parse(raw).context("Invalid storage origin")?;
    if url.scheme() != "http"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        anyhow::bail!("Expected a plain HTTP origin");
    }
    Ok(url.origin().ascii_serialization())
}

pub fn validate_provider_url(raw: &str, _approvals: &StorageApprovals) -> Result<()> {
    let url = Url::parse(raw).context("Invalid provider URL")?;
    if url.host_str().is_none() || !url.username().is_empty() || url.password().is_some() {
        anyhow::bail!("Provider URL has no host or contains credentials");
    }
    if url.scheme() == "https" {
        Ok(())
    } else {
        anyhow::bail!("Provider URL must use HTTPS")
    }
}

pub fn validate_session(session: &UploadSession, approvals: &StorageApprovals) -> Result<()> {
    if let Some(urls) = &session.credential().upload_urls {
        for url in urls {
            if !url.is_empty() && !url.starts_with('/') {
                validate_provider_url(url, approvals)?;
            }
        }
    }
    if !session.is_relay()
        && matches!(
            session.policy_type(),
            PolicyType::Oss
                | PolicyType::Qiniu
                | PolicyType::Cos
                | PolicyType::S3
                | PolicyType::Ks3
                | PolicyType::Obs
        )
    {
        let complete = session.complete_url();
        if !complete.is_empty() {
            validate_provider_url(complete, approvals)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_origin_and_credentials() {
        let approvals = Arc::new(RwLock::new(HashSet::from([
            "http://storage.example:8080".to_string()
        ])));
        assert!(
            validate_provider_url(
                "http://storage.example:8080/object?signature=secret",
                &approvals
            )
            .is_err()
        );
        assert!(validate_provider_url("http://storage.example:8081/object", &approvals).is_err());
        assert!(
            validate_provider_url(
                "http://user:password@storage.example:8080/object",
                &approvals
            )
            .is_err()
        );
        assert!(validate_provider_url("ftp://storage.example/object", &approvals).is_err());
    }
}
