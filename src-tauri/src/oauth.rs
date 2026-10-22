//! OAuth state and token exchange stay in Rust. The shared registration secret
//! remains extractable from a desktop binary; PKCE protects the authorization code.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::sync::Mutex;
use url::Url;
use uuid::Uuid;

use crate::{
    commands::{add_drive_with_tokens, AddDriveArgs},
    AppStateHandle,
};

const CLIENT_ID: &str = "393a1839-f52e-498e-9972-e77cc2241eee";
// Compatibility credential required by Cloudreve's current token endpoint.
// A native-app bundle cannot keep this value confidential.
const CLIENT_SECRET: &str = "8GaQIu3lOSdqYoDHi9cR8IZ4pvuMH8ya";
const SCOPES: &str =
    "profile email openid offline_access UserInfo.Write Workflow.Write Files.Write Shares.Write";
const CALLBACK_PATH: &str = "/callback/desktop";
const SESSION_TTL: Duration = Duration::from_secs(600);

#[derive(Default)]
pub struct OAuthManager {
    sessions: Mutex<HashMap<String, OAuthSession>>,
}

struct OAuthSession {
    site_url: String,
    state: String,
    verifier: String,
    started: Instant,
    callback: Option<OAuthCallback>,
}

struct OAuthCallback {
    code: String,
    name: String,
    path: String,
    user_id: String,
}

#[derive(Serialize)]
pub struct BeginOAuthResult {
    session_id: String,
    authorize_url: String,
    site_version: String,
}

#[derive(Serialize, Clone)]
struct CallbackNotice {
    session_id: String,
    name: String,
}

#[derive(Deserialize)]
struct PingResponse {
    code: i32,
    data: String,
    msg: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    refresh_token: String,
    expires_in: u64,
    refresh_token_expires_in: u64,
}

fn normalise_site(input: &str) -> Result<Url, String> {
    let mut url = Url::parse(input.trim()).map_err(|_| "Invalid site URL".to_string())?;
    if url.host_str().is_none() || url.username() != "" || url.password().is_some() {
        return Err("Site URL must have a host and no embedded credentials".into());
    }
    if url.scheme() != "https" {
        return Err("HTTPS is required".into());
    }
    url.set_path("/");
    url.set_query(None);
    url.set_fragment(None);
    Ok(url)
}

fn http_client() -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(30))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| "Failed to initialize site connection".into())
}

async fn small_json<T: serde::de::DeserializeOwned>(
    mut response: reqwest::Response,
) -> Result<T, String> {
    const LIMIT: usize = 1024 * 1024;
    if response
        .content_length()
        .is_some_and(|size| size > LIMIT as u64)
    {
        return Err("Authorization response is too large".into());
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| "Cannot read authorization response")?
    {
        if chunk.len() > LIMIT.saturating_sub(body.len()) {
            return Err("Authorization response is too large".into());
        }
        body.extend_from_slice(&chunk);
    }
    serde_json::from_slice(&body).map_err(|_| "Invalid authorization response".into())
}

fn version_supported(version: &str) -> bool {
    let parse = |s: &str| -> Option<Vec<u32>> {
        s.trim_end_matches("-pro")
            .split('.')
            .map(|part| part.parse().ok())
            .collect()
    };
    parse(version).is_some_and(|parts| {
        let mut parts = parts;
        parts.resize(3, 0);
        parts.as_slice() >= [4, 12, 0].as_slice()
    })
}

#[tauri::command]
pub async fn begin_oauth(
    manager: State<'_, OAuthManager>,
    site_url: String,
) -> Result<BeginOAuthResult, String> {
    let site = normalise_site(&site_url)?;
    let client = http_client()?;
    let ping_url = site
        .join("/api/v4/site/ping")
        .map_err(|_| "Invalid site URL")?;
    let response = client
        .get(ping_url)
        .send()
        .await
        .map_err(|_| "Cannot reach Cloudreve site")?;
    if !response.status().is_success() {
        return Err(format!("Site ping failed: HTTP {}", response.status()));
    }
    let ping: PingResponse = small_json(response).await?;
    if ping.code != 0 {
        return Err(format!("Cloudreve ping failed: {}", ping.msg));
    }
    if !version_supported(&ping.data) {
        return Err(format!(
            "Cloudreve 4.12.0 or later is required (found {})",
            ping.data
        ));
    }

    let session_id = Uuid::new_v4().to_string();
    let state = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let verifier = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    let mut auth = site
        .join("/session/authorize")
        .map_err(|_| "Invalid authorization URL")?;
    auth.query_pairs_mut()
        .append_pair("response_type", "code")
        .append_pair("client_id", CLIENT_ID)
        .append_pair("scope", SCOPES)
        .append_pair("redirect_uri", CALLBACK_PATH)
        .append_pair("code_challenge", &challenge)
        .append_pair("code_challenge_method", "S256")
        .append_pair("state", &state);

    let mut sessions = manager.sessions.lock().await;
    sessions.retain(|_, session| session.started.elapsed() < SESSION_TTL);
    sessions.insert(
        session_id.clone(),
        OAuthSession {
            site_url: site.origin().ascii_serialization(),
            state,
            verifier,
            started: Instant::now(),
            callback: None,
        },
    );
    Ok(BeginOAuthResult {
        session_id,
        authorize_url: auth.to_string(),
        site_version: ping.data,
    })
}

pub async fn handle_callback(app: AppHandle, raw: String) {
    let Some(manager) = app.try_state::<OAuthManager>() else {
        return;
    };
    let mut sessions = manager.sessions.lock().await;
    let Some(notice) = accept_callback(&mut sessions, &raw) else {
        return;
    };
    drop(sessions);
    let _ = app.emit("oauth-callback", notice);
}

fn accept_callback(
    sessions: &mut HashMap<String, OAuthSession>,
    raw: &str,
) -> Option<CallbackNotice> {
    if raw.len() > 8192 {
        return None;
    }
    let url = Url::parse(raw).ok()?;
    if url.scheme() != "cloudreve"
        || url.host_str() != Some("callback")
        || url.path() != "/desktop"
        || url.username() != ""
        || url.password().is_some()
        || url.port().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    let unique = |key: &str| {
        let mut values = url.query_pairs().filter(|(name, _)| name == key);
        let value = values.next()?.1.into_owned();
        if values.next().is_some() {
            None
        } else {
            Some(value)
        }
    };
    let (Some(state), Some(code)) = (unique("state"), unique("code")) else {
        return None;
    };
    if state.is_empty() || code.is_empty() {
        return None;
    }
    let Some((session_id, session)) = sessions
        .iter_mut()
        .find(|(_, session)| session.state == state)
    else {
        return None;
    };
    if session.started.elapsed() >= SESSION_TTL || session.callback.is_some() {
        return None;
    }
    let notice = CallbackNotice {
        session_id: session_id.clone(),
        name: unique("name").unwrap_or_default(),
    };
    session.callback = Some(OAuthCallback {
        code,
        name: notice.name.clone(),
        path: unique("path").unwrap_or_default(),
        user_id: unique("user_id").unwrap_or_default(),
    });
    Some(notice)
}

#[tauri::command]
pub async fn finish_oauth(
    manager: State<'_, OAuthManager>,
    state: State<'_, AppStateHandle>,
    session_id: String,
    drive_name: String,
    local_path: String,
    drive_id: Option<String>,
) -> Result<String, String> {
    let session = manager
        .sessions
        .lock()
        .await
        .remove(&session_id)
        .ok_or_else(|| "Authorization session expired".to_string())?;
    if session.started.elapsed() >= SESSION_TTL {
        return Err("Authorization session expired".into());
    }
    let callback = session
        .callback
        .ok_or_else(|| "Authorization callback has not arrived".to_string())?;
    let base = Url::parse(&session.site_url).map_err(|_| "Invalid site URL")?;
    let token_url = base
        .join("/api/v4/session/oauth/token")
        .map_err(|_| "Invalid token URL")?;
    let client = http_client()?;
    let response = client
        .post(token_url)
        .form(&[
            ("grant_type", "authorization_code"),
            ("client_id", CLIENT_ID),
            ("client_secret", CLIENT_SECRET),
            ("code", callback.code.as_str()),
            ("redirect_uri", CALLBACK_PATH),
            ("code_verifier", session.verifier.as_str()),
        ])
        .send()
        .await
        .map_err(|_| "Token exchange failed")?;
    if !response.status().is_success() {
        return Err(format!("Token exchange failed: HTTP {}", response.status()));
    }
    let token: TokenResponse = small_json(response).await?;
    let app_state = state
        .get()
        .ok_or_else(|| "App not yet initialized".to_string())?;
    add_drive_with_tokens(
        app_state,
        AddDriveArgs {
            site_url: session.site_url,
            access_token: token.access_token,
            refresh_token: token.refresh_token,
            access_token_expires: token.expires_in,
            refresh_token_expires: token.refresh_token_expires_in,
            drive_name: if drive_name.is_empty() {
                callback.name
            } else {
                drive_name
            },
            remote_path: callback.path,
            local_path,
            user_id: callback.user_id,
            drive_id,
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_http_without_consent_and_wrong_schemes() {
        assert!(normalise_site("http://example.com").is_err());
        assert!(normalise_site("file:///tmp").is_err());
        assert!(normalise_site("https://example.com").is_ok());
    }

    #[test]
    fn version_gate_is_numeric() {
        assert!(!version_supported("4.11.99"));
        assert!(version_supported("4.12.0-pro"));
        assert!(!version_supported("garbage"));
    }

    #[test]
    fn callback_requires_exact_target_state_and_single_use() {
        let mut sessions = HashMap::from([(
            "session".into(),
            OAuthSession {
                site_url: "https://cloud.example".into(),
                state: "expected".into(),
                verifier: "verifier".into(),
                started: Instant::now(),
                callback: None,
            },
        )]);
        assert!(accept_callback(
            &mut sessions,
            "cloudreve://callback/other?state=expected&code=secret"
        )
        .is_none());
        assert!(accept_callback(
            &mut sessions,
            "cloudreve://callback/desktop?state=wrong&code=secret"
        )
        .is_none());
        assert!(accept_callback(
            &mut sessions,
            &format!(
                "cloudreve://callback/desktop?state=expected&code={}",
                "x".repeat(8192)
            )
        )
        .is_none());
        assert!(accept_callback(
            &mut sessions,
            "cloudreve://callback/desktop?state=expected&state=expected&code=secret"
        )
        .is_none());
        assert!(accept_callback(
            &mut sessions,
            "cloudreve://callback/desktop?state=expected&code=secret"
        )
        .is_some());
        assert!(accept_callback(
            &mut sessions,
            "cloudreve://callback/desktop?state=expected&code=secret"
        )
        .is_none());

        sessions.get_mut("session").unwrap().callback = None;
        sessions.get_mut("session").unwrap().started = Instant::now() - SESSION_TTL;
        assert!(accept_callback(
            &mut sessions,
            "cloudreve://callback/desktop?state=expected&code=secret"
        )
        .is_none());
    }
}
