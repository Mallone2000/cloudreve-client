//! Shared test harness: real Mount + real SQLite inventory + real temp files,
//! with a mock HTTP server standing in for the Cloudreve API.
//!
//! Tests written on top of this harness describe *user-visible behavior*
//! (data safety, conflict handling, deletions) rather than implementation
//! details.
//!
//! Each integration test file compiles this module independently and only
//! uses part of the harness, so dead_code warnings are suppressed.
#![allow(dead_code)]

use std::ops::Deref;
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use cloudreve_sync::drive::commands::ManagerCommand;
use cloudreve_sync::drive::mounts::{Credentials, DriveConfig, Mount};
use cloudreve_sync::inventory::{InventoryDb, MetadataEntry, TaskRecord};
use cloudreve_sync::{EventBroadcaster, SummaryNotifier};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::sync::mpsc;
use uuid::Uuid;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct TestTlsIdentity {
    cert_pem: Vec<u8>,
    cert_der: Vec<u8>,
    key_der: Vec<u8>,
}

fn test_tls_identity() -> &'static TestTlsIdentity {
    static IDENTITY: OnceLock<TestTlsIdentity> = OnceLock::new();
    IDENTITY.get_or_init(|| {
        use std::process::Command;

        let temp = tempfile::tempdir().expect("test certificate directory");
        let cert_pem = temp.path().join("localhost.cert.pem");
        let cert_der = temp.path().join("localhost.cert.der");
        let key_pem = temp.path().join("localhost.key.pem");
        let key_der = temp.path().join("localhost.key.der");
        let status = Command::new("openssl")
            .args([
                "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            ])
            .arg("-keyout")
            .arg(&key_pem)
            .arg("-out")
            .arg(&cert_pem)
            .args([
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-addext",
                "extendedKeyUsage=serverAuth",
                "-addext",
                "keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign",
            ])
            .output()
            .expect("run openssl for test certificate");
        assert!(
            status.status.success(),
            "test certificate generation failed: {}",
            String::from_utf8_lossy(&status.stderr)
        );
        let status = Command::new("openssl")
            .args(["x509", "-in"])
            .arg(&cert_pem)
            .args(["-outform", "DER", "-out"])
            .arg(&cert_der)
            .output()
            .expect("convert test certificate");
        assert!(
            status.status.success(),
            "test certificate conversion failed"
        );
        let status = Command::new("openssl")
            .args(["pkcs8", "-topk8", "-nocrypt", "-in"])
            .arg(&key_pem)
            .args(["-outform", "DER", "-out"])
            .arg(&key_der)
            .output()
            .expect("convert test private key");
        assert!(
            status.status.success(),
            "test private key conversion failed"
        );
        TestTlsIdentity {
            cert_pem: std::fs::read(cert_pem).unwrap(),
            cert_der: std::fs::read(cert_der).unwrap(),
            key_der: std::fs::read(key_der).unwrap(),
        }
    })
}

/// Serve wiremock through a local TLS endpoint. Transfer tests therefore use
/// the same HTTPS requirement as the shipped client.
pub struct TlsMockServer {
    mock: MockServer,
    uri: String,
    task: tokio::task::JoinHandle<()>,
}

impl TlsMockServer {
    pub async fn start() -> Self {
        use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

        let mock = MockServer::start().await;
        let upstream = *mock.address();
        let identity = test_tls_identity();
        let certificate = CertificateDer::from(identity.cert_der.clone());
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(identity.key_der.clone()));
        let tls = tokio_rustls::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate], key)
            .expect("test TLS certificate");
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test TLS listener");
        let uri = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    if let Ok(mut tls_stream) = acceptor.accept(socket).await {
                        if let Ok(mut upstream_stream) =
                            tokio::net::TcpStream::connect(upstream).await
                        {
                            let _ = tokio::io::copy_bidirectional(
                                &mut tls_stream,
                                &mut upstream_stream,
                            )
                            .await;
                        }
                    }
                });
            }
        });
        Self { mock, uri, task }
    }

    pub fn uri(&self) -> String {
        self.uri.clone()
    }
}

impl Deref for TlsMockServer {
    type Target = MockServer;

    fn deref(&self) -> &Self::Target {
        &self.mock
    }
}

impl Drop for TlsMockServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub const REMOTE_BASE: &str = "cloudreve://my/sync";

/// A real, temporary HFS+ volume backed by RAM, detached on drop.
///
/// Lets tests exercise behavior that only shows up on a separate filesystem
/// (free space limits, cross-device moves) instead of the single temp volume
/// every other test shares. macOS only: building a filesystem on Linux needs
/// root, which a test suite must not require.
#[cfg(target_os = "macos")]
pub struct RamDisk {
    device: String,
    pub mount_point: PathBuf,
}

#[cfg(target_os = "macos")]
impl RamDisk {
    /// Returns `None` when the volume cannot be created (no `hdiutil`,
    /// restricted CI sandbox, ...) so callers can skip rather than fail on
    /// something unrelated to the behavior under test.
    pub fn new(size_mb: u64, label: &str) -> Option<Self> {
        use std::process::Command;

        let sectors = size_mb * 1024 * 1024 / 512;
        let attach = Command::new("hdiutil")
            .args(["attach", "-nomount", &format!("ram://{sectors}")])
            .output()
            .ok()?;
        if !attach.status.success() {
            return None;
        }
        let device = String::from_utf8_lossy(&attach.stdout).trim().to_string();
        if device.is_empty() {
            return None;
        }

        let format = Command::new("diskutil")
            .args(["erasevolume", "HFS+", label, &device])
            .output()
            .ok()?;
        if !format.status.success() {
            let _ = Command::new("hdiutil")
                .args(["detach", "-force", &device])
                .output();
            return None;
        }

        Some(Self {
            device,
            mount_point: PathBuf::from(format!("/Volumes/{label}")),
        })
    }
}

#[cfg(target_os = "macos")]
impl Drop for RamDisk {
    fn drop(&mut self) {
        let _ = std::process::Command::new("hdiutil")
            .args(["detach", "-force", &self.device])
            .output();
    }
}

pub struct TestEnv {
    pub server: TlsMockServer,
    pub mount: Arc<Mount>,
    pub inventory: Arc<InventoryDb>,
    pub sync_dir: PathBuf,
    pub drive_id: String,
    _tmp: TempDir,
    _manager_rx: mpsc::UnboundedReceiver<ManagerCommand>,
}

impl TestEnv {
    pub async fn new() -> Self {
        Self::with_max_file_size(1024).await
    }

    pub async fn with_max_file_size(max_file_size_mb: u64) -> Self {
        Self::build(max_file_size_mb, Vec::new(), None).await
    }

    pub async fn with_ignore_patterns(patterns: Vec<String>) -> Self {
        Self::build(1024, patterns, None).await
    }

    /// Put the sync folder on a caller-chosen volume, so tests can exercise the
    /// disk space guard against real free-space numbers.
    pub async fn with_sync_dir(sync_dir: PathBuf, max_file_size_mb: u64) -> Self {
        Self::build(max_file_size_mb, Vec::new(), Some(sync_dir)).await
    }

    async fn build(
        max_file_size_mb: u64,
        ignore_patterns: Vec<String>,
        sync_dir_override: Option<PathBuf>,
    ) -> Self {
        let tmp = TempDir::new().expect("create temp dir");
        // The inventory DB deliberately stays in the temp dir: on an overridden
        // sync volume it would eat into the very free space under test.
        let sync_dir = sync_dir_override.unwrap_or_else(|| tmp.path().join("sync"));
        std::fs::create_dir_all(&sync_dir).expect("create sync dir");

        let inventory = Arc::new(
            InventoryDb::with_path(tmp.path().join("meta.db")).expect("create inventory db"),
        );

        let server = TlsMockServer::start().await;
        let drive_id = Uuid::new_v4().to_string();

        let config = DriveConfig {
            id: drive_id.clone(),
            name: "Test Drive".to_string(),
            instance_url: server.uri(),
            remote_path: REMOTE_BASE.to_string(),
            credentials: Credentials {
                access_token: Some("test-access-token".to_string()),
                refresh_token: "test-refresh-token".to_string(),
                refresh_expires: "2099-01-01T00:00:00Z".to_string(),
                access_expires: Some("2099-01-01T00:00:00Z".to_string()),
            },
            sync_path: sync_dir.clone(),
            enabled: true,
            allow_insecure_http: false,
            approved_http_storage_origins: Vec::new(),
            tls_root_certificate_pem: Some(test_tls_identity().cert_pem.clone()),
            user_id: "test-user".to_string(),
            ignore_patterns,
            max_file_size_mb,
            sse_client_id: Uuid::new_v4().to_string(),
            ..Default::default()
        };

        let (manager_tx, manager_rx) = mpsc::unbounded_channel();
        let notifier = Arc::new(SummaryNotifier::new(Arc::new(EventBroadcaster::new(16))));
        let mount = Arc::new(Mount::new(config, inventory.clone(), manager_tx, notifier).await);

        Self {
            server,
            mount,
            inventory,
            sync_dir,
            drive_id,
            _tmp: tmp,
            _manager_rx: manager_rx,
        }
    }

    /// Configure the mock server so the remote listing returns these files.
    /// Replaces any previously registered listing.
    pub async fn set_remote_files(&self, files: Vec<Value>) {
        self.server.reset().await;
        let body = json!({
            "code": 0,
            "msg": "",
            "data": {
                "files": files,
                "pagination": { "page": 1, "page_size": 500, "total_items": 0 },
                "props": {
                    "max_page_size": 10000,
                    "order_by_options": ["name"],
                    "order_direction_options": ["asc"],
                },
            },
        });
        Mock::given(method("GET"))
            .and(path("/api/v4/file"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&self.server)
            .await;
    }

    /// Run a full 3-way sync against the mock remote.
    pub async fn full_sync(&self) -> anyhow::Result<()> {
        cloudreve_sync::drive::sync::full_sync(&self.mount, &self.sync_dir, REMOTE_BASE).await
    }

    /// Simulate the app being relaunched: rebuild the Mount over the same
    /// inventory database and config, exactly like the next boot would.
    /// Whatever startup does with the tasks left in the database has happened
    /// by the time this returns.
    pub async fn restart_mount(&mut self) {
        let config = self.mount.get_config().await;
        let (manager_tx, manager_rx) = mpsc::unbounded_channel();
        let notifier = Arc::new(SummaryNotifier::new(Arc::new(EventBroadcaster::new(16))));
        self.mount =
            Arc::new(Mount::new(config, self.inventory.clone(), manager_tx, notifier).await);
        self._manager_rx = manager_rx;
    }

    /// Absolute path of a file inside the local sync directory.
    pub fn local_path(&self, rel: &str) -> PathBuf {
        self.sync_dir.join(rel)
    }

    /// Create/overwrite a local file with the given content.
    pub fn write_local(&self, rel: &str, content: &[u8]) -> PathBuf {
        let path = self.local_path(rel);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("create parent dirs");
        }
        std::fs::write(&path, content).expect("write local file");
        path
    }

    /// Set the mtime (unix seconds) of a local file — simulates an edit at a
    /// specific point in time without relying on sleeps.
    pub fn set_local_mtime(&self, rel: &str, unix_secs: i64) {
        let path = self.local_path(rel);
        let ft = filetime::FileTime::from_unix_time(unix_secs, 0);
        filetime::set_file_mtime(&path, ft).expect("set file mtime");
    }

    /// Record a file in the inventory as "synced" with the given etag,
    /// using the file's current size and mtime on disk (the state a real
    /// upload/download would have recorded).
    pub fn track_synced(&self, rel: &str, etag: &str) {
        let path = self.local_path(rel);
        let meta = std::fs::metadata(&path).expect("stat local file");
        let mtime = cloudreve_sync::inventory::local_mtime_secs(&path);
        let entry = MetadataEntry::new(
            Uuid::parse_str(&self.drive_id).unwrap(),
            path.to_str().unwrap(),
            false,
        )
        .with_etag(etag)
        .with_size(meta.len() as i64)
        .with_local_mtime(mtime)
        .with_local_fingerprint(&path);
        self.inventory
            .upsert(&entry)
            .expect("upsert inventory entry");
    }

    /// All task records ever enqueued for this drive (any status).
    pub fn all_tasks(&self) -> Vec<TaskRecord> {
        self.inventory
            .list_tasks(Some(&self.drive_id), None)
            .expect("list tasks")
    }

    /// Task records of a given type ("upload" / "download") for this drive.
    pub fn tasks_of_type(&self, task_type: &str) -> Vec<TaskRecord> {
        self.all_tasks()
            .into_iter()
            .filter(|t| t.task_type == task_type)
            .collect()
    }

    /// The inventory entry for a local file, if tracked.
    pub fn db_entry(&self, rel: &str) -> Option<cloudreve_sync::inventory::FileMetadata> {
        let path = self.local_path(rel);
        self.inventory
            .query_by_path(path.to_str().unwrap())
            .expect("query inventory")
    }
}

/// Build the JSON for a remote file as the Cloudreve list API would return it.
pub fn remote_file(name: &str, size: i64, etag: &str) -> Value {
    json!({
        "type": 0,
        "id": format!("file-{name}"),
        "name": name,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-02T00:00:00Z",
        "size": size,
        "path": format!("{REMOTE_BASE}/{name}"),
        "primary_entity": etag,
    })
}
