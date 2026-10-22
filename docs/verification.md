# Release verification

Run these checks on a machine with the locked Cargo and Yarn dependencies available:

```sh
cd ui && yarn install --frozen-lockfile && yarn lint && yarn build
yarn audit
cd .. && cargo fmt --all -- --check
cargo test --locked -p cloudreve-api -p cloudreve-sync
cargo test --locked -p cloudreve-sync --test reconciliation_benchmark recovery_1000_files -- --ignored --exact --nocapture
cargo test --locked -p cloudreve-sync --test reconciliation_benchmark recovery_10000_files -- --ignored --exact --nocapture
cargo check --locked -p cloudreve-desktop
cargo test --locked -p cloudreve-desktop --lib
cargo audit --target-os macos
```

Each recovery fixture runs in its own process and prints scan time, SQLite connection checkouts, and process peak RSS. The 10,000-file scan must finish in under 300 seconds on the CI reference runner. Save the output for both the baseline and changed revision before comparing them; machine and dependency versions must be the same. Connection checkouts are a database activity proxy, not a count of SQL statements.

After packaging on macOS, launch the local application and check:

1. A saved HTTP drive, including one with old consent, stays paused. Settings says **HTTPS required** and offers no HTTP approval. Verify the site receives no ping, favicon, event, or transfer request. New HTTP sites and HTTP storage URLs are rejected.
2. Add an HTTPS drive through OAuth. Confirm the callback opens the add-drive window, a replay or wrong-state callback is rejected, and authorization completes. Check the WebView network and IPC inspectors for access tokens, refresh tokens, authorization codes, and the PKCE verifier.
3. Confirm the site icon loads in the popup and Settings. Confirm the CSP blocks an injected inline script and a cross-origin fetch from the WebView while normal IPC and icon asset URLs work.
4. Resolve sample conflicts with **Keep Local**, **Keep Remote**, and **Keep Both**, then verify the resulting local and remote contents.
5. Confirm the packaged app makes no request to the upstream updater endpoint and exposes no updater commands. Install subsequent versions using `scripts/build-local-macos.sh`.

These packaged checks require a test Cloudreve instance and an interactive desktop session. Record the app version, macOS version, and observed result.

The Rust advisory scan currently reports only unmaintained `proc-macro-error`, `zvariant`, `zvariant_derive`, and `zbus_names`. They are Linux/build dependency chains absent from the macOS target tree (`cargo tree --target aarch64-apple-darwin -i <crate>`). Treat any new vulnerability as a failing security check.
