# Release verification

Run these checks on a machine with the locked Cargo and Yarn dependencies available:

```sh
cd ui && yarn install --frozen-lockfile && yarn lint && yarn build
cd .. && cargo test --locked -p cloudreve-api -p cloudreve-sync
cargo test --locked -p cloudreve-sync --test reconciliation_benchmark -- --ignored --nocapture
cargo check --locked -p cloudreve-desktop
cargo test --locked -p cloudreve-desktop --lib
```

The recovery fixture prints scan time, SQLite connection checkouts, and process peak RSS for 1,000 and 10,000 files. The 10,000-file scan must finish in under 300 seconds on the CI reference runner. Save the output for both the baseline and changed revision before comparing them; machine and dependency versions must be the same. Connection checkouts are a database activity proxy, not a count of SQL statements.

After packaging on macOS and Linux, launch the installed application and check:

1. A saved HTTP drive starts paused and Settings shows **Allow HTTP**. Verify the site receives no ping, favicon, event, or transfer request before consent. Grant consent, then revoke it during a transfer and verify requests stop.
2. Add an HTTPS drive through OAuth. Confirm the callback opens the add-drive window, a replay or wrong-state callback is rejected, and authorization completes. Check the WebView network and IPC inspectors for access tokens, refresh tokens, authorization codes, and the PKCE verifier.
3. Confirm the site icon loads in the popup and Settings. Confirm the CSP blocks an injected inline script and a cross-origin fetch from the WebView while normal IPC and icon asset URLs work.
4. Resolve sample conflicts with **Keep Local**, **Keep Remote**, and **Keep Both**, then verify the resulting local and remote contents.

These packaged checks require a test Cloudreve instance and an interactive desktop session. Record the package version, OS version, and observed result for each platform.
