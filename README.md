# Cloudreve MacOS & Linux Desktop Client

> [!IMPORTANT]
> This client is still young — grab the [latest release](https://github.com/cedev-1/cloudreve-client/releases/latest).

> [!WARNING]
> Cloudreve server version **4.16.1** has a bug where OAuth scope enforcement blocks desktop clients from accessing `/user/me` and other user endpoints ([details](https://github.com/cloudreve/cloudreve/issues/3471)). Please upgrade to **4.17.0**.

Desktop sync client for [Cloudreve](https://cloudreve.org) - fork from [cloudreve windows client](https://github.com/cloudreve/desktop).

## Transport and OAuth security

The client uses HTTPS by default. Existing HTTP drives remain paused until you allow HTTP in Settings; new HTTP sites require the same choice before site validation. Revoking consent pauses that drive. HTTP exposes credentials and file contents to anyone able to observe or alter the connection.

OAuth runs in the Rust backend with PKCE. The shared OAuth registration is retained for compatibility because [Cloudreve currently requires `client_secret` during token exchange](https://docs.cloudreve.org/en/api/auth). The bundled secret is extractable from a desktop application and must not be treated as confidential. Authorization codes, access tokens, refresh tokens, and the PKCE verifier are not sent to the WebView.

Reconciliation runs every 300 seconds, with no overlapping scan for a drive. The five-minute recovery target is for the documented 10,000-file benchmark on its CI reference runner; it is not a guarantee for unavailable servers or arbitrarily large trees.

See [release verification](docs/verification.md) for the benchmark and packaged desktop checks.

<img width="689" height="586" alt="image" src="https://github.com/user-attachments/assets/a199526f-c6ab-467b-8f3d-f8e1330f91ef" />
