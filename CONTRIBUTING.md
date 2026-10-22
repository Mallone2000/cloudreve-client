# Contributing to Cloudreve Desktop Client

Thanks for your interest in contributing! This document explains how to set up your environment and submit changes.

## Project overview

Desktop sync client for [Cloudreve](https://cloudreve.org) (macOS & Linux), built with [Tauri](https://tauri.app):

| Directory | Description |
|---|---|
| `ui/` | React frontend (Vite + TypeScript + MUI) |
| `src-tauri/` | Tauri shell — IPC commands, tray, windows |
| `crates/cloudreve-sync/` | Sync engine — drives, tasks, conflict handling, SQLite inventory |
| `crates/cloudreve-api/` | Cloudreve API client |

## Development setup

### With Nix (recommended)

The repository ships a `flake.nix` providing all dependencies (rustup, pkg-config, openssl, yarn, cargo-tauri):

```sh
nix develop
```

If you use [direnv](https://direnv.net), an `.envrc` is already present — just run `direnv allow` once and the shell loads automatically.

### Manual setup

You will need:

- [Rust](https://rustup.rs) (stable toolchain)
- [Node.js](https://nodejs.org) + [Yarn](https://yarnpkg.com)
- The [Tauri platform prerequisites](https://tauri.app/start/prerequisites/) for your OS. The Tauri CLI is installed with the frontend dependencies.

### Building and running

```sh
# Install frontend dependencies
cd ui && yarn install --frozen-lockfile && cd ..

# Run in development mode (starts Vite + Tauri)
ui/node_modules/.bin/tauri dev

# Debug build without release bundles (runs `tsc -b && vite build` first)
ui/node_modules/.bin/tauri build --debug --no-bundle

# Quick Rust-only check
cargo check -p cloudreve-sync -p cloudreve-desktop

# Lint the frontend
cd ui && yarn lint
```

### Build for local use on macOS

From the repository root, run:

```sh
./scripts/build-local-macos.sh
```

This creates `target/release/bundle/macos/Cloudreve.app` with an ad hoc signature
and verifies the bundle. You can copy the app into `/Applications` or run it from
the build directory. This local build does not require an Apple Developer ID or
the release updater signing key. Run `yarn install --frozen-lockfile` in `ui/`
first if you have not installed the frontend dependencies yet.

### Running the tests

```sh
# All tests (unit + integration)
cargo test -p cloudreve-sync

# Only the sync integration scenarios
cargo test -p cloudreve-sync --test sync_scenarios

# Only unit tests (inventory, conflict lifecycle, fs events...)
cargo test -p cloudreve-sync --lib

# A single test by name
cargo test -p cloudreve-sync both_sides_modified
```

Tests are **behavior-driven**: they describe real user situations (edits on both sides, deletions, same-size edits...) and assert the outcome the user cares about — no silent data loss, conflicts surfaced instead of overwritten, deletions never propagated to the server.

The integration tests in `crates/cloudreve-sync/tests/` run against a real `Mount`, a real SQLite inventory, and real files on disk; only the Cloudreve API is faked with a [wiremock](https://docs.rs/wiremock) HTTP server (see the `TestEnv` harness in `tests/common/mod.rs`). No network access or real server is needed.

If your change touches the sync engine, add a scenario to `tests/sync_scenarios.rs` describing the user-visible behavior — reuse `TestEnv` (`write_local`, `track_synced`, `set_remote_files`, `full_sync`, `all_tasks`...).

## How to contribute

### 1. Fork and branch

1. **Fork** the repository on GitHub.
2. Create a branch from `main` with a descriptive name:

```
feat/conflict-resolution-ui
fix/reauth-loop-on-scope-error
docs/update-readme
```

Use the `<type>/<short-description>` pattern, with the same types as conventional commits (see below).

### 2. Commit messages — Conventional Commits

We follow [Conventional Commits](https://www.conventionalcommits.org):

```
<type>(<optional scope>): <description>
```

Common types:

- `feat:` — new feature
- `fix:` — bug fix
- `docs:` — documentation only
- `refactor:` — code change that neither fixes a bug nor adds a feature
- `chore:` — maintenance (dependencies, release bumps, tooling)
- `test:` — adding or fixing tests

Examples from this repository:

```
feat: detect sync conflicts and prevent silent overwrites
fix: retry upload with overwrite when file already exists on server
chore(release): bump version to 0.2.0-beta.2
```

Keep commits focused: one logical change per commit.

### 3. Translations

The UI is translated in 11 languages under `ui/public/locales/<lng>/common.json`. If your change adds user-facing strings:

- Add the key to **all** locale files (de, en-US, es, fr, it, ja, ko, pl, ru, zh-CN, zh-TW), keeping the same key position in each file.
- Use the `t("key", "English fallback")` pattern in components.

### 4. Before opening a pull request

- Make sure `cargo test -p cloudreve-sync` passes.
- Make sure `cargo check` passes and the frontend builds (`ui/node_modules/.bin/tauri build --debug --no-bundle`).
- Run `yarn lint` in `ui/` for frontend changes.
- Rebase on the latest `main` if needed.

### 5. Open the pull request

- Target the `main` branch.
- Describe **what** the change does and **why**.
- Link related issues (`Fixes #123`).
- Screenshots are appreciated for UI changes.

## Releasing

Everything is driven by the git tag. There is no flag to tick anywhere else.

```bash
./scripts/release.sh 0.4.0          # stable
./scripts/release.sh 0.4.0-beta.1   # prerelease
```

The script bumps the version in `Cargo.toml` (workspace) and
`src-tauri/tauri.conf.json`, commits, and creates the tag. It stops there and
prints the `git push` command — pushing the tag is what publishes the release, so
that step stays manual.

```bash
git push origin main v0.4.0
```

Pushing the tag runs [`.github/workflows/release.yml`](.github/workflows/release.yml):
git-cliff turns the commits since the previous tag into release notes, a **draft**
release is created, macOS (Apple Silicon) and Linux (x86_64) runners attach the
bundles plus the signed `latest.json` the updater reads, and only then is the draft
published.

Nothing is visible until every build has succeeded. A failed build leaves an
unpublished draft rather than an empty release that `releases/latest` would send
the updater to.

### Stable vs prerelease

A version carrying a `-` suffix is a prerelease, per semver:

| Tag | GitHub | Auto-update |
|-----|--------|-------------|
| `v0.4.0` | Latest | offered to everyone |
| `v0.4.0-beta.1`, `v1.0.0-rc.1` | prerelease | not offered |

The updater polls `releases/latest`, which GitHub never resolves to a prerelease.
So a beta reaches only the people who download it by hand — which is the point,
but it also means **shipping only prereleases leaves existing users stranded on
the last stable one**.

> [!WARNING]
> Release publishing requires a Developer ID Application certificate and Apple
> notarization credentials. Add `APPLE_CERTIFICATE` (base64 encoded `.p12`),
> `APPLE_CERTIFICATE_PASSWORD`, `APPLE_ID`, an app-specific `APPLE_PASSWORD`, and
> `APPLE_TEAM_ID` as GitHub Actions secrets. The workflow also requires
> `TAURI_SIGNING_PRIVATE_KEY` for updater signatures. It fails before creating
> a draft when these are missing. On Linux only the AppImage can update itself;
> the `.deb` is manual.


## Reporting bugs

Open an issue with:

- Your OS and version (macOS / Linux distribution)
- Cloudreve **server** version
- Client version
- Steps to reproduce and relevant logs

## Questions

Feel free to open a discussion or an issue if anything is unclear. Thanks for contributing!
