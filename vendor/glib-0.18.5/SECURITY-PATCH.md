# glib 0.18.5 security backport

This directory is the published `glib` 0.18.5 crate, with one source change in
`src/variant_iter.rs`: `VariantStrIter::impl_get` passes a mutable pointer to
`g_variant_get_child`. This backports the upstream fix for
[RUSTSEC-2024-0429](https://rustsec.org/advisories/RUSTSEC-2024-0429.html).

Tauri 2's Linux GTK 3 bindings require `glib` 0.18, so Cargo cannot select the
upstream patched `glib` 0.20 release. The workspace's `[patch.crates-io]` entry
applies this copy to every dependency, and `Cargo.lock` records the local path.

`cargo audit` skips this local path dependency, so the CI security job audits a
synthetic registry lockfile for `glib` 0.18.5. It excludes only
RUSTSEC-2024-0429, whose fix is backported here, and fails on any new advisory.
The Linux CI job runs the `VariantStrIter` tests with optimizations enabled,
which is where the original bug caused crashes. Remove this patch when Tauri's
Linux runtime can use a patched upstream `glib` release.
