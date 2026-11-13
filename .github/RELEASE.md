# Release workflow

## CI

- **CI** (`ci.yml`): runs on every PR targeting `master`/`main` (and manually). Frontend tests, e2e, Clippy, and the per-database integration suites. The `CI Required` check gates merging; branch protection requires PRs to be up to date with `master`.
- **Nightly** (`nightly.yml`): runs at 04:00 UTC on `master` (and manually). Dependency audit (`cargo audit`, `npm audit`), fuzzing, SSH tunnel soak, legacy database versions, big-data scenarios, and builds plus e2e on Linux, macOS and Windows.

## Release (tagged builds)

- **Trigger**: Push a version tag matching `v*` (e.g. `v0.1.0`, `v1.2.3-beta.1`). Tags containing `-` are marked as pre-releases.
- **Gate**: the full test matrix runs again before anything is built.
- **Artifacts**:
  - **Windows**: `.msi` installer, `.exe` (NSIS).
  - **macOS**: `.dmg` and `.app.tar.gz` for Apple Silicon (aarch64) and Intel (x86_64).
  - **Linux**: `.deb` (Debian/Ubuntu), `.rpm` (Fedora/RHEL), `.AppImage` (portable; works on Arch and others).
- **Downstream**: after the release job, the workflow publishes to the AUR, opens the Flathub update, and refreshes the APT and RPM repositories.

## Cutting a release

1. Bump the version in `package.json`, `src-tauri/tauri.conf.json` and `src-tauri/Cargo.toml`, then run `cargo update -p tablio --manifest-path src-tauri/Cargo.toml` so `Cargo.lock` matches.
2. Open a PR, wait for `CI Required`, and merge it.
3. Make sure the latest Nightly on `master` is green.
4. Create and push the tag from the merged `master` (version must match):
   ```bash
   git checkout master && git pull
   git tag v0.1.0
   git push origin v0.1.0
   ```
5. The **Release** workflow tests, builds on Windows, macOS and Linux, and **publishes the GitHub Release directly** (not as a draft) so the AUR, Flathub and package-repository jobs can download the assets. Review the release page once it finishes; if something is wrong, fix forward with a new patch version rather than editing assets.

## Repo permissions

In **Settings → Actions → General**, set **Workflow permissions** to **Read and write** so the release workflow can create releases and upload assets.
