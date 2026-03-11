# Release workflow

## CI

- **Trigger**: Push to `master`/`main` or open a PR targeting them.
- **Steps**: Frontend tests (`npm run test`), Rust tests (`cargo test`), Clippy, frontend build, full Tauri build on Ubuntu.

## Release (tagged builds)

- **Trigger**: Push a version tag matching `v*` (e.g. `v0.1.0`, `v1.2.3-beta.1`).
- **Artifacts**:
  - **Windows**: `.msi` installer, `.exe` (NSIS).
  - **macOS**: `.dmg` for Apple Silicon (aarch64) and Intel (x86_64).
  - **Linux**: `.deb` (Debian/Ubuntu), `.rpm` (Fedora/RHEL), `.AppImage` (portable; works on Arch and others).

## Cutting a release

1. Bump version in `src-tauri/tauri.conf.json` and `package.json` (e.g. `0.1.0`).
2. Commit and push.
3. Create and push the tag (version must match):
   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```
4. The **Release** workflow runs and builds on Windows, macOS, and Linux. A draft GitHub Release is created with all installers attached.
5. In the repo’s **Releases** tab, open the draft, review the assets, and publish.

## Repo permissions

In **Settings → Actions → General**, set **Workflow permissions** to **Read and write** so the release workflow can create releases and upload assets.
