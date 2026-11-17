# Releasing @burrowmq/sdk

Releases go out from GitHub Actions with npm **trusted publishing** (`.github/workflows/release-sdk.yml`). GitHub gives the job a short-lived identity, and npm trusts only this repo and that workflow file. No npm token is stored anywhere.

The trusted publisher is **stage-only**. The workflow stages each version on npm, and it goes live only after a maintainer approves it with 2FA. npm adds provenance to each version, which links it to the commit and the workflow run that built it.

## Each release

1. In a PR, raise `version` in `sdk/package.json` (0.1.1 → 0.1.2 for fixes, 0.2.0 for new features while we are on 0.x). Merge it.
2. Tag the merge on main and push the tag:
   ```bash
   git tag sdk-v0.1.2 && git push origin sdk-v0.1.2
   ```
3. The **Release SDK** workflow checks that the tag matches the version, type-checks, builds and stages the version on npm.
4. A maintainer approves it. npm asks for 2FA (npm 11.15 or later):
   ```bash
   npm stage list @burrowmq/sdk
   npm stage approve <stage-id>
   ```
   To look first, use `npm stage view <stage-id>`. To drop it, use `npm stage reject <stage-id>`.

## Notes

- **No take-backs.** After 72 hours a version can't be unpublished, only deprecated. Fix a bad release with a new version.
- **What ships.** Only `dist/`, `examples/quickstart.mjs`, `README.md` and `LICENSE` (the `files` list). Check with `npm pack --dry-run`.
