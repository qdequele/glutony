# Vendored Lab contracts

`lab-events.schema.json` is a byte copy of `contracts/lab-events.schema.json` in
`meilisearch/lab` (platform contract v2, 2026-10-08). The Lab owns it.

- Never edit it here. Change it in the Lab, then copy the file:
  `curl -fsSL -H "Authorization: Bearer $LAB_REPO_TOKEN" https://raw.githubusercontent.com/meilisearch/lab/main/contracts/lab-events.schema.json -o contracts/vendor/lab/lab-events.schema.json`
- `crates/usage/src/lab.rs` validates every event it builds against this copy.
- CI (`lab-contract-drift` in `.github/workflows/ci.yml`) diffs the copy against the
  Lab's `main` and fails on drift. It needs the `LAB_REPO_TOKEN` repository secret
  (read access to `meilisearch/lab`); without it the job is skipped with a notice.
