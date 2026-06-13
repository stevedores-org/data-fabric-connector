# AGENTS.md — data-fabric-connector (DFC)

## Role

DFC is a **stateless** Rust service. It normalizes events and correlates IDs between AIVCS, HITL, and data-fabric. Never add local SQLite/Postgres for business state.

## Conventions

- Workspace crates depend inward: `dfc-server` → adapters → `dfc-core`
- Schema version constant: `dfc_core::SCHEMA_VERSION` (`dfc.v1`)
- All event POSTs require `idempotency_key` and `X-Tenant-Id`
- Mock upstream clients (`MockAivcsClient`, `MockDataFabricClient`) are the E1 default; replace in E6
- Public FQDN: `dfc.aivcs.io` (`DFC_PUBLIC_FQDN` env, default in code)

## Worktrees

Use `worktrees/dfc-*` branches off this repo. Do not edit the primary checkout for PR work.

## Epics

| Issue | Scope |
|-------|-------|
| #2 E1 | Foundation, health, schemas, k8s |
| #3 E2 | ID correlation |
| #4 E3 | Event ingestion |
| #5 E4 | HITL bundles |
| #6 E5 | Replay/rollback |
| #7 E6 | Reliability + tenant isolation |

## Commands

```bash
cargo build --workspace
cargo test --workspace
cargo run -p dfc-server
cargo fmt --all
cargo clippy --all --all-targets -- -D warnings

# OCI image (Linux)
nix build .#dfc-image
nix flake check
```

## Related repos

- `stevedores-org/data-fabric` — canonical runs/tasks/events
- `stevedores-org/aivcs-api` — snapshots, replay, rollback
- `stevedores-org/aivcs-human-in-the-loop` — review UI
