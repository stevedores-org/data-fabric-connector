# DFC (data-fabric connector)

Stateless anti-corruption layer and event bridge between `aivcs-human-in-the-loop`, `aivcs-api`, and `data-fabric`.

See [issue #1](https://github.com/stevedores-org/data-fabric-connector/issues/1) for strategy and architecture. Implementation is tracked in epics [#2–#7](https://github.com/stevedores-org/data-fabric-connector/issues).

## What DFC owns

- ID correlation across AIVCS, HITL, and data-fabric
- Event normalization into `dfc.v1` envelopes
- HITL review bundle assembly
- Replay/rollback request bridging
- Reliability envelope (idempotency, tenant checks, OTel — epics E3–E6)

## What DFC does not own

Brains ranking, policy evaluation, AIVCS semantics, or durable storage.

## Layout

```text
crates/dfc-core/          schemas, IDs, event types
crates/dfc-aivcs/         aivcs-api client
crates/dfc-data-fabric/   data-fabric client
crates/dfc-hitl/          review bundle assembly
crates/dfc-server/        HTTP server (axum)
schemas/                  JSON schemas + fixtures
deploy/base/              k8s manifests
flake.nix                 OCI image via nix (`nix build .#dfc-image`)
tests/                    contract & integration tests (future epics)
```

## Run locally

```bash
cargo run -p dfc-server
curl localhost:8080/healthz
curl localhost:8080/v1/version
```

**Production FQDN:** `https://dfc.aivcs.io` (Gateway API → `shared-tls-gateway`, listener `https-aivcs-io-wildcard`).

E1 uses in-memory mock upstreams. Set `DATA_FABRIC_TENANT_ID` when wiring real clients (E6).

## OCI image (Nix)

```bash
# Linux builder (CI / remote builder)
nix build .#dfc-image
skopeo copy docker-archive:result docker://ghcr.io/stevedores-org/data-fabric-connector:latest
```

On macOS, build the image via a Linux remote builder or CI — same pattern as `mom`.

## API surface

| Method | Path | Epic |
|--------|------|------|
| GET | `/healthz` | E1 |
| GET | `/readyz` | E1 |
| GET | `/v1/version` | E1 |
| POST | `/v1/correlate` | E2 |
| GET | `/v1/correlate/:kind/:id` | E2 |
| POST | `/v1/events/aivcs` | E3 |
| POST | `/v1/events/hitl` | E3 |
| GET | `/v1/hitl/reviews/:id` | E4 |
| POST | `/v1/hitl/reviews/:id/decision` | E4 |
| POST | `/v1/replay/request` | E5 |
| POST | `/v1/rollback/request` | E5 |

All mutating routes require `X-Tenant-Id`.

## License

Apache-2.0
