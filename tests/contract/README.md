# Contract tests

| Schema | Fixture | Epic |
|--------|---------|------|
| `schemas/dfc-event.schema.json` | `schemas/fixtures/dfc-event.v1.json` | E1/E3 |
| `schemas/correlate-request.schema.json` | `schemas/fixtures/correlate-request.v1.json` | E2 (#3) |
| `schemas/replay-request.schema.json` | `schemas/fixtures/replay-request.v1.json` | E5 (#6) |
| `schemas/rollback-request.schema.json` | `schemas/fixtures/rollback-request.v1.json` | E5 (#6) |

Conformance tests live in `crates/dfc-core/tests/schema_conformance.rs`.

HTTP acceptance for correlation (US-C1–C3) lives in `crates/dfc-server` integration tests.
