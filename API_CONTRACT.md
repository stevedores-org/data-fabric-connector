# Data Fabric Connector API Contract

## Input Validation

### Path & Identifier Parameters (H5 Defense-in-Depth)

All route parameters and identifier fields must conform to a strict charset for defense-in-depth validation. While URL encoding in PR #28 prevents routing escapes, this charset validation provides additional safeguards:

1. **Rejects malformed input at the handler boundary** — invalid data is flagged with `400 Bad Request` rather than silently encoded and forwarded upstream
2. **Reduces blast radius** — ensures any future code path that touches these inputs without encoding still receives safe data
3. **Provides clearer diagnostics** — a `400` with an explicit error message is more actionable than an upstream `404`

#### Accepted Charset

Route identifiers, path parameters, and ID fields must contain **only**:
- Alphanumeric characters: `A-Z`, `a-z`, `0-9`
- Special characters: `.` (dot), `_` (underscore), `:` (colon), `-` (dash)

**Regex:** `^[A-Za-z0-9._:-]+$`

#### Validated Fields

The following fields are validated at the handler boundary:

**Path Parameters:**
- `kind` — correlation kind in `/v1/correlate/{kind}/{id}`
- `id` — correlation ID in `/v1/correlate/{kind}/{id}`
- `review_id` — HITL review ID in `/v1/hitl/reviews/{review_id}` endpoints

**Request Body Fields:**
- `idempotency_key` — idempotency key for deduplication (present in aivcs, hitl, replay, rollback endpoints)
- `run_id` — data fabric run ID (present in correlate, aivcs events, replay endpoints)
- `task_id` — data fabric task ID (present in correlate endpoints)
- `branch_id` — AIVCS branch ID (present in rollback endpoints)

#### Validation Behavior

When an invalid character is detected:
1. The request is rejected with `400 Bad Request`
2. The response body includes an error message: `"{field} contains forbidden characters (allowed: alphanumeric, '.', '_', ':', '-')"`
3. No upstream requests are made; the error boundary is at the handler entry point

#### Examples

**Valid:**
- `run-123`
- `evt_deploy:v1.2.3`
- `branch_main.prod`
- `rev-2024-06-23_audit`
- `task-123:456_789.0`

**Invalid:**
- `run with space` (spaces not allowed)
- `../admin` (forward slash not allowed)
- `run@host` (@ not allowed)
- `run#123` (# not allowed)
- `review-/admin` (/ not allowed)

## Rationale

This contract is unchanged from upstream systems — the constraint applies only at the HTTP boundary. Upstream services (Data Fabric, AIVCS) may have their own validation requirements; this layer adds defense-in-depth.

See: [PR #28 (URL encoding)](https://github.com/stevedores-org/data-fabric-connector/pull/28), [Issue #29 (charset validation)](https://github.com/stevedores-org/data-fabric-connector/issues/29)
