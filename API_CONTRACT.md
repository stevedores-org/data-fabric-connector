# API Contract and Validation Rules

This document outlines the validation rules and constraints for the Data Fabric Connector (DFC) API.

## Input Character Validation (H5 Defense-in-Depth)

To prevent routing-escape and invalid character injection, the API enforces a strict character validation policy at the handler boundary on path parameters and request body fields.

### Valid Charset

All values for the fields listed below must contain only characters within the following charset:
`[A-Za-z0-9._:-]` (letters, digits, dot, underscore, colon, and hyphen).

Any request that violates these constraints is rejected with a `400 Bad Request` and a JSON response containing an explicit error message:
`{"error": "<field_name> contains forbidden characters"}`

### Validated Fields

The following fields (path parameters and JSON request fields) are subject to this character validation rule:

- **Path Parameters**:
  - `kind` (e.g. in `/v1/correlate/{kind}/{id}`)
  - `id` (e.g. in `/v1/correlate/{kind}/{id}`)
  - `review_id` (e.g. in `/v1/hitl/reviews/{review_id}`)

- **Request Body Fields**:
  - `tenant_id`
  - `idempotency_key`
  - `run_id` (and legacy/prefixed fields like `data_fabric_run_id`)
  - `task_id` (and legacy/prefixed fields like `data_fabric_task_id`)
  - `review_id` (in events/decisions)
  - `branch_id` (in rollback requests)
  - `target_snapshot_id` / `aivcs_snapshot_id` / `from_snapshot` / `to_snapshot`
  - `aivcs_ref`
  - `aivcs_branch`
  - `source_id`
  - `target_id`
