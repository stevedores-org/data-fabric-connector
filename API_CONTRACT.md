# API Contract

## Path Parameter Validation

All inbound route handlers validate incoming path parameters representing IDs, keys, or kinds against a strict character set as a defense-in-depth security measure. This ensures that potentially malicious inputs (e.g. traversal sequences like `..`, shell commands, special characters, or spaces) are rejected immediately at the API server boundary before reaching downstream processing or database queries.

### Accepted Character Set
Only characters matching the regex pattern `^[A-Za-z0-9._:-]+$` are allowed.

This includes:
- Alphanumeric characters: `A-Z`, `a-z`, `0-9`
- Period: `.`
- Underscore: `_`
- Hyphen: `-`
- Colon: `:`

Any path parameters containing characters outside of this set (such as `/`, `\`, spaces, double quotes, single quotes, etc.) are strictly rejected.

### Error Response
If validation fails, the server responds with:
- **Status Code**: `400 Bad Request`
- **Response Body**:
```json
{
  "error": "{parameter_name} contains forbidden characters"
}
```
e.g. `{"error": "review_id contains forbidden characters"}`
