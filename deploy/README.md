# Deploy — DFC

## Production (`dfc.aivcs.io`)

DNS and GitOps for **`dfc.aivcs.io`** are managed in **lornu.ai** (Crossplane + Flux on `lornu-gke-prod`), following the same pattern as `human.aivcs.io` and `api.aivcs.io`:

- **DNS:** Cloudflare CNAME `dfc` → `shared-tls-gateway` static IP (wildcard `*.aivcs.io` cert)
- **Gateway:** `shared-tls-gateway` listener `https-aivcs-io-wildcard`
- **HTTPRoute hostname:** `dfc.aivcs.io`
- **OCI image:** built from this repo via Nix (`nix build .#dfc-image`) and published by dockworker → GAR

The manifests under `deploy/base/k8s/` are the **app-owned reference** for Deployment/Service/HTTPRoute shape. Keep them aligned with the Flux-deployed copy in lornu.ai when changing ports, probes, or env.

## Verify (once image + Flux reconcile)

```bash
curl -sS https://dfc.aivcs.io/healthz
curl -sS https://dfc.aivcs.io/v1/version
```

Expected `/v1/version` fields include `"fqdn": "dfc.aivcs.io"` and `"public_url": "https://dfc.aivcs.io"`.

## Local / dev

```bash
cargo run -p dfc-server
curl localhost:8080/healthz
```

No in-cluster DNS required for local work — mock upstreams are the E1 default.

## Files

| File | Purpose |
|------|---------|
| `dfc.yaml` | Namespace, Deployment, Service, ConfigMap |
| `httproute.yaml` | Gateway API route for `dfc.aivcs.io` (mirrors lornu.ai Crossplane) |

## Related

- App repo: [stevedores-org/data-fabric-connector](https://github.com/stevedores-org/data-fabric-connector)
- GitOps SoT: `lornu-ai/lornu.ai` → `crossplane/gcp/hub/spoke/apps/` + `flux/clusters/lornu-gke-prod/`
