# Deploy — DFC

## Production (`dfc.aivcs.io`)

Infra is wired in **[lornu-ai/infra-code](https://github.com/lornu-ai/infra-code)** (GitOps source of truth). `lornu-gke-prod` Flux bootstraps from `flux/clusters/lornu-gke-prod` in that repo (handoff #19).

| Item | Value |
|------|-------|
| **FQDN** | `dfc.aivcs.io` |
| **Flux Kustomization** | `apps-dfc-gke-prod` |
| **Crossplane path** | `crossplane/gcp/hub/spoke/apps/dfc/overlays/gke/prod` |
| **Gateway** | `shared-tls-gateway` listener `https-aivcs-io-wildcard` |
| **Depends on** | `platform-gke-gateway` (listener Ready) |
| **OCI image** | `us-central1-docker.pkg.dev/gcp-lornu-ai/lornu/dfc:0.1.0` |
| **Replicas (prod overlay)** | `0` until GAR has the image |

Merged infra PRs:

- DNS: [infra-code #119](https://github.com/lornu-ai/infra-code/pull/119)
- GitOps manifests: [infra-code #120](https://github.com/lornu-ai/infra-code/pull/120)

### DNS (Cloudflare)

`dfc.aivcs.io` is a **proxied Cloudflare A record** → `${AIVCS_ORIGIN_GKE_ADDRESS}` (shared GKE gateway IP). Not a CNAME.

The record will not appear publicly until `infra-crossplane-cloudflare-dns` is Ready (cloudflare-eso chain still catching up).

### App-owned reference (`deploy/base/k8s/`)

Manifests here define the Deployment/Service/HTTPRoute **shape** for the app. The Flux-deployed copy lives under infra-code above — align ports, probes, hostnames, and env when you change these files.

## Reconcile (ops)

```bash
flux reconcile source git flux-system -n flux-system
flux reconcile kustomization flux-system -n flux-system --with-source
flux reconcile kustomization apps-dfc-gke-prod -n flux-system --with-source
kubectl -n dfc get httproute,deployment,pods
```

## Go-live order

1. **Publish OCI image** — merge `dockworker.toml` + `oci-build` workflow; ensure repo secrets `GCP_WIF_PROVIDER` and `GCP_WIF_SERVICE_ACCOUNT` match `stevedores-org/aivcs-human-in-the-loop`, then:
   ```bash
   gh workflow run oci-build --repo stevedores-org/data-fabric-connector
   ```
   Or push to `main` (tags include `0.1.0` per `Cargo.toml` / overlay).
2. **Bump tag + scale** — set replicas to `1` in infra-code overlay (`crossplane/gcp/hub/spoke/apps/dfc/overlays/gke/prod`)
3. **Wait for Ready** — `apps-dfc-gke-prod` + `platform-gke-gateway`
4. **Smoke:**

```bash
curl -sS https://dfc.aivcs.io/healthz
curl -sS https://dfc.aivcs.io/v1/version   # expect fqdn + public_url
```

Expected `/v1/version` fields include `"fqdn": "dfc.aivcs.io"` and `"public_url": "https://dfc.aivcs.io"`.

## Local / dev

```bash
cargo run -p dfc-server
curl localhost:8080/healthz
```

Mock upstreams are the E1 default — no cluster DNS required.

## Files (this repo)

| File | Purpose |
|------|---------|
| `base/k8s/dfc.yaml` | Namespace, Deployment, Service, ConfigMap (reference) |
| `base/k8s/httproute.yaml` | Gateway API route for `dfc.aivcs.io` (reference) |

## Related

- App: [stevedores-org/data-fabric-connector](https://github.com/stevedores-org/data-fabric-connector)
- GitOps SoT: [lornu-ai/infra-code](https://github.com/lornu-ai/infra-code)
