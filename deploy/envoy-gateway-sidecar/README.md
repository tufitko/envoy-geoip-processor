# Sidecar deployment with EnvoyPatchPolicy

Runs geoip-processor as a **sidecar container inside the Envoy proxy pods** of
Envoy Gateway, wired over `127.0.0.1` — no extra Deployment, no network hop.
Compare with the default setup (separate Deployment + `EnvoyExtensionPolicy`,
see `charts/geoip-processor`), which is simpler to operate and uses only
stable APIs.

## How it fits together

```
Envoy proxy pod (managed by Envoy Gateway)
├── envoy            ── ext_proc filter ──► 127.0.0.1:9000
└── geoip-processor  ── downloads/refreshes MaxMind DBs, answers lookups
```

| File | What it does |
|------|--------------|
| `geoip-configmap.yaml` | Sidecar config (in `envoy-gateway-system`, where proxy pods run) |
| `envoyproxy.yaml` | `EnvoyProxy` with a StrategicMerge patch adding the sidecar + volumes |
| `gateway.yaml` | Gateway referencing the `EnvoyProxy` via `infrastructure.parametersRef` |
| `envoy-patch-policy.yaml` | `EnvoyPatchPolicy`: localhost cluster + ext_proc filter in the HCM chain |

## Setup

1. Enable the EnvoyPatchPolicy extension API (off by default):

   ```yaml
   # envoy-gateway-config ConfigMap, envoy-gateway.yaml key
   extensionApis:
     enableEnvoyPatchPolicy: true
   ```

   ```bash
   kubectl rollout restart deployment envoy-gateway -n envoy-gateway-system
   ```

2. Create the MaxMind credentials secret in the proxy pods' namespace:

   ```bash
   kubectl create secret generic maxmind -n envoy-gateway-system \
     --from-literal=license='<account_id>:<license_key>'
   ```

3. Apply the manifests (edit image, namespaces and Gateway/listener names first):

   ```bash
   kubectl apply -f geoip-configmap.yaml -f envoyproxy.yaml -f gateway.yaml -f envoy-patch-policy.yaml
   ```

4. Verify: `kubectl get envoypatchpolicy -n default` must show `Accepted` and
   `Programmed`; requests through the Gateway should carry `x-geoip-*` headers
   upstream.

## Things to adjust

- **Listener name** in `envoy-patch-policy.yaml`: xDS listeners are named
  `<gateway-namespace>/<gateway-name>/<listener-name>` (here `default/eg/http`).
  One filter patch per listener that should get geoip headers.
- **Ports**: Gateway listener ports above 1024 map 1:1 to proxy containerPorts,
  so the sidecar avoids common ones (gRPC `:9000`, admin `:9902`). If a
  listener uses one of those, move the sidecar ports in both the ConfigMap and
  `envoyproxy.yaml`.
- **Cache is per-pod** (`emptyDir`): every proxy replica downloads its own DB
  copy and re-downloads on pod restart (conditional requests + retries make
  this cheap).

## Caveats

- `EnvoyPatchPolicy` patches raw xDS and is documented as an **unstable API**:
  generated resource names/structure may change between Envoy Gateway
  versions. Re-check patches after upgrades. Envoy Gateway >= v1.3 is
  assumed (for `source.address` request attributes support in ext_proc).
- The sidecar's readiness gates the whole proxy pod: a pod won't serve until
  required DBs are loaded. With `failure_mode_allow: true` in the filter,
  runtime lookup failures stay fail-open exactly like the standalone setup.
