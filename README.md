# envoy-geoip-processor

An [Envoy `ext_proc`](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/ext_proc_filter) gRPC
service that enriches HTTP requests with geoip headers, resolved from
[MaxMind](https://www.maxmind.com/) `.mmdb` databases (GeoIP2 / GeoLite2). It plays the same role for
Envoy / Envoy Gateway that [`ngx_http_geoip2_module`](https://github.com/leev/ngx_http_geoip2_module)
plays for nginx: pick a client IP, look it up, set headers — but as an external process, with
auto-downloaded and auto-refreshed databases and a configurable header mapping.

## Features

- **Auto-download & refresh** — each configured database is fetched over `https://`, `http://`, or
  `s3://bucket/key` and re-checked on its own `check_interval`. HTTP downloads use conditional
  requests (`If-None-Match` / `If-Modified-Since`); S3 downloads compare the object's `ETag` via
  `HeadObject` first. A `tar.gz` payload (as served by MaxMind's download API) is detected by its
  gzip magic bytes and the first `*.mmdb` member inside is extracted automatically. Transient HTTP
  failures (network errors, 5xx) are retried up to 3 times with backoff, so a briefly unavailable
  origin at startup doesn't delay the first load until the next `check_interval`.
- **Zero-downtime reload** — a new database is swapped in atomically (`atomic.Pointer`); the
  previous reader is kept open for one more minute so in-flight lookups never see a closed file.
- **Arbitrary header mapping** — each output header is declared as `{db, path, default}`; `path` is
  a dot-separated walk into the mmdb record (numeric segments are array indices, e.g.
  `subdivisions.0.iso_code`), with an optional static fallback value.
- **Ordered client-IP resolution** — `ip_sources` is a chain tried in order: request headers (first
  element of a comma-separated list, e.g. `X-Forwarded-For`) and/or Envoy's `source.address`
  connection attribute. The first entry that parses as an IP wins.
- **Fail-open by design** — a lookup error, a database not yet loaded, or a resolvable-but-unknown
  IP never blocks the request: the header is simply omitted (or removed, in overwrite mode, to
  prevent a client from spoofing it). Envoy itself is configured with `failure_mode_allow: true` /
  `failOpen: true`, so an unreachable or slow processor doesn't affect the request path either.
- **Readiness gate** — `/readyz` returns `503` until every database marked `required: true` has
  loaded at least once, so orchestrators hold traffic back until geoip data is actually available.
- **Prometheus metrics** for database refresh, lookups, and IP resolution (see below).

### Full header set example

`examples/config.yaml` maps the complete GeoLite2 City + ASN field set:

| Header | Database | Path | Value |
|---|---|---|---|
| `x-geoip-country-code` | city | `country.iso_code` | ISO country code, e.g. `GB` |
| `x-geoip-country-name` | city | `country.names.en` | Country name, e.g. `United Kingdom` |
| `x-geoip-region` | city | `subdivisions.0.iso_code` | Top subdivision ISO code |
| `x-geoip-region-name` | city | `subdivisions.0.names.en` | Top subdivision name |
| `x-geoip-city` | city | `city.names.en` | City name |
| `x-geoip-latitude` | city | `location.latitude` | Latitude |
| `x-geoip-longitude` | city | `location.longitude` | Longitude |
| `x-geoip-postal-code` | city | `postal.code` | Postal code |
| `x-geoip-timezone` | city | `location.time_zone` | IANA timezone |
| `x-geoip-asn` | asn | `autonomous_system_number` | Autonomous system number |
| `x-geoip-org` | asn | `autonomous_system_organization` | AS organization name |

Any header not resolvable for a given IP (no match, no database loaded, path missing) is left
unset (or removed, see [Fail-open behavior](#fail-open-behavior-and-readiness)) unless the rule
declares a `default`.

## Quickstart

The compose stack in `deploy/compose/` wires: an nginx server serving the test `.mmdb` files from
`testdata/`, the `geoip-processor` itself, an `envoy` proxy with the `ext_proc` filter enabled, and
an `http-https-echo` upstream that reflects request headers back as JSON.

```bash
docker compose -f deploy/compose/docker-compose.yaml up -d --build
```

Send a request through Envoy (port `10000`) with a client IP set via `x-real-ip` and inspect the
geoip headers the echo backend received:

```bash
curl -s -H 'x-real-ip: 2.125.160.216' localhost:10000/ \
  | python3 -c "import json,sys; h=json.load(sys.stdin)['headers']; print(h.get('x-geoip-country-code'), h.get('x-geoip-city'))"
# GB Boxford

curl -s -H 'x-real-ip: 1.128.0.0' localhost:10000/ \
  | python3 -c "import json,sys; h=json.load(sys.stdin)['headers']; print(h.get('x-geoip-asn'), h.get('x-geoip-org'))"
# 1221 Telstra Pty Ltd
```

Tear down with:

```bash
docker compose -f deploy/compose/docker-compose.yaml down
```

The processor's own endpoints are on `deploy/compose/config.yaml`'s `listen.admin` port
(`:8080`, not published by the compose file) — see [Metrics](#metrics) below for what's on
`/healthz`, `/readyz`, and `/metrics`.

## Running the binary

```bash
go build -o geoip-processor ./cmd/geoip-processor
./geoip-processor -config /etc/geoip/config.yaml
```

The only flag is `-config` (default `/etc/geoip/config.yaml`), the path to the YAML config
described below. The config file is parsed strictly — unknown fields are rejected.

## Config reference

All fields, from `internal/config/config.go`:

| Field | Type | Default | Notes |
|---|---|---|---|
| `listen.grpc` | string | `:9000` | `ext_proc` gRPC listener address |
| `listen.admin` | string | `:8080` | HTTP listener for `/healthz`, `/readyz`, `/metrics` |
| `cache_dir` | string | `/var/cache/geoip` | Directory for cached `<db>.mmdb` + `<db>.meta.json` files |
| `ip_sources` | list of `{header}` or `{envoy}` | — | **Required**, non-empty. Ordered client-IP resolution chain. `header: <name>` reads a lowercased request header (comma-separated value → first element used); `envoy: source_address` reads the downstream connection attribute (needs Envoy Gateway ≥ v1.3, see below). Exactly one of `header`/`envoy` per entry. See [Security note](#security-note-on-ip_sources) below. |
| `overwrite` | bool | `true` | `true`: geoip headers always replace/are added (`OVERWRITE_IF_EXISTS_OR_ADD`), and an unresolvable header is actively removed if the client sent one (anti-spoofing). `false`: only added if absent (`ADD_IF_ABSENT`); a client-sent header is left untouched. |
| `databases.<name>.source` | string | — | **Required**. `https://…`, `http://…`, or `s3://bucket/key`. `s3://` uses the default AWS credential chain (env vars, IRSA, shared config). |
| `databases.<name>.auth.basic_env` | string | — | Name of an env var holding `user:password` for HTTP Basic auth on `source` (e.g. `MAXMIND_LICENSE`, see below). Ignored for `s3://`. |
| `databases.<name>.check_interval` | duration | `6h` | Refresh check period (±10% jitter applied at runtime); must parse as a positive Go duration (e.g. `6h`, `1m`). |
| `databases.<name>.required` | bool | `false` | If `true`, this database must have loaded at least once for `/readyz` to return `200`. |
| `headers.<name>.db` | string | — | **Required**; must reference a key under `databases`. Header names are lowercased. |
| `headers.<name>.path` | string | — | **Required**. Dot-separated path into the mmdb record; numeric segments index arrays (e.g. `subdivisions.0.iso_code`). A path resolving to an object/array (not a scalar) is treated as a lookup error. |
| `headers.<name>.default` | string \| null | `null` | Static fallback used when the path lookup misses, the IP isn't found, or the database isn't loaded yet. |

Minimal example (`deploy/compose/config.yaml`, used by the Quickstart stack):

```yaml
listen:
  grpc: :9000
  admin: :8080
cache_dir: /tmp/geoip-cache
ip_sources:
  - header: x-real-ip
  - header: x-forwarded-for
  - envoy: source_address
databases:
  city:
    source: http://dbserver/GeoIP2-City-Test.mmdb
    check_interval: 1m
    required: true
  asn:
    source: http://dbserver/GeoLite2-ASN-Test.mmdb
    check_interval: 1m
headers:
  x-geoip-country-code: {db: city, path: country.iso_code}
  x-geoip-city:         {db: city, path: city.names.en}
  x-geoip-timezone:     {db: city, path: location.time_zone}
  x-geoip-latitude:     {db: city, path: location.latitude}
  x-geoip-asn:          {db: asn, path: autonomous_system_number}
  x-geoip-org:          {db: asn, path: autonomous_system_organization}
```

For downloading real MaxMind databases, see `examples/config.yaml`, which points `source` at
MaxMind's download API and reads credentials from `MAXMIND_LICENSE`:

```yaml
databases:
  city:
    source: https://download.maxmind.com/geoip/databases/GeoLite2-City/download?suffix=tar.gz
    auth:
      basic_env: MAXMIND_LICENSE   # "account_id:license_key"
    check_interval: 6h
    required: true
```

`MAXMIND_LICENSE` must be set to `account_id:license_key` (a MaxMind account ID and license key,
colon-separated) — the same credential pair used with `geoipupdate`. It is sent as the HTTP Basic
auth header on every request to `source`.

### Security note on `ip_sources`

Header-based `ip_sources` entries are client-controlled unless a trusted edge proxy sets or strips
them before the request reaches Envoy. Only list headers your edge proxy actually sanitizes
(overwrites or removes on the way in) — never a header an end client can set directly. In
particular, the leftmost value of `x-forwarded-for` is spoofable by the client itself unless your
edge is guaranteed to be the first hop to append to (or replace) that header. Where Envoy is the
internet-facing edge, prefer `envoy: source_address`, which reads the actual TCP connection's
address rather than trusting any header.

## Fail-open behavior and readiness

- **Filter level**: the Envoy `ext_proc` HTTP filter is configured with `failure_mode_allow: true`
  (compose, `deploy/compose/envoy.yaml`) / `failOpen: true` (Helm chart default). If the processor
  is unreachable or exceeds `message_timeout`, Envoy forwards the request unmodified instead of
  failing it.
- **Processor level**: `Processor.mutate` never returns an error. Any problem — IP not resolved,
  database not loaded yet, IP not found in the database, or a lookup error — simply results in that
  header being omitted; when `overwrite: true`, a client-supplied header with the same name is
  additionally stripped instead of being trusted verbatim.
- **Readiness gate**: `GET /readyz` on the admin listener returns `200` once every `required: true`
  database has a loaded reader, and `503` otherwise (`"databases not loaded"`). `GET /healthz`
  always returns `200` (liveness only). Both are wired into the Helm chart's `readinessProbe` /
  `livenessProbe`.

## Deploy (Helm)

The chart lives at `charts/geoip-processor` (`geoip-processor`, appVersion `0.1.0`). It renders a
`ConfigMap` (the entire `.Values.config` tree becomes `/etc/geoip/config.yaml`), a `Deployment`
(2 replicas by default, ports `grpc`/9000 and `admin`/8080, config mounted at `/etc/geoip`, cache
in an `emptyDir` at `/var/cache/geoip`, readiness/liveness probes on `/readyz` / `/healthz`), a
`Service` exposing both ports, and — when enabled — an `EnvoyExtensionPolicy` for
[Envoy Gateway](https://gateway.envoyproxy.io/).

```bash
kubectl create secret generic maxmind --from-literal=license='<account_id>:<license_key>'
```

Create a values override (`my-values.yaml`) with the config from `examples/config.yaml`, the
`MAXMIND_LICENSE` env var sourced from that secret, and the extension policy enabled:

```yaml
env:
  - name: MAXMIND_LICENSE
    valueFrom: {secretKeyRef: {name: maxmind, key: license}}

config:
  listen: {grpc: ":9000", admin: ":8080"}
  cache_dir: /var/cache/geoip
  ip_sources:
    - header: x-real-ip
    - header: x-forwarded-for
    - envoy: source_address
  databases:
    city:
      source: https://download.maxmind.com/geoip/databases/GeoLite2-City/download?suffix=tar.gz
      auth: {basic_env: MAXMIND_LICENSE}
      check_interval: 6h
      required: true
    asn:
      source: https://download.maxmind.com/geoip/databases/GeoLite2-ASN/download?suffix=tar.gz
      auth: {basic_env: MAXMIND_LICENSE}
      check_interval: 6h
  headers:
    x-geoip-country-code: {db: city, path: country.iso_code}
    x-geoip-city:         {db: city, path: city.names.en}
    x-geoip-asn:          {db: asn, path: autonomous_system_number}
    x-geoip-org:          {db: asn, path: autonomous_system_organization}

envoyExtensionPolicy:
  enabled: true
  targetRef: {group: gateway.networking.k8s.io, kind: Gateway, name: <your-gateway-name>}
```

```bash
helm install geoip charts/geoip-processor -f my-values.yaml
```

Key `values.yaml` fields:

| Field | Default | Notes |
|---|---|---|
| `image.repository` / `image.tag` / `image.pullPolicy` | `geoip-processor` / `latest` / `IfNotPresent` | |
| `replicas` | `2` | |
| `resources` | `requests: {cpu: 100m, memory: 128Mi}`, `limits: {memory: 512Mi}` | |
| `env` | `[]` | Extra container env vars, e.g. `MAXMIND_LICENSE` from a `secretKeyRef` |
| `config` | see `values.yaml` | Rendered verbatim as `/etc/geoip/config.yaml`; use the full schema from [Config reference](#config-reference) |
| `envoyExtensionPolicy.enabled` | `false` | Set `true` to attach the processor to a Gateway |
| `envoyExtensionPolicy.targetRef.{group,kind,name}` | `gateway.networking.k8s.io` / `Gateway` / `my-gateway` | The Gateway to attach `ext_proc` to |
| `envoyExtensionPolicy.failOpen` | `true` | Passed through to `EnvoyExtensionPolicy.spec.extProc[].failOpen` |
| `envoyExtensionPolicy.messageTimeout` | `200ms` | Passed through to `.spec.extProc[].messageTimeout` |
| `envoyExtensionPolicy.requestAttributes` | `[source.address]` | Passed through to `.spec.extProc[].processingMode.request.attributes` |

**Envoy Gateway ≥ v1.3 is required** if `envoyExtensionPolicy.requestAttributes` (or the
`envoy: source_address` entry in `ip_sources`) is used: `processingMode.request.attributes` — the
field that lets Envoy Gateway forward `source.address` to the `ext_proc` service as a request
attribute — was only added to `EnvoyExtensionPolicy` in that release. On an older Envoy Gateway,
drop `envoy: source_address` from `ip_sources` (and the `requestAttributes` entry) and rely solely
on header-based IP sources (`x-real-ip`, `x-forwarded-for`, ...).

## Deploy (sidecar + EnvoyPatchPolicy)

Alternative topology: run the processor as a **sidecar inside the Envoy proxy pods** and wire the
`ext_proc` filter to `127.0.0.1` with an `EnvoyPatchPolicy` — no separate Deployment and no extra
network hop, at the cost of relying on the (explicitly unstable) xDS-patching API. Full manifests
and a walkthrough live in [`deploy/envoy-gateway-sidecar/`](deploy/envoy-gateway-sidecar/README.md).

## Metrics

Exposed via Prometheus text format at `GET /metrics` on the admin listener (default `:8080`),
alongside the standard Go/process collectors:

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `geoip_db_update_total` | counter | `db`, `result` | Database refresh attempts. `result` ∈ `updated`, `unchanged`, `invalid` (downloaded file failed to open as mmdb), `error` (fetch/rename failure). |
| `geoip_db_loaded_timestamp_seconds` | gauge | `db` | Unix time the database reader was last (re)loaded — from cache at startup and on every successful refresh. |
| `geoip_db_last_check_timestamp_seconds` | gauge | `db` | Unix time of the last completed update check, regardless of outcome (updated, unchanged, invalid, or error) — useful for alerting on a refresh loop that's stopped running. |
| `geoip_lookups_total` | counter | `db`, `result` | Per-header-rule lookup outcomes. `result` ∈ `hit`, `miss` (IP or path not found), `error`. |
| `geoip_requests_total` | counter | `result` | Processed request-header messages by IP resolution outcome. `result` ∈ `found`, `not_found`. |

Health/readiness endpoints on the same listener:

- `GET /healthz` — always `200` (process liveness).
- `GET /readyz` — `200` once all `required: true` databases have loaded, `503` otherwise.
