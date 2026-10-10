# Security

## Reporting a vulnerability

Email security@meilisearch.com with a description, reproduction steps and the commit or image tag. Do not open a public issue. You will get an acknowledgement within 3 business days and a fix or mitigation plan within 30 days for confirmed issues.

## Supported versions

Only the latest tagged release (`ghcr.io/qdequele/glutony:latest` and its semver tag) receives fixes.

## Deployment notes

- The control plane is internal. `CONTROL_PLANE_TOKEN` guards its `/internal/*` routes only: `/pipelines`, `/jobs` and `/plugins` on port 9000 have no authentication, so port 9000 must never be reachable from outside the cluster network. Set `CONTROL_PLANE_TOKEN` on every service (`CONTROL_PLANE_TOKEN_DISABLED=true` is for local development and the one-time upgrade step only).
- `ENVOY_TRUSTED_HEADER` must be set wherever `X-Meili-*` headers can come from untrusted clients; the gateway refuses to start without it when `LAB_SERVICE_TOKEN` or `LAB_INSTANCE_*` is set.
- `k8s/secrets.example.yaml` is an example, never applied by the kustomization. The binaries refuse to boot on its `CHANGE_ME` for `CONTROL_PLANE_TOKEN`, `ADMIN_API_KEY` and `ENVOY_TRUSTED_HEADER`.
- `SOURCE_SECRET_KEY` seals stored Meilisearch keys and source credentials; keep a copy somewhere safe: losing or rotating it makes stored secrets unreadable.
- `LAB_SERVICE_TOKEN` is the Lab's credential on the gateway (`CREDENTIAL=` of `lab:hosted_engine:create`); never reuse `ADMIN_API_KEY` for it.
- `LAB_INSTANCE_SECRET` is minted and rotated by the Meilisearch Lab; revoke it there if it leaks.
- Tenant jobs can only write with their own tenant's connections; global connections are templates.
