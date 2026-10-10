# Security

## Reporting a vulnerability

Email security@meilisearch.com with a description, reproduction steps and the commit or image tag. Do not open a public issue. You will get an acknowledgement within 3 business days and a fix or mitigation plan within 30 days for confirmed issues.

## Supported versions

Only the latest tagged release (`ghcr.io/qdequele/glutony:latest` and its semver tag) receives fixes.

## Deployment notes

- The control plane is internal: keep port 9000 off any public network and set `CONTROL_PLANE_TOKEN` on every service (`CONTROL_PLANE_TOKEN_DISABLED=true` is for local development only).
- `ENVOY_TRUSTED_HEADER` must be set wherever `X-Meili-*` headers can come from untrusted clients; `LAB_SERVICE_TOKEN` refuses to start without it.
- `SOURCE_SECRET_KEY` seals stored Meilisearch keys and source credentials; rotating it makes stored secrets unreadable.
- `LAB_INSTANCE_SECRET` is minted and rotated by the Meilisearch Lab; revoke it there if it leaks.
- Tenant jobs can only write with their own tenant's connections; global connections are templates.
