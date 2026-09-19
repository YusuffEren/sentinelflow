# ADR 0002: JWT RBAC with fail-fast secrets

- Status: accepted
- Date: 2026-09-19

## Context

Originally only the ML route required auth; alerts, cases, graph and chat were
open despite the README claiming otherwise. Database/Neo4j credentials had
silent defaults committed to the repo, and migration 002 inserted a well-known
admin password. As more developers join, "it works on my machine" defaults and
implicit access control become unmanageable and unsafe.

## Decision

1. **RBAC**: every data route declares an auth dependency —
   `require_viewer` (read), `require_analyst` (investigate/mutate),
   `require_admin` (manage). `POST /api/v1/transactions` accepts either a JWT
   or `X-API-Key: SENTINELFLOW_API_KEY` for service-to-service ingestion.
2. **Fail-fast secrets**: `POSTGRES_PASSWORD`, `NEO4J_PASSWORD`,
   `JWT_SECRET_KEY` have no defaults. Missing secrets raise at startup
   (`ValueError`) or fail `docker compose` config (`${VAR:?...}`).
3. **No default users**: admins are created only via `scripts/seed_admin.py`
   with an operator-provided or auto-generated password.

## Consequences

- Frontend/demo flows must authenticate; local setup requires a populated
  `.env` (documented in README and `.env.example`).
- CI test suites override auth dependencies with a fake analyst
  (`tests/conftest.py`); real JWT behavior is covered in `tests/test_api_auth.py`.
- Leaked credentials in git history remain an issue for the existing repo
  history (rotation advised if ever deployed).

## Alternatives considered

- OAuth2/OIDC via external IdP (Keycloak): better for SSO, deferred until
  there is an identity provider to integrate with.
- Keeping silent dev defaults with warnings: rejected — warnings get ignored.
