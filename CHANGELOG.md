# Changelog

All notable changes to this project are documented in this file.
Format based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/);
versioning follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- JWT/RBAC enforcement on all data routes (`viewer` / `analyst` / `admin`), incl. `X-API-Key` fallback for transaction ingestion (`tests/test_api_auth.py`).
- `POST /api/v1/kyc/screen` endpoint backed by `CombinedScreener`.
- CORS whitelist via `CORS_ORIGINS` (wildcard mode disables credentials).
- Frontend Docker image (multi-stage, standalone) + compose `frontend` profile fix.
- Team workflow: `CONTRIBUTING.md`, PR template, `CODEOWNERS`, `.editorconfig`, pre-commit hooks, API contract gate (`scripts/export_openapi.py`), coverage gate (`--cov-fail-under=45`), `tsc --noEmit` in CI.
- Integration smoke tests for Redis geo (impossible travel) and Neo4j ring detection (`tests/test_integration_services.py`, marker `integration`); the CI integration job now runs real tests instead of selecting zero.

### Changed
- Required secrets are now fail-fast: `POSTGRES_PASSWORD`, `NEO4J_PASSWORD`, `JWT_SECRET_KEY` (no silent defaults in app, detector, or compose).
- Dependencies pruned to what is actually imported (dropped spacy, geopy, haversine, pyvis, slowapi, asyncio-throttle, aiofiles, hyperopt, tensorboard and the unused `[mlops]` extra).
- Version unified at `2.1.0` across `pyproject.toml`, package `__version__`, the API and the Docker image.
- `docs/api.md` and `docs/architecture.md` rewritten from the real route table and module layout (previous drafts described non-existent modules and services).
- `seed_admin.py`: no default admin password; auto-generates unless `SEED_ADMIN_PASSWORD` or `--no-generate`.
- Alembic migration 002 no longer inserts a well-known admin user (use `seed_admin.py`).
- README synced with actual routes, roles, versions and test counts.

### Removed
- Dead CLI subcommands (`detectors graph|geo`) targeting non-existent entry points.
- Echo-only CI jobs (`model-version`, `deploy-staging`, `deploy-production`).
- Stray root launcher scripts (`*.bat`, `test_api.py`, `verify_kafka.py`, `scripts/live_test.py` — stale ports/paths), unused Next.js placeholder SVGs and the dead `src/sentinelflow/security/` duplicate-auth package with its tests.
- Dead CI/config options: ML pipeline `data/**`/`models/**` push triggers (gitignored), the `deploy` dispatch option, `MLFLOW_TRACKING_URI`, and the phantom `sentinelflow-detector` Prometheus target.

### Fixed
- Ingestion path works end-to-end again: `requests` is now a declared dependency (kafka ingestor / http generator / replay scripts crashed on clean installs), the Docker builder copies `README.md` (hatchling metadata), and ingestion clients send `X-API-Key` (the endpoint 401'd everything since RBAC).
- Compose passes `SENTINELFLOW_API_KEY` + `CORS_ORIGINS` to the api container and auto-creates the Neo4j schema on first boot (`init_neo4j_schema.cypher` mounted into `/docker-entrypoint-initdb.d`).
- CI: pin `bcrypt>=4.0.1,<4.1` (passlib 1.7.4 incompatible with bcrypt>=4.1 — broke JWT tests), coverage floor set to 45%.
- CI: pin black (`>=26.5,<27`) and ruff (`>=0.15.22,<0.16`) to same ranges in pyproject, CI and pre-commit — formatter minor versions changed formatting and caused lint drift; `tool.black.target-version` pinned to `py310` to silence the 3.11-runner AST safety warning.
- Frontend called non-existent endpoints (`/graph/nodes`, `/graph/edges`, `/ml/predict`, `/ml/info`); aligned with backend + generated contract snapshot.

### Security
- Removed all hardcoded credentials from repo (incl. bcrypt hash of `Admin123!` in migration history and a Neo4j password in `init_neo4j_schema.cypher` comments).
- JWT: the silent insecure-key fallback is gone; `JWT_SECRET_KEY` unset now fails at startup and at every token operation.
- CI security scans are blocking gates (bandit + pip-audit, scanning the real dependency tree); MD5 fingerprinting/bucketing marked `usedforsecurity=False`; pickle/torch loads of self-saved artifacts annotated with justified `# nosec`.
