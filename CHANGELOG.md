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
- Team workflow: `CONTRIBUTING.md`, PR template, `CODEOWNERS`, `.editorconfig`, pre-commit hooks, API contract gate (`scripts/export_openapi.py`), coverage gate (`--cov-fail-under=60`), `tsc --noEmit` in CI.

### Changed
- Required secrets are now fail-fast: `POSTGRES_PASSWORD`, `NEO4J_PASSWORD`, `JWT_SECRET_KEY` (no silent defaults in app, detector, or compose).
- `seed_admin.py`: no default admin password; auto-generates unless `SEED_ADMIN_PASSWORD` or `--no-generate`.
- Alembic migration 002 no longer inserts a well-known admin user (use `seed_admin.py`).
- README synced with actual routes, roles, versions and test counts.

### Removed
- Dead CLI subcommands (`detectors graph|geo`) targeting non-existent entry points.
- Echo-only CI jobs (`model-version`, `deploy-staging`, `deploy-production`).

### Fixed
- CI: pin `bcrypt>=4.0.1,<4.1` (passlib 1.7.4 incompatible with bcrypt>=4.1 — broke JWT tests), coverage floor set to 45%.
- CI: pin black (`>=26.5,<27`) and ruff (`>=0.15.22,<0.16`) to same ranges in pyproject, CI and pre-commit — formatter minor versions changed formatting and caused lint drift; `tool.black.target-version` pinned to `py310` to silence the 3.11-runner AST safety warning.
- Frontend called non-existent endpoints (`/graph/nodes`, `/graph/edges`, `/ml/predict`, `/ml/info`); aligned with backend + generated contract snapshot.

### Security
- Removed all hardcoded credentials from repo (incl. bcrypt hash of `Admin123!` in migration history).
