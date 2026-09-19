# SentinelFlow — Contribution Guide

Welcome! This guide keeps multi-developer work traceable and scalable.
Read it once before your first PR.

## 1. Branching

| Branch | Purpose | Protection |
|---|---|---|
| `main` | Production-ready. Only via PR from `develop`. | Required PR review (1+), CI green, no force-push |
| `develop` | Integration branch for the next release. | PR review required, CI green |
| `feature/<scope>-<short-desc>` | Your work, e.g. `feature/kyc-batch-screen` | Rebase onto `develop` before opening a PR |

Hotfixes: `hotfix/<desc>` branched from `main`, merged back to **both** `main` and `develop`.

## 2. Commits (Conventional Commits — required for changelog)

```
<type>(<scope>): <short imperative summary>

feat(api): add batch KYC screening endpoint
fix(detector): handle empty IBAN in geo check
docs(readme): sync endpoint table with routes
test(auth): cover analyst-only dismiss
chore(ci): raise coverage gate to 75
refactor(ml): split ensemble weights config
```

Types: `feat fix docs test chore refactor perf ci build`.

## 3. Pull Requests

- Fill the PR template completely; link issues with `Closes #<n>`.
- Keep PRs small (< ~400 changed lines). Split refactors from behavior changes.
- CI must be green: **lint + typecheck, tests, contract check, frontend build**.
- One domain owner review is required (see `CODEOWNERS`).
- Squash-merge into `develop`; `develop` → `main` only via a **release PR** with changelog entry.

## 4. Local quality gate (run before pushing)

```bash
# Python (from repo root)
black --check src/ tests/
ruff check src/ tests/
mypy src/sentinelflow --ignore-missing-imports
pytest tests/ -q

# Frontend (from sentinelflow-web/)
npm run lint
npx tsc --noEmit
```

Or install the pre-commit hooks once: `pre-commit install`.

## 5. Secrets & config

- Never commit secrets. Required env vars are documented in `.env.example`
  (`POSTGRES_PASSWORD`, `NEO4J_PASSWORD`, `JWT_SECRET_KEY`).
- The app **fail-fasts** on missing secrets by design — no silent defaults.
- New config must go through `src/sentinelflow/config/settings.py` (backend)
  or `NEXT_PUBLIC_*` build args (frontend), and be documented in `.env.example`.

## 6. API contract discipline

- FastAPI OpenAPI schema is the single source of truth.
- After changing any route/request/response, run:
  `python scripts/export_openapi.py`
  and commit the regenerated `sentinelflow-web/src/lib/api-schema.ts`.
- CI fails if the committed schema is stale.
- Frontend must use `config.endpoints.*` from `src/lib/config.ts` — no hardcoded URLs.
