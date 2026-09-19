## Summary

<!-- What changed and why (1-3 sentences). Link ADRs if relevant: docs/adr/NNNN-title.md -->

## Type

<!-- Check one: feat / fix / docs / test / chore / refactor / perf / ci -->

- [ ] feat
- [ ] fix
- [ ] docs / chore / refactor / other

## Checklist

- [ ] Conventional Commits format (`type(scope): summary`)
- [ ] `CONTRIBUTING.md` local quality gate passed (`black`, `ruff`, `mypy`, `pytest`, `npm run lint`, `tsc --noEmit`)
- [ ] Tests added/updated; coverage gate still green
- [ ] No secrets committed (`.env` untouched, no hardcoded credentials)
- [ ] Docs updated (`README.md` / `docs/` / `.env.example` if behavior or config changed)

## API contract (backend PRs only)

- [ ] No route/request/response changes — schema untouched
- [ ] Routes changed → `python scripts/export_openapi.py` run and `sentinelflow-web/src/lib/api-schema.ts` regenerated
- [ ] Breaking change → marked below, `/v1` untouched, migration path described

## Breaking changes

<!-- None / describe impact + migration -->

## Test evidence

<!-- Paste pytest / npm output summary -->

## Linked issues

<!-- Closes #<n> -->
