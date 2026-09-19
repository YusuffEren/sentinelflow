# =============================================================================
# SentinelFlow - Export OpenAPI Schema + Frontend Types
# =============================================================================
"""
Export the FastAPI OpenAPI schema and generate TypeScript types for the frontend.

Usage:
    python scripts/export_openapi.py [--check]

- Default: writes sentinelflow-web/src/lib/api-schema.ts
- --check: exits non-zero if the committed file is stale (used in CI).

Requires: fastapi, pydantic (the app module must import without live services).
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
OUT_TS = ROOT / "sentinelflow-web" / "src" / "lib" / "api-schema.ts"

sys.path.insert(0, str(SRC))


def build_schema() -> dict:
    from sentinelflow.api.app import app

    return app.openapi()


def write_typescript(schema: dict, check: bool) -> int:
    header = (
        "// =============================================================================\n"
        "// AUTO-GENERATED - do not edit by hand.\n"
        "// Source: FastAPI OpenAPI schema. Regenerate with:\n"
        "//   python scripts/export_openapi.py\n"
        "// =============================================================================\n"
    )
    body = json.dumps(schema, indent=2, ensure_ascii=False)
    content = f"{header}\nexport const apiSchema = {body} as const;\n\nexport type ApiSchema = typeof apiSchema;\n"

    if check:
        existing = OUT_TS.read_text(encoding="utf-8") if OUT_TS.exists() else ""
        if existing != content:
            print(
                "ERROR: sentinelflow-web/src/lib/api-schema.ts is stale.\n"
                "Run `python scripts/export_openapi.py` and commit the result."
            )
            return 1
        print("api-schema.ts is up to date.")
        return 0

    OUT_TS.parent.mkdir(parents=True, exist_ok=True)
    OUT_TS.write_text(content, encoding="utf-8")
    print(f"Wrote {OUT_TS} ({len(body)} bytes)")

    # Best-effort prettier formatting (skipped if prettier is unavailable)
    try:
        subprocess.run(
            ["npx", "--yes", "prettier", "--write", str(OUT_TS)],
            cwd=ROOT / "sentinelflow-web",
            check=False,
            capture_output=True,
            timeout=120,
        )
    except Exception:
        pass
    return 0


def main() -> int:
    check = "--check" in sys.argv
    schema = build_schema()
    return write_typescript(schema, check)


if __name__ == "__main__":
    sys.exit(main())
