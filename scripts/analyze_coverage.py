"""Analyze test coverage across modules."""
import os, re

test_dir = os.path.join(os.path.dirname(__file__), "..", "tests")
modules = [
    "api", "security", "ml", "compliance", "kyc",
    "generator", "ingestor", "monitoring", "mlops",
    "processor", "database", "repository", "auth",
    "dashboard", "detectors",
]

coverage = {}
for f in sorted(os.listdir(test_dir)):
    if not f.endswith(".py") or f == "conftest.py":
        continue
    path = os.path.join(test_dir, f)
    with open(path) as fh:
        content = fh.read()
    for m in modules:
        pattern = r"sentinelflow\." + re.escape(m)
        if re.search(pattern, content):
            coverage.setdefault(m, []).append(f)

print(f"{'Status':7s} | {'Module':15s} | {'Test Files'}")
print("-" * 60)
for m in modules:
    tests = coverage.get(m, [])
    status = "[OK]" if tests else "[--]"
    test_str = ", ".join(tests) if tests else "NO TESTS"
    print(f"{status:7s} | {m:15s} | {test_str}")
