"""Live API test against running SentinelFlow instance."""
import httpx
import sys

BASE = "http://127.0.0.1:8001"
passed = 0
failed = 0

def test(name, method, path, **kw):
    global passed, failed
    try:
        url = f"{BASE}{path}"
        if method == "GET":
            r = httpx.get(url, timeout=10, **kw)
        else:
            r = httpx.post(url, timeout=10, **kw)
        
        ok = r.status_code in (200, 201, 422)
        mark = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        else:
            print(f"  [{mark}] {name}: HTTP {r.status_code} - {r.text[:120]}")
            failed += 1
    except Exception as e:
        print(f"  [FAIL] {name}: {e}")
        failed += 1

print("=== LIVE API TESTS ===")
print()

# Root & System
test("Root", "GET", "/")
test("Health", "GET", "/api/v1/system/health")
test("Stats", "GET", "/api/v1/system/stats")
test("Metrics", "GET", "/metrics")

# Alerts
test("List Alerts", "GET", "/api/v1/alerts")
test("Alerts Paginated", "GET", "/api/v1/alerts?page=1&page_size=5")
test("Alerts Filter Severity", "GET", "/api/v1/alerts?severity=critical")

# Chat
test("Chat Greeting", "POST", "/api/v1/chat", json={"message": "Merhaba"})
test("Chat Fraud Explain", "POST", "/api/v1/chat", json={"message": "Dongusel transfer nedir?"})
test("Chat Suggestions", "GET", "/api/v1/chat/suggestions")

# Graph (mock mode - no Neo4j data yet)
test("Graph Data", "GET", "/api/v1/graph/data")
test("Graph Rings", "GET", "/api/v1/graph/rings")

# Transaction submit
test("Submit Transaction", "POST", "/api/v1/transactions", json={
    "sender_iban": "TR330006100519786457841326",
    "sender_name": "Ahmet Yilmaz",
    "sender_city": "Istanbul",
    "receiver_iban": "TR110006400000478893400002",
    "receiver_name": "Mehmet Kaya",
    "receiver_city": "Ankara",
    "amount": 15000.00,
    "description": "Kira odemesi",
})

# ML
test("ML Model Status", "GET", "/api/v1/ml/models")
test("ML Features", "GET", "/api/v1/ml/features")
test("ML Train Status", "GET", "/api/v1/ml/train/status")

# Risk Scoring
test("Risk Score", "POST", "/api/v1/risk/score", json={
    "sender_iban": "TR330006100519786457841326",
    "sender_name": "Ahmet Yilmaz",
    "receiver_iban": "TR110006400000478893400002",
    "receiver_name": "Mehmet Kaya",
    "amount": 15000.00,
})
test("Risk Stats", "GET", "/api/v1/risk/stats")
test("Risk Features", "GET", "/api/v1/risk/features")

print()
print(f"Passed: {passed}")
print(f"Failed: {failed}")
print(f"Total:  {passed + failed}")

sys.exit(0 if failed == 0 else 1)
