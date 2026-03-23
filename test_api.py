"""Quick API endpoint test."""

import urllib.request, json

BASE = "http://127.0.0.1:8001"


def get(path):
    r = urllib.request.urlopen(f"{BASE}{path}")
    return json.loads(r.read())


def post(path, data):
    req = urllib.request.Request(
        f"{BASE}{path}", json.dumps(data).encode(), {"Content-Type": "application/json"}
    )
    r = urllib.request.urlopen(req)
    return json.loads(r.read())


# 1. Health
h = get("/api/v1/system/health")
print(f"HEALTH: status={h['status']}, components={list(h['components'].keys())}")

# 2. ML Models
m = get("/api/v1/ml/models")
print(f"MODELS: ensemble_ready={m['ensemble_ready']}")

# 3. Fraud transaction
res = post(
    "/api/v1/transactions",
    {
        "sender_iban": "TR33000610",
        "sender_name": "Test",
        "sender_city": "Istanbul",
        "receiver_iban": "TR11000640",
        "receiver_name": "Alici",
        "receiver_city": "Ankara",
        "amount": 250000,
        "description": "bitcoin acil transfer",
    },
)
print(f"FRAUD TX: is_fraud={res['is_fraud']}, score={res['fraud_score']}")

# 4. Clean transaction
res2 = post(
    "/api/v1/transactions",
    {
        "sender_iban": "TR33000610",
        "sender_name": "Ali",
        "sender_city": "Istanbul",
        "receiver_iban": "TR11000640",
        "receiver_name": "Veli",
        "receiver_city": "Istanbul",
        "amount": 500,
        "description": "kira odemesi",
    },
)
print(f"CLEAN TX: is_fraud={res2['is_fraud']}, score={res2['fraud_score']}")

# 5. Stats
s = get("/api/v1/system/stats")
print(f"STATS: processed={s['transactions_processed']}, fraud={s['fraud_detected']}")

# 6. Alerts
a = get("/api/v1/alerts")
print(f"ALERTS: total={a['total']}")

print("\n*** ALL ENDPOINTS OK ***")
