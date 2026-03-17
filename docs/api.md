# SentinelFlow API Dökümantasyonu

## 🌐 Genel Bilgiler

**Base URL:** `http://localhost:8000/api/v1`

**Authentication:** Bearer Token (JWT)

**Content-Type:** `application/json`

---

## 🔐 Kimlik Doğrulama

### Token Alma

```http
POST /token
Content-Type: application/x-www-form-urlencoded

username=admin&password=supersecret
```

**Yanıt:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

### Kullanıcı Bilgisi

```http
GET /users/me
Authorization: Bearer <token>
```

**Yanıt:**
```json
{
  "username": "admin",
  "email": "admin@sentinelflow.dev",
  "full_name": "Admin User",
  "scopes": ["admin", "monitor", "detect", "train"]
}
```

---

## 📊 İşlem Analizi

### Tekil İşlem Analizi

```http
POST /transactions/analyze
Authorization: Bearer <token>
Content-Type: application/json

{
  "transaction_id": "TXN-2024-001",
  "sender_iban": "TR330006100519786457841326",
  "receiver_iban": "TR330006100519786457841327",
  "amount": 15000.00,
  "currency": "TRY",
  "timestamp": "2024-01-15T14:30:00Z",
  "description": "Transfer",
  "sender_name": "John Doe",
  "receiver_name": "Jane Smith",
  "sender_city": "Istanbul",
  "receiver_city": "Ankara",
  "latitude": 41.0082,
  "longitude": 28.9784
}
```

**Yanıt:**
```json
{
  "transaction_id": "TXN-2024-001",
  "is_fraud": false,
  "fraud_score": 0.15,
  "risk_level": "LOW",
  "detected_patterns": [],
  "model_predictions": {
    "isolation_forest": 0.12,
    "xgboost": 0.18,
    "autoencoder": 0.14
  },
  "explanation": {
    "top_features": [
      {"feature": "amount", "contribution": 0.05},
      {"feature": "hour_of_day", "contribution": 0.03}
    ],
    "human_readable": "İşlem normal parametreler içinde."
  },
  "processing_time_ms": 42
}
```

### Toplu İşlem Analizi

```http
POST /transactions/batch
Authorization: Bearer <token>
Content-Type: application/json

{
  "transactions": [
    {
      "transaction_id": "TXN-001",
      "sender_iban": "TR33...",
      "receiver_iban": "TR44...",
      "amount": 5000.00,
      ...
    },
    {
      "transaction_id": "TXN-002",
      ...
    }
  ]
}
```

**Yanıt:**
```json
{
  "total_processed": 2,
  "fraud_detected": 1,
  "results": [
    {"transaction_id": "TXN-001", "is_fraud": false, "fraud_score": 0.1},
    {"transaction_id": "TXN-002", "is_fraud": true, "fraud_score": 0.87}
  ],
  "processing_time_ms": 156
}
```

---

## 🚨 Alarmlar

### Alarm Listesi

```http
GET /alerts?status=active&severity=high&limit=50
Authorization: Bearer <token>
```

**Yanıt:**
```json
{
  "total": 12,
  "alerts": [
    {
      "alert_id": "ALT-2024-001",
      "transaction_id": "TXN-2024-099",
      "fraud_type": "circular_ring",
      "severity": "HIGH",
      "status": "active",
      "detected_at": "2024-01-15T14:35:00Z",
      "description": "5 hesap arasında döngüsel para transferi tespit edildi",
      "amount": 150000.00,
      "accounts_involved": ["TR33...", "TR44...", "TR55..."]
    }
  ]
}
```

### Alarm Detayı

```http
GET /alerts/{alert_id}
Authorization: Bearer <token>
```

### Alarm Güncelleme

```http
PATCH /alerts/{alert_id}
Authorization: Bearer <token>
Content-Type: application/json

{
  "status": "investigating",
  "assigned_to": "analyst_01",
  "notes": "Manuel inceleme başlatıldı"
}
```

---

## 🔗 Graf Analizi

### Döngüsel Halka Tespiti

```http
POST /graph/rings
Authorization: Bearer <token>
Content-Type: application/json

{
  "min_depth": 3,
  "max_depth": 6,
  "min_amount": 10000,
  "time_window_hours": 24
}
```

**Yanıt:**
```json
{
  "rings_detected": 2,
  "rings": [
    {
      "ring_id": "RING-001",
      "accounts": ["TR33...", "TR44...", "TR55...", "TR33..."],
      "total_amount": 250000.00,
      "transaction_count": 5,
      "first_transaction": "2024-01-15T10:00:00Z",
      "last_transaction": "2024-01-15T14:00:00Z",
      "risk_score": 0.95
    }
  ]
}
```

### Hesap Ağı Görselleştirme

```http
GET /graph/network/{iban}?depth=2
Authorization: Bearer <token>
```

**Yanıt:**
```json
{
  "center_account": "TR33...",
  "nodes": [
    {"id": "TR33...", "type": "account", "label": "Merkez Hesap"},
    {"id": "TR44...", "type": "account", "label": "Bağlantılı Hesap"}
  ],
  "edges": [
    {"source": "TR33...", "target": "TR44...", "amount": 15000, "count": 3}
  ]
}
```

---

## 📋 Uyum (Compliance)

### STR Rapor Listesi

```http
GET /compliance/str?status=pending
Authorization: Bearer <token>
```

**Yanıt:**
```json
{
  "total": 5,
  "reports": [
    {
      "report_id": "STR-2024-001",
      "transaction_id": "TXN-2024-099",
      "status": "pending",
      "created_at": "2024-01-15T15:00:00Z",
      "fraud_type": "circular_ring",
      "reason": "Döngüsel para transferi şüphesi"
    }
  ]
}
```

### STR Rapor Gönderme

```http
POST /compliance/str/{report_id}/submit
Authorization: Bearer <token>
```

### KYC Risk Skoru

```http
GET /compliance/kyc/{iban}
Authorization: Bearer <token>
```

**Yanıt:**
```json
{
  "iban": "TR33...",
  "risk_level": "medium",
  "risk_score": 0.45,
  "factors": {
    "high_risk_country": 0.2,
    "unverified_document": 0.15,
    "high_volume_value": 0.1
  },
  "last_updated": "2024-01-15T12:00:00Z",
  "cdd_status": "required"
}
```

---

## 🤖 ML Modeller

### Model Durumu

```http
GET /ml/models
Authorization: Bearer <token>
```

**Yanıt:**
```json
{
  "models": [
    {
      "name": "IsolationForest",
      "version": "1.2.0",
      "status": "ready",
      "last_trained": "2024-01-10T00:00:00Z",
      "metrics": {
        "accuracy": 0.96,
        "precision": 0.94,
        "recall": 0.92,
        "f1_score": 0.93,
        "auc": 0.97
      }
    },
    {
      "name": "XGBoost",
      "version": "2.1.0",
      "status": "ready",
      "metrics": {...}
    },
    {
      "name": "GNN",
      "version": "1.0.0",
      "status": "ready",
      "metrics": {...}
    }
  ]
}
```

### Model Eğitimi Başlatma

```http
POST /ml/train
Authorization: Bearer <token>
Content-Type: application/json

{
  "model_name": "XGBoost",
  "dataset_path": "data/training_v3.csv",
  "hyperparameters": {
    "n_estimators": 200,
    "max_depth": 6,
    "learning_rate": 0.1
  }
}
```

---

## 📈 Metrikler

### Sistem Metrikleri

```http
GET /metrics
```

**Yanıt:** (Prometheus format)
```
# HELP sentinelflow_transactions_processed_total Total transactions processed
# TYPE sentinelflow_transactions_processed_total counter
sentinelflow_transactions_processed_total{status="success"} 1523456

# HELP sentinelflow_fraud_alerts_total Total fraud alerts
# TYPE sentinelflow_fraud_alerts_total counter
sentinelflow_fraud_alerts_total{fraud_type="circular_ring"} 145
sentinelflow_fraud_alerts_total{fraud_type="impossible_travel"} 67

# HELP sentinelflow_transaction_latency_seconds Transaction processing latency
# TYPE sentinelflow_transaction_latency_seconds histogram
sentinelflow_transaction_latency_seconds_bucket{le="0.05"} 1234
```

### Sağlık Kontrolü

```http
GET /health
```

**Yanıt:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T15:30:00Z",
  "services": {
    "neo4j": "connected",
    "redis": "connected",
    "kafka": "connected",
    "ml_models": "ready"
  },
  "version": "2.0.0"
}
```

---

## ❌ Hata Kodları

| Kod | Açıklama |
|-----|----------|
| 400 | Geçersiz istek parametreleri |
| 401 | Kimlik doğrulama gerekli |
| 403 | Yetkisiz erişim |
| 404 | Kaynak bulunamadı |
| 429 | Rate limit aşıldı |
| 500 | Sunucu hatası |

**Hata Yanıt Formatı:**
```json
{
  "error": {
    "code": "INVALID_IBAN",
    "message": "Geçersiz IBAN formatı",
    "details": {
      "field": "sender_iban",
      "value": "TR123"
    }
  },
  "timestamp": "2024-01-15T15:30:00Z",
  "request_id": "req-abc123"
}
```

---

## 📝 Rate Limiting

| Endpoint | Limit |
|----------|-------|
| `/transactions/analyze` | 100/dakika |
| `/transactions/batch` | 10/dakika |
| `/graph/*` | 50/dakika |
| `/ml/train` | 5/saat |

**Headers:**
```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1705332600
```
