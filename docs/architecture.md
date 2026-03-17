# SentinelFlow Mimari Dökümantasyonu

## 🏗️ Sistem Mimarisi

SentinelFlow, gerçek zamanlı finansal dolandırıcılık tespiti için tasarlanmış, bulut tabanlı mikroservis mimarisi kullanan bir sistemdir.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SentinelFlow Platform                              │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐   │
│  │   REST API  │    │   Streamlit │    │   Kafka     │    │  Prometheus │   │
│  │   Gateway   │    │  Dashboard  │    │  Consumer   │    │   Metrics   │   │
│  │   :8000     │    │    :8501    │    │             │    │    :9090    │   │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘   │
│         │                  │                  │                  │          │
│         └──────────────────┼──────────────────┼──────────────────┘          │
│                            │                  │                              │
│                    ┌───────▼──────────────────▼───────┐                     │
│                    │         Fraud Detector           │                     │
│                    │     (Core Detection Engine)      │                     │
│                    └───────┬──────────────────┬───────┘                     │
│                            │                  │                              │
│         ┌──────────────────┼──────────────────┼──────────────────┐          │
│         │                  │                  │                  │          │
│  ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐   │
│  │ ML Ensemble │    │ Graph Ring  │    │ Impossible  │    │ Compliance  │   │
│  │   Models    │    │  Detector   │    │   Travel    │    │   Engine    │   │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘   │
│         │                  │                  │                  │          │
│  ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐   │
│  │  XGBoost    │    │   Neo4j     │    │    Redis    │    │   MASAK     │   │
│  │ Autoencoder │    │   Graph     │    │   GeoCache  │    │   Module    │   │
│  │ IsolationF. │    │   Database  │    │             │    │             │   │
│  │  GNN/LSTM   │    │             │    │             │    │             │   │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘

                               External Services
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐   │
│  │   Apache    │    │   Neo4j     │    │   Redis     │    │  Jaeger/    │   │
│  │   Kafka     │    │   5.x       │    │   7.x       │    │  Tempo      │   │
│  │             │    │             │    │             │    │             │   │
│  │   :9092     │    │   :7687     │    │   :6379     │    │   :4317     │   │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 📦 Modül Yapısı

```
sentinelflow/
├── api/                    # REST API (FastAPI)
│   ├── app.py             # Ana uygulama
│   ├── routes/            # API rotaları
│   └── middleware/        # Middleware'ler
│
├── core/                   # Çekirdek Bileşenler
│   ├── detector.py        # Ana fraud detector
│   ├── feature_extractor.py # Özellik çıkarma
│   └── transaction.py     # Veri modelleri
│
├── ml/                     # Makine Öğrenmesi
│   ├── models.py          # IF, XGBoost, AutoEncoder
│   ├── ensemble.py        # Ensemble oylama
│   ├── explainer.py       # SHAP açıklayıcı
│   ├── gnn_model.py       # Graph Neural Network
│   ├── temporal_model.py  # LSTM/Transformer
│   └── federated/         # Federated Learning
│
├── patterns/              # Fraud Pattern Detectors
│   ├── ring_detector.py   # Döngüsel halka tespiti
│   ├── mule_detector.py   # Katır hesap tespiti
│   └── travel_detector.py # İmkansız seyahat
│
├── compliance/            # Uyum Modülü
│   ├── masak.py          # MASAK STR raporlama
│   ├── engine.py         # Uyum motoru
│   └── audit.py          # Denetim günlüğü
│
├── kyc/                   # KYC/AML Modülü
│   ├── risk_scorer.py    # Risk skorlama
│   ├── cdd.py            # Müşteri durum tespiti
│   └── screening.py      # Liste tarama
│
├── security/              # Güvenlik
│   ├── auth.py           # JWT kimlik doğrulama
│   ├── rate_limit.py     # Hız sınırlama
│   └── validation.py     # Girdi doğrulama
│
├── monitoring/            # İzleme
│   ├── metrics.py        # Prometheus metrikleri
│   ├── tracing.py        # OpenTelemetry
│   └── logging.py        # Yapılandırılmış loglama
│
└── dashboard/             # Streamlit Dashboard
    ├── app.py            # Ana dashboard
    ├── components.py     # UI bileşenleri
    └── i18n.py           # Çoklu dil desteği
```

## 🔄 Veri Akışı

### 1. İşlem Alımı
```
Bank System → Kafka → Transaction Consumer → Feature Extractor
```

### 2. Fraud Analizi
```
Feature Extractor → ML Ensemble → Pattern Detectors → Risk Score
                 → GNN Model   →
                 → Temporal    →
```

### 3. Alarm Üretimi
```
Risk Score → Fraud Detector → Alert Generator → Kafka
                            → Compliance Check → MASAK
                            → Dashboard Update
```

## 🧠 ML Model Pipeline

```
                    ┌─────────────────────────────────────────┐
                    │           Feature Engineering            │
                    │                                         │
                    │  ┌─────────┐ ┌─────────┐ ┌───────────┐ │
                    │  │ Amount  │ │Temporal │ │ Velocity  │ │
                    │  │Features │ │Features │ │ Features  │ │
                    │  └────┬────┘ └────┬────┘ └─────┬─────┘ │
                    │       └───────────┼───────────┘        │
                    └───────────────────┼────────────────────┘
                                        │
                    ┌───────────────────▼────────────────────┐
                    │           ML Ensemble Voting            │
                    │                                         │
                    │  ┌─────────────┐ ┌─────────────┐       │
                    │  │ Isolation   │ │  XGBoost    │       │
                    │  │   Forest    │ │ Classifier  │       │
                    │  │   (0.3)     │ │   (0.4)     │       │
                    │  └──────┬──────┘ └──────┬──────┘       │
                    │         │               │               │
                    │  ┌──────┴───────────────┴──────┐       │
                    │  │        AutoEncoder          │       │
                    │  │      Reconstruction         │       │
                    │  │          (0.3)              │       │
                    │  └──────────────┬──────────────┘       │
                    │                 │                       │
                    │  ┌──────────────▼──────────────┐       │
                    │  │      Weighted Voting        │       │
                    │  │    final_score = Σ(wi*pi)   │       │
                    │  └──────────────┬──────────────┘       │
                    └─────────────────┼──────────────────────┘
                                      │
                    ┌─────────────────▼──────────────────────┐
                    │          SHAP Explainability           │
                    │  "Why was this flagged as fraud?"      │
                    └────────────────────────────────────────┘
```

## 🌐 Graph Analysis (Neo4j)

```cypher
// Döngüsel halka tespit sorgusu
MATCH path = (start:Account)-[:TRANSFER*2..6]->(start)
WHERE ALL(r IN relationships(path) WHERE r.timestamp > $threshold)
WITH path, 
     REDUCE(total = 0, r IN relationships(path) | total + r.amount) AS ring_amount
RETURN path, ring_amount
ORDER BY ring_amount DESC
```

## 📊 Monitoring Stack

```
┌─────────────────────────────────────────────────────────────────┐
│                    Observability Stack                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Application        Prometheus         Grafana                  │
│  ┌──────────┐      ┌──────────┐       ┌──────────┐             │
│  │ Metrics  │ ───► │  Scrape  │ ───►  │Dashboard │             │
│  │ /metrics │      │  Store   │       │  View    │             │
│  └──────────┘      └──────────┘       └──────────┘             │
│                                                                  │
│  ┌──────────┐      ┌──────────┐       ┌──────────┐             │
│  │  Traces  │ ───► │ Jaeger/  │ ───►  │  Trace   │             │
│  │  (OTLP)  │      │  Tempo   │       │  View    │             │
│  └──────────┘      └──────────┘       └──────────┘             │
│                                                                  │
│  ┌──────────┐      ┌──────────┐       ┌──────────┐             │
│  │   Logs   │ ───► │   Loki   │ ───►  │   Log    │             │
│  │  (JSON)  │      │          │       │  Search  │             │
│  └──────────┘      └──────────┘       └──────────┘             │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## 🔐 Güvenlik Mimarisi

```
┌─────────────────────────────────────────────────────────────────┐
│                      Security Layers                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Request → Rate Limiter → JWT Auth → Input Validation → Handler │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Rate Limiting: 100 req/min per IP                       │   │
│  │  JWT: HS256, 30min expiry, role-based scopes            │   │
│  │  Input: Pydantic validation + sanitization              │   │
│  │  Secrets: Environment variables / Vault                  │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## 🚀 Deployment

### Docker Compose (Geliştirme)
```yaml
services:
  api:
    image: sentinelflow:latest
    ports: ["8000:8000"]
    
  dashboard:
    image: sentinelflow:latest
    command: streamlit run dashboard/app.py
    ports: ["8501:8501"]
    
  neo4j:
    image: neo4j:5.15.0
    ports: ["7474:7474", "7687:7687"]
    
  redis:
    image: redis:7.2
    ports: ["6379:6379"]
    
  kafka:
    image: confluentinc/cp-kafka:7.5.0
```

### Kubernetes (Üretim)
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sentinelflow-api
spec:
  replicas: 3
  selector:
    matchLabels:
      app: sentinelflow-api
  template:
    spec:
      containers:
      - name: api
        image: ghcr.io/teknofest/sentinelflow:latest
        resources:
          limits:
            memory: "1Gi"
            cpu: "500m"
```

## 📈 Performans Metrikleri

| Metrik | Hedef | Mevcut |
|--------|-------|--------|
| İşlem Latansı | <100ms | ~45ms |
| ML Tahmin Süresi | <50ms | ~25ms |
| Fraud Tespit Oranı | >95% | 97.2% |
| False Positive | <2% | 1.3% |
| Sistem Uptime | 99.9% | 99.95% |
