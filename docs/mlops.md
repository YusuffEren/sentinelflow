# SentinelFlow MLOps Guide

## 📋 İçindekiler

1. [Genel Bakış](#genel-bakış)
2. [Model Registry](#model-registry)
3. [Experiment Tracking](#experiment-tracking)
4. [Drift Detection](#drift-detection)
5. [Feature Store](#feature-store)
6. [A/B Testing](#ab-testing)
7. [CI/CD Pipeline](#cicd-pipeline)
8. [Model Cards](#model-cards)

---

## 🎯 Genel Bakış

SentinelFlow MLOps modülü, fraud detection modellerinin production ortamında yönetimi için enterprise-grade bir altyapı sağlar.

### Mimari

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          SentinelFlow MLOps Architecture                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐ │
│   │   Feature   │───►│  Training   │───►│   Model     │───►│   Model     │ │
│   │   Store     │    │  Pipeline   │    │  Registry   │    │  Serving    │ │
│   └─────────────┘    └─────────────┘    └──────┬──────┘    └─────────────┘ │
│         │                  │                   │                  │        │
│         │           ┌──────▼──────┐            │                  │        │
│         │           │  Experiment │            │                  │        │
│         │           │  Tracking   │            │                  │        │
│         │           └─────────────┘            │                  │        │
│         │                                      │                  │        │
│   ┌─────▼─────────────────────────────────────▼──────────────────▼─────┐  │
│   │                        Monitoring Layer                             │  │
│   │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────────────┐│  │
│   │  │Data Drift │  │Model Drift│  │ A/B Test  │  │    Alerting       ││  │
│   │  │ Detector  │  │  Monitor  │  │  Manager  │  │   & Logging       ││  │
│   │  └───────────┘  └───────────┘  └───────────┘  └───────────────────┘│  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📦 Model Registry

Model Registry, model versiyonlama ve yaşam döngüsü yönetimi sağlar.

### Temel Kullanım

```python
from sentinelflow.mlops import ModelRegistry

# Registry başlat
registry = ModelRegistry(registry_path="mlops/registry")

# Model kaydet
version = registry.register_model(
    model=trained_model,
    name="fraud_detector",
    metrics={"f1": 0.9952, "auc": 0.9978},
    description="Production fraud detection model",
    tags={"team": "ml", "environment": "prod"},
)

print(f"Registered: {version.model_name} v{version.version}")
# Output: Registered: fraud_detector v1.0.0
```

### Stage Yönetimi

```python
# Development → Staging → Production
registry.transition_stage(version.version_id, "staging")
registry.transition_stage(version.version_id, "production")

# Production model yükle
model = registry.load_model("fraud_detector", stage="production")
```

### Model Karşılaştırma

```python
# İki versiyon karşılaştır
comparison = registry.compare_versions(version_id_1, version_id_2)

print(f"F1 improvement: {comparison['metrics_comparison']['f1']['diff']:.4f}")
```

### Rollback

```python
# Sorun durumunda önceki versiyona dön
previous = registry.rollback("fraud_detector")
print(f"Rolled back to: v{previous.version}")
```

---

## 🔬 Experiment Tracking

Experiment Tracking, ML deneylerinin takibi ve tekrarlanabilirliğini sağlar.

### Temel Kullanım

```python
from sentinelflow.mlops import ExperimentTracker

tracker = ExperimentTracker()

with tracker.start_run(experiment_name="fraud_detection_v2") as run:
    # Parametreleri logla
    run.log_params({
        "n_estimators": 200,
        "max_depth": 10,
        "learning_rate": 0.1,
    })
    
    # Model eğit
    model.fit(X_train, y_train)
    
    # Metrikleri logla
    run.log_metrics({
        "f1": f1_score(y_test, y_pred),
        "auc": roc_auc_score(y_test, y_prob),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
    })
    
    # Model artifact kaydet
    run.log_artifact(model, "model.pkl")
```

### Hyperparameter Tuning

```python
# Optuna ile otomatik hyperparameter tuning
best_params = tracker.tune_hyperparameters(
    model_class=XGBClassifier,
    param_space={
        "n_estimators": ("int", 50, 500),
        "max_depth": ("int", 3, 15),
        "learning_rate": ("float_log", 0.001, 0.3),
        "subsample": ("float", 0.5, 1.0),
    },
    X_train=X_train,
    y_train=y_train,
    scoring="f1",
    n_trials=50,
)

print(f"Best F1: {best_params['best_score']:.4f}")
print(f"Best params: {best_params['best_params']}")
```

### En İyi Run'ı Bul

```python
best_run = tracker.get_best_run(
    experiment_name="fraud_detection_v2",
    metric="f1",
    mode="max",
)

print(f"Best run: {best_run.run_id}")
print(f"F1 Score: {best_run.metrics['f1']:.4f}")
```

---

## 📊 Drift Detection

Production'da data ve model drift tespiti.

### Data Drift

```python
from sentinelflow.mlops import DriftDetector

detector = DriftDetector(
    drift_threshold=0.05,    # p-value threshold
    psi_threshold=0.2,       # PSI threshold
)

# Reference vs Current data karşılaştır
report = detector.detect_data_drift(
    reference_data=training_df,
    current_data=production_df,
)

print(f"Has Drift: {report.has_drift}")
print(f"Severity: {report.severity.value}")
print(f"Drifted Features: {report.drifted_features}")

if report.has_drift:
    for rec in report.recommendations:
        print(f"  - {rec}")
```

### Model Drift

```python
# Model performans değişimini izle
model_report = detector.detect_model_drift(
    reference_metrics={"f1": 0.95, "auc": 0.98},  # Eğitim metrikleri
    current_metrics={"f1": 0.88, "auc": 0.92},    # Production metrikleri
)

if model_report.has_drift:
    print("⚠️ Model performance degradation detected!")
    print(f"F1 change: {model_report.metric_changes['f1']*100:.1f}%")
```

### Continuous Monitoring

```python
# Sürekli drift izleme
monitor = detector.create_drift_monitor(
    reference_data=training_df,
    window_size=1000,
)

# Yeni veri ekle
for transaction in production_stream:
    report = monitor.add_data(transaction)
    
    if monitor.should_alert():
        send_alert("Drift detected!", monitor.latest_report)
```

### Alert Callbacks

```python
def slack_alert(report):
    """Send Slack notification on drift."""
    if report.severity.value in ["high", "critical"]:
        send_slack_message(
            channel="#ml-alerts",
            message=f"🚨 {report.severity.value.upper()} drift detected!"
        )

detector.add_alert_callback(slack_alert)
```

---

## 🗃️ Feature Store

Merkezi feature yönetimi ve serving.

### Feature Group Oluşturma

```python
from sentinelflow.mlops import FeatureStore, Feature, FeatureType

store = FeatureStore()

# Feature group tanımla
fg = store.create_feature_group(
    name="transaction_features",
    description="Real-time transaction features",
    features=[
        Feature("amount_zscore", FeatureType.FLOAT, "Z-score of transaction amount"),
        Feature("tx_velocity_1h", FeatureType.FLOAT, "Transactions in last hour"),
        Feature("unique_receivers_1d", FeatureType.INT, "Unique receivers in last day"),
        Feature("is_new_receiver", FeatureType.BOOL, "First time sending to receiver"),
    ],
    entity_column="user_id",
    timestamp_column="event_timestamp",
)
```

### Feature Ingestion

```python
# Feature'ları kaydet
store.ingest(
    feature_group="transaction_features",
    data=features_df,
    update_statistics=True,
)
```

### Online Serving (Real-time)

```python
# Tek kullanıcı için feature'lar
features = store.get_online_features(
    feature_group="transaction_features",
    entity_id="user_123",
)

# Batch serving
features_batch = store.get_online_features_batch(
    feature_group="transaction_features",
    entity_ids=["user_1", "user_2", "user_3"],
)
```

### Offline Serving (Batch)

```python
# Historical feature'ları çek
features_df = store.get_offline_features(
    feature_group="transaction_features",
    start_time=datetime(2024, 1, 1),
    end_time=datetime(2024, 3, 1),
)
```

### Point-in-Time Correct Feature Retrieval

```python
# Entity dataframe'i hazırla
entity_df = pd.DataFrame({
    "user_id": ["user_1", "user_2"],
    "event_timestamp": [datetime(2024, 2, 1), datetime(2024, 2, 15)],
})

# Point-in-time correct feature join
features = store.get_historical_features(
    feature_group="transaction_features",
    entity_df=entity_df,
    ttl=timedelta(days=1),  # Feature'ın geçerlilik süresi
)
```

---

## 🧪 A/B Testing

Güvenli model deployment için A/B testing framework.

### Test Oluşturma

```python
from sentinelflow.mlops import ABTestManager

ab_manager = ABTestManager()

# Yeni test oluştur
test = ab_manager.create_test(
    name="fraud_model_v2_test",
    variants=[
        {"name": "control", "model": model_v1, "traffic": 0.5},
        {"name": "treatment", "model": model_v2, "traffic": 0.5},
    ],
    min_sample_size=1000,
    confidence_level=0.95,
    auto_start=True,
)
```

### Traffic Routing

```python
# Kullanıcı için variant belirle
variant = ab_manager.get_variant(test.test_id, user_id="user_123")

# Prediction yap
prediction = variant.model.predict(features)
```

### Outcome Logging

```python
# Sonucu kaydet
ab_manager.log_outcome(
    test_id=test.test_id,
    variant_name=variant.name,
    success=prediction_was_correct,
)
```

### Sonuçları Analiz Et

```python
results = ab_manager.get_results(test.test_id)

print(f"Result: {results.result.value}")
print(f"P-value: {results.p_value:.4f}")
print(f"Significant: {results.is_significant}")
print(f"Effect size: {results.effect_size:.4f}")
print(f"Improvement: {results.relative_improvement*100:.2f}%")

if results.can_conclude:
    print(f"\n{results.recommendation}")
```

### Multi-Armed Bandit

```python
# Thompson Sampling ile adaptive traffic allocation
test = ab_manager.create_test(
    name="mab_test",
    variants=[
        {"name": "model_a", "model": model_a},
        {"name": "model_b", "model": model_b},
        {"name": "model_c", "model": model_c},
    ],
    split_strategy=SplitStrategy.MULTI_ARMED_BANDIT,
)
```

---

## 🔄 CI/CD Pipeline

GitHub Actions ile otomatik ML pipeline.

### Workflow Triggers

```yaml
# .github/workflows/ml-pipeline.yml
on:
  push:
    paths:
      - 'src/sentinelflow/ml/**'
  workflow_dispatch:
    inputs:
      action:
        type: choice
        options: [validate, train, benchmark, deploy]
  schedule:
    - cron: '0 2 * * 0'  # Weekly retraining
```

### Pipeline Stages

1. **Data Validation**: Schema ve data quality kontrolleri
2. **Model Training**: Otomatik model eğitimi
3. **Model Validation**: Metrik threshold kontrolleri
4. **Benchmark**: Latency ve throughput testleri
5. **Model Card**: Otomatik dokümantasyon
6. **Drift Check**: Haftalık drift kontrolü
7. **Register Model**: Model registry'ye kayıt

### Manual Training

```bash
# GitHub Actions'ta manuel training tetikle
gh workflow run ml-pipeline.yml \
  -f action=train \
  -f model_type=ensemble \
  -f dataset=competition
```

---

## 📝 Model Cards

Model dokümantasyonu ve şeffaflık.

### Otomatik Model Card Oluşturma

```python
from sentinelflow.mlops import generate_model_card

card = generate_model_card(
    model=trained_model,
    model_name="fraud_detector_v2",
    training_data=train_df,
    test_data=test_df,
    metrics={"f1": 0.9952, "auc": 0.9978},
    target_column="is_fraud",
)

# Markdown'a export et
card.to_markdown("docs/MODEL_CARD.md")

# JSON'a export et
card.to_json("models/model_card.json")
```

### Model Card İçeriği

- **Model Details**: İsim, versiyon, tip, eğitim tarihi
- **Intended Use**: Kullanım alanları, hedef kullanıcılar
- **Training Data**: Dataset bilgileri, preprocessing
- **Quantitative Analysis**: Performans metrikleri
- **Ethical Considerations**: Bias, fairness analizi
- **Caveats and Recommendations**: Limitasyonlar, öneriler

---

## 🚀 Quick Start

```python
from sentinelflow.mlops import (
    ModelRegistry,
    ExperimentTracker,
    DriftDetector,
    FeatureStore,
    ABTestManager,
    generate_model_card,
)

# 1. Model eğit ve experiment tracking
tracker = ExperimentTracker()

with tracker.start_run("fraud_detection") as run:
    run.log_params(model_params)
    model.fit(X_train, y_train)
    run.log_metrics(metrics)

# 2. Model'i registry'ye kaydet
registry = ModelRegistry()
version = registry.register_model(model, "fraud_detector", metrics)

# 3. A/B test başlat
ab_manager = ABTestManager()
test = ab_manager.create_test("new_model_test", [
    {"name": "current", "model": current_model, "traffic": 0.5},
    {"name": "new", "model": model, "traffic": 0.5},
])

# 4. Drift monitoring başlat
detector = DriftDetector()
detector.detect_data_drift(reference_data, current_data)

# 5. Model card oluştur
card = generate_model_card(model, "fraud_detector")
card.to_markdown("MODEL_CARD.md")
```

---

## 📊 TEKNOFEST İçin Önemli Notlar

1. **Experiment Tracking**: Tüm deneyleri loglayın, jüri tekrarlanabilirlik bekler
2. **Model Registry**: Versiyon kontrolü profesyonellik gösterir
3. **Drift Detection**: Production-ready sistem kanıtı
4. **A/B Testing**: Güvenli deployment yaklaşımı
5. **Model Cards**: Şeffaflık ve dokümantasyon

Bu MLOps altyapısı, projenizi TEKNOFEST'te öne çıkaracak enterprise-grade bir ML operasyonu sağlar! 🏆
