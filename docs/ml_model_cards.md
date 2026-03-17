# SentinelFlow ML Model Cards

## 📋 Genel Bakış

Bu belge, SentinelFlow sisteminde kullanılan tüm makine öğrenmesi modellerinin detaylı açıklamalarını içerir. Her model kartı, modelin amacı, mimarisi, performans metrikleri ve sınırlamalarını kapsar.

---

## 🌲 Model 1: Isolation Forest

### Model Detayları

| Özellik | Değer |
|---------|-------|
| **Model Türü** | Unsupervised Anomaly Detection |
| **Framework** | scikit-learn |
| **Versiyon** | 1.2.0 |
| **Eğitim Tarihi** | 2024-01-10 |
| **Model Boyutu** | ~5 MB |

### Amaç

Isolation Forest, etiketlenmemiş işlem verilerinde anormal davranışları tespit etmek için kullanılır. Özellikle yeni ve bilinmeyen fraud türlerini yakalamada etkilidir.

### Mimari

```
Input Features (16) → Random Forest Ensemble (100 trees) → Anomaly Score
```

**Hiperparametreler:**
- `n_estimators`: 100
- `max_samples`: auto
- `contamination`: 0.01
- `max_features`: 1.0

### Performans Metrikleri

| Metrik | Değer |
|--------|-------|
| AUC-ROC | 0.94 |
| Precision @ 1% FPR | 0.89 |
| Recall @ 1% FPR | 0.78 |
| F1-Score | 0.83 |
| Inference Time | ~2ms |

### Girdi Özellikleri

| # | Özellik | Açıklama | Tip |
|---|---------|----------|-----|
| 1 | amount | İşlem tutarı | float |
| 2 | hour_of_day | Saat (0-23) | int |
| 3 | day_of_week | Haftanın günü (0-6) | int |
| 4 | tx_count_1h | Son 1 saatteki işlem sayısı | int |
| 5 | tx_count_24h | Son 24 saatteki işlem sayısı | int |
| 6 | avg_amount_30d | 30 günlük ortalama tutar | float |
| 7 | std_amount_30d | 30 günlük standart sapma | float |
| 8 | unique_receivers_7d | 7 günde benzersiz alıcı sayısı | int |
| ... | ... | ... | ... |

### Sınırlamalar

- ⚠️ Etiketli veriye ihtiyaç duymaz ancak contamination oranı manuel belirlenir
- ⚠️ Yüksek boyutlu özellik uzayında performans düşebilir
- ⚠️ Yorumlanabilirlik düşük (SHAP ile desteklenir)

### Kullanım Örneği

```python
from sentinelflow.ml.models import IsolationForestModel

model = IsolationForestModel(n_estimators=100, contamination=0.01)
model.train(X_train)

anomaly_score = model.predict_proba(features)[0][1]  # 0-1 arası skor
```

---

## 🚀 Model 2: XGBoost Classifier

### Model Detayları

| Özellik | Değer |
|---------|-------|
| **Model Türü** | Supervised Gradient Boosting |
| **Framework** | XGBoost |
| **Versiyon** | 2.1.0 |
| **Eğitim Tarihi** | 2024-01-12 |
| **Model Boyutu** | ~15 MB |

### Amaç

XGBoost, etiketli fraud verisiyle eğitilmiş denetimli bir sınıflandırıcıdır. En yüksek doğruluk oranına sahip modelimizdir ve ensemble'da en yüksek ağırlığa sahiptir.

### Mimari

```
Input Features (16) → Gradient Boosted Trees (200 trees) → Fraud Probability
```

**Hiperparametreler:**
- `n_estimators`: 200
- `max_depth`: 6
- `learning_rate`: 0.1
- `scale_pos_weight`: 50 (class imbalance)
- `subsample`: 0.8
- `colsample_bytree`: 0.8

### Performans Metrikleri

| Metrik | Değer |
|--------|-------|
| AUC-ROC | 0.98 |
| Precision | 0.95 |
| Recall | 0.93 |
| F1-Score | 0.94 |
| Accuracy | 0.99 |
| Inference Time | ~3ms |

### Özellik Önem Sıralaması

```
1. amount                    ████████████████████ 0.18
2. tx_count_24h              ██████████████████   0.16
3. unique_receivers_7d       █████████████████    0.15
4. hour_of_day               ████████████████     0.14
5. avg_amount_30d            ██████████████       0.12
6. velocity_1h               ████████████         0.10
7. time_since_last_tx        ██████████           0.08
8. receiver_risk_score       ████████             0.07
```

### Sınırlamalar

- ⚠️ Etiketli veri gerektirir (supervised)
- ⚠️ Yeni fraud türlerine adaptasyon için yeniden eğitim gerekir
- ⚠️ Overfit riski - cross-validation ile kontrol edilir

### Kullanım Örneği

```python
from sentinelflow.ml.models import XGBoostFraudModel

model = XGBoostFraudModel(n_estimators=200, max_depth=6)
model.train_model(X_train, y_train)

fraud_prob = model.predict_proba(features)[0][1]
```

---

## 🧠 Model 3: AutoEncoder

### Model Detayları

| Özellik | Değer |
|---------|-------|
| **Model Türü** | Neural Network (Reconstruction) |
| **Framework** | TensorFlow/Keras |
| **Versiyon** | 1.5.0 |
| **Eğitim Tarihi** | 2024-01-11 |
| **Model Boyutu** | ~2 MB |

### Amaç

AutoEncoder, normal işlem dağılımını öğrenir ve anormal işlemleri yüksek rekonstrüksiyon hatası ile tespit eder. Unsupervised yaklaşımla yeni fraud türlerini yakalayabilir.

### Mimari

```
Input (16) → Encoder [64, 32] → Latent (16) → Decoder [32, 64] → Output (16)
```

```
Layer           Output Shape        Params
─────────────────────────────────────────────
Input           (None, 16)          0
Dense           (None, 64)          1,088
BatchNorm       (None, 64)          256
ReLU            (None, 64)          0
Dropout(0.2)    (None, 64)          0
Dense           (None, 32)          2,080
BatchNorm       (None, 32)          128
ReLU            (None, 32)          0
Dropout(0.2)    (None, 32)          0
Dense (Latent)  (None, 16)          528
Dense           (None, 32)          544
Dense           (None, 64)          2,112
Dense (Output)  (None, 16)          1,040
─────────────────────────────────────────────
Total params: 7,776
```

### Performans Metrikleri

| Metrik | Değer |
|--------|-------|
| AUC-ROC | 0.92 |
| Precision @ 1% FPR | 0.85 |
| Recall @ 1% FPR | 0.72 |
| Reconstruction MSE (normal) | 0.001 |
| Reconstruction MSE (fraud) | 0.15 |
| Inference Time | ~5ms |

### Eğitim Detayları

- **Epochs**: 100
- **Batch Size**: 256
- **Optimizer**: Adam (lr=0.001)
- **Loss**: MSE
- **Early Stopping**: patience=10

### Sınırlamalar

- ⚠️ Sadece normal verilerle eğitilir
- ⚠️ Threshold belirleme kritik (percentile based)
- ⚠️ Yüksek boyutlu girdilerde performans düşebilir

---

## 🔗 Model 4: Graph Neural Network (GNN)

### Model Detayları

| Özellik | Değer |
|---------|-------|
| **Model Türü** | Graph Convolutional Network |
| **Framework** | PyTorch Geometric |
| **Versiyon** | 1.0.0 |
| **Eğitim Tarihi** | 2024-01-14 |
| **Model Boyutu** | ~10 MB |

### Amaç

GNN, hesap ağındaki ilişkileri analiz ederek düğüm seviyesinde fraud tespiti yapar. Circular ring ve mule account gibi network-based fraud kalıplarını tespit eder.

### Mimari

```
Node Features (10) → GCN Layer (16) → ReLU → Dropout
                  → GCN Layer (16) → ReLU → Dropout
                  → Linear (2) → Softmax → Node Classification
```

**Desteklenen GNN Türleri:**
- Graph Convolutional Network (GCN)
- GraphSAGE
- Graph Attention Network (GAT)

### Performans Metrikleri

| Metrik | Değer |
|--------|-------|
| Node Classification Accuracy | 0.91 |
| AUC-ROC | 0.93 |
| Precision | 0.89 |
| Recall | 0.87 |
| Ring Detection Rate | 0.96 |
| Inference Time | ~15ms |

### Girdi Yapısı

**Node Features:**
- Hesap yaşı
- Toplam gelen işlem sayısı
- Toplam giden işlem sayısı
- Ortalama işlem tutarı
- Son 7 gün aktivitesi
- ...

**Edge Features:**
- İşlem tutarı
- İşlem zamanı
- İşlem sıklığı

### Sınırlamalar

- ⚠️ Graf yapısı gerektirir (Neo4j entegrasyonu)
- ⚠️ Büyük graflarda hesaplama maliyeti yüksek
- ⚠️ GPU önerilir

---

## 📈 Model 5: Temporal Model (LSTM/Transformer)

### Model Detayları

| Özellik | Değer |
|---------|-------|
| **Model Türü** | Sequence-to-Label |
| **Framework** | PyTorch |
| **Versiyon** | 1.0.0 |
| **Eğitim Tarihi** | 2024-01-14 |
| **Model Boyutu** | ~8 MB |

### Amaç

Temporal model, bir kullanıcının işlem geçmişini analiz ederek zaman serisi anomalilerini tespit eder. Impossible travel ve velocity anomalileri gibi zamansal kalıpları yakalar.

### Mimari (LSTM)

```
Input (seq_len, 10) → LSTM (32, 2 layers) → Last Hidden State
                   → Linear (2) → Softmax → Fraud Classification
```

### Mimari (Transformer)

```
Input (seq_len, 10) → TransformerEncoder (2 layers, 8 heads)
                   → Mean Pooling → Linear (2) → Softmax
```

### Performans Metrikleri

| Metrik | LSTM | Transformer |
|--------|------|-------------|
| AUC-ROC | 0.90 | 0.92 |
| Precision | 0.88 | 0.90 |
| Recall | 0.85 | 0.87 |
| F1-Score | 0.86 | 0.88 |
| Inference Time | ~8ms | ~12ms |

### Sınırlamalar

- ⚠️ Yeterli geçmiş veri gerektirir (min 10 işlem)
- ⚠️ Yeni kullanıcılarda cold-start problemi
- ⚠️ Sequence uzunluğu sabit (padding gerekli)

---

## ⚖️ Ensemble Voting

### Ağırlık Dağılımı

```
Final Score = 0.15 × IF + 0.40 × XGB + 0.20 × AE + 0.15 × GNN + 0.10 × LSTM
```

| Model | Ağırlık | Neden |
|-------|---------|-------|
| XGBoost | 0.40 | En yüksek doğruluk, supervised |
| AutoEncoder | 0.20 | Novel anomaly detection |
| Isolation Forest | 0.15 | Hızlı, unsupervised |
| GNN | 0.15 | Network-based patterns |
| Temporal | 0.10 | Sequence anomalies |

### Ensemble Performansı

| Metrik | Değer |
|--------|-------|
| AUC-ROC | 0.98 |
| Precision | 0.96 |
| Recall | 0.94 |
| F1-Score | 0.95 |

---

## 🔄 Model Yaşam Döngüsü

```
1. Veri Toplama    ─────────────────────────────────────────┐
                                                            │
2. Özellik Mühendisliği  ←──────────────────────────────────┤
                                                            │
3. Model Eğitimi         ←──── Hiperparametre Optimizasyonu │
                                                            │
4. Validasyon            ─────────────────────────────────────┤
                                                            │
5. A/B Test              ────────────────────────────────────┤
                                                            │
6. Deployment            ────────────────────────────────────┤
                                                            │
7. Monitoring            ─────────────────────────────────────┘
                               ↓
8. Model Drift Detection → Retrain Trigger
```

### Yeniden Eğitim Tetikleyicileri

- ⏰ Zamanlanmış: Her hafta
- 📉 Performans düşüşü: AUC < 0.95
- 📊 Data drift: KL-divergence > 0.1
- 🆕 Yeni fraud türü keşfi
