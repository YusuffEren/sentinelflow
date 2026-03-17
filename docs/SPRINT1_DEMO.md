# Sprint 1 Demo Senaryosu

## Genel Bakış

Sprint 1, SentinelFlow'un temel altyapısını kurar:
- **PostgreSQL** ile kalıcı veri depolama
- **Kafka** ile event streaming
- **FastAPI** ile REST API
- **Next.js** ile modern dashboard

---

## Başlangıç

### 1. Altyapıyı Başlat

```bash
# Docker servisleri başlat (Postgres, Kafka, Neo4j, Redis)
docker compose up -d

# Servislerin hazır olduğunu kontrol et
docker compose ps
```

Beklenen çıktı:
- `sentinelflow-postgres`: healthy
- `sentinelflow-kafka`: healthy
- `sentinelflow-redis`: healthy
- `sentinelflow-neo4j`: healthy

### 2. Database Migration

```bash
# Tabloları oluştur
alembic upgrade head
```

### 3. API'yi Başlat

```bash
# Terminal 1 - Backend API
cd c:\Users\yusuf\Desktop\sentinelflow
python -m sentinelflow.api.app
```

API şu adreste çalışacak: `http://localhost:8000`

### 4. Dashboard'u Başlat

```bash
# Terminal 2 - Next.js Frontend
cd sentinelflow-web
npm run dev
```

Dashboard: `http://localhost:3000`

---

## Demo Senaryoları

### Senaryo 1: Normal İşlem Akışı

1. **API Swagger UI'ı aç**: http://localhost:8000/docs

2. **Sağlık kontrolü**:
   - `GET /api/v1/system/health`
   - Tüm bileşenlerin "healthy" olduğunu göster

3. **Normal işlem gönder**:
   ```bash
   curl -X POST http://localhost:8000/api/v1/transactions \
     -H "Content-Type: application/json" \
     -d '{
       "sender_iban": "TR330006100519786457841326",
       "sender_name": "Ahmet Yılmaz",
       "sender_city": "İstanbul",
       "receiver_iban": "TR110006400000468521793064",
       "receiver_name": "Mehmet Demir",
       "receiver_city": "Ankara",
       "amount": 5000,
       "description": "Kira ödemesi"
     }'
   ```

4. **Dashboard'da göster**:
   - İşlem sayısının arttığını
   - Fraud oranının düşük kaldığını

---

### Senaryo 2: ML Ensemble ile Fraud Tespiti

1. **Replay script ile yüksek tutarlı işlemler gönder**:
   ```bash
   python scripts/replay_transactions.py --scenario high_amount
   ```

2. **Dashboard'da canlı alert akışını göster**:
   - Alert feed'de yeni alertlerin belirdiğini
   - Severity renklerini
   - Confidence skorlarını

3. **Alerts sayfasına git** (http://localhost:3000/alerts):
   - Filtreleme özelliklerini göster
   - Alert detayını aç
   - Dismiss işlemini dene

---

### Senaryo 3: Döngüsel Transfer (Circular Ring)

1. **Circular ring senaryosu**:
   ```bash
   python scripts/replay_transactions.py --scenario circular_ring --count 5
   ```

2. **Açıkla**:
   - A → B → C → D → E → A döngüsü
   - Her işlem 10.000-50.000 TRY arası
   - Graf analizi ile tespit

3. **Alert detayında**:
   - `fraud_type: circular_ring` göster
   - İlişkili hesapları göster

---

### Senaryo 4: API Entegrasyonu

1. **Alert listesi**:
   ```bash
   curl http://localhost:8000/api/v1/alerts?page_size=5
   ```

2. **Alert detayı**:
   ```bash
   curl http://localhost:8000/api/v1/alerts/ALERT-XXXXXXXXXXXX
   ```

3. **Case oluşturma**:
   ```bash
   curl -X POST http://localhost:8000/api/v1/cases \
     -H "Content-Type: application/json" \
     -d '{
       "title": "Şüpheli Transfer Zinciri",
       "alert_ids": ["ALERT-123", "ALERT-456"],
       "priority": "P2"
     }'
   ```

4. **Case listesi**:
   ```bash
   curl http://localhost:8000/api/v1/cases
   ```

---

### Senaryo 5: WebSocket Canlı Akış

1. **WebSocket bağlantısını göster**:
   - Dashboard header'daki yeşil "Connected" durumu
   - Network tab'da WebSocket bağlantısı

2. **Yeni işlemler gönder**:
   ```bash
   python scripts/replay_transactions.py --generate 10 --delay 1
   ```

3. **Canlı olarak alert'lerin dashboard'a düştüğünü göster**

---

### Senaryo 6: Prometheus Metrikleri

1. **Metrics endpoint**:
   ```bash
   curl http://localhost:8000/metrics
   ```

2. **Gösterilecek metrikler**:
   - `sentinelflow_transactions_processed_total`
   - `sentinelflow_fraud_detected_total`
   - `sentinelflow_websocket_clients`

---

## Karşılaştırma: Önce vs Sonra

| Özellik | Sprint 0 | Sprint 1 |
|---------|----------|----------|
| Veri Depolama | In-memory (kaybolur) | PostgreSQL (kalıcı) |
| Schema | Tutarsız | Merkezi contracts |
| Alert Yönetimi | Yok | CRUD + dismiss |
| Case Yönetimi | Yok | Temel flow |
| Dashboard | Streamlit | Next.js + WebSocket |
| API | Dağınık | Modüler routes |
| Monitoring | Yok | Prometheus metrics |
| Demo | Manuel | Replay script |

---

## Teknik Değerler

- **Schema Version**: 1.0.0
- **API Version**: 2.1.0
- **Database**: PostgreSQL 16
- **Frontend**: Next.js 14 + Tailwind CSS

---

## Sonraki Adımlar (Sprint 2)

1. **JWT Authentication** - Kullanıcı girişi
2. **RBAC** - Rol tabanlı erişim
3. **STR Entegrasyonu** - MASAK bildirimi
4. **Model Training UI** - Dashboard'dan eğitim
5. **Kubernetes Deployment** - Production ready
