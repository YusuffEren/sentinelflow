#!/usr/bin/env python3
"""
SentinelFlow — GitHub Issues Oluşturucu
========================================
Kullanım:
    export GITHUB_TOKEN=ghp_...
    python scripts/create_github_issues.py

Veya token parametre olarak:
    python scripts/create_github_issues.py --token ghp_...

Gereksinim: requests
    pip install requests
"""

import argparse
import os
import sys
import time

import requests

REPO = "YusuffEren/sentinelflow"
API_BASE = "https://api.github.com"

# ---------------------------------------------------------------------------
# Labels (renk + açıklama)
# ---------------------------------------------------------------------------
LABELS = [
    # Öncelik
    {"name": "P0", "color": "d73a4a", "description": "Kritik / Sprint'e girmeli"},
    {"name": "P1", "color": "e4e669", "description": "Yüksek öncelik"},
    # Epic'ler
    {"name": "epic:standards", "color": "0075ca", "description": "E0 – Ürün kararları ve standartlar"},
    {"name": "epic:data-layer", "color": "0075ca", "description": "E1 – Veri katmanı"},
    {"name": "epic:streaming", "color": "0075ca", "description": "E2 – Streaming mimarisi"},
    {"name": "epic:detection", "color": "0075ca", "description": "E3 – Detection engine"},
    {"name": "epic:ml", "color": "0075ca", "description": "E4 – ML sistemi"},
    {"name": "epic:security", "color": "0075ca", "description": "E5 – API Gateway + Güvenlik"},
    {"name": "epic:case-mgmt", "color": "0075ca", "description": "E6 – Case Management & Dashboard"},
    {"name": "epic:observability", "color": "0075ca", "description": "E7 – Observability & SRE"},
    {"name": "epic:ci-cd", "color": "0075ca", "description": "E8 – CI/CD + Kalite kapıları"},
    # Sprint
    {"name": "sprint-1", "color": "bfd4f2", "description": "Sprint 1 – Tek akış + kalıcılık"},
    {"name": "sprint-2", "color": "bfd4f2", "description": "Sprint 2 – Case management + korelasyon"},
    {"name": "sprint-3", "color": "bfd4f2", "description": "Sprint 3 – MLOps kanıtı"},
    {"name": "sprint-4", "color": "bfd4f2", "description": "Sprint 4 – Performans & üretim cilası"},
    # Tür
    {"name": "type:feature", "color": "a2eeef", "description": "Yeni özellik"},
    {"name": "type:refactor", "color": "e4e669", "description": "Yeniden yapılandırma"},
    {"name": "type:infra", "color": "f9d0c4", "description": "Altyapı / DevOps"},
    {"name": "type:test", "color": "c2e0c6", "description": "Test ve kalite"},
    {"name": "type:security", "color": "d73a4a", "description": "Güvenlik"},
]

# ---------------------------------------------------------------------------
# Issues
# ---------------------------------------------------------------------------
ISSUES = [
    # -----------------------------------------------------------------------
    # EPIC 0 — Ürün kararları ve standartlar
    # -----------------------------------------------------------------------
    {
        "title": "[E0.1] Tek Alert/Transaction JSON şeması tanımla",
        "labels": ["P0", "epic:standards", "sprint-1", "type:feature"],
        "body": """\
## Amaç
API, Kafka mesajları ve dashboard'un aynı JSON şemasını kullanmasını sağla.

## Kapsam
- `transaction`, `alert`, `evidence`, `risk_score`, `severity`, `detector_versions`, `schema_version` alanlarını içeren tek şema
- Pydantic (veya JSON Schema) ile tanımla; tüm servisler bu modeli import etsin
- Eski/tutarsız alanları temizle veya deprecate et

## Kabul Kriterleri
- [ ] API, Kafka consumer ve dashboard aynı şemayı kullanıyor
- [ ] `schema_version` alanı her mesajda mevcut
- [ ] Şema değişikliği CI'de schema diff kontrolü ile yakalanıyor

## Tahmini Efor
S (< 1 gün)
""",
    },
    {
        "title": "[E0.2] Tek decision pipeline mimarisi: compliance → rules → ML → fusion",
        "labels": ["P0", "epic:standards", "sprint-1", "type:refactor"],
        "body": """\
## Amaç
Her transaction için deterministik bir karar zinciri kur.

## Kapsam
Sıra: **compliance check → rule/graph/geo/NLP dedektörler → ML ensemble → fusion → final decision**

- Her aşama `evidence` objesi üretiyor (hangi kural tetiklendi, hangi model ne dedi)
- Final skor 0–1 arası normalize edilmiş float
- `severity` deterministik eşiklerle (ör. 0.8 → CRITICAL, 0.5 → HIGH) belirleniyor

## Kabul Kriterleri
- [ ] Pipeline kodu tek modüle toplandı (`src/pipeline/` veya benzeri)
- [ ] Her aşamanın çıktısı `evidence[]` listesine ekleniyor
- [ ] Final skor ve severity'nin hesaplandığı tek fonksiyon var
- [ ] Unit testler mevcut

## Tahmini Efor
M (2–3 gün)
""",
    },
    {
        "title": "[E0.3] Başarı metrikleri ve hedefler tanımla (kanıt panosu)",
        "labels": ["P0", "epic:standards", "sprint-1", "type:feature"],
        "body": """\
## Amaç
Sistem ve ML başarısını ölçülebilir hedeflerle tanımla; dashboard'da göster.

## Kapsam
**Sistem metrikleri:** p95 latency, throughput (tps), uptime, error rate  
**ML metrikleri:** PR-AUC, recall@FPR=0.01, cost-weighted score, drift alarmları

- Hedef değerler konfigürasyon dosyasına yaz (ör. `config/targets.yaml`)
- Dashboard'a "Metrics / Kanıt Panosu" sayfası ekle

## Kabul Kriterleri
- [ ] Hedef değerler dokümante edilmiş
- [ ] Dashboard'da metrics sayfası var
- [ ] Prometheus metriklerinin listesi README/docs'ta açıklanmış

## Tahmini Efor
S (1 gün)
""",
    },

    # -----------------------------------------------------------------------
    # EPIC 1 — Veri katmanı
    # -----------------------------------------------------------------------
    {
        "title": "[E1.1] Postgres kalıcı veri modeli: transactions, alerts, cases, audit_log, model_versions",
        "labels": ["P0", "epic:data-layer", "sprint-1", "type:infra"],
        "body": """\
## Amaç
Restart'tan sonra verilerin kaybolmamasını sağla; sorgular hızlı çalışsın.

## Kapsam
Tablolar:
- `transactions` (özet: id, amount, account_id, timestamp, status)
- `alerts` (id, transaction_id, risk_score, severity, detector_versions, schema_version)
- `cases` (id, status, assigned_to, created_at, closed_at)
- `case_alerts` (case_id, alert_id)
- `case_events` / `audit_log` (case_id, actor, action, timestamp, detail_json)
- `model_versions` (id, name, version, deployed_at, metrics_json)
- `features_snapshot` (transaction_id, feature_json, created_at)

İndeksler: `account_id`, `timestamp`, `status`, `severity` üzerinde.

## Kabul Kriterleri
- [ ] Alembic migration'ları mevcut ve `alembic upgrade head` ile uygulanıyor
- [ ] Docker Compose restart sonrası veriler korunuyor
- [ ] Tüm tablolarda gerekli indeksler tanımlı
- [ ] Integration testi: 1000 satır yaz, okuma < 50ms

## Tahmini Efor
M (2–3 gün)
""",
    },
    {
        "title": "[E1.2] Idempotency & deduplication: aynı transaction_id tekrar gelince double alert açma",
        "labels": ["P0", "epic:data-layer", "sprint-1", "type:feature"],
        "body": """\
## Amaç
Kafka retries veya tekrar gönderim nedeniyle aynı transaction'ın iki kez işlenip çift alert/case oluşturmamasını sağla.

## Kapsam
- `transactions.id` üzerinde UNIQUE constraint (veya `ON CONFLICT DO NOTHING`)
- Alert oluşturmadan önce idempotency key kontrolü
- Kafka consumer'da `enable.idempotence=true` (veya uygulama katmanında kontrol)

## Kabul Kriterleri
- [ ] Aynı `transaction_id` 3 kez gönderildiğinde DB'de tek kayıt var
- [ ] Sadece 1 alert oluşuyor
- [ ] Unit test: duplicate mesaj → tek kayıt

## Tahmini Efor
S (< 1 gün)
""",
    },
    {
        "title": "[E1.3] Event sourcing lite: case durum değişimlerini audit_log'a yaz",
        "labels": ["P0", "epic:data-layer", "sprint-1", "type:feature"],
        "body": """\
## Amaç
Her case durum geçişini (triage → investigating → closed) kalıcı olarak logla; MASAK/uyum denetimi için iz bırak.

## Kapsam
- `case_events` tablosuna: `case_id`, `from_status`, `to_status`, `actor` (user/system), `timestamp`, `note`
- Case API endpoint'leri her durum değişiminde event yazıyor
- `GET /cases/{id}/events` endpoint'i

## Kabul Kriterleri
- [ ] Triage → investigating → closed geçişleri loglanıyor
- [ ] Loglar silinemez (soft-delete bile olsa event log'a düşüyor)
- [ ] API endpoint events listesini döndürüyor

## Tahmini Efor
S (1 gün)
""",
    },

    # -----------------------------------------------------------------------
    # EPIC 2 — Streaming mimarisi
    # -----------------------------------------------------------------------
    {
        "title": "[E2.1] Kafka topic standardizasyonu: transactions, alerts, cases.events, features",
        "labels": ["P0", "epic:streaming", "sprint-1", "type:infra"],
        "body": """\
## Amaç
Tüm Kafka topic'lerini standartlaştır; her mesajda `schema_version` olsun.

## Kapsam
Topic'ler:
- `transactions` — ham gelen işlemler
- `alerts` — üretilen alarmlar
- `cases.events` — case durum geçişleri
- `features` (opsiyonel) — ML feature vektörleri

Her mesaj body'sinde `schema_version` alanı zorunlu.  
`scripts/init_kafka_topics.sh` güncellenmeli.

## Kabul Kriterleri
- [ ] Topic'ler script ile otomatik oluşturuluyor
- [ ] Her mesajda `schema_version` var
- [ ] Topic adları dokümante edilmiş (`docs/`)

## Tahmini Efor
S (< 1 gün)
""",
    },
    {
        "title": "[E2.2] Kafka consumer grupları & replay: belirli zaman aralığını yeniden işle",
        "labels": ["P0", "epic:streaming", "sprint-2", "type:feature"],
        "body": """\
## Amaç
Demo ve benchmark için belirli bir zaman aralığındaki transaction'ları replay edebil.

## Kapsam
- Consumer group offset yönetimi (by timestamp seek)
- `scripts/replay_transactions.py` güncellenerek zaman aralığı parametresi eklenmeli
- Replay modu production consumer'ı etkilememeli (ayrı consumer group)

## Kabul Kriterleri
- [ ] `--from` / `--to` parametresiyle zaman aralığı replay çalışıyor
- [ ] Replay consumer group'u production'dan izole
- [ ] Replay sonrası metrikler raporlanıyor

## Tahmini Efor
S (1 gün)
""",
    },
    {
        "title": "[E2.3] At-least-once + idempotent write güvencesi",
        "labels": ["P0", "epic:streaming", "sprint-1", "type:feature"],
        "body": """\
## Amaç
Mesaj kaybını önle; idempotent write ile tutarlılığı sağla.

## Kapsam
- Kafka producer `acks=all`, `retries=3`
- Consumer: başarılı DB write sonrası commit (manual commit)
- E1.2 ile birlikte duplicate-safe

## Kabul Kriterleri
- [ ] Broker restart senaryosunda mesaj kaybı yok
- [ ] Consumer crash → restart sonrası mesaj tekrar işleniyor ama DB'de duplicate yok
- [ ] Integration test: broker restart + consumer restart

## Tahmini Efor
S (1 gün)
""",
    },

    # -----------------------------------------------------------------------
    # EPIC 3 — Detection engine
    # -----------------------------------------------------------------------
    {
        "title": "[E3.1] Config-driven rule engine: eşikler, whitelist/blacklist, hız kuralları",
        "labels": ["P0", "epic:detection", "sprint-2", "type:feature"],
        "body": """\
## Amaç
Kuralları koddan ayır; operasyon ekibi kod değiştirmeden kural yönetebilsin.

## Kapsam
- `config/rules.yaml` (veya DB tablosu): eşik değerleri, whitelist/blacklist hesap listesi, velocity kuralları (ör. 5 dk içinde 3'ten fazla işlem)
- Rule engine `config/rules.yaml`'ı yükleyip uygular
- Hot-reload: konfigürasyon değişince restart gerekmemeli (opsiyonel)

## Kabul Kriterleri
- [ ] Eşik değerleri config dosyasından okunuyor (hardcoded değil)
- [ ] Whitelist/blacklist config'ten yönetiliyor
- [ ] Velocity kuralları configurable
- [ ] Unit testler: çeşitli kural senaryoları

## Tahmini Efor
M (2 gün)
""",
    },
    {
        "title": "[E3.2] Graph detection güçlendirme: ring, mule, fan-in/fan-out, rapid layering",
        "labels": ["P0", "epic:detection", "sprint-2", "type:feature"],
        "body": """\
## Amaç
Graph tabanlı kara para aklama örüntülerini tespit et; evidence zenginleştir.

## Kapsam
Tespit edilecek örüntüler:
- **Ring**: dairesel para akışı
- **Mule**: çok fazla para alan/gönderen ara düğüm
- **Fan-in / Fan-out**: tek hesaba/hesaptan yüksek sayıda transfer
- **Rapid layering**: kısa sürede art arda transfer zinciri

Her tespitte `evidence` objesi: `path[]`, `amount_aggregation`, `time_window`, `node_count`, `edge_stats`

## Kabul Kriterleri
- [ ] 4 örüntü Neo4j Cypher sorgusu ile tespit ediliyor
- [ ] Evidence objesi ilgili path/node/edge bilgisini içeriyor
- [ ] Integration test: sentetik veri seti ile 4 örüntü testi
- [ ] Dashboard'da graph view evidence'ı gösteriyor

## Tahmini Efor
L (3–4 gün)
""",
    },
    {
        "title": "[E3.3] Geo & device intelligence: IP ASN/ülke, impossible travel tespiti",
        "labels": ["P0", "epic:detection", "sprint-2", "type:feature"],
        "body": """\
## Amaç
Coğrafi imkânsızlık ve şüpheli cihaz sinyallerini tespite ekle.

## Kapsam
- IP → ASN/country lookup (MaxMind GeoLite2 veya benzeri ücretsiz DB)
- Device fingerprint (demo seviyesinde: user_agent + ip hash)
- Impossible travel: son işlemin lokasyonuyla bu işlemin lokasyonu arasındaki mesafe/süre kontrolü
- Evidence: `country`, `asn`, `distance_km`, `time_delta_min`, `travel_speed_kmh`

## Kabul Kriterleri
- [ ] IP'den ülke/ASN tespit ediliyor
- [ ] Impossible travel kuralı çalışıyor (ör. 10 dk içinde 1000 km)
- [ ] Evidence geo bilgisini içeriyor
- [ ] Unit test: sentetik geo senaryoları

## Tahmini Efor
M (2 gün)
""",
    },

    # -----------------------------------------------------------------------
    # EPIC 4 — ML sistemi
    # -----------------------------------------------------------------------
    {
        "title": "[E4.1] Feature store yaklaşımı: online (Redis/DB) + offline (parquet) aynı tanım",
        "labels": ["P0", "epic:ml", "sprint-3", "type:feature"],
        "body": """\
## Amaç
Train ve serve'da aynı feature tanımını kullanarak training-serving skew'i ortadan kaldır.

## Kapsam
- Feature tanımları `src/features/` modülünde merkezi olarak tanımlanmış
- **Online store**: Redis veya materialized view (son N transaction istatistikleri)
- **Offline store**: Parquet/CSV dosyaları (eğitim dataseti)
- Feature oluşturma fonksiyonları hem train pipeline'ında hem serve'da import ediliyor

## Kabul Kriterleri
- [ ] Tek `compute_features(transaction)` fonksiyonu; train ve serve kullanıyor
- [ ] Online feature'lar Redis'te cache'leniyor (TTL ile)
- [ ] Offline dataset üretme scripti mevcut
- [ ] Train-serve feature uyumluluğu için integration test

## Tahmini Efor
M (2–3 gün)
""",
    },
    {
        "title": "[E4.2] Model registry + versioning + rollback",
        "labels": ["P0", "epic:ml", "sprint-3", "type:feature"],
        "body": """\
## Amaç
Her deploy edilen model versiyonunu takip et; rollback yapılabilsin.

## Kapsam
- `model_versions` tablosu (E1.1'de tanımlandı): `id`, `name`, `version`, `artifact_path`, `deployed_at`, `metrics_json`, `is_active`
- Model yükleme kodu: en son aktif versiyonu yükler
- CLI veya API ile: `activate_version(id)`, `rollback_to(id)`
- Her alert kaydında `model_version` alanı dolu

## Kabul Kriterleri
- [ ] Yeni model deploy edildiğinde versiyonlanıyor
- [ ] `GET /ml/models` aktif ve geçmiş versiyonları listeler
- [ ] `POST /ml/models/{id}/activate` rollback yapıyor
- [ ] Her alert'te `model_version` dolu

## Tahmini Efor
M (2 gün)
""",
    },
    {
        "title": "[E4.3] Drift detection: PSI/KS istatistikleri + dashboard raporu",
        "labels": ["P1", "epic:ml", "sprint-3", "type:feature"],
        "body": """\
## Amaç
Model ve veri driftini otomatik tespit et; dashboard'da raporla.

## Kapsam
- **Data drift**: PSI (Population Stability Index) ve KS testi ile feature dağılımı karşılaştırması
- **Performance drift**: Production'da recall/precision zaman serisi
- Drift alarmı: PSI > 0.2 veya recall < eşik olduğunda alarm
- Dashboard'da "Drift Raporu" sayfası

## Kabul Kriterleri
- [ ] Günlük/saatlik PSI hesaplanıyor
- [ ] Drift alarmı oluştuğunda alert/notification üretiliyor
- [ ] Dashboard'da zaman serisi drift grafikleri var
- [ ] Unit test: sentetik drift verisi ile PSI hesaplama

## Tahmini Efor
M (2–3 gün)
""",
    },
    {
        "title": "[E4.4] Explainability: SHAP + rule/graph kanıtları → insan okunur gerekçe",
        "labels": ["P0", "epic:ml", "sprint-3", "type:feature"],
        "body": """\
## Amaç
Her kritik alert için jürinin anlayacağı 3–5 maddelik gerekçe üret.

## Kapsam
- SHAP değerleri ile top-N feature önem sıralaması
- Rule engine ve graph tespitlerinden gelen evidence'ları birleştir
- `explanation` objesi: `{"summary": "...", "reasons": ["...", "..."], "evidence_links": [...]}`
- Her CRITICAL/HIGH alert'te `explanation` alanı dolu

## Kabul Kriterleri
- [ ] SHAP değerleri hesaplanıyor ve en önemli 5 feature listeleniyor
- [ ] Rule ve graph evidence'ları açıklamaya dahil ediliyor
- [ ] `explanation.summary` insan okunur cümle (template tabanlı da olur)
- [ ] Dashboard'da alert detayında açıklama paneli var
- [ ] Unit test: sahte model ile SHAP değerleri testi

## Tahmini Efor
L (3–4 gün)
""",
    },

    # -----------------------------------------------------------------------
    # EPIC 5 — API Gateway + Güvenlik
    # -----------------------------------------------------------------------
    {
        "title": "[E5.1] JWT tabanlı AuthN/AuthZ + RBAC (admin, analyst, auditor, mlops rolleri)",
        "labels": ["P0", "epic:security", "sprint-2", "type:security"],
        "body": """\
## Amaç
API endpoint'lerini role dayalı erişim kontrolüyle koru.

## Kapsam
Roller: `admin`, `analyst`, `auditor`, `mlops`

| Eylem | Gerekli Rol |
|---|---|
| Case kapat/ata | analyst, admin |
| Model deploy/rollback | mlops, admin |
| Audit log görüntüle | auditor, admin |
| Alert sil | admin |

- JWT (HS256 veya RS256) ile token üretimi/doğrulaması
- FastAPI dependency ile endpoint koruması
- Login endpoint: `POST /auth/login`

## Kabul Kriterleri
- [ ] 4 rol tanımlı ve endpoint'ler korunuyor
- [ ] Yetkisiz erişim → 403 döndürüyor
- [ ] JWT expiry çalışıyor
- [ ] Integration test: her rol için erişim senaryoları

## Tahmini Efor
M (2 gün)
""",
    },
    {
        "title": "[E5.2] Rate limiting + input validation + audit log",
        "labels": ["P0", "epic:security", "sprint-2", "type:security"],
        "body": """\
## Amaç
Abuse senaryolarını engelle; tüm aksiyonları audit log'a düşür.

## Kapsam
- **Rate limit**: IP başına / kullanıcı başına (slowapi veya benzeri); aşımda 429
- **Input validation**: Pydantic şemaları, özellikle transaction ingest endpoint
- **Audit log**: Her API çağrısı (özellikle mutation'lar) için `actor`, `action`, `resource_id`, `timestamp`, `ip_address`

## Kabul Kriterleri
- [ ] Rate limit: 100 req/dk aşınca 429 dönüyor
- [ ] Geçersiz input → 422 + açıklayıcı hata mesajı
- [ ] Tüm POST/PUT/DELETE aksiyonları audit log'a yazılıyor
- [ ] Integration test: rate limit + malformed input

## Tahmini Efor
M (1–2 gün)
""",
    },
    {
        "title": "[E5.3] Secrets yönetimi: hardcoded secret yok, .env.example + CI check",
        "labels": ["P0", "epic:security", "sprint-1", "type:security"],
        "body": """\
## Amaç
Repo'da hardcoded secret bırakma; güvenli konfigürasyon yönetimi.

## Kapsam
- Tüm secret'lar environment variable'dan okunuyor
- `.env.example` güncel ve tüm değişkenleri içeriyor (değersiz)
- `.env` `.gitignore`'da
- CI'de `git-secrets` veya `trufflesecurity/trufflehog` veya bandit secret scan

## Kabul Kriterleri
- [ ] `grep -r "password|secret|api_key" src/` → hardcoded değer yok
- [ ] `.env.example` tüm gerekli değişkenleri listeliyor
- [ ] CI'de secret scan adımı var ve başarılı

## Tahmini Efor
S (< 1 gün)
""",
    },

    # -----------------------------------------------------------------------
    # EPIC 6 — Case Management & Dashboard
    # -----------------------------------------------------------------------
    {
        "title": "[E6.1] Alert → Case korelasyonu: aynı aktör/hesap kümesi için case birleştir",
        "labels": ["P0", "epic:case-mgmt", "sprint-2", "type:feature"],
        "body": """\
## Amaç
Alert flood'unu önle; ilgili alertleri tek case altında topla.

## Kapsam
- Korelasyon mantığı: aynı `account_id` veya `entity_id`'ye ait, son X saatte gelen alertler → tek case
- Alert storm threshold: N alertten sonra yeni case açılmaz, var olana eklenir
- `POST /alerts` işlenirken korelasyon motoru çalışıyor

## Kabul Kriterleri
- [ ] Demo senaryosu: 10 alert → 2 case'e toplandı (farklı aktörler)
- [ ] Aynı hesaba ait alertler tek case'de birleşiyor
- [ ] Korelasyon penceresi configurable (ör. 24 saat)
- [ ] Integration test: 10 alert → case count kontrolü

## Tahmini Efor
M (2 gün)
""",
    },
    {
        "title": "[E6.2] Analyst workflow: triage kuyruğu, assign, notlar, etiketler, false positive",
        "labels": ["P0", "epic:case-mgmt", "sprint-2", "type:feature"],
        "body": """\
## Amaç
Analistlerin case'leri verimli yönetmesini sağla.

## Kapsam
- **Triage kuyruğu**: `GET /cases?status=triage` yüksek severity'liler önce
- **Assign**: `PUT /cases/{id}/assign` — analist ataması
- **Notlar**: `POST /cases/{id}/notes` — serbest metin, timestamp + actor
- **Etiketler**: `PUT /cases/{id}/labels` — özel etiketler
- **False positive**: `POST /cases/{id}/fp` → status = false_positive, ML geri bildirim olarak loglanıyor
- Dashboard'da tüm bu aksiyonlar UI üzerinden yapılabilir

## Kabul Kriterleri
- [ ] Triage kuyruğu UI'da listeniyor ve sıralanıyor
- [ ] Assign, not ve etiket aksiyonları çalışıyor
- [ ] False positive işaretlemesi audit log'a düşüyor
- [ ] Demo: 50 alert → 5 case → 2 FP işaretlendi

## Tahmini Efor
L (3–4 gün)
""",
    },
    {
        "title": "[E6.3] Dashboard güçlendirme: graph view, timeline, coğrafi harita, model explanation paneli",
        "labels": ["P0", "epic:case-mgmt", "sprint-2", "type:feature"],
        "body": """\
## Amaç
Case ekranında "kanıt paketi" tek sayfada görünsün; jüriye görsel kanıt sağla.

## Kapsam
- **Graph view**: Case'e bağlı transaction ağını interaktif göster (vis.js / D3 / Cytoscape)
- **Timeline**: Case'e bağlı alert ve event'lerin kronolojik görünümü
- **Coğrafi harita**: Transaction lokasyonları (Leaflet veya benzeri)
- **Model explanation paneli**: SHAP değerleri + rule/graph evidence özeti (E4.4 çıktısı)

## Kabul Kriterleri
- [ ] Case detay sayfasında 4 panel var
- [ ] Graph view: node = hesap/entity, edge = transaction
- [ ] Timeline: alertlerin sıralı görünümü
- [ ] Harita: lokasyon verisi olan transaction'lar için pin gösteriliyor
- [ ] Explanation paneli SHAP top-5 ve rule evidence'ı gösteriyor

## Tahmini Efor
L (4–5 gün)
""",
    },

    # -----------------------------------------------------------------------
    # EPIC 7 — Observability & SRE
    # -----------------------------------------------------------------------
    {
        "title": "[E7.1] Prometheus metrikleri: latency, detector hit rate, consumer lag, error rate",
        "labels": ["P0", "epic:observability", "sprint-1", "type:infra"],
        "body": """\
## Amaç
Sistem sağlığını ve SLA'ları ölçülebilir şekilde izle.

## Kapsam
Metrikler:
- `sentinelflow_request_duration_seconds` (histogram, p50/p95/p99)
- `sentinelflow_detector_hits_total` (counter, detector_name label)
- `sentinelflow_kafka_consumer_lag` (gauge, topic + group label)
- `sentinelflow_error_total` (counter, error_type label)
- `sentinelflow_alerts_total` (counter, severity label)

`/metrics` endpoint'i (Prometheus scrape)  
Dashboard'da "SLA Paneli" sayfası

## Kabul Kriterleri
- [ ] `/metrics` endpoint aktif ve Prometheus formatında
- [ ] p95 latency grafiği dashboard'da
- [ ] Consumer lag grafiği dashboard'da
- [ ] Alert oranı grafiği dashboard'da

## Tahmini Efor
S (1 gün)
""",
    },
    {
        "title": "[E7.2] OpenTelemetry tracing: transaction'ın uçtan uca izi",
        "labels": ["P0", "epic:observability", "sprint-4", "type:infra"],
        "body": """\
## Amaç
Bir transaction'ın ingest → detect → persist → notify yolculuğunu trace et.

## Kapsam
- `opentelemetry-sdk` ile FastAPI ve Kafka consumer'ı enstrümanla
- Trace: `ingest → compliance → rule_engine → ml_inference → db_write → ws_notify`
- Jaeger veya OTLP collector (Docker Compose'a ekle)
- Her span'da `transaction_id` attribute

## Kabul Kriterleri
- [ ] Tek transaction için uçtan uca trace görülebiliyor
- [ ] Jaeger UI veya benzeri Docker Compose'da ayakta
- [ ] Her span'da `transaction_id` var
- [ ] p95 latency trace'den hesaplanabilir

## Tahmini Efor
M (2 gün)
""",
    },
    {
        "title": "[E7.3] Load/benchmark suite: tek komutla 1k/5k/10k TPS benchmark + rapor",
        "labels": ["P0", "epic:observability", "sprint-4", "type:test"],
        "body": """\
## Amaç
Performans iddialarını kanıtlanabilir benchmark raporuyla destekle.

## Kapsam
- `scripts/benchmark.py` veya `locust` / `k6` tabanlı yük testi
- Parametreler: `--tps 1000 --duration 60s`
- Çıktı: p50/p95/p99 latency, throughput, error rate, Kafka lag
- Sonuçlar `reports/benchmark_{timestamp}.json` ve `reports/benchmark_{timestamp}.html`

## Kabul Kriterleri
- [ ] `make benchmark TPS=1000` ile tek komut çalışıyor
- [ ] 1k, 5k, 10k TPS için rapor üretiliyor
- [ ] p95 latency < 500ms @ 1k TPS (hedef)
- [ ] Rapor CI artifact olarak saklanıyor

## Tahmini Efor
M (2 gün)
""",
    },

    # -----------------------------------------------------------------------
    # EPIC 8 — CI/CD + Kalite kapıları
    # -----------------------------------------------------------------------
    {
        "title": "[E8.1] Test piramidi: unit + integration (Kafka/Redis/Neo4j/Postgres) + e2e demo",
        "labels": ["P0", "epic:ci-cd", "sprint-4", "type:test"],
        "body": """\
## Amaç
CI'de kritik testler yeşil olmadan merge engellensin.

## Kapsam
- **Unit**: Detector, pipeline, feature hesaplama fonksiyonları
- **Integration**: Kafka → consumer → DB yazma; Redis feature store; Neo4j graph sorguları; Postgres CRUD
- **E2E**: `scripts/run_demo.py` ile 1000 tx replay → alertler DB'de → dashboard listeler

Test altyapısı: `pytest`, `testcontainers-python` (Kafka/Postgres/Redis/Neo4j için)

## Kabul Kriterleri
- [ ] `pytest tests/unit/` tüm yeşil
- [ ] `pytest tests/integration/` tüm yeşil (testcontainers ile)
- [ ] E2E demo scripti CI'de çalışıyor
- [ ] Coverage ≥ 70% (unit + integration)
- [ ] CI'de başarısız test → merge bloklanıyor

## Tahmini Efor
L (4–5 gün)
""",
    },
    {
        "title": "[E8.2] Lint/type/security scan CI: ruff, mypy, bandit + dependency audit",
        "labels": ["P0", "epic:ci-cd", "sprint-4", "type:ci-cd"],
        "body": """\
## Amaç
Kod kalitesini ve güvenliğini otomatik kontrol et; CI "enterprise" görünsün.

## Kapsam
- **ruff**: linting + formatting (`ruff check` + `ruff format --check`)
- **mypy**: strict type checking
- **bandit**: Python güvenlik taraması (SAST)
- **pip-audit** veya **safety**: dependency vulnerability audit
- **trufflehog** / `detect-secrets`: hardcoded secret scan

Tümü `.github/workflows/ci.yml`'de ayrı job olarak

## Kabul Kriterleri
- [ ] ruff, mypy, bandit CI'de çalışıyor
- [ ] dependency audit CI'de çalışıyor
- [ ] Secret scan CI'de çalışıyor
- [ ] Herhangi biri başarısız → PR merge bloklanıyor
- [ ] CI badge README'de görünüyor

## Tahmini Efor
S (1 gün)
""",
    },
    {
        "title": "[E8.3] Containerization: tek `docker compose up` ile tüm sistem ayağa kalkıyor",
        "labels": ["P0", "epic:ci-cd", "sprint-1", "type:infra"],
        "body": """\
## Amaç
Demo için tek komutla tam sistem başlatılabilsin.

## Kapsam
`docker-compose.yml` servisleri:
- `postgres`, `redis`, `kafka` + `zookeeper`, `neo4j`
- `backend` (FastAPI)
- `frontend` (dashboard)
- `kafka-ingestor` (transaction üreteci)
- `prometheus` + `grafana` (opsiyonel ama etkileyici)
- `jaeger` (E7.2 için)

Health check'ler: tüm servisler için `healthcheck` tanımlı  
`README.md`'de "Quick Start" bölümü güncellenmeli

## Kabul Kriterleri
- [ ] `docker compose up --build` → tüm servisler healthy
- [ ] `docker compose down && docker compose up` → veriler korunuyor (volume)
- [ ] README'de tek komut talimatı var
- [ ] CI'de `docker compose up` + smoke test çalışıyor

## Tahmini Efor
M (1–2 gün)
""",
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def gh(token: str, method: str, path: str, **kwargs):
    url = f"{API_BASE}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    resp = requests.request(method, url, headers=headers, **kwargs)
    resp.raise_for_status()
    return resp.json()


def ensure_labels(token: str):
    print("🏷  Labels oluşturuluyor...")
    existing = {lbl["name"] for lbl in gh(token, "GET", f"/repos/{REPO}/labels")}
    for lbl in LABELS:
        if lbl["name"] in existing:
            print(f"   ✓ (var) {lbl['name']}")
            continue
        try:
            gh(token, "POST", f"/repos/{REPO}/labels", json=lbl)
            print(f"   + {lbl['name']}")
        except requests.HTTPError as e:
            print(f"   ! {lbl['name']}: {e}")
        time.sleep(0.3)


def create_issues(token: str, dry_run: bool = False):
    print(f"\n📋 {len(ISSUES)} issue oluşturuluyor...\n")
    for i, issue in enumerate(ISSUES, 1):
        if dry_run:
            print(f"  [DRY-RUN] #{i} {issue['title']}")
            continue
        try:
            result = gh(token, "POST", f"/repos/{REPO}/issues", json=issue)
            print(f"  ✅ #{result['number']} {issue['title']}")
        except requests.HTTPError as e:
            print(f"  ❌ {issue['title']}: {e.response.text}")
        time.sleep(1)  # GitHub API rate limit için


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="SentinelFlow GitHub Issues oluşturucu")
    parser.add_argument("--token", help="GitHub token (GITHUB_TOKEN env de kullanılabilir)")
    parser.add_argument("--dry-run", action="store_true", help="Issue oluşturmadan listele")
    args = parser.parse_args()

    token = args.token or os.environ.get("GITHUB_TOKEN")
    if not token and not args.dry_run:
        print("❌ GITHUB_TOKEN bulunamadı. --token parametresi veya GITHUB_TOKEN env kullanın.")
        sys.exit(1)

    if args.dry_run:
        print("🔍 DRY-RUN modu — hiçbir şey oluşturulmaz\n")
        create_issues(token="", dry_run=True)
        return

    ensure_labels(token)
    create_issues(token)
    print("\n✨ Tamamlandı!")


if __name__ == "__main__":
    main()
