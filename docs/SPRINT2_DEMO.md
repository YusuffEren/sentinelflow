# Sprint 2 Demo Senaryosu

## Genel Bakış

Sprint 2, SentinelFlow'a kurumsal güvenlik özellikleri ekler:
- **JWT Authentication** - Kullanıcı kimlik doğrulama
- **RBAC** - Rol tabanlı erişim kontrolü (admin, analyst, viewer)
- **User Management** - Kullanıcı yönetimi
- **Protected Routes** - Korumalı frontend sayfaları
- **ML Training API** - Model eğitim endpoint'leri
- **Cases UI** - Vaka yönetimi sayfası

---

## Başlangıç

### 1. Altyapıyı Başlat

```bash
cd C:\Users\yusuf\Desktop\sentinelflow
docker compose up -d
```

### 2. Database Migration

```bash
alembic upgrade head
```

### 3. API'yi Başlat

```bash
python -m sentinelflow.api.app
```

### 4. Dashboard'u Başlat

```bash
cd sentinelflow-web
npm run dev
```

---

## Demo Senaryoları

### Senaryo 1: Kullanıcı Girişi

1. **Dashboard'a git**: http://localhost:3000

2. **Login sayfasına yönlendirileceksin**: http://localhost:3000/login

3. **Demo credentials ile giriş yap**:
   - Username: `admin`
   - Password: `Admin123!`

4. **Dashboard'a yönlendirileceksin** ve header'da kullanıcı bilgilerini göreceksin

5. **Logout test**: Header'daki çıkış butonuna tıkla

---

### Senaryo 2: JWT Token Flow

1. **Login API çağrısı**:
   ```powershell
   $body = @{ username = "admin"; password = "Admin123!" } | ConvertTo-Json
   Invoke-RestMethod -Uri http://localhost:8000/api/v1/auth/login -Method POST -Body $body -ContentType "application/json"
   ```

2. **Response**:
   ```json
   {
     "access_token": "eyJhbGc...",
     "refresh_token": "xxx...",
     "token_type": "bearer",
     "expires_in": 1800
   }
   ```

3. **Protected endpoint çağrısı**:
   ```powershell
   $headers = @{ Authorization = "Bearer <ACCESS_TOKEN>" }
   Invoke-RestMethod -Uri http://localhost:8000/api/v1/auth/me -Headers $headers
   ```

4. **Token refresh**:
   ```powershell
   $body = @{ refresh_token = "<REFRESH_TOKEN>" } | ConvertTo-Json
   Invoke-RestMethod -Uri http://localhost:8000/api/v1/auth/refresh -Method POST -Body $body -ContentType "application/json"
   ```

---

### Senaryo 3: RBAC (Rol Tabanlı Erişim)

1. **Yeni viewer kullanıcısı oluştur**:
   ```powershell
   $body = @{
     username = "viewer1"
     email = "viewer@test.com"
     password = "Viewer123!"
     full_name = "Test Viewer"
     role = "viewer"
   } | ConvertTo-Json
   
   Invoke-RestMethod -Uri http://localhost:8000/api/v1/auth/register -Method POST -Body $body -ContentType "application/json"
   ```

2. **Viewer ile giriş yap ve ML training endpoint'ine eriş**:
   - Viewer rolü training endpoint'ine erişemez (403 Forbidden)
   - Admin rolü tüm endpoint'lere erişebilir

---

### Senaryo 4: ML Model Training

1. **Model durumunu kontrol et**:
   ```powershell
   Invoke-RestMethod -Uri http://localhost:8000/api/v1/ml/models
   ```

2. **Training başlat** (admin token ile):
   ```powershell
   $headers = @{ Authorization = "Bearer <ADMIN_TOKEN>" }
   $body = @{ n_samples = 5000; fraud_ratio = 0.05 } | ConvertTo-Json
   
   Invoke-RestMethod -Uri http://localhost:8000/api/v1/ml/train -Method POST -Headers $headers -Body $body -ContentType "application/json"
   ```

3. **Training durumunu takip et**:
   ```powershell
   Invoke-RestMethod -Uri http://localhost:8000/api/v1/ml/train/status
   ```

4. **Feature listesini göster**:
   ```powershell
   Invoke-RestMethod -Uri http://localhost:8000/api/v1/ml/features
   ```

---

### Senaryo 5: Cases Sayfası

1. **Cases sayfasına git**: http://localhost:3000/cases

2. **Gösterilecekler**:
   - Vaka listesi (pagination ile)
   - Durum ve öncelik filtreleri
   - İstatistik kartları (toplam, açık, çözümlenen)
   - Vaka detayları

3. **API üzerinden case oluştur**:
   ```powershell
   $headers = @{ Authorization = "Bearer <TOKEN>" }
   $body = @{
     title = "Şüpheli Transfer Zinciri"
     alert_ids = @("ALERT-XXX")
     priority = "P2"
   } | ConvertTo-Json
   
   Invoke-RestMethod -Uri http://localhost:8000/api/v1/cases -Method POST -Headers $headers -Body $body -ContentType "application/json"
   ```

---

### Senaryo 6: Navigation & User Experience

1. **Header navigation**:
   - Dashboard
   - Alarmlar
   - Vakalar

2. **Her sayfada user info gösterimi**:
   - Kullanıcı adı
   - Rol
   - Logout butonu

3. **Protected routes**:
   - Token olmadan dashboard'a erişim → login'e redirect
   - Login olduktan sonra → dashboard'a redirect

---

## Sprint 2 vs Sprint 1 Karşılaştırması

| Özellik | Sprint 1 | Sprint 2 |
|---------|----------|----------|
| Authentication | Yok | JWT + Refresh Token |
| Authorization | Yok | RBAC (admin, analyst, viewer) |
| User Management | Yok | Register, login, password change |
| Frontend Auth | Yok | Protected routes, auth context |
| Navigation | Minimal | Full header nav |
| Cases UI | Yok | Full CRUD sayfası |
| ML Management | API only | Training endpoint + status |

---

## API Endpoints (Sprint 2'de eklenenler)

### Auth
- `POST /api/v1/auth/login` - Kullanıcı girişi
- `POST /api/v1/auth/logout` - Çıkış
- `POST /api/v1/auth/register` - Kayıt
- `POST /api/v1/auth/refresh` - Token yenileme
- `GET /api/v1/auth/me` - Mevcut kullanıcı bilgisi
- `POST /api/v1/auth/change-password` - Şifre değiştirme

### ML
- `GET /api/v1/ml/models` - Model durumları
- `POST /api/v1/ml/train` - Training başlat
- `GET /api/v1/ml/train/status` - Training durumu
- `GET /api/v1/ml/features` - Feature listesi

---

## Teknik Detaylar

- **JWT Algorithm**: HS256
- **Access Token Expiry**: 30 dakika
- **Refresh Token Expiry**: 7 gün
- **Password Hash**: bcrypt
- **Account Lock**: 5 başarısız deneme → 15 dakika kilit

---

## Sonraki Adımlar (Sprint 3)

1. **STR (Suspicious Transaction Report)** - MASAK bildirimi
2. **Audit Log UI** - Case timeline görüntüleme
3. **Email Notifications** - Alert bildirimleri
4. **API Rate Limiting** - DDoS koruması
5. **Kubernetes Deployment** - Production hazırlığı
