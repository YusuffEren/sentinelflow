# SentinelFlow - API Route Authentication Tests
#
# Verifies that previously-unprotected endpoints now enforce JWT auth.

import importlib.util
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("sqlalchemy") is None,
    reason="sqlalchemy not installed",
)


@pytest.fixture()
def auth_client():
    """TestClient backed by an in-memory SQLite DB with real seeded users."""
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool

    from sentinelflow.api.app import app
    from sentinelflow.api.deps import get_db_session
    from sentinelflow.database.models import RefreshTokenModel, UserModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    # UserModel uses PostgreSQL JSONB; teach SQLite compiler to render it as JSON.
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.dialects.sqlite.base import SQLiteTypeCompiler

    def _visit_JSONB(self, type_, **kw):  # noqa: N802
        return self.visit_JSON(type_, **kw)

    SQLiteTypeCompiler.visit_JSONB = _visit_JSONB

    UserModel.__table__.create(engine)
    RefreshTokenModel.__table__.create(engine)
    SessionLocal = sessionmaker(bind=engine)
    session = SessionLocal()

    from sentinelflow.auth.service import AuthService
    from sentinelflow.contracts import UserCreate, UserRole

    svc = AuthService(session)
    svc.register(
        UserCreate(
            username="viewer1",
            email="viewer@example.com",
            password="Viewer123",
            full_name="View User",
            role=UserRole.VIEWER,
        )
    )
    svc.register(
        UserCreate(
            username="analyst1",
            email="analyst@example.com",
            password="Analyst123",
            full_name="Analyst User",
            role=UserRole.ANALYST,
        )
    )
    session.commit()

    viewer_token = svc._create_access_token(svc._get_user_by_username("viewer1"))
    analyst_token = svc._create_access_token(svc._get_user_by_username("analyst1"))

    def _override_session():
        yield session

    app.dependency_overrides[get_db_session] = _override_session

    from fastapi.testclient import TestClient

    client = TestClient(app)

    yield client, viewer_token, analyst_token

    app.dependency_overrides.clear()
    session.close()


class TestAlertsAuth:
    def test_list_alerts_requires_auth(self, auth_client):
        client, _, _ = auth_client
        resp = client.get("/api/v1/alerts")
        assert resp.status_code == 401

    def test_graph_data_with_viewer_token(self, auth_client):
        # graph/data has no DB dependency beyond auth and falls back to mock
        # data when Neo4j is unavailable, so it exercises a protected endpoint.
        client, viewer_token, _ = auth_client
        resp = client.get("/api/v1/graph/data", headers={"Authorization": f"Bearer {viewer_token}"})
        assert resp.status_code != 401

    def test_list_alerts_rejects_bad_token(self, auth_client):
        client, _, _ = auth_client
        resp = client.get("/api/v1/alerts", headers={"Authorization": "Bearer not.a.token"})
        assert resp.status_code == 401


class TestDismissAuth:
    def test_dismiss_requires_analyst(self, auth_client):
        client, viewer_token, _ = auth_client
        resp = client.post(
            "/api/v1/alerts/ALR-1/dismiss",
            headers={"Authorization": f"Bearer {viewer_token}"},
        )
        assert resp.status_code == 403

    def test_dismiss_unauthenticated(self, auth_client):
        client, _, _ = auth_client
        resp = client.post("/api/v1/alerts/ALR-1/dismiss")
        assert resp.status_code == 401


_TX = {
    "transaction_id": "TX-1",
    "sender_iban": "TR1",
    "sender_name": "A",
    "sender_city": "Istanbul",
    "receiver_iban": "TR2",
    "receiver_name": "B",
    "receiver_city": "Ankara",
    "amount": 100.0,
}


class TestTransactionsAuth:
    def test_submit_requires_auth(self, auth_client):
        client, _, _ = auth_client
        resp = client.post("/api/v1/transactions", json=_TX)
        assert resp.status_code == 401

    def test_submit_with_jwt(self, auth_client):
        client, _, analyst_token = auth_client
        resp = client.post(
            "/api/v1/transactions",
            json=_TX,
            headers={"Authorization": f"Bearer {analyst_token}"},
        )
        assert resp.status_code != 401

    def test_submit_with_api_key(self, auth_client, monkeypatch):
        import sentinelflow.api.app as app_module

        monkeypatch.setattr(app_module, "_INGEST_API_KEY", "test-secret-key")
        client, _, _ = auth_client
        resp = client.post(
            "/api/v1/transactions", json=_TX, headers={"X-API-Key": "test-secret-key"}
        )
        assert resp.status_code != 401

    def test_submit_with_wrong_api_key(self, auth_client, monkeypatch):
        import sentinelflow.api.app as app_module

        monkeypatch.setattr(app_module, "_INGEST_API_KEY", "test-secret-key")
        client, _, _ = auth_client
        resp = client.post("/api/v1/transactions", json=_TX, headers={"X-API-Key": "wrong-key"})
        assert resp.status_code == 401


class TestKYCAuth:
    def test_screen_requires_auth(self, auth_client):
        client, _, _ = auth_client
        resp = client.post("/api/v1/kyc/screen", json={"name": "Test Person"})
        assert resp.status_code == 401

    def test_screen_with_analyst(self, auth_client):
        client, _, analyst_token = auth_client
        resp = client.post(
            "/api/v1/kyc/screen",
            json={"name": "Ahmet Politikacı"},
            headers={"Authorization": f"Bearer {analyst_token}"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["has_matches"] is True
        assert data["risk_score"] > 0

    def test_screen_clean_name(self, auth_client):
        client, _, analyst_token = auth_client
        resp = client.post(
            "/api/v1/kyc/screen",
            json={"name": "Random Person XYZ"},
            headers={"Authorization": f"Bearer {analyst_token}"},
        )
        assert resp.status_code == 200
        assert resp.json()["has_matches"] is False


class TestCORS:
    def test_preflight_allows_configured_origin(self):
        import importlib

        import sentinelflow.api.app as app_module

        importlib.reload(app_module)
        from fastapi.testclient import TestClient

        client = TestClient(app_module.app)
        resp = client.options(
            "/api/v1/alerts",
            headers={
                "Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "GET",
                "Access-Control-Request-Headers": "Authorization",
            },
        )
        assert resp.status_code == 200
        assert resp.headers.get("access-control-allow-origin") == "http://localhost:3000"

    def test_wildcard_origin_disables_credentials(self, monkeypatch):
        monkeypatch.setenv("CORS_ORIGINS", "*")
        from sentinelflow.api.app import _cors_settings

        origins, allow_credentials = _cors_settings()
        assert origins == ["*"]
        assert allow_credentials is False
