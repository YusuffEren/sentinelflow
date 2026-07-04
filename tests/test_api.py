# =============================================================================
# SentinelFlow - API Endpoint Tests
# =============================================================================
"""
Tests for FastAPI endpoints.

Run with: pytest tests/test_api.py -v
"""

import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from fastapi.testclient import TestClient


@pytest.fixture
def client(mock_db_session):
    """Create test client."""
    from sentinelflow.api.app import app
    return TestClient(app)


class TestHealthEndpoint:
    """Tests for /api/v1/system/health endpoint."""
    
    def test_health_check_returns_200(self, client):
        """Health endpoint should return 200 OK."""
        response = client.get("/api/v1/system/health")
        assert response.status_code == 200
    
    def test_health_check_returns_status(self, client):
        """Health endpoint should return status field."""
        response = client.get("/api/v1/system/health")
        data = response.json()
        assert "status" in data
        assert data["status"] in ["healthy", "degraded", "unhealthy"]
    
    def test_health_check_returns_version(self, client):
        """Health endpoint should return version."""
        response = client.get("/api/v1/system/health")
        data = response.json()
        assert "version" in data


class TestStatsEndpoint:
    """Tests for /api/v1/system/stats endpoint."""
    
    def test_stats_returns_200(self, client):
        """Stats endpoint should return 200 OK."""
        response = client.get("/api/v1/system/stats")
        assert response.status_code == 200
    
    def test_stats_contains_required_fields(self, client):
        """Stats endpoint should contain required fields."""
        response = client.get("/api/v1/system/stats")
        data = response.json()
        
        required_fields = [
            "transactions_processed",
            "fraud_detected",
            "uptime_seconds",
        ]
        
        for field in required_fields:
            assert field in data, f"Missing field: {field}"


class TestRootEndpoint:
    """Tests for root endpoint."""
    
    def test_root_returns_200(self, client):
        """Root endpoint should return 200 OK."""
        response = client.get("/")
        assert response.status_code == 200
    
    def test_root_returns_service_info(self, client):
        """Root endpoint should return service info."""
        response = client.get("/")
        data = response.json()
        assert "service" in data
        assert "version" in data


class TestMetricsEndpoint:
    """Tests for /metrics endpoint."""
    
    def test_metrics_returns_200(self, client):
        """Metrics endpoint should return 200 OK."""
        response = client.get("/metrics")
        assert response.status_code == 200
    
    def test_metrics_returns_prometheus_format(self, client):
        """Metrics endpoint should return Prometheus format."""
        response = client.get("/metrics")
        content = response.text
        
        assert "sentinelflow_transactions_processed_total" in content
        assert "sentinelflow_fraud_detected_total" in content


class TestTransactionEndpoint:
    """Tests for /api/v1/transactions endpoint."""
    
    def test_submit_transaction_valid(self, client):
        """Valid transaction should be accepted."""
        transaction = {
            "sender_iban": "TR1234567890123456789012",
            "sender_name": "Test Sender",
            "sender_city": "Istanbul",
            "receiver_iban": "TR9876543210987654321098",
            "receiver_name": "Test Receiver",
            "receiver_city": "Ankara",
            "amount": 1500.00,
            "description": "Test transfer",
        }
        
        response = client.post("/api/v1/transactions", json=transaction)
        assert response.status_code == 200
        
        data = response.json()
        assert "transaction_id" in data
        assert "is_fraud" in data
        assert "fraud_score" in data
    
    def test_submit_transaction_missing_amount(self, client):
        """Transaction without amount should fail."""
        transaction = {
            "sender_iban": "TR1234567890123456789012",
            "receiver_iban": "TR9876543210987654321098",
        }
        
        response = client.post("/api/v1/transactions", json=transaction)
        assert response.status_code == 422


class TestAlertsEndpoint:
    """Tests for /api/v1/alerts endpoints."""
    
    def test_list_alerts_returns_200(self, client):
        """List alerts should return 200 OK."""
        response = client.get("/api/v1/alerts")
        assert response.status_code == 200
    
    def test_list_alerts_with_pagination(self, client):
        """List alerts with pagination should work."""
        response = client.get("/api/v1/alerts?page=1&page_size=10")
        assert response.status_code == 200
        
        data = response.json()
        assert "alerts" in data
        assert "total" in data
        assert "page" in data
    
    def test_list_alerts_with_filter(self, client):
        """List alerts with severity filter should work."""
        response = client.get("/api/v1/alerts?severity=critical")
        assert response.status_code == 200


class TestChatEndpoint:
    """Tests for /api/v1/chat endpoint."""
    
    def test_chat_returns_200(self, client):
        """Chat endpoint should return 200 OK."""
        response = client.post(
            "/api/v1/chat",
            json={"message": "Merhaba"}
        )
        assert response.status_code == 200
    
    def test_chat_greeting(self, client):
        """Chat should respond to greeting."""
        response = client.post(
            "/api/v1/chat",
            json={"message": "Merhaba"}
        )
        data = response.json()
        
        assert "response" in data
        assert len(data["response"]) > 0
    
    def test_chat_fraud_explanation(self, client):
        """Chat should explain fraud types."""
        response = client.post(
            "/api/v1/chat",
            json={"message": "Döngüsel transfer nedir?"}
        )
        data = response.json()
        
        assert "response" in data
        assert "circular" in data["response"].lower() or "döngüsel" in data["response"].lower()
    
    def test_chat_with_context(self, client):
        """Chat should accept context."""
        response = client.post(
            "/api/v1/chat",
            json={
                "message": "Bu işlem neden şüpheli?",
                "context": {
                    "fraud_type": "circular_ring",
                    "amount": 150000
                }
            }
        )
        data = response.json()
        
        assert "response" in data
    
    def test_chat_suggestions_endpoint(self, client):
        """Suggestions endpoint should return list."""
        response = client.get("/api/v1/chat/suggestions")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0


class TestGraphEndpoint:
    """Tests for /api/v1/graph endpoints."""
    
    def test_graph_data_returns_200(self, client):
        """Graph data endpoint should return 200 OK."""
        response = client.get("/api/v1/graph/data")
        assert response.status_code == 200
    
    def test_graph_data_structure(self, client):
        """Graph data should have nodes and links."""
        response = client.get("/api/v1/graph/data")
        data = response.json()
        
        assert "nodes" in data
        assert "links" in data
        assert isinstance(data["nodes"], list)
        assert isinstance(data["links"], list)
    
    def test_graph_rings_returns_200(self, client):
        """Fraud rings endpoint should return 200 OK."""
        response = client.get("/api/v1/graph/rings")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)


class TestWebSocket:
    """Tests for WebSocket endpoint."""
    
    def test_websocket_connection(self, client):
        """WebSocket should accept connection."""
        with client.websocket_connect("/ws/alerts") as websocket:
            data = websocket.receive_json()
            assert data["type"] == "connection"
    
    def test_websocket_ping_pong(self, client):
        """WebSocket should respond to ping."""
        with client.websocket_connect("/ws/alerts") as websocket:
            websocket.receive_json()
            
            websocket.send_text("ping")
            response = websocket.receive_json()
            
            assert response["type"] == "pong"
