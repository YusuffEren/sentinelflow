# =============================================================================
# SentinelFlow - AI Chat API Routes
# =============================================================================
"""
AI-powered chat interface for fraud analysis assistance.

Provides natural language interface for:
- Asking about suspicious activities
- Getting explanations for fraud alerts
- Querying system statistics
- Understanding fraud patterns

Note: This is a rule-based chatbot. For production, integrate with:
- OpenAI GPT-4
- Local LLM (Llama, Mistral)
- RAG with fraud knowledge base
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException
from loguru import logger
from pydantic import BaseModel, Field

router = APIRouter(prefix="/chat", tags=["Chat"])


# =============================================================================
# Schemas
# =============================================================================


class ChatMessage(BaseModel):
    """User chat message."""
    message: str = Field(..., min_length=1, max_length=1000)
    context: Optional[Dict[str, Any]] = Field(default_factory=dict)
    session_id: Optional[str] = None


class ChatResponse(BaseModel):
    """AI chat response."""
    response: str
    suggestions: List[str] = Field(default_factory=list)
    sources: List[str] = Field(default_factory=list)
    confidence: float = 0.9
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


# =============================================================================
# Knowledge Base (Rule-based responses)
# =============================================================================

FRAUD_KNOWLEDGE = {
    "circular_ring": {
        "description": "Döngüsel transfer (Circular Ring), paranın A → B → C → A şeklinde döngüsel olarak transfer edilmesidir. Bu, kara para aklama işlemlerinin tipik bir göstergesidir.",
        "indicators": [
            "3-6 hesap arasında döngüsel para akışı",
            "Her transferde %5-10 azalan tutarlar",
            "Kısa sürede (24-72 saat) tamamlanan döngü",
            "Hesapların daha önce birbiriyle işlem yapmamış olması"
        ],
        "action": "Bu tür bir tespit, MASAK'a bildirilmesi gereken Şüpheli İşlem Bildirimi (ŞİB) kapsamındadır."
    },
    "impossible_travel": {
        "description": "İmkansız Seyahat tespiti, fiziksel olarak mümkün olmayan lokasyon değişikliklerini tespit eder. Örneğin, İstanbul'da işlem yapıldıktan 10 dakika sonra Berlin'de işlem yapılması.",
        "indicators": [
            "900 km/h üzerinde hesaplanan seyahat hızı",
            "Farklı ülkelerden kısa sürede işlemler",
            "Yeni cihaz/IP adresi kullanımı"
        ],
        "action": "Hesap ele geçirme (Account Takeover) şüphesi ile müşteri ile acil iletişime geçilmelidir."
    },
    "structuring": {
        "description": "Yapılandırma (Structuring/Smurfing), MASAK bildirim eşiğinin (75.000 TL) altında kalacak şekilde işlemlerin parçalara bölünmesidir.",
        "indicators": [
            "74.000-74.999 TL arası çoklu işlemler",
            "Aynı gün içinde farklı alıcılara transferler",
            "Toplam tutar eşiği geçen parçalı işlemler"
        ],
        "action": "MASAK mevzuatı ihlali - Şüpheli İşlem Bildirimi zorunludur."
    },
    "ml_ensemble": {
        "description": "ML Ensemble tespiti, IsolationForest, XGBoost ve AutoEncoder modellerinin birleşik oylama sonucuyla tespit edilmiştir.",
        "indicators": [
            "İstatistiksel olarak anormal işlem tutarı",
            "Kullanıcı davranış profilinden sapma",
            "Çoklu model tarafından şüpheli olarak işaretlenme"
        ],
        "action": "Yapay zeka güvenilirlik skoru %85+ ise manuel inceleme önerilir."
    }
}

COMMON_RESPONSES = {
    "greeting": [
        "Merhaba! SentinelFlow AI asistanıyım. Size fraud tespiti ve uyarılar hakkında yardımcı olabilirim.",
        "Hoş geldiniz! Şüpheli işlemler veya fraud kalıpları hakkında sorularınızı yanıtlayabilirim."
    ],
    "help": [
        "Size şu konularda yardımcı olabilirim:\n"
        "• Fraud türlerini açıklama\n"
        "• Şüpheli işlem kalıplarını analiz etme\n"
        "• MASAK uyumluluğu hakkında bilgi\n"
        "• Alert'ler hakkında detay sağlama\n\n"
        "Örnek sorular:\n"
        "- 'Döngüsel transfer nedir?'\n"
        "- 'Bu işlem neden şüpheli?'\n"
        "- 'MASAK eşiği ne kadar?'"
    ],
    "masak": [
        "MASAK (Mali Suçları Araştırma Kurulu), Türkiye'nin mali istihbarat birimidir.\n\n"
        "Önemli eşikler:\n"
        "• 75.000 TL üzeri nakit işlemler bildirilmelidir\n"
        "• Şüpheli İşlem Bildirimi (ŞİB) zorunluluğu\n"
        "• 10 iş günü içinde bildirim yapılmalı\n\n"
        "SentinelFlow, MASAK uyumlu raporlama desteği sağlar."
    ],
    "unknown": [
        "Üzgünüm, bu konuda kesin bir yanıtım yok. Fraud türleri, MASAK uyumluluğu veya şüpheli işlem kalıpları hakkında sorular sorabilirsiniz.",
        "Bu soruyu tam anlayamadım. 'yardım' yazarak neler sorabileceğinizi görebilirsiniz."
    ]
}


# =============================================================================
# Intent Detection
# =============================================================================


def detect_intent(message: str) -> tuple[str, Dict[str, Any]]:
    """Detect user intent from message."""
    message_lower = message.lower().strip()
    
    if any(w in message_lower for w in ["merhaba", "selam", "hello", "hi"]):
        return "greeting", {}
    
    if any(w in message_lower for w in ["yardım", "help", "ne sorabilir", "neler"]):
        return "help", {}
    
    if any(w in message_lower for w in ["masak", "bddk", "bildirim", "eşik"]):
        return "masak", {}
    
    for fraud_type in FRAUD_KNOWLEDGE.keys():
        if fraud_type.replace("_", " ") in message_lower or fraud_type in message_lower:
            return "fraud_explain", {"fraud_type": fraud_type}
    
    if any(w in message_lower for w in ["döngüsel", "circular", "ring", "halka"]):
        return "fraud_explain", {"fraud_type": "circular_ring"}
    
    if any(w in message_lower for w in ["imkansız", "seyahat", "travel", "impossible"]):
        return "fraud_explain", {"fraud_type": "impossible_travel"}
    
    if any(w in message_lower for w in ["yapılandırma", "smurfing", "structuring", "parçala"]):
        return "fraud_explain", {"fraud_type": "structuring"}
    
    if any(w in message_lower for w in ["ml", "makine", "yapay zeka", "ensemble", "ai"]):
        return "fraud_explain", {"fraud_type": "ml_ensemble"}
    
    if any(w in message_lower for w in ["neden şüpheli", "why suspicious", "neden fraud"]):
        return "explain_alert", {}
    
    if any(w in message_lower for w in ["istatistik", "kaç", "toplam", "stat"]):
        return "stats", {}
    
    return "unknown", {}


def generate_response(intent: str, params: Dict[str, Any], context: Dict[str, Any]) -> ChatResponse:
    """Generate response based on intent."""
    import random
    
    if intent == "greeting":
        return ChatResponse(
            response=random.choice(COMMON_RESPONSES["greeting"]),
            suggestions=["Fraud türlerini açıkla", "MASAK hakkında bilgi ver", "Yardım"]
        )
    
    if intent == "help":
        return ChatResponse(
            response=random.choice(COMMON_RESPONSES["help"]),
            suggestions=["Döngüsel transfer nedir?", "İmkansız seyahat ne demek?", "MASAK eşiği"]
        )
    
    if intent == "masak":
        return ChatResponse(
            response=random.choice(COMMON_RESPONSES["masak"]),
            suggestions=["Şüpheli işlem bildirimi nasıl yapılır?", "Fraud türleri"]
        )
    
    if intent == "fraud_explain":
        fraud_type = params.get("fraud_type", "")
        if fraud_type in FRAUD_KNOWLEDGE:
            knowledge = FRAUD_KNOWLEDGE[fraud_type]
            indicators_text = "\n".join(f"• {ind}" for ind in knowledge["indicators"])
            
            response = f"""**{fraud_type.replace('_', ' ').title()}**

{knowledge['description']}

**Göstergeler:**
{indicators_text}

**Önerilen Aksiyon:**
{knowledge['action']}"""
            
            return ChatResponse(
                response=response,
                suggestions=["Diğer fraud türleri", "MASAK bildirimi", "İstatistikleri göster"],
                sources=["SentinelFlow Fraud Knowledge Base"],
                confidence=0.95
            )
    
    if intent == "explain_alert":
        alert_context = context.get("alert", {})
        fraud_type = alert_context.get("fraud_type", context.get("fraud_type", ""))
        amount = alert_context.get("amount", context.get("amount", 0))
        
        if fraud_type and fraud_type in FRAUD_KNOWLEDGE:
            knowledge = FRAUD_KNOWLEDGE[fraud_type]
            response = f"""Bu işlem **{fraud_type.replace('_', ' ').title()}** olarak tespit edildi.

{knowledge['description']}

İşlem tutarı: {amount:,.2f} TL

Bu tespitin nedeni, sistemin yukarıdaki kalıpları bu işlemde tespit etmiş olmasıdır."""
            
            return ChatResponse(
                response=response,
                suggestions=["Detaylı analiz", "Benzer vakalar", "Aksiyon öner"],
                confidence=0.9
            )
        
        return ChatResponse(
            response="Bu alert için ek bağlam bilgisi gerekiyor. Hangi fraud türü hakkında bilgi istiyorsunuz?",
            suggestions=list(FRAUD_KNOWLEDGE.keys())
        )
    
    if intent == "stats":
        return ChatResponse(
            response="İstatistikler için dashboard'daki 'System Stats' bölümünü kontrol edebilirsiniz. `/api/v1/system/stats` endpoint'i üzerinden de verilere ulaşabilirsiniz.",
            suggestions=["Fraud oranı nedir?", "En yaygın fraud türü hangisi?"]
        )
    
    return ChatResponse(
        response=random.choice(COMMON_RESPONSES["unknown"]),
        suggestions=["Yardım", "Fraud türleri", "MASAK bilgisi"],
        confidence=0.5
    )


# =============================================================================
# Endpoints
# =============================================================================


@router.post("", response_model=ChatResponse)
@router.post("/", response_model=ChatResponse)
async def chat(message: ChatMessage) -> ChatResponse:
    """
    Process a chat message and return AI response.
    
    This is a rule-based chatbot for fraud analysis assistance.
    For production, integrate with LLM APIs.
    """
    try:
        intent, params = detect_intent(message.message)
        
        logger.debug(f"Chat intent: {intent}, params: {params}")
        
        response = generate_response(intent, params, message.context or {})
        
        return response
        
    except Exception as e:
        logger.error(f"Chat error: {e}")
        return ChatResponse(
            response="Bir hata oluştu. Lütfen tekrar deneyin.",
            suggestions=["Yardım"],
            confidence=0.0
        )


@router.get("/suggestions")
async def get_suggestions() -> List[str]:
    """Get suggested questions for the chat interface."""
    return [
        "Döngüsel transfer nedir?",
        "İmkansız seyahat tespiti nasıl çalışır?",
        "MASAK bildirimi ne zaman yapılmalı?",
        "ML modelleri nasıl fraud tespit eder?",
        "Yapılandırma (smurfing) nedir?",
    ]


@router.get("/knowledge/{fraud_type}")
async def get_fraud_knowledge(fraud_type: str) -> Dict[str, Any]:
    """Get detailed knowledge about a specific fraud type."""
    if fraud_type not in FRAUD_KNOWLEDGE:
        raise HTTPException(status_code=404, detail=f"Unknown fraud type: {fraud_type}")
    
    return {
        "fraud_type": fraud_type,
        **FRAUD_KNOWLEDGE[fraud_type]
    }
