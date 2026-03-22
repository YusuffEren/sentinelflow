# =============================================================================
# SentinelFlow - PEP and Sanctions Screening
# =============================================================================
"""
PEP (Politically Exposed Persons) and Sanctions screening module.

Provides screening capabilities against:
- PEP databases
- International sanctions lists (OFAC, EU, UN)
- Adverse media
- Custom watchlists

In production, these would integrate with external providers like:
- Dow Jones Risk & Compliance
- World-Check
- OFAC SDN List
- EU Consolidated List
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
import re
import unicodedata

from loguru import logger


# =============================================================================
# Enums
# =============================================================================

class ScreeningType(str, Enum):
    """Type of screening."""
    
    PEP = "PEP"
    SANCTIONS = "YAPTIRIMLAR"
    ADVERSE_MEDIA = "OLUMSUZ_MEDYA"
    WATCHLIST = "IZLEME_LISTESI"


class MatchType(str, Enum):
    """Type of match found."""
    
    EXACT = "TAM_ESLESME"
    PARTIAL = "KISMI_ESLESME"
    FUZZY = "BULANIK_ESLESME"
    ALIAS = "TAKMA_AD"


class PEPLevel(str, Enum):
    """PEP exposure level."""
    
    DOMESTIC_HIGH = "YURT_ICI_YUKSEK"  # President, minister, etc.
    DOMESTIC_MEDIUM = "YURT_ICI_ORTA"  # MP, mayor, etc.
    FOREIGN_HIGH = "YURT_DISI_YUKSEK"
    FOREIGN_MEDIUM = "YURT_DISI_ORTA"
    RELATIVE = "YAKIN_AILE"
    ASSOCIATE = "IS_ORTAGI"


class SanctionsList(str, Enum):
    """Sanctions lists."""
    
    OFAC_SDN = "OFAC_SDN"
    EU_CONSOLIDATED = "AB_KONSOLIDE"
    UN_CONSOLIDATED = "BM_KONSOLIDE"
    UK_SANCTIONS = "INGILTERE"
    TURKEY_MASAK = "TURKIYE_MASAK"


# =============================================================================
# Demo Data
# =============================================================================

# Demo PEP database (in production, use external service)
DEMO_PEP_DATABASE = {
    "Ahmet Politikacı": {
        "level": PEPLevel.DOMESTIC_HIGH,
        "position": "Eski Bakan",
        "country": "Türkiye",
        "active": True,
        "since": "2015-01-01",
        "until": None,
    },
    "Mehmet Vali": {
        "level": PEPLevel.DOMESTIC_MEDIUM,
        "position": "Vali",
        "country": "Türkiye",
        "active": True,
        "since": "2020-06-15",
        "until": None,
    },
    "John Foreign": {
        "level": PEPLevel.FOREIGN_HIGH,
        "position": "Foreign Minister",
        "country": "USA",
        "active": False,
        "since": "2010-01-01",
        "until": "2020-12-31",
    },
    "Ali Akraba": {
        "level": PEPLevel.RELATIVE,
        "position": "Bakan Eşi",
        "country": "Türkiye",
        "active": True,
        "related_pep": "Ahmet Politikacı",
    },
}

# Demo sanctions database (in production, use external service)
DEMO_SANCTIONS_DATABASE = {
    "Yasak Kişi": {
        "list": SanctionsList.OFAC_SDN,
        "reason": "Money laundering",
        "added_date": "2020-01-15",
        "country": "Unknown",
        "aliases": ["Banned Person", "Y. Kişi"],
    },
    "Terror Financer": {
        "list": SanctionsList.UN_CONSOLIDATED,
        "reason": "Terrorist financing",
        "added_date": "2019-06-01",
        "country": "Syria",
        "aliases": ["TF", "Terrorist"],
    },
    "Kara Para": {
        "list": SanctionsList.TURKEY_MASAK,
        "reason": "Para aklama",
        "added_date": "2022-03-10",
        "country": "Türkiye",
        "aliases": ["Kara Para Aklayıcı"],
    },
}

# Demo adverse media keywords
ADVERSE_MEDIA_KEYWORDS = [
    "dolandırıcılık",
    "yolsuzluk",
    "rüşvet",
    "kara para",
    "terör",
    "fraud",
    "corruption",
    "bribery",
    "money laundering",
    "terrorism",
]


# =============================================================================
# Data Structures
# =============================================================================

@dataclass
class ScreeningMatch:
    """A match found during screening."""
    
    match_id: str = ""
    matched_name: str = ""
    query_name: str = ""
    match_type: MatchType = MatchType.PARTIAL
    match_score: float = 0.0  # 0-100
    screening_type: ScreeningType = ScreeningType.PEP
    details: dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "match_id": self.match_id,
            "matched_name": self.matched_name,
            "query_name": self.query_name,
            "match_type": self.match_type.value,
            "match_score": round(self.match_score, 2),
            "screening_type": self.screening_type.value,
            "details": self.details,
        }


@dataclass
class ScreeningResult:
    """Result of a screening check."""
    
    screening_id: str = ""
    query_name: str = ""
    screening_types: list[ScreeningType] = field(default_factory=list)
    has_matches: bool = False
    matches: list[ScreeningMatch] = field(default_factory=list)
    risk_score: float = 0.0  # Aggregate risk from matches
    recommendation: str = ""
    screened_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    
    def to_dict(self) -> dict[str, Any]:
        return {
            "screening_id": self.screening_id,
            "query_name": self.query_name,
            "screening_types": [t.value for t in self.screening_types],
            "has_matches": self.has_matches,
            "matches_count": len(self.matches),
            "matches": [m.to_dict() for m in self.matches],
            "risk_score": round(self.risk_score, 2),
            "recommendation": self.recommendation,
            "screened_at": self.screened_at,
        }
    
    def summary(self) -> str:
        """Generate summary."""
        status = "⚠️ EŞLEŞME VAR" if self.has_matches else "✅ TEMİZ"
        return (
            f"[{status}] '{self.query_name}' | "
            f"Matches: {len(self.matches)} | "
            f"Risk: {self.risk_score:.0f}/100"
        )


# =============================================================================
# PEP Screener
# =============================================================================

class PEPScreener:
    """
    Politically Exposed Persons (PEP) screening.
    
    Screens customers against PEP databases to identify:
    - Domestic and foreign PEPs
    - PEP relatives and close associates
    - Former PEPs (historical)
    
    Example:
        >>> screener = PEPScreener()
        >>> result = screener.screen("Ahmet Politikacı")
        >>> if result.has_matches:
        ...     print("PEP found!")
    """
    
    def __init__(self, include_inactive: bool = True):
        """
        Initialize PEP screener.
        
        Args:
            include_inactive: Include historical/inactive PEPs
        """
        self._include_inactive = include_inactive
        self._screen_count = 0
        
        logger.info("PEPScreener initialized")
    
    def screen(
        self,
        name: str,
        country: str | None = None,
        additional_info: dict[str, Any] | None = None,
    ) -> ScreeningResult:
        """
        Screen a name against PEP database.
        
        Args:
            name: Name to screen
            country: Optional country filter
            additional_info: Additional context
        
        Returns:
            ScreeningResult with matches
        """
        self._screen_count += 1
        
        result = ScreeningResult(
            screening_id=f"PEP-{self._screen_count:06d}",
            query_name=name,
            screening_types=[ScreeningType.PEP],
        )
        
        matches = []
        
        # Normalize query
        query_normalized = self._normalize_name(name)
        
        for pep_name, pep_info in DEMO_PEP_DATABASE.items():
            # Skip inactive if not included
            if not self._include_inactive and not pep_info.get("active", True):
                continue
            
            # Country filter
            if country and pep_info.get("country", "").lower() != country.lower():
                continue
            
            # Check for matches
            match_score = self._calculate_match_score(query_normalized, pep_name)
            
            if match_score >= 70:
                match_type = (
                    MatchType.EXACT if match_score >= 95
                    else MatchType.PARTIAL if match_score >= 85
                    else MatchType.FUZZY
                )
                
                matches.append(ScreeningMatch(
                    match_id=f"PEP-{hash(pep_name) % 100000:05d}",
                    matched_name=pep_name,
                    query_name=name,
                    match_type=match_type,
                    match_score=match_score,
                    screening_type=ScreeningType.PEP,
                    details={
                        "pep_level": pep_info.get("level", PEPLevel.DOMESTIC_MEDIUM).value if isinstance(pep_info.get("level"), PEPLevel) else str(pep_info.get("level", "")),
                        "position": pep_info.get("position", ""),
                        "country": pep_info.get("country", ""),
                        "active": pep_info.get("active", True),
                        "related_pep": pep_info.get("related_pep"),
                    },
                ))
        
        result.matches = matches
        result.has_matches = len(matches) > 0
        
        # Calculate aggregate risk
        if matches:
            max_score = max(m.match_score for m in matches)
            level_multiplier = 1.0
            
            for match in matches:
                level = match.details.get("pep_level", "")
                if "YUKSEK" in level or "HIGH" in level:
                    level_multiplier = 1.5
                    break
            
            result.risk_score = min(100, max_score * level_multiplier)
            result.recommendation = "EDD gerekli - PEP tespit edildi"
        else:
            result.risk_score = 0.0
            result.recommendation = "PEP eşleşmesi yok"
        
        logger.info(f"PEP screening: {result.summary()}")
        
        return result
    
    def _normalize_name(self, name: str) -> str:
        """Normalize name for comparison."""
        # Remove accents/diacritics, special characters, lowercase
        normalized = unicodedata.normalize("NFKD", name)
        normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        normalized = re.sub(r'[^\w\s]', '', normalized.lower())
        # Remove extra whitespace
        normalized = ' '.join(normalized.split())
        return normalized
    
    def _calculate_match_score(self, query: str, target: str) -> float:
        """Calculate similarity score between names."""
        query_norm = self._normalize_name(query)
        target_norm = self._normalize_name(target)
        
        # Exact match
        if query_norm == target_norm:
            return 100.0
        
        # Substring match
        if query_norm in target_norm or target_norm in query_norm:
            return 90.0
        
        # Word overlap
        query_words = set(query_norm.split())
        target_words = set(target_norm.split())
        
        if not query_words or not target_words:
            return 0.0
        
        overlap = len(query_words & target_words)
        total = max(len(query_words), len(target_words))
        
        return (overlap / total) * 100


# =============================================================================
# Sanctions Checker
# =============================================================================

class SanctionsChecker:
    """
    International sanctions screening.
    
    Screens against:
    - OFAC SDN List (US)
    - EU Consolidated List
    - UN Consolidated List
    - Turkey MASAK List
    
    Example:
        >>> checker = SanctionsChecker()
        >>> result = checker.check("Yasak Kişi")
        >>> if result.has_matches:
        ...     print("SANCTIONS HIT!")
    """
    
    def __init__(
        self,
        lists: list[SanctionsList] | None = None,
        check_aliases: bool = True,
    ):
        """
        Initialize sanctions checker.
        
        Args:
            lists: Specific lists to check (default: all)
            check_aliases: Also check aliases
        """
        self._lists = lists or list(SanctionsList)
        self._check_aliases = check_aliases
        self._screen_count = 0
        
        logger.info(f"SanctionsChecker initialized (lists: {len(self._lists)})")
    
    def check(
        self,
        name: str,
        additional_info: dict[str, Any] | None = None,
    ) -> ScreeningResult:
        """
        Check a name against sanctions lists.
        
        Args:
            name: Name to check
            additional_info: Additional context (DOB, country, etc.)
        
        Returns:
            ScreeningResult with matches
        """
        self._screen_count += 1
        
        result = ScreeningResult(
            screening_id=f"SAN-{self._screen_count:06d}",
            query_name=name,
            screening_types=[ScreeningType.SANCTIONS],
        )
        
        matches = []
        query_norm = self._normalize_name(name)
        
        for sanctioned_name, sanction_info in DEMO_SANCTIONS_DATABASE.items():
            # Check if list is in our scope
            sanction_list = sanction_info.get("list")
            if sanction_list not in self._lists:
                continue
            
            names_to_check = [sanctioned_name]
            
            if self._check_aliases:
                names_to_check.extend(sanction_info.get("aliases", []))
            
            for check_name in names_to_check:
                match_score = self._calculate_match_score(query_norm, check_name)
                
                if match_score >= 70:
                    match_type = (
                        MatchType.EXACT if match_score >= 95
                        else MatchType.ALIAS if check_name != sanctioned_name
                        else MatchType.PARTIAL if match_score >= 85
                        else MatchType.FUZZY
                    )
                    
                    matches.append(ScreeningMatch(
                        match_id=f"SAN-{hash(sanctioned_name) % 100000:05d}",
                        matched_name=sanctioned_name,
                        query_name=name,
                        match_type=match_type,
                        match_score=match_score,
                        screening_type=ScreeningType.SANCTIONS,
                        details={
                            "list": sanction_list.value if isinstance(sanction_list, SanctionsList) else str(sanction_list),
                            "reason": sanction_info.get("reason", ""),
                            "added_date": sanction_info.get("added_date", ""),
                            "country": sanction_info.get("country", ""),
                            "matched_alias": check_name if check_name != sanctioned_name else None,
                        },
                    ))
                    break  # One match per sanctioned entity
        
        result.matches = matches
        result.has_matches = len(matches) > 0
        
        # Sanctions are high-severity
        if matches:
            result.risk_score = 100.0  # Always critical
            result.recommendation = "İŞLEM YAPILAMAZ - Yaptırım listesinde"
        else:
            result.risk_score = 0.0
            result.recommendation = "Yaptırım eşleşmesi yok"
        
        logger.info(f"Sanctions check: {result.summary()}")
        
        return result
    
    def _normalize_name(self, name: str) -> str:
        """Normalize name for comparison."""
        normalized = unicodedata.normalize("NFKD", name)
        normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
        normalized = re.sub(r'[^\w\s]', '', normalized.lower())
        normalized = ' '.join(normalized.split())
        return normalized
    
    def _calculate_match_score(self, query: str, target: str) -> float:
        """Calculate similarity score."""
        query_norm = self._normalize_name(query)
        target_norm = self._normalize_name(target)
        
        if query_norm == target_norm:
            return 100.0
        
        if query_norm in target_norm or target_norm in query_norm:
            return 90.0
        
        query_words = set(query_norm.split())
        target_words = set(target_norm.split())
        
        if not query_words or not target_words:
            return 0.0
        
        overlap = len(query_words & target_words)
        total = max(len(query_words), len(target_words))
        
        return (overlap / total) * 100


# =============================================================================
# Combined Screener
# =============================================================================

class CombinedScreener:
    """
    Combined screening against all databases.
    
    Runs PEP, sanctions, and adverse media checks in one call.
    
    Example:
        >>> screener = CombinedScreener()
        >>> result = screener.screen_full("Customer Name")
    """
    
    def __init__(self):
        self._pep_screener = PEPScreener()
        self._sanctions_checker = SanctionsChecker()
        self._screen_count = 0
    
    def screen_full(
        self,
        name: str,
        country: str | None = None,
        additional_info: dict[str, Any] | None = None,
    ) -> ScreeningResult:
        """
        Perform full screening (PEP + Sanctions).
        
        Args:
            name: Name to screen
            country: Optional country
            additional_info: Additional context
        
        Returns:
            Combined ScreeningResult
        """
        self._screen_count += 1
        
        # Run both screenings
        pep_result = self._pep_screener.screen(name, country, additional_info)
        sanctions_result = self._sanctions_checker.check(name, additional_info)
        
        # Combine results
        combined = ScreeningResult(
            screening_id=f"FULL-{self._screen_count:06d}",
            query_name=name,
            screening_types=[ScreeningType.PEP, ScreeningType.SANCTIONS],
        )
        
        combined.matches = pep_result.matches + sanctions_result.matches
        combined.has_matches = len(combined.matches) > 0
        combined.risk_score = max(pep_result.risk_score, sanctions_result.risk_score)
        
        # Generate recommendation
        if sanctions_result.has_matches:
            combined.recommendation = "İŞLEM YAPILAMAZ - Yaptırım eşleşmesi"
        elif pep_result.has_matches:
            combined.recommendation = "EDD gerekli - PEP tespit edildi"
        else:
            combined.recommendation = "Tarama temiz"
        
        return combined
