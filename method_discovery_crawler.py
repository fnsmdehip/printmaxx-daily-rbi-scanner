#!/usr/bin/env python3
"""
METHOD DISCOVERY CRAWLER
=========================
Daily crawls Twitter, Reddit, and other sources for NEW money-making methods
not yet in the PRINTMAXX system.  Scores each against Capital Genesis criteria
and routes high-scoring discoveries for ops generation.

This is NOT the same as the existing alpha scrapers. Those scrape specific
accounts for general alpha. This specifically hunts for NEW REVENUE METHODS --
business models, monetization strategies, side hustles -- and scores them
against Capital Genesis criteria before routing to ventures.

Recommended cron: 0 5 * * * (5 AM daily, before alpha scrapers at 6 AM)
  crontab entry:
  0 5 * * * cd /Users/macbookpro/Documents/p/PRINTMAXX_STARTER_KITttttt && python3 AUTOMATIONS/method_discovery_crawler.py --crawl >> AUTOMATIONS/logs/method_discovery.log 2>&1

Usage:
    python3 AUTOMATIONS/method_discovery_crawler.py --crawl          # full scan
    python3 AUTOMATIONS/method_discovery_crawler.py --score          # score pending
    python3 AUTOMATIONS/method_discovery_crawler.py --report         # summary
    python3 AUTOMATIONS/method_discovery_crawler.py --new-only       # only show NEW methods
    python3 AUTOMATIONS/method_discovery_crawler.py --dry-run        # preview without writes

Stdlib only. Zero external dependencies.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
import urllib.parse

csv.field_size_limit(10 * 1024 * 1024)

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
LEDGER_DIR = PROJECT_ROOT / "LEDGER"
OPS_DIR = PROJECT_ROOT / "OPS"
AUTOMATIONS_DIR = PROJECT_ROOT / "AUTOMATIONS"
LOG_DIR = AUTOMATIONS_DIR / "logs"

ALPHA_STAGING = LEDGER_DIR / "ALPHA_STAGING.csv"
DISCOVERY_LOG = LEDGER_DIR / "METHOD_DISCOVERY_LOG.csv"
DISCOVERED_METHODS_DIR = AUTOMATIONS_DIR / "auto_ops" / "discovered_methods"
MASTER_OPS_CACHE = AUTOMATIONS_DIR / "master_ops_cache.json"
LAST_RUN_MARKER = AUTOMATIONS_DIR / ".method_discovery_last_run"
LOCK_FILE = AUTOMATIONS_DIR / "locks" / "method_discovery.lock"
LOG_FILE = LOG_DIR / "method_discovery.log"

# Canonical ALPHA_STAGING schema
ALPHA_FIELDS = [
    "alpha_id", "source", "source_url", "category", "tactic",
    "roi_potential", "priority", "status", "applicable_methods",
    "applicable_niches", "synergy_score", "cross_sell_products",
    "implementation_priority", "engagement_authenticity",
    "earnings_verified", "extracted_method", "compliance_notes",
    "reviewer_notes", "created_at", "ops_generated",
]

DISCOVERY_LOG_FIELDS = [
    "date", "source", "method_name", "method_summary", "source_url",
    "composite_score", "capital_genesis_priority", "revenue_potential",
    "speed_to_revenue", "automation_potential", "upfront_cost",
    "synergy_score", "status", "alpha_id",
]

# ---------------------------------------------------------------------------
# Resilience imports (fallbacks for standalone execution)
# pyright: reportAssignmentType=false, reportRedeclaration=false
# ---------------------------------------------------------------------------
try:
    sys.path.insert(0, str(PROJECT_ROOT / "AUTOMATIONS"))
    from agent_resilience import (  # type: ignore
        retry, safe_path, sanitize_for_prompt,
        TrajectoryLogger, CircuitBreaker, CircuitBreakerOpen,
    )
except ImportError:
    def safe_path(target):  # type: ignore
        resolved = Path(target).resolve()
        if not str(resolved).startswith(str(PROJECT_ROOT.resolve())):
            raise ValueError(f"BLOCKED: {resolved} outside {PROJECT_ROOT}")
        return resolved

    def retry(max_attempts=5, base_delay=2.0, on_failure_return=None):  # type: ignore
        def decorator(fn):
            def wrapper(*args, **kwargs):
                for attempt in range(max_attempts):
                    try:
                        return fn(*args, **kwargs)
                    except Exception:
                        if attempt == max_attempts - 1:
                            return on_failure_return if on_failure_return is not None else ""
                        time.sleep(base_delay ** (attempt + 1))
                return on_failure_return if on_failure_return is not None else ""
            return wrapper
        return decorator

    def sanitize_for_prompt(text, field_name="input"):  # type: ignore
        if not isinstance(text, str):
            return ""
        return text[:10_000]

    class TrajectoryLogger:  # type: ignore
        def __init__(self, name): self.name = name
        def log_attempt(self, action, **extra): return time.time()
        def log_success(self, action, start=0.0, **extra): pass
        def log_failure(self, action, error="", start=0.0, **extra): pass

    class CircuitBreaker:  # type: ignore
        def __init__(self, name="default", **kwargs): self.name = name
        def __enter__(self): return self
        def __exit__(self, *args): return False

    class CircuitBreakerOpen(Exception):  # type: ignore
        pass


# ---------------------------------------------------------------------------
# Logging / helpers
# ---------------------------------------------------------------------------
def _now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _log(msg: str) -> None:
    ts = _now_iso()
    line = f"[{ts}] [METHOD_DISCOVERY] {msg}"
    print(line, file=sys.stderr)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def _content_hash(text: str) -> str:
    """MD5 hash of normalized text for dedup."""
    normalized = re.sub(r"\s+", " ", text.lower().strip())
    return hashlib.md5(normalized.encode()).hexdigest()[:24]


def _ensure_dirs() -> None:
    for d in [LOG_DIR, DISCOVERED_METHODS_DIR, AUTOMATIONS_DIR / "locks"]:
        safe_path(d)
        d.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# Source configuration
# ---------------------------------------------------------------------------

# Twitter search queries for method discovery
TWITTER_SEARCH_QUERIES = [
    # Proof-pattern queries (people sharing real numbers)
    "made $10k from",
    "hit $5k MRR",
    "revenue screenshot",
    "first $1000 from",
    "quit my job built",
    # Method-specific queries (targeted, not keyword soup)
    "cold email closed deal",
    "faceless channel monetized",
    "micro saas launched",
    "newsletter sponsor revenue",
    "digital product sales proof",
    "affiliate commission earned",
    "AI automation client",
    "lead gen agency results",
    "print on demand store profit",
    "TikTok shop sales",
    "indie app revenue",
    "sold templates gumroad",
    "SEO niche site income",
    "dropshipping profit margin",
    # Brokering / connector plays
    "referral fee business",
    "broker connector business",
    "lead gen service revenue",
    "white label report sold",
    "domain flipping profit",
    "api arbitrage income",
    "equipment financing broker",
    "merchant processing referral",
]

# Reddit subreddits for method discovery
METHOD_SUBREDDITS = [
    {"name": "Entrepreneur", "category": "BUSINESS"},
    {"name": "juststart", "category": "SEO"},
    {"name": "passive_income", "category": "PASSIVE"},
    {"name": "SideHustle", "category": "SIDE_HUSTLE"},
    {"name": "dropship", "category": "ECOM"},
    {"name": "Affiliatemarketing", "category": "AFFILIATE"},
    {"name": "ecommerce", "category": "ECOM"},
    {"name": "SaaS", "category": "SAAS"},
    {"name": "startups", "category": "STARTUP"},
    {"name": "beermoney", "category": "MICRO_INCOME"},
    {"name": "WorkOnline", "category": "REMOTE"},
    {"name": "sweatystartup", "category": "LOCAL_BIZ"},
    {"name": "EntrepreneurRideAlong", "category": "BUSINESS"},
    {"name": "MicroSaas", "category": "SAAS"},
    {"name": "Flipping", "category": "ARB"},
    {"name": "digitalnomad", "category": "REMOTE"},
    {"name": "growthhacking", "category": "GROWTH"},
    {"name": "indiehackers", "category": "BUILDING"},
]

# Method detection keywords (signals that a post describes a money method)
METHOD_SIGNAL_KEYWORDS = [
    # Revenue proof signals
    r"\$\d[\d,]*(?:\.\d+)?(?:/mo|/month|/yr|/year|/day|/week|k|K)?",
    r"\b\d+k?\s*(?:mrr|arr|revenue|profit|income)\b",
    r"\b(?:made|earned|generated|cleared|netted|grossed)\s+\$",
    # Method description signals
    r"\b(?:how\s+(?:i|to|we)|here'?s?\s+(?:how|my)|step[- ]by[- ]step|my\s+method)\b",
    r"\b(?:business\s+model|revenue\s+(?:stream|model|source)|income\s+source)\b",
    r"\b(?:side\s+hustle|passive\s+income|money\s+(?:making|online))\b",
    r"\b(?:monetiz|profit\s+margin|cash\s+flow|break\s*even)\b",
    # Specific method types
    r"\b(?:affiliate|dropship|print\s+on\s+demand|saas|subscription|freemium)\b",
    r"\b(?:course|ebook|template|digital\s+product|info\s+product)\b",
    r"\b(?:agency|freelanc|consult|coaching|mentor)\b",
    r"\b(?:newsletter|substack|beehiiv|convertkit)\b",
    r"\b(?:ecom|shopify|etsy|amazon\s+(?:fba|kdp)|tiktok\s+shop)\b",
    r"\b(?:lead\s+gen|cold\s+email|outbound|b2b)\b",
    r"\b(?:faceless|youtube|tiktok|content\s+(?:creation|farm))\b",
    r"\b(?:automation|bot|scraper|api|tool|chrome\s+extension)\b",
    r"\b(?:flipping|arbitrage|resell|wholesale)\b",
    # Brokering / connector signals
    r"\b(?:referral\s+fee|broker|connector\s+business|lead\s+gen\s+service)\b",
    r"\b(?:white\s+label|domain\s+flip|api\s+arbitrage|merchant\s+processing)\b",
    r"\b(?:equipment\s+financ|insurance\s+referral|sba\s+loan|freight\s+broker)\b",
]

# Low-value signals (deprioritize or skip)
LOW_VALUE_SIGNALS = [
    r"\b(?:survey|captcha|click\s+ads|watch\s+videos|gpt\s+wrapper)\b",
    r"\b(?:mlm|pyramid|ponzi|get\s+rich\s+quick)\b",
    r"\b(?:crypto\s+trading|forex\s+signal|binary\s+option)\b",
    r"\b(?:gambling|betting\s+tip|casino)\b",
    r"\b(?:survey\s+site|referral\s+code|cash\s*back|sign\s*up\s+bonus)\b",
    r"\b(?:data\s+entry|microtask|transcription\s+job)\b",
]


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

class DeduplicationEngine:
    """Check discovered methods against existing alpha and ops."""

    def __init__(self):
        self._existing_urls: set[str] = set()
        self._existing_hashes: set[str] = set()
        self._ops_methods: set[str] = set()
        self._load()

    def _load(self) -> None:
        # Load existing ALPHA_STAGING URLs and content hashes
        if ALPHA_STAGING.exists():
            try:
                with open(ALPHA_STAGING, "r", encoding="utf-8", errors="replace", newline="") as f:
                    for row in csv.DictReader(f):
                        url = (row.get("source_url") or "").strip()
                        if url:
                            self._existing_urls.add(url)
                        tactic = (row.get("tactic") or "").strip()
                        if tactic:
                            self._existing_hashes.add(_content_hash(tactic[:300]))
            except Exception as e:
                _log(f"Warning: could not load ALPHA_STAGING: {e}")

        # Load master ops cache for method names
        if MASTER_OPS_CACHE.exists():
            try:
                cache = json.loads(MASTER_OPS_CACHE.read_text())
                for sheet_name, rows in cache.items():
                    if isinstance(rows, list):
                        for row in rows:
                            if isinstance(row, dict):
                                for key in ["method", "tactic", "name", "title", "ops_name"]:
                                    val = row.get(key, "")
                                    if val:
                                        self._ops_methods.add(val.lower().strip())
            except Exception as e:
                _log(f"Warning: could not load master ops cache: {e}")

        # Load existing discovery log hashes
        if DISCOVERY_LOG.exists():
            try:
                with open(DISCOVERY_LOG, "r", encoding="utf-8", errors="replace", newline="") as f:
                    for row in csv.DictReader(f):
                        summary = (row.get("method_summary") or "").strip()
                        if summary:
                            self._existing_hashes.add(_content_hash(summary[:300]))
            except Exception:
                pass

        _log(f"Dedup loaded: {len(self._existing_urls)} URLs, {len(self._existing_hashes)} hashes, {len(self._ops_methods)} ops methods")

    def is_duplicate(self, url: str, text: str) -> tuple[bool, str]:
        """Returns (is_dup, reason)."""
        url = url.strip()
        if url and url in self._existing_urls:
            return True, f"URL already in alpha: {url[:60]}"

        text_hash = _content_hash(text[:300])
        if text_hash in self._existing_hashes:
            return True, f"Content hash match: {text[:60]}..."

        # Check if method name matches ops
        text_lower = text.lower()
        for method in self._ops_methods:
            if len(method) > 10 and method in text_lower:
                return True, f"Method already in ops: {method[:60]}"

        return False, ""

    def register(self, url: str, text: str) -> None:
        """Register a new entry to prevent within-run duplicates."""
        if url:
            self._existing_urls.add(url.strip())
        if text:
            self._existing_hashes.add(_content_hash(text[:300]))


# ---------------------------------------------------------------------------
# Capital Genesis Scoring
# ---------------------------------------------------------------------------

class CapitalGenesisScorer:
    """Score discovered methods against Capital Genesis criteria."""

    # Weights for composite score
    WEIGHTS = {
        "revenue_potential": 0.25,
        "speed_to_revenue": 0.20,
        "automation_potential": 0.20,
        "synergy_score": 0.15,
        "downside_risk_inv": 0.10,   # inverted: lower risk = higher score
        "liability_risk_inv": 0.10,  # inverted: lower risk = higher score
    }

    # Venture types for synergy matching
    EXISTING_VENTURES = [
        "OUTBOUND", "CONTENT", "APP_FACTORY", "LOCAL_BIZ",
        "RESEARCH", "MONETIZE", "PRODUCT", "SCRAPING", "EAS",
        "BROKERING",
    ]

    def score(self, method: dict[str, Any]) -> dict[str, Any]:
        """Score a method and return full scoring breakdown."""
        text = (method.get("text") or method.get("title") or "").lower()
        source = (method.get("source") or "").lower()

        # 1. Revenue potential
        rev_potential = self._score_revenue_potential(text)
        rev_label = self._revenue_label(rev_potential)

        # 2. Speed to revenue (days to first dollar, scale 1-10)
        speed = self._score_speed(text)

        # 3. Downside risk (1=no risk, 10=high risk)
        downside = self._score_downside_risk(text)

        # 4. Automation potential (1-10)
        automation = self._score_automation(text)

        # 5. Upfront cost
        cost = self._estimate_cost(text)
        cost_label = self._cost_label(cost)

        # 6. Synergy score (1-10)
        synergy = self._score_synergy(text)

        # 7. Liability risk (1-10)
        liability = self._score_liability(text)

        # Composite: weighted average, normalized to 0-10
        composite = (
            self.WEIGHTS["revenue_potential"] * rev_potential +
            self.WEIGHTS["speed_to_revenue"] * speed +
            self.WEIGHTS["automation_potential"] * automation +
            self.WEIGHTS["synergy_score"] * synergy +
            self.WEIGHTS["downside_risk_inv"] * (10 - downside) +
            self.WEIGHTS["liability_risk_inv"] * (10 - liability)
        )

        # Engagement bonus -- source-aware thresholds
        # HN 100 points = significant, Reddit 500 = moderate, Twitter 1000 = moderate
        engagement = method.get("engagement", 0)
        source = (method.get("source") or "").lower()
        if "hackernews" in source:
            eng_thresholds = (50, 150, 500)
        elif "reddit" in source:
            eng_thresholds = (200, 1000, 5000)
        else:
            eng_thresholds = (500, 2000, 10000)

        if engagement > eng_thresholds[2]:
            composite = min(composite + 1.5, 10)
        elif engagement > eng_thresholds[1]:
            composite = min(composite + 0.8, 10)
        elif engagement > eng_thresholds[0]:
            composite = min(composite + 0.3, 10)

        # Low-value penalty
        for pat in LOW_VALUE_SIGNALS:
            if re.search(pat, text, re.IGNORECASE):
                composite = max(composite - 2.0, 0)
                break

        # Determine priority
        priority = self._determine_priority(
            cost, automation, speed, synergy, composite
        )

        return {
            "revenue_potential": rev_label,
            "revenue_potential_raw": round(rev_potential, 1),
            "speed_to_revenue": round(speed, 1),
            "downside_risk": round(downside, 1),
            "automation_potential": round(automation, 1),
            "upfront_cost": cost_label,
            "synergy_score": round(synergy, 1),
            "liability_risk": round(liability, 1),
            "composite_score": round(composite, 2),
            "capital_genesis_priority": priority,
        }

    def _score_revenue_potential(self, text: str) -> float:
        score = 3.0  # base

        # Dollar amounts
        amounts = re.findall(r"\$(\d[\d,]*(?:\.\d+)?)\s*(k|K)?", text)
        if amounts:
            try:
                max_amt = max(
                    float(a.replace(",", "")) * (1000 if suffix else 1)
                    for a, suffix in amounts
                )
                if max_amt >= 50000:
                    score = 9.5
                elif max_amt >= 10000:
                    score = 8.5
                elif max_amt >= 5000:
                    score = 7.5
                elif max_amt >= 1000:
                    score = 6.5
                elif max_amt >= 100:
                    score = 5.0
            except (ValueError, TypeError):
                pass

        # MRR/ARR mentions
        if re.search(r"\b(?:mrr|arr)\b", text):
            score = max(score, 7.0)

        # Revenue keywords
        rev_kw = ["revenue", "profit", "income", "earning", "commission"]
        hits = sum(1 for kw in rev_kw if kw in text)
        score = min(score + hits * 0.3, 10.0)

        # Scale signals
        if re.search(r"\b(?:scale|scaling|scaled)\b", text):
            score = min(score + 0.5, 10.0)

        return score

    def _revenue_label(self, score: float) -> str:
        if score >= 8.0:
            return "HIGHEST"
        if score >= 6.0:
            return "HIGH"
        if score >= 4.0:
            return "MEDIUM"
        return "LOW"

    def _score_speed(self, text: str) -> float:
        """Higher = faster to revenue. 10 = same day, 1 = 6+ months."""
        score = 5.0

        # Immediate signals
        if re.search(r"\b(?:today|same\s+day|instant|immediately|first\s+day)\b", text):
            score = 9.0
        elif re.search(r"\b(?:this\s+week|within\s+a?\s*week|few\s+days)\b", text):
            score = 8.0
        elif re.search(r"\b(?:this\s+month|30\s+days?|first\s+month)\b", text):
            score = 7.0
        elif re.search(r"\b(?:few\s+months|2-3\s+months|quarter)\b", text):
            score = 4.0
        elif re.search(r"\b(?:6\s+months|year|long[\s-]term)\b", text):
            score = 2.0

        # Method type speed estimates -- only apply if no explicit time signal was found
        # (explicit evidence like "this week" should beat category defaults)
        has_time_signal = score != 5.0
        if not has_time_signal:
            fast_methods = ["freelanc", "consult", "cold email", "service", "lead gen", "flipping"]
            medium_methods = ["affiliate", "dropship", "agency", "template", "digital product"]
            slow_methods = ["saas", "app", "course", "newsletter"]

            for m in fast_methods:
                if m in text:
                    score = max(score, 7.5)
            for m in medium_methods:
                if m in text:
                    score = max(score, 5.5)
            for m in slow_methods:
                if m in text:
                    score = max(score, 3.0)

        return min(score, 10.0)

    def _score_downside_risk(self, text: str) -> float:
        """1=no risk, 10=high risk."""
        score = 3.0

        # High risk signals
        if re.search(r"\b(?:invest|capital|funding|loan|debt)\b", text):
            score = max(score, 6.0)
        if re.search(r"\b(?:inventory|warehouse|physical\s+product)\b", text):
            score = max(score, 5.5)
        if re.search(r"\b(?:legal|lawsuit|compliance|regulat)\b", text):
            score = max(score, 6.5)
        if re.search(r"\b(?:risky|gambl|speculative|volatile)\b", text):
            score = max(score, 7.5)

        # Low risk signals
        if re.search(r"\b(?:free|zero\s+cost|\$0|no\s+(?:money|investment|cost))\b", text):
            score = min(score, 2.0)
        if re.search(r"\b(?:digital|online|remote|laptop)\b", text):
            score = min(score, 3.5)

        return min(score, 10.0)

    def _score_automation(self, text: str) -> float:
        """1=fully manual, 10=fully automatable."""
        score = 4.0

        auto_signals = [
            r"\bautomat", r"\bbot\b", r"\bscript", r"\bcron\b", r"\bschedul",
            r"\bapi\b", r"\bno-code\b", r"\bzapier\b", r"make\.com", r"\bn8n\b",
            r"\bai agent", r"\bclaude\b", r"\bgpt\b", r"\bllm\b",
            r"\bscrap(?:er|ing)\b", r"\bcrawl", r"\bprogrammatic",
            r"\bfaceless\b", r"\bpassive\b", r"\bhands-off\b",
        ]
        manual_signals = [
            r"\bmanual", r"\bhands-on\b", r"\bpersonal\b", r"\bface-to-face\b",
            r"\bphone call", r"\bin-person\b", r"\bcoaching\b", r"\bmentor",
            r"\bcustom\b", r"\bbespoke\b", r"\bhandmade\b",
        ]

        for sig in auto_signals:
            if re.search(sig, text, re.IGNORECASE):
                score = min(score + 1.0, 10.0)
        for sig in manual_signals:
            if re.search(sig, text, re.IGNORECASE):
                score = max(score - 1.0, 1.0)

        return score

    def _estimate_cost(self, text: str) -> float:
        """Estimated upfront cost in dollars."""
        if re.search(r"\b(?:free|zero\s+cost|\$0|no\s+(?:money|investment|cost|upfront))\b", text):
            return 0.0
        if re.search(r"\b(?:cheap|low[\s-]cost|budget|minimal\s+investment)\b", text):
            return 50.0

        # Look for explicit cost mentions
        costs = re.findall(r"\b(?:cost|invest|spend|budget|need)\s+\$?(\d[\d,]*)", text)
        if costs:
            try:
                return min(float(c.replace(",", "")) for c in costs)
            except (ValueError, TypeError):
                pass

        # Defaults by method type
        if re.search(r"\b(?:saas|app|software)\b", text):
            return 200.0
        if re.search(r"\b(?:ecom|inventory|product)\b", text):
            return 300.0
        if re.search(r"\b(?:freelanc|consult|service|digital\s+product)\b", text):
            return 0.0

        return 100.0  # default moderate cost

    def _cost_label(self, cost: float) -> str:
        if cost <= 0:
            return "$0"
        if cost <= 100:
            return "$low"
        if cost <= 500:
            return "$medium"
        return "$high"

    def _score_synergy(self, text: str) -> float:
        """How well does this feed existing PRINTMAXX ventures?"""
        score = 3.0
        synergy_map = {
            "OUTBOUND": ["cold email", "outbound", "lead gen", "b2b", "outreach"],
            "CONTENT": ["content", "social media", "twitter", "tiktok", "youtube", "newsletter", "posting"],
            "APP_FACTORY": ["app", "saas", "software", "tool", "chrome extension", "mobile"],
            "LOCAL_BIZ": ["local", "smb", "small business", "restaurant", "contractor"],
            "MONETIZE": ["monetiz", "revenue", "pricing", "subscription", "freemium"],
            "PRODUCT": ["digital product", "ebook", "course", "template", "notion"],
            "SCRAPING": ["scraper", "crawl", "data", "monitor", "intelligence"],
            "EAS": ["automation", "enterprise", "agency", "consulting"],
            "BROKERING": ["broker", "referral fee", "connector", "lead gen service", "white label", "domain flip", "api arbitrage"],
        }

        hits = 0
        for venture, keywords in synergy_map.items():
            for kw in keywords:
                if kw in text:
                    hits += 1
                    break

        # Capital Genesis cross-pollination: multi-venture synergy is exponentially
        # more valuable. 1 venture = baseline, 2 = good, 3+ = strong cross-pollination.
        if hits >= 3:
            score = min(3.0 + hits * 1.8, 10.0)
        else:
            score = min(3.0 + hits * 1.5, 10.0)
        return score

    def _score_liability(self, text: str) -> float:
        """1=no legal concerns, 10=high legal risk."""
        score = 2.0

        if re.search(r"\b(?:health|medical|supplement|pharma)\b", text):
            score = max(score, 7.0)
        if re.search(r"\b(?:financial\s+advice|invest|securities|crypto\s+trading)\b", text):
            score = max(score, 6.5)
        if re.search(r"\b(?:grey\s*hat|black\s*hat|exploit|hack|scrape.*tos)\b", text):
            score = max(score, 6.0)
        if re.search(r"\b(?:fake|impersonat|mislead|decepti)\b", text):
            score = max(score, 8.0)
        if re.search(r"\b(?:copyright|trademark|patent)\b", text):
            score = max(score, 5.5)
        if re.search(r"\b(?:ftc|gdpr|hipaa|compliance)\b", text):
            score = max(score, 4.0)

        return min(score, 10.0)

    def _determine_priority(
        self, cost: float, automation: float, speed: float,
        synergy: float, composite: float,
    ) -> str:
        """
        P0: $0 cost, automatable, <14 days to revenue, synergy >7
        P1: <$100 cost, semi-automatable, <30 days to revenue
        P2: <$500 cost, requires manual work, <60 days
        P3: everything else worth tracking
        """
        if cost <= 0 and automation >= 6 and speed >= 7 and synergy >= 7:
            return "P0"
        if cost <= 100 and automation >= 4 and speed >= 6:
            return "P1"
        if cost <= 500 and speed >= 4:
            return "P2"
        # High composite can promote P3 to P2 (safety net for methods that score
        # well overall but miss specific thresholds above)
        if composite >= 7.5:
            return "P2"
        return "P3"


# ---------------------------------------------------------------------------
# Method extraction
# ---------------------------------------------------------------------------

def _extract_method_name(text: str) -> str:
    """Extract a short method name from descriptive text."""
    text = text.strip()[:300]

    # Try to find "how I/to X" pattern
    m = re.search(r"how\s+(?:i|to|we)\s+(.{10,60}?)(?:\.|$|\n)", text, re.IGNORECASE)
    if m:
        return m.group(1).strip().rstrip(".,!?")[:80]

    # Try "X method/strategy/approach"
    m = re.search(r"(\b\w[\w\s]{5,40})\s+(?:method|strategy|approach|framework|system|playbook)\b",
                  text, re.IGNORECASE)
    if m:
        return m.group(1).strip()[:80]

    # Fall back to first meaningful segment
    sentences = re.split(r"[.!?\n]", text)
    for s in sentences:
        s = s.strip()
        if 15 < len(s) < 100:
            return s[:80]

    return text[:80] if text else ""


def _has_method_signal(text: str) -> bool:
    """Check if text describes a money-making method (not just general alpha)."""
    text_lower = text.lower()
    hits = 0
    for pattern in METHOD_SIGNAL_KEYWORDS:
        if re.search(pattern, text_lower, re.IGNORECASE):
            hits += 1
            if hits >= 2:
                return True
    return False


def _is_low_value(text: str) -> bool:
    """Check for low-value / scammy signals."""
    text_lower = text.lower()
    for pattern in LOW_VALUE_SIGNALS:
        if re.search(pattern, text_lower, re.IGNORECASE):
            return True
    return False


def _categorize_method(text: str) -> str:
    """Categorize the method type."""
    text_lower = text.lower()
    categories = [
        (["cold email", "outbound", "lead gen", "b2b outreach"], "OUTBOUND"),
        (["saas", "mrr", "arr", "subscription", "micro saas"], "SAAS"),
        (["app", "mobile app", "chrome extension", "ios", "android"], "APP_FACTORY"),
        (["affiliate", "commission", "referral program"], "AFFILIATE"),
        (["dropship", "ecom", "shopify", "amazon fba", "etsy", "tiktok shop"], "ECOM"),
        (["amazon kdp", "kindle direct"], "DIGITAL_PRODUCT"),
        (["content", "youtube", "tiktok", "faceless", "shorts", "newsletter", "substack"], "CONTENT_FARM"),
        (["freelanc", "upwork", "fiverr", "consulting", "agency"], "FREELANCE"),
        (["digital product", "course", "ebook", "template", "notion", "gumroad"], "DIGITAL_PRODUCT"),
        (["seo", "organic traffic", "keyword", "backlink", "niche site"], "SEO"),
        (["automation", "bot", "scraper", "tool", "ai agent"], "AUTOMATION"),
        (["flipping", "arbitrage", "resell", "wholesale"], "ARBITRAGE"),
        (["local", "smb", "small business", "brick and mortar"], "LOCAL_BIZ"),
        (["print on demand", "pod", "merch", "t-shirt"], "POD"),
        (["referral fee", "broker", "connector business", "lead gen service", "white label", "domain flip", "api arbitrage", "merchant processing", "equipment financing", "insurance referral"], "BROKERING"),
    ]
    for keywords, cat in categories:
        if any(kw in text_lower for kw in keywords):
            return cat
    return "GENERAL"


# ---------------------------------------------------------------------------
# Reddit crawler (JSON API, no auth needed)
# ---------------------------------------------------------------------------

class RedditMethodCrawler:
    """Crawl Reddit for new money-making method posts."""

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/131.0.0.0 Safari/537.36"
    }

    def __init__(self):
        self.cb = CircuitBreaker(name="reddit_method_crawler", failure_threshold=5,
                                 recovery_timeout=120)
        self.traj = TrajectoryLogger("method_discovery_reddit")

    @retry(max_attempts=3, base_delay=2.0, on_failure_return=[])
    def _fetch_subreddit(self, subreddit: str, sort: str = "hot",
                         time_filter: str = "week", limit: int = 25) -> list[dict]:
        """Fetch posts from a subreddit via JSON API."""
        url = f"https://www.reddit.com/r/{subreddit}/{sort}.json?t={time_filter}&limit={limit}"
        req = urllib.request.Request(url, headers=self.HEADERS)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError) as e:
            _log(f"Reddit fetch error r/{subreddit}: {e}")
            raise

        posts = []
        for child in data.get("data", {}).get("children", []):
            post = child.get("data", {})
            title = post.get("title", "")
            selftext = post.get("selftext", "")
            permalink = post.get("permalink", "")
            score = post.get("score", 0)
            num_comments = post.get("num_comments", 0)
            created_utc = post.get("created_utc", 0)

            full_text = f"{title} {selftext}"
            if not _has_method_signal(full_text):
                continue
            if _is_low_value(full_text):
                continue
            if len(title) < 20:
                continue

            post_url = f"https://www.reddit.com{permalink}" if permalink else ""

            posts.append({
                "source": f"reddit/r/{subreddit}",
                "source_url": post_url,
                "title": title,
                "text": sanitize_for_prompt(f"{title}\n\n{selftext[:1500]}", "reddit_post"),
                "engagement": score,
                "comments": num_comments,
                "created_utc": created_utc,
                "method_name": _extract_method_name(title),
                "category": _categorize_method(full_text),
            })

        return posts

    def crawl(self) -> list[dict]:
        """Crawl all method subreddits."""
        all_methods = []
        _log(f"Reddit: crawling {len(METHOD_SUBREDDITS)} subreddits...")

        try:
            with self.cb:
                for i, sub in enumerate(METHOD_SUBREDDITS, 1):
                    name = sub["name"]
                    start = self.traj.log_attempt("crawl_subreddit", subreddit=name)
                    try:
                        # Hot posts (current trends)
                        hot = self._fetch_subreddit(name, sort="hot", limit=15)
                        time.sleep(1.0)  # Rate limit between requests to same subreddit
                        # Top of the week (proven engagement)
                        top = self._fetch_subreddit(name, sort="top", time_filter="week", limit=15)
                        time.sleep(1.0)
                        # New posts (freshest methods before they get upvoted)
                        new = self._fetch_subreddit(name, sort="new", limit=10)

                        combined = hot + top + new
                        # Dedup within subreddit
                        seen_urls = set()
                        unique = []
                        for p in combined:
                            if p["source_url"] not in seen_urls:
                                seen_urls.add(p["source_url"])
                                unique.append(p)

                        all_methods.extend(unique)
                        self.traj.log_success("crawl_subreddit", start, count=len(unique))
                        _log(f"  [{i}/{len(METHOD_SUBREDDITS)}] r/{name}: {len(unique)} method posts")

                        time.sleep(1.5)  # Rate limiting
                    except Exception as e:
                        self.traj.log_failure("crawl_subreddit", str(e), start)
                        _log(f"  [{i}/{len(METHOD_SUBREDDITS)}] r/{name}: ERROR - {str(e)[:60]}")
                        continue

        except CircuitBreakerOpen:
            _log("Reddit circuit breaker OPEN, skipping")

        _log(f"Reddit: {len(all_methods)} total method posts found")
        return all_methods


# ---------------------------------------------------------------------------
# Twitter method crawler (JSON search API - public, no auth)
# ---------------------------------------------------------------------------

class TwitterMethodCrawler:
    """Crawl Twitter/X for money method posts via syndication/search APIs."""

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/131.0.0.0 Safari/537.36"
    }

    def __init__(self):
        self.cb = CircuitBreaker(name="twitter_method_crawler", failure_threshold=5,
                                 recovery_timeout=120)
        self.traj = TrajectoryLogger("method_discovery_twitter")

    @retry(max_attempts=3, base_delay=3.0, on_failure_return=[])
    def _search_nitter(self, query: str) -> list[dict]:
        """Search via nitter instances for tweets matching method queries.
        Falls back gracefully if nitter instances are down."""
        # Nitter instances (try multiple, they go up/down)
        nitter_instances = [
            "nitter.privacydev.net",
            "nitter.poast.org",
            "nitter.1d4.us",
        ]

        for instance in nitter_instances:
            try:
                safe_query = urllib.parse.quote(query)
                url = f"https://{instance}/search?f=tweets&q={safe_query}"
                req = urllib.request.Request(url, headers=self.HEADERS)
                with urllib.request.urlopen(req, timeout=15) as resp:
                    html = resp.read().decode("utf-8", errors="replace")

                # Parse tweets from nitter HTML
                return self._parse_nitter_html(html, instance)
            except Exception:
                continue

        return []

    def _parse_nitter_html(self, html: str, instance: str) -> list[dict]:
        """Extract tweet data from nitter search results HTML."""
        posts = []

        # Extract tweet containers
        tweet_blocks = re.findall(
            r'<div class="timeline-item[^"]*">(.*?)</div>\s*</div>\s*</div>',
            html, re.DOTALL
        )

        if not tweet_blocks:
            # Fallback: try to find tweet text blocks
            tweet_blocks = re.findall(
                r'<div class="tweet-content[^"]*">(.*?)</div>',
                html, re.DOTALL
            )

        for block in tweet_blocks[:20]:
            # Extract text
            text_match = re.search(
                r'<div class="tweet-content[^"]*">(.*?)</div>',
                block, re.DOTALL
            )
            if not text_match:
                text_match = re.search(r'>([^<]{50,500})<', block)
            if not text_match:
                continue

            text = re.sub(r"<[^>]+>", " ", text_match.group(1)).strip()
            text = re.sub(r"\s+", " ", text)

            if not _has_method_signal(text):
                continue
            if _is_low_value(text):
                continue
            if len(text) < 40:
                continue

            # Extract handle
            handle_match = re.search(r'/@(\w+)', block)
            handle = handle_match.group(1) if handle_match else "unknown"

            # Extract stats
            stats_match = re.findall(r'<span class="tweet-stat[^"]*"[^>]*>.*?(\d[\d,]*)', block, re.DOTALL)
            engagement = 0
            if stats_match:
                try:
                    engagement = sum(int(s.replace(",", "")) for s in stats_match[:3])
                except (ValueError, TypeError):
                    pass

            # Extract link
            link_match = re.search(r'/(\w+)/status/(\d+)', block)
            tweet_url = f"https://x.com/{handle}/status/{link_match.group(2)}" if link_match else ""

            posts.append({
                "source": f"twitter/@{handle}",
                "source_url": tweet_url,
                "title": text[:120],
                "text": sanitize_for_prompt(text, "twitter_method"),
                "engagement": engagement,
                "method_name": _extract_method_name(text),
                "category": _categorize_method(text),
            })

        return posts

    @retry(max_attempts=3, base_delay=3.0, on_failure_return=[])
    def _search_syndication(self, query: str) -> list[dict]:
        """Try Twitter syndication API (public, limited but works)."""
        safe_query = urllib.parse.quote(query)
        url = f"https://syndication.twitter.com/srv/timeline-profile/screen-name/{safe_query}"
        try:
            req = urllib.request.Request(url, headers=self.HEADERS)
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = resp.read().decode("utf-8", errors="replace")
            # Syndication returns HTML, not JSON - parse accordingly
            posts = []
            tweet_texts = re.findall(r'"full_text"\s*:\s*"([^"]{40,500})"', data)
            for text in tweet_texts[:10]:
                text = text.encode().decode("unicode_escape", errors="replace")
                if _has_method_signal(text) and not _is_low_value(text):
                    posts.append({
                        "source": f"twitter/search",
                        "source_url": "",
                        "title": text[:120],
                        "text": sanitize_for_prompt(text, "twitter_method"),
                        "engagement": 0,
                        "method_name": _extract_method_name(text),
                        "category": _categorize_method(text),
                    })
            return posts
        except Exception:
            return []

    def crawl(self) -> list[dict]:
        """Run all Twitter method searches."""
        all_methods = []
        _log(f"Twitter: running {len(TWITTER_SEARCH_QUERIES)} search queries...")

        try:
            with self.cb:
                for i, query in enumerate(TWITTER_SEARCH_QUERIES, 1):
                    start = self.traj.log_attempt("twitter_search", query=query)
                    try:
                        results = self._search_nitter(query)
                        if not results:
                            results = self._search_syndication(query)

                        all_methods.extend(results)
                        self.traj.log_success("twitter_search", start, count=len(results))
                        if results:
                            _log(f"  [{i}/{len(TWITTER_SEARCH_QUERIES)}] \"{query[:40]}\": {len(results)} results")

                        time.sleep(2.0)  # Rate limiting
                    except Exception as e:
                        self.traj.log_failure("twitter_search", str(e), start)
                        continue

        except CircuitBreakerOpen:
            _log("Twitter circuit breaker OPEN, skipping")

        _log(f"Twitter: {len(all_methods)} total method posts found")
        return all_methods


# ---------------------------------------------------------------------------
# Hacker News crawler
# ---------------------------------------------------------------------------

class HackerNewsCrawler:
    """Crawl Hacker News for method-related posts via Algolia API."""

    SEARCH_URL = "https://hn.algolia.com/api/v1/search"

    HN_QUERIES = [
        "side project revenue",
        "launched making money",
        "MRR indie",
        "passive income automated",
        "built sold business",
        "SaaS revenue growth",
    ]

    def __init__(self):
        self.cb = CircuitBreaker(name="hn_method_crawler", failure_threshold=5,
                                 recovery_timeout=120)
        self.traj = TrajectoryLogger("method_discovery_hn")

    @retry(max_attempts=3, base_delay=2.0, on_failure_return=[])
    def _search_hn(self, query: str) -> list[dict]:
        params = urllib.parse.urlencode({
            "query": query,
            "tags": "story",
            "numericFilters": "points>20",
            "hitsPerPage": 15,
        })
        url = f"{self.SEARCH_URL}?{params}"
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/131.0.0.0 Safari/537.36"
        })

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            _log(f"HN search error: {e}")
            raise

        posts = []
        for hit in data.get("hits", []):
            title = hit.get("title", "")
            hn_url = f"https://news.ycombinator.com/item?id={hit.get('objectID', '')}"
            points = hit.get("points", 0)
            comments = hit.get("num_comments", 0)

            if not _has_method_signal(title):
                continue
            if len(title) < 15:
                continue

            posts.append({
                "source": "hackernews",
                "source_url": hn_url,
                "title": title,
                "text": sanitize_for_prompt(title, "hn_post"),
                "engagement": points,
                "comments": comments,
                "method_name": _extract_method_name(title),
                "category": _categorize_method(title),
            })

        return posts

    def crawl(self) -> list[dict]:
        all_methods = []
        _log(f"HN: running {len(self.HN_QUERIES)} queries...")

        try:
            with self.cb:
                for i, query in enumerate(self.HN_QUERIES, 1):
                    start = self.traj.log_attempt("hn_search", query=query)
                    try:
                        results = self._search_hn(query)
                        all_methods.extend(results)
                        self.traj.log_success("hn_search", start, count=len(results))
                        if results:
                            _log(f"  [{i}/{len(self.HN_QUERIES)}] \"{query}\": {len(results)} results")
                        time.sleep(1.0)
                    except Exception as e:
                        self.traj.log_failure("hn_search", str(e), start)
                        continue
        except CircuitBreakerOpen:
            _log("HN circuit breaker OPEN, skipping")

        _log(f"HN: {len(all_methods)} total method posts found")
        return all_methods


# ---------------------------------------------------------------------------
# Alpha ID management
# ---------------------------------------------------------------------------

def _get_next_alpha_id() -> int:
    max_id = 0
    if ALPHA_STAGING.exists():
        try:
            with open(ALPHA_STAGING, "r", encoding="utf-8", errors="replace", newline="") as f:
                for row in csv.DictReader(f):
                    m = re.match(r"ALPHA(\d+)", row.get("alpha_id", ""))
                    if m:
                        max_id = max(max_id, int(m.group(1)))
        except Exception:
            pass
    return max_id + 1


# ---------------------------------------------------------------------------
# Output: write to ALPHA_STAGING, discovery log, method stubs
# ---------------------------------------------------------------------------

def _write_alpha_staging(methods: list[dict], dry_run: bool = False) -> int:
    """Append new methods to ALPHA_STAGING.csv. Returns count written."""
    if not methods:
        return 0

    next_id = _get_next_alpha_id()
    rows = []

    for m in methods:
        alpha_id = f"ALPHA{next_id}"
        m["alpha_id"] = alpha_id
        next_id += 1

        score_data = m.get("score", {})
        composite = score_data.get("composite_score", 0)
        priority_label = score_data.get("capital_genesis_priority", "P3")

        row = {
            "alpha_id": alpha_id,
            "source": m.get("source", ""),
            "source_url": m.get("source_url", ""),
            "category": m.get("category", "GENERAL"),
            "tactic": m.get("text", "")[:500],
            "roi_potential": score_data.get("revenue_potential", "MEDIUM"),
            "priority": f"METHOD_{priority_label}",
            "status": "NEW_METHOD",
            "applicable_methods": m.get("method_name", ""),
            "applicable_niches": "",
            "synergy_score": str(score_data.get("synergy_score", "")),
            "cross_sell_products": "",
            "implementation_priority": priority_label,
            "engagement_authenticity": "UNCHECKED",
            "earnings_verified": "N/A",
            "extracted_method": m.get("method_name", ""),
            "compliance_notes": "",
            "reviewer_notes": (
                f"Score: {composite}/10 | "
                f"Rev: {score_data.get('revenue_potential', '?')} | "
                f"Speed: {score_data.get('speed_to_revenue', '?')}/10 | "
                f"Auto: {score_data.get('automation_potential', '?')}/10 | "
                f"Cost: {score_data.get('upfront_cost', '?')} | "
                f"Eng: {m.get('engagement', 0)}"
            ),
            "created_at": _now_iso(),
            "ops_generated": "FALSE",
        }
        rows.append(row)

    if dry_run:
        _log(f"DRY RUN: would write {len(rows)} entries to ALPHA_STAGING.csv")
        return len(rows)

    # Read existing header to match schema
    fieldnames = ALPHA_FIELDS
    if ALPHA_STAGING.exists():
        try:
            with open(ALPHA_STAGING, "r", encoding="utf-8", errors="replace", newline="") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if header:
                    fieldnames = header
        except Exception:
            pass

    file_exists = ALPHA_STAGING.exists() and ALPHA_STAGING.stat().st_size > 0
    with open(safe_path(ALPHA_STAGING), "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    _log(f"Wrote {len(rows)} entries to ALPHA_STAGING.csv (ALPHA{next_id - len(rows)}-ALPHA{next_id - 1})")
    return len(rows)


def _write_discovery_log(methods: list[dict], dry_run: bool = False) -> None:
    """Append to METHOD_DISCOVERY_LOG.csv."""
    if not methods or dry_run:
        return

    file_exists = DISCOVERY_LOG.exists() and DISCOVERY_LOG.stat().st_size > 0
    with open(safe_path(DISCOVERY_LOG), "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DISCOVERY_LOG_FIELDS, extrasaction="ignore")
        if not file_exists:
            writer.writeheader()

        for m in methods:
            score = m.get("score", {})
            writer.writerow({
                "date": datetime.now().strftime("%Y-%m-%d"),
                "source": m.get("source", ""),
                "method_name": m.get("method_name", ""),
                "method_summary": m.get("text", "")[:300],
                "source_url": m.get("source_url", ""),
                "composite_score": score.get("composite_score", 0),
                "capital_genesis_priority": score.get("capital_genesis_priority", "P3"),
                "revenue_potential": score.get("revenue_potential", ""),
                "speed_to_revenue": score.get("speed_to_revenue", ""),
                "automation_potential": score.get("automation_potential", ""),
                "upfront_cost": score.get("upfront_cost", ""),
                "synergy_score": score.get("synergy_score", ""),
                "status": "DISCOVERED",
                "alpha_id": m.get("alpha_id", ""),
            })


def _create_method_stub(method: dict, dry_run: bool = False) -> str | None:
    """Create a stub file for high-scoring methods."""
    score_data = method.get("score", {})
    composite = score_data.get("composite_score", 0)
    if composite <= 7:
        return None

    alpha_id = method.get("alpha_id", "UNKNOWN")
    category = method.get("category", "GENERAL")
    method_name = method.get("method_name", "Unknown")
    slug = re.sub(r"[^a-z0-9]+", "_", method_name.lower().strip())[:40]
    filename = f"{alpha_id}_{slug}.md"
    filepath = safe_path(DISCOVERED_METHODS_DIR / filename)

    content = (
        f"# Discovered Method: {method_name}\n\n"
        f"**Alpha ID:** {alpha_id}\n"
        f"**Source:** {method.get('source', '')}\n"
        f"**URL:** {method.get('source_url', '')}\n"
        f"**Category:** {category}\n"
        f"**Composite Score:** {composite}/10\n"
        f"**Capital Genesis Priority:** {score_data.get('capital_genesis_priority', 'P3')}\n"
        f"**Discovered:** {_now_iso()}\n\n"
        f"## Scoring Breakdown\n\n"
        f"| Metric | Score |\n"
        f"|--------|-------|\n"
        f"| Revenue Potential | {score_data.get('revenue_potential', '?')} ({score_data.get('revenue_potential_raw', '?')}/10) |\n"
        f"| Speed to Revenue | {score_data.get('speed_to_revenue', '?')}/10 |\n"
        f"| Downside Risk | {score_data.get('downside_risk', '?')}/10 |\n"
        f"| Automation Potential | {score_data.get('automation_potential', '?')}/10 |\n"
        f"| Upfront Cost | {score_data.get('upfront_cost', '?')} |\n"
        f"| Synergy Score | {score_data.get('synergy_score', '?')}/10 |\n"
        f"| Liability Risk | {score_data.get('liability_risk', '?')}/10 |\n\n"
        f"## Method Description\n\n"
        f"{method.get('text', '')[:800]}\n\n"
        f"## Engagement\n\n"
        f"- Source engagement: {method.get('engagement', 0)}\n\n"
        f"## Implementation Plan\n\n"
        f"1. Validate method is still active/working\n"
        f"2. Assess fit with current PRINTMAXX ventures\n"
        f"3. Build minimum viable test (smallest possible version)\n"
        f"4. Measure results over 7-14 days\n"
        f"5. Scale or kill based on data\n\n"
        f"## Synergy Opportunities\n\n"
        f"- Feeds into: TBD (analyze against venture map)\n"
        f"- Cross-sell: TBD\n\n"
        f"---\n"
        f"*Auto-generated by method_discovery_crawler.py*\n"
    )

    if dry_run:
        _log(f"DRY RUN: would create stub {filepath.name}")
        return str(filepath)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    _log(f"Created method stub: {filepath.name}")
    return str(filepath)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

class MethodDiscoveryPipeline:
    """Orchestrate the full discovery pipeline."""

    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.dedup = DeduplicationEngine()
        self.scorer = CapitalGenesisScorer()
        self.traj = TrajectoryLogger("method_discovery_pipeline")

    def crawl(self) -> list[dict]:
        """Run all crawlers and return raw methods."""
        _log("=" * 60)
        _log("METHOD DISCOVERY CRAWLER - Starting full scan")
        _log("=" * 60)

        all_methods = []

        # 1. Reddit (most reliable, no auth needed)
        reddit = RedditMethodCrawler()
        reddit_methods = reddit.crawl()
        all_methods.extend(reddit_methods)

        # 2. Hacker News (reliable API, good signal)
        hn = HackerNewsCrawler()
        hn_methods = hn.crawl()
        all_methods.extend(hn_methods)

        # 3. Twitter (least reliable, depends on nitter uptime)
        twitter = TwitterMethodCrawler()
        twitter_methods = twitter.crawl()
        all_methods.extend(twitter_methods)

        _log(f"Total raw methods found: {len(all_methods)} "
             f"(Reddit: {len(reddit_methods)}, HN: {len(hn_methods)}, "
             f"Twitter: {len(twitter_methods)})")

        return all_methods

    def deduplicate(self, methods: list[dict]) -> list[dict]:
        """Filter out duplicates."""
        unique = []
        duped = 0

        for m in methods:
            url = m.get("source_url", "")
            text = m.get("text", "")
            is_dup, reason = self.dedup.is_duplicate(url, text)
            if is_dup:
                duped += 1
                continue
            self.dedup.register(url, text)
            unique.append(m)

        _log(f"Dedup: {len(unique)} unique, {duped} duplicates removed")
        return unique

    def score_methods(self, methods: list[dict]) -> list[dict]:
        """Score each method against Capital Genesis criteria."""
        scored = []
        for m in methods:
            score = self.scorer.score(m)
            m["score"] = score
            scored.append(m)

        # Sort by composite score descending
        scored.sort(key=lambda x: x["score"]["composite_score"], reverse=True)

        # Log distribution
        p0 = sum(1 for m in scored if m["score"]["capital_genesis_priority"] == "P0")
        p1 = sum(1 for m in scored if m["score"]["capital_genesis_priority"] == "P1")
        p2 = sum(1 for m in scored if m["score"]["capital_genesis_priority"] == "P2")
        p3 = sum(1 for m in scored if m["score"]["capital_genesis_priority"] == "P3")

        _log(f"Scoring complete: P0={p0}, P1={p1}, P2={p2}, P3={p3}")
        return scored

    def output(self, methods: list[dict]) -> dict:
        """Write all outputs. Returns summary stats."""
        stats = Counter()
        stats["total_discovered"] = len(methods)

        # 1. Write to ALPHA_STAGING
        written = _write_alpha_staging(methods, self.dry_run)
        stats["alpha_written"] = written

        # 2. Write to discovery log
        _write_discovery_log(methods, self.dry_run)

        # 3. Create stubs for high-scoring methods
        stubs = 0
        for m in methods:
            result = _create_method_stub(m, self.dry_run)
            if result:
                stubs += 1
        stats["stubs_created"] = stubs

        # 4. Priority distribution
        for m in methods:
            p = m.get("score", {}).get("capital_genesis_priority", "P3")
            stats[f"priority_{p}"] += 1

        return stats

    def run_full(self) -> dict:
        """Execute the full pipeline: crawl -> dedup -> score -> output."""
        pipeline_start = self.traj.log_attempt("full_pipeline")

        try:
            # Crawl
            raw = self.crawl()
            if not raw:
                _log("No methods found from any source")
                self.traj.log_success("full_pipeline", pipeline_start, methods=0)
                return {"total_discovered": 0}

            # Dedup
            unique = self.deduplicate(raw)
            if not unique:
                _log("All methods were duplicates")
                self.traj.log_success("full_pipeline", pipeline_start, methods=0, all_dupes=True)
                return {"total_discovered": 0, "all_duplicates": True}

            # Score
            scored = self.score_methods(unique)

            # Output
            stats = self.output(scored)

            # Save last run marker
            if not self.dry_run:
                safe_path(LAST_RUN_MARKER).write_text(_now_iso())

            self.traj.log_success("full_pipeline", pipeline_start, **stats)

            # Print report
            self._print_report(scored, stats)

            return stats

        except Exception as e:
            self.traj.log_failure("full_pipeline", str(e), pipeline_start)
            _log(f"Pipeline FAILED: {e}")
            raise

    def score_pending(self) -> dict:
        """Re-score pending NEW_METHOD entries in ALPHA_STAGING."""
        if not ALPHA_STAGING.exists():
            _log("No ALPHA_STAGING.csv found")
            return {}

        fieldnames = []
        rows = []
        with open(ALPHA_STAGING, "r", encoding="utf-8", errors="replace", newline="") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])
            rows = list(reader)

        scored_count = 0
        for row in rows:
            if row.get("status") != "NEW_METHOD":
                continue

            text = row.get("tactic", "")
            method = {"text": text, "engagement": 0}

            # Try to extract engagement from reviewer_notes
            eng_match = re.search(r"Eng:\s*(\d+)", row.get("reviewer_notes", ""))
            if eng_match:
                method["engagement"] = int(eng_match.group(1))

            score = self.scorer.score(method)
            priority = score["capital_genesis_priority"]

            row["implementation_priority"] = priority
            row["synergy_score"] = str(score["synergy_score"])
            row["reviewer_notes"] = (
                f"Score: {score['composite_score']}/10 | "
                f"Rev: {score['revenue_potential']} | "
                f"Speed: {score['speed_to_revenue']}/10 | "
                f"Auto: {score['automation_potential']}/10 | "
                f"Cost: {score['upfront_cost']} | "
                f"Eng: {method['engagement']}"
            )
            scored_count += 1

        if scored_count > 0 and not self.dry_run:
            with open(safe_path(ALPHA_STAGING), "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)

        _log(f"Re-scored {scored_count} pending NEW_METHOD entries")
        return {"scored": scored_count}

    def report(self, new_only: bool = False) -> None:
        """Print summary report of discovered methods."""
        if not DISCOVERY_LOG.exists():
            _log("No discovery log found. Run --crawl first.")
            return

        entries = []
        with open(DISCOVERY_LOG, "r", encoding="utf-8", errors="replace", newline="") as f:
            entries = list(csv.DictReader(f))

        if not entries:
            print("No methods discovered yet.")
            return

        if new_only:
            today = datetime.now().strftime("%Y-%m-%d")
            entries = [e for e in entries if e.get("date") == today]
            if not entries:
                print(f"No new methods discovered today ({today}).")
                return

        # Summary
        print(f"\n{'=' * 60}")
        print(f"METHOD DISCOVERY REPORT")
        print(f"{'=' * 60}")
        print(f"Total entries: {len(entries)}")

        # Priority distribution
        priority_counts = Counter(e.get("capital_genesis_priority", "P3") for e in entries)
        print(f"\nPriority distribution:")
        for p in ["P0", "P1", "P2", "P3"]:
            count = priority_counts.get(p, 0)
            bar = "#" * min(count, 40)
            print(f"  {p}: {count:>4d}  {bar}")

        # Source distribution
        source_counts = Counter()
        for e in entries:
            src = e.get("source", "unknown")
            if "reddit" in src:
                source_counts["Reddit"] += 1
            elif "twitter" in src:
                source_counts["Twitter"] += 1
            elif "hackernews" in src:
                source_counts["HackerNews"] += 1
            else:
                source_counts["Other"] += 1

        print(f"\nSource distribution:")
        for src, count in source_counts.most_common():
            print(f"  {src}: {count}")

        # Top methods by score
        scored_entries = []
        for e in entries:
            try:
                score = float(e.get("composite_score", 0))
                scored_entries.append((score, e))
            except (ValueError, TypeError):
                pass

        scored_entries.sort(key=lambda x: x[0], reverse=True)

        print(f"\nTop 10 highest-scoring methods:")
        for i, (score, e) in enumerate(scored_entries[:10], 1):
            priority = e.get("capital_genesis_priority", "?")
            name = e.get("method_name", "?")[:50]
            source = e.get("source", "?")
            print(f"  {i:>2}. [{priority}] {score:.1f}/10 | {name} ({source})")

        # Recent activity
        dates = Counter(e.get("date", "") for e in entries)
        recent = sorted(dates.items(), reverse=True)[:7]
        print(f"\nRecent daily counts:")
        for date, count in recent:
            bar = "#" * min(count, 40)
            print(f"  {date}: {count:>4d}  {bar}")

        # Last run info
        if LAST_RUN_MARKER.exists():
            print(f"\nLast crawl: {LAST_RUN_MARKER.read_text().strip()}")

        print(f"{'=' * 60}\n")

    def _print_report(self, methods: list[dict], stats: dict) -> None:
        """Print summary after a crawl run."""
        mode = "DRY RUN" if self.dry_run else "LIVE"
        print(f"\n{'=' * 60}")
        print(f"METHOD DISCOVERY CRAWLER COMPLETE ({mode})")
        print(f"{'=' * 60}")
        print(f"Methods discovered: {stats.get('total_discovered', 0)}")
        print(f"Written to ALPHA_STAGING: {stats.get('alpha_written', 0)}")
        print(f"Method stubs created: {stats.get('stubs_created', 0)}")
        print(f"")
        print(f"Priority breakdown:")
        print(f"  P0 (immediate): {stats.get('priority_P0', 0)}")
        print(f"  P1 (this week): {stats.get('priority_P1', 0)}")
        print(f"  P2 (this month): {stats.get('priority_P2', 0)}")
        print(f"  P3 (backlog): {stats.get('priority_P3', 0)}")

        # Show top 5
        if methods:
            print(f"\nTop 5 discoveries:")
            for i, m in enumerate(methods[:5], 1):
                score = m.get("score", {})
                p = score.get("capital_genesis_priority", "?")
                c = score.get("composite_score", 0)
                name = m.get("method_name", "?")[:50]
                src = m.get("source", "?")
                print(f"  {i}. [{p}] {c:.1f}/10 | {name} ({src})")

        print(f"{'=' * 60}\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Method Discovery Crawler - hunts for NEW revenue methods "
                    "and scores them against Capital Genesis criteria."
    )
    parser.add_argument(
        "--crawl", action="store_true",
        help="Full scan: crawl all sources, dedup, score, output"
    )
    parser.add_argument(
        "--score", action="store_true",
        help="Re-score pending NEW_METHOD entries in ALPHA_STAGING"
    )
    parser.add_argument(
        "--report", action="store_true",
        help="Show summary report of all discovered methods"
    )
    parser.add_argument(
        "--new-only", action="store_true",
        help="Only show methods discovered today"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview without writing any files"
    )
    args = parser.parse_args()

    _ensure_dirs()

    if not any([args.crawl, args.score, args.report, args.new_only]):
        parser.print_help()
        return

    pipeline = MethodDiscoveryPipeline(dry_run=args.dry_run)

    if args.crawl:
        pipeline.run_full()
    if args.score:
        pipeline.score_pending()
    if args.report or args.new_only:
        pipeline.report(new_only=args.new_only)


if __name__ == "__main__":
    main()
