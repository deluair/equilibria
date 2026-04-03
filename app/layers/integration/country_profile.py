"""Country Profile Generator.

Generates comprehensive country risk profiles across all 6 layers.
Identifies strengths, vulnerabilities, and provides peer comparison.
Used by the country deep dive briefing.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np
from scipy import stats as sp_stats

from app.config import LAYER_WEIGHTS
from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

LAYER_IDS = ["l1", "l2", "l3", "l4", "l5"]
LAYER_NAMES = {
    "l1": "Trade",
    "l2": "Macro",
    "l3": "Labor",
    "l4": "Development",
    "l5": "Agricultural",
}

# Default peer groups by income level
PEER_GROUPS = {
    "high_income": ["USA", "DEU", "JPN", "GBR", "FRA", "CAN", "AUS", "KOR"],
    "upper_middle": ["CHN", "BRA", "MEX", "TUR", "THA", "MYS", "COL", "ZAF"],
    "lower_middle": ["IND", "BGD", "VNM", "PHL", "EGY", "NGA", "PAK", "IDN"],
    "low_income": ["ETH", "MOZ", "TZA", "UGA", "MLI", "BFA", "NER", "TCD"],
}

# Thresholds for strength/vulnerability classification
STRENGTH_THRESHOLD = 25.0  # Below this = strength
VULNERABILITY_THRESHOLD = 60.0  # Above this = vulnerability


class CountryProfile(LayerBase):
    layer_id = "l6"
    name = "Country Profile"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        peer_group = kwargs.get("peer_group")
        include_history = kwargs.get("include_history", True)

        # Fetch country info
        country_info = await self._fetch_country_info(db, country_iso3)

        # Fetch current layer scores
        layer_scores = await self._fetch_layer_scores(db, country_iso3)

        if not layer_scores:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "country_iso3": country_iso3,
                "reason": "No layer scores available",
            }

        # Composite score
        composite = self._compute_composite(layer_scores)

        # Identify strengths and vulnerabilities
        strengths = self._identify_strengths(layer_scores)
        vulnerabilities = self._identify_vulnerabilities(layer_scores)

        # Determine peer group
        if peer_group is None:
            income_group = (country_info or {}).get("income_group", "")
            peer_group = self._resolve_peer_group(income_group)
        peers = PEER_GROUPS.get(peer_group, [])
        peers = [p for p in peers if p != country_iso3]

        # Peer comparison
        peer_comparison = await self._peer_comparison(
            db, country_iso3, layer_scores, peers
        )

        # Historical trend (if requested)
        trend = None
        if include_history:
            trend = await self._compute_trend(db, country_iso3)

        # Layer detail cards
        layer_details = self._build_layer_details(layer_scores, peer_comparison)

        # Overall risk assessment
        risk_assessment = self._risk_assessment(
            composite, strengths, vulnerabilities, trend
        )

        await self._store_result(
            db, country_iso3, composite, risk_assessment
        )

        return {
            "score": round(composite, 2),
            "signal": self.classify_signal(composite),
            "country": country_info or {"iso3": country_iso3},
            "composite_score": round(composite, 2),
            "layer_scores": {k: round(v, 2) for k, v in layer_scores.items()},
            "strengths": strengths,
            "vulnerabilities": vulnerabilities,
            "risk_assessment": risk_assessment,
            "peer_comparison": peer_comparison,
            "layer_details": layer_details,
            "trend": trend,
            "peer_group": peer_group,
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def _fetch_country_info(self, db, country_iso3: str) -> dict | None:
        return await db.fetch_one(
            "SELECT * FROM countries WHERE iso3 = ?", (country_iso3,)
        )

    async def _fetch_layer_scores(
        self, db, country_iso3: str
    ) -> dict[str, float]:
        scores = {}
        for lid in LAYER_IDS:
            row = await db.fetch_one(
                """
                SELECT score FROM analysis_results
                WHERE layer = ? AND country_iso3 = ? AND score IS NOT NULL
                ORDER BY created_at DESC LIMIT 1
                """,
                (lid, country_iso3),
            )
            if row and row["score"] is not None:
                scores[lid] = float(row["score"])
        return scores

    def _compute_composite(self, scores: dict[str, float]) -> float:
        total_w = sum(LAYER_WEIGHTS.get(lid, 0.20) for lid in scores)
        if total_w == 0:
            return 50.0
        return sum(
            scores[lid] * LAYER_WEIGHTS.get(lid, 0.20) / total_w
            for lid in scores
        )

    def _identify_strengths(
        self, scores: dict[str, float]
    ) -> list[dict]:
        strengths = []
        for lid, score in scores.items():
            if score < STRENGTH_THRESHOLD:
                strengths.append({
                    "layer": lid,
                    "name": LAYER_NAMES.get(lid, lid),
                    "score": round(score, 2),
                    "assessment": self._strength_description(lid, score),
                })
        return sorted(strengths, key=lambda x: x["score"])

    def _identify_vulnerabilities(
        self, scores: dict[str, float]
    ) -> list[dict]:
        vulns = []
        for lid, score in scores.items():
            if score >= VULNERABILITY_THRESHOLD:
                vulns.append({
                    "layer": lid,
                    "name": LAYER_NAMES.get(lid, lid),
                    "score": round(score, 2),
                    "severity": (
                        "critical" if score >= 80 else
                        "high" if score >= 70 else "elevated"
                    ),
                    "assessment": self._vulnerability_description(lid, score),
                })
        return sorted(vulns, key=lambda x: x["score"], reverse=True)

    def _strength_description(self, lid: str, score: float) -> str:
        descs = {
            "l1": "Strong trade position with diversified markets",
            "l2": "Stable macroeconomic fundamentals",
            "l3": "Healthy labor market conditions",
            "l4": "Solid development trajectory",
            "l5": "Resilient agricultural sector and food security",
        }
        base = descs.get(lid, "Strong performance")
        if score < 10:
            return f"{base}. Exceptionally robust."
        return base

    def _vulnerability_description(self, lid: str, score: float) -> str:
        descs = {
            "l1": "Trade sector under significant stress",
            "l2": "Macroeconomic imbalances detected",
            "l3": "Labor market deterioration underway",
            "l4": "Development indicators weakening",
            "l5": "Agricultural sector and food security concerns",
        }
        base = descs.get(lid, "Vulnerability detected")
        if score >= 80:
            return f"{base}. Requires immediate attention."
        return base

    async def _peer_comparison(
        self, db, country_iso3: str,
        target_scores: dict[str, float],
        peers: list[str],
    ) -> dict:
        """Compare country scores against peer group."""
        if not peers:
            return {"peers": [], "percentiles": {}, "relative_position": "no_peers"}

        peer_data = {}
        for peer in peers:
            peer_scores = {}
            for lid in LAYER_IDS:
                row = await db.fetch_one(
                    """
                    SELECT score FROM analysis_results
                    WHERE layer = ? AND country_iso3 = ? AND score IS NOT NULL
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (lid, peer),
                )
                if row and row["score"] is not None:
                    peer_scores[lid] = float(row["score"])
            if peer_scores:
                peer_data[peer] = peer_scores

        if not peer_data:
            return {"peers": peers, "percentiles": {}, "relative_position": "no_data"}

        # Compute percentile rank for each layer
        percentiles = {}
        for lid in target_scores:
            target = target_scores[lid]
            peer_values = [
                peer_data[p][lid] for p in peer_data if lid in peer_data[p]
            ]
            if not peer_values:
                continue

            peer_values + [target]
            # Lower score is better (less stress), so percentile = fraction below
            rank = sum(1 for v in peer_values if v > target)
            pctile = rank / len(peer_values) * 100.0
            percentiles[lid] = {
                "percentile": round(pctile, 1),
                "peer_mean": round(float(np.mean(peer_values)), 2),
                "peer_median": round(float(np.median(peer_values)), 2),
                "country_score": round(target, 2),
                "better_than_mean": target < float(np.mean(peer_values)),
            }

        # Overall relative position
        avg_pctile = float(np.mean([p["percentile"] for p in percentiles.values()])) if percentiles else 50.0

        return {
            "peers_with_data": list(peer_data.keys()),
            "percentiles": percentiles,
            "avg_percentile": round(avg_pctile, 1),
            "relative_position": (
                "top_quartile" if avg_pctile >= 75 else
                "above_median" if avg_pctile >= 50 else
                "below_median" if avg_pctile >= 25 else
                "bottom_quartile"
            ),
        }

    async def _compute_trend(
        self, db, country_iso3: str
    ) -> dict | None:
        """Compute recent trend in composite score."""
        rows = await db.fetch_all(
            """
            SELECT score, created_at FROM analysis_results
            WHERE analysis_type = 'composite_score' AND country_iso3 = ?
              AND score IS NOT NULL
            ORDER BY created_at DESC LIMIT 24
            """,
            (country_iso3,),
        )

        if len(rows) < 3:
            return None

        scores = np.array([r["score"] for r in reversed(rows)])
        n = len(scores)
        x = np.arange(n, dtype=float)

        # Linear trend
        slope, intercept, r_value, p_value, std_err = sp_stats.linregress(x, scores)

        # Recent momentum (last 4 vs previous 4)
        if n >= 8:
            recent = float(np.mean(scores[-4:]))
            previous = float(np.mean(scores[-8:-4]))
            momentum = recent - previous
        else:
            momentum = float(scores[-1] - scores[0])

        return {
            "slope": round(float(slope), 4),
            "r_squared": round(float(r_value ** 2), 4),
            "p_value": round(float(p_value), 4),
            "direction": "deteriorating" if slope > 0.5 else "improving" if slope < -0.5 else "stable",
            "momentum": round(momentum, 2),
            "periods": n,
            "current": round(float(scores[-1]), 2),
            "period_start": round(float(scores[0]), 2),
        }

    def _build_layer_details(
        self, scores: dict[str, float], peer_comparison: dict
    ) -> list[dict]:
        """Build detail cards for each layer."""
        details = []
        percentiles = peer_comparison.get("percentiles", {})

        for lid in LAYER_IDS:
            if lid not in scores:
                continue

            score = scores[lid]
            pctile = percentiles.get(lid, {})

            detail = {
                "layer": lid,
                "name": LAYER_NAMES.get(lid, lid),
                "score": round(score, 2),
                "signal": self.classify_signal(score),
                "peer_percentile": pctile.get("percentile"),
                "peer_mean": pctile.get("peer_mean"),
                "relative": (
                    "better" if pctile.get("better_than_mean") else "worse"
                ) if pctile else "unknown",
            }
            details.append(detail)

        return details

    def _risk_assessment(
        self, composite: float,
        strengths: list[dict],
        vulnerabilities: list[dict],
        trend: dict | None,
    ) -> dict:
        """Overall risk assessment narrative components."""
        n_strengths = len(strengths)
        n_vulns = len(vulnerabilities)
        trend_dir = (trend or {}).get("direction", "stable")

        # Risk level
        if composite >= 75:
            risk_level = "critical"
        elif composite >= 50:
            risk_level = "elevated"
        elif composite >= 25:
            risk_level = "moderate"
        else:
            risk_level = "low"

        # Outlook based on trend + current level
        if trend_dir == "improving" and composite < 50:
            outlook = "positive"
        elif trend_dir == "deteriorating" and composite >= 50:
            outlook = "negative"
        elif trend_dir == "deteriorating":
            outlook = "cautious"
        else:
            outlook = "neutral"

        # Key concern
        if vulnerabilities:
            top_vuln = vulnerabilities[0]
            key_concern = f"{top_vuln['name']} ({top_vuln['severity']} stress, score {top_vuln['score']})"
        else:
            key_concern = None

        return {
            "risk_level": risk_level,
            "outlook": outlook,
            "n_strengths": n_strengths,
            "n_vulnerabilities": n_vulns,
            "key_concern": key_concern,
            "balanced": n_strengths > 0 and n_vulns > 0,
        }

    def _resolve_peer_group(self, income_group: str) -> str:
        """Map income group string to peer group key."""
        ig = income_group.lower().replace(" ", "_") if income_group else ""
        if "high" in ig:
            return "high_income"
        elif "upper" in ig:
            return "upper_middle"
        elif "lower" in ig or "middle" in ig:
            return "lower_middle"
        elif "low" in ig:
            return "low_income"
        return "upper_middle"  # default

    async def _store_result(
        self, db, country_iso3: str, composite: float,
        risk_assessment: dict,
    ):
        await db.execute(
            """
            INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "country_profile",
                country_iso3,
                "l6",
                json.dumps({"type": "comprehensive_profile"}),
                json.dumps(risk_assessment),
                round(composite, 2),
                self.classify_signal(composite),
            ),
        )
