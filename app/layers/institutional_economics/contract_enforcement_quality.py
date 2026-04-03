"""Contract Enforcement Quality module.

Uses World Bank IC.LGL.DURS (time in days to enforce a contract through courts).
Longer enforcement time reflects weaker contract enforcement institutions, higher
transaction costs, and greater uncertainty for economic agents.

Benchmark: OECD average ~590 days. Values above 1000 days indicate severe weakness.
Stress is nonlinearly mapped so extreme delays register near maximum stress.

References:
    World Bank. (2023). Doing Business / Business Ready Indicators.
    Djankov, S. et al. (2003). Courts. QJE 118(2), 453-517.
    North, D.C. (1990). Institutions, Institutional Change and Economic Performance.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class ContractEnforcementQuality(LayerBase):
    layer_id = "lIE"
    name = "Contract Enforcement Quality"

    async def compute(self, db, **kwargs) -> dict:
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("IC.LGL.DURS", "%time to enforce contract%"),
        )

        if not rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no contract enforcement data"}

        days = float(rows[0]["value"])

        # Stress: 0-300d = low stress, 300-600 moderate, 600-1000 high, >1000 severe
        if days <= 300:
            stress = days / 300.0 * 0.25
        elif days <= 600:
            stress = 0.25 + (days - 300) / 300.0 * 0.30
        elif days <= 1000:
            stress = 0.55 + (days - 600) / 400.0 * 0.30
        else:
            stress = 0.85 + min((days - 1000) / 500.0 * 0.15, 0.15)

        stress = max(0.0, min(1.0, stress))
        score = round(stress * 100.0, 2)

        tier = (
            "adequate" if days <= 300
            else "slow" if days <= 600
            else "very_slow" if days <= 1000
            else "dysfunctional"
        )

        return {
            "score": score,
            "metrics": {
                "enforcement_days": round(days, 1),
                "oecd_benchmark_days": 590,
                "stress": round(stress, 4),
                "tier": tier,
                "n_obs": len(rows),
            },
            "reference": "WB IC.LGL.DURS; Djankov et al. 2003 QJE",
        }
