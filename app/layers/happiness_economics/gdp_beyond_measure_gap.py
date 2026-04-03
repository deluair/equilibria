"""GDP beyond measure gap: HDI vs GDP rank divergence as beyond-GDP proxy.

Pure GDP rankings fail to capture human development outcomes: education,
health, and living standards. The Human Development Index (HDI) integrates
income, health (life expectancy), and education dimensions. A country ranked
much higher on HDI than GDP signals strong non-income development -- good
conversion of resources into human welfare. A country ranked much lower on
HDI than GDP signals poor conversion: wealth without human development.

This module proxies the beyond-GDP gap using the WDI HDI component indicators:
life expectancy (SP.DYN.LE00.IN), expected years of schooling (SE.SCH.LIFE),
and GNI per capita (NY.GNP.PCAP.PP.KD), computing a simple HDI approximation
and comparing it to the income dimension alone.

Score: strong HDI vs income conversion -> STABLE, large negative gap -> STRESS.
"""

from __future__ import annotations

import math

from app.layers.base import LayerBase


class GDPBeyondMeasureGap(LayerBase):
    layer_id = "lHE"
    name = "GDP Beyond Measure Gap"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        le_code = "SP.DYN.LE00.IN"
        school_code = "SE.SCH.LIFE"
        gni_code = "NY.GNP.PCAP.PP.KD"

        le_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (le_code, "%life expectancy at birth%"),
        )
        school_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (school_code, "%school life expectancy%"),
        )
        gni_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (gni_code, "%GNI per capita%PPP%"),
        )

        le_vals = [r["value"] for r in le_rows if r["value"] is not None]
        school_vals = [r["value"] for r in school_rows if r["value"] is not None]
        gni_vals = [r["value"] for r in gni_rows if r["value"] is not None]

        if not any([le_vals, school_vals, gni_vals]):
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SP.DYN.LE00.IN, SE.SCH.LIFE, or NY.GNP.PCAP.PP.KD",
            }

        # Compute approximate HDI dimensions (UNDP formula)
        # Health index: (LE - 20) / (85 - 20)
        health_idx = ((le_vals[0] - 20.0) / 65.0) if le_vals else None
        # Education index (simplified single dimension): years / 18 (max assumed)
        edu_idx = min(1.0, school_vals[0] / 18.0) if school_vals else None
        # Income index: (ln(GNI) - ln(100)) / (ln(75000) - ln(100))
        if gni_vals and gni_vals[0] > 0:
            income_idx = (math.log(gni_vals[0]) - math.log(100.0)) / (
                math.log(75_000.0) - math.log(100.0)
            )
            income_idx = max(0.0, min(1.0, income_idx))
        else:
            income_idx = None

        # Composite HDI proxy (geometric mean of available dimensions)
        available = [x for x in [health_idx, edu_idx, income_idx] if x is not None]
        if not available:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "insufficient data to compute HDI proxy",
            }

        hdi_proxy = (
            math.prod([max(0.001, x) for x in available]) ** (1 / len(available))
        )

        # Gap: income_idx vs full HDI proxy. Positive gap = income > HDI (poor conversion).
        if income_idx is not None and len(available) > 1:
            gap = income_idx - hdi_proxy
        else:
            gap = 0.0

        # Score: large positive gap (wealth without development) -> STRESS/CRISIS
        # Negative gap (HDI > income: good conversion) -> STABLE
        if gap < -0.05:
            score = 8.0  # HDI exceeds income: excellent conversion
        elif gap < 0:
            score = 8.0 + abs(gap) * 40.0
        elif gap < 0.1:
            score = 10.0 + gap * 400.0
        elif gap < 0.2:
            score = 50.0 + (gap - 0.1) * 250.0
        else:
            score = min(100.0, 75.0 + (gap - 0.2) * 125.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "hdi_proxy": round(hdi_proxy, 4),
                "income_index": round(income_idx, 4) if income_idx is not None else None,
                "health_index": round(health_idx, 4) if health_idx is not None else None,
                "education_index": round(edu_idx, 4) if edu_idx is not None else None,
                "income_vs_hdi_gap": round(gap, 4),
                "conversion_tier": (
                    "excellent"
                    if gap < -0.05
                    else "good"
                    if gap < 0.05
                    else "moderate"
                    if gap < 0.15
                    else "poor"
                ),
                "n_obs_le": len(le_vals),
                "n_obs_school": len(school_vals),
                "n_obs_gni": len(gni_vals),
            },
        }
