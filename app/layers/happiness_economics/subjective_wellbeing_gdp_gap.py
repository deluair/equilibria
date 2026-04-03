"""Subjective wellbeing vs GDP gap: Easterlin paradox proxy.

The Easterlin paradox observes that beyond a threshold, rising GDP per capita
does not produce proportional gains in life satisfaction. This module proxies
the divergence by comparing GDP per capita growth trajectory against available
life satisfaction or wellbeing proxy indicators (WDI: self-reported happiness,
suicide rates as inverse proxy, life expectancy as development proxy).

A wide divergence -- rapid GDP growth with stagnant or declining wellbeing
proxies -- signals the paradox is active, implying economic gains are not
translating into population welfare.

Score: low divergence -> STABLE, moderate -> WATCH, high -> STRESS,
extreme reversal (GDP up, wellbeing down) -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class SubjectiveWellbeingGDPGap(LayerBase):
    layer_id = "lHE"
    name = "Subjective Wellbeing GDP Gap"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        gdp_code = "NY.GDP.PCAP.KD.ZG"
        life_exp_code = "SP.DYN.LE00.IN"

        gdp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (gdp_code, "%GDP per capita growth%"),
        )
        le_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (life_exp_code, "%life expectancy at birth%"),
        )

        gdp_vals = [r["value"] for r in gdp_rows if r["value"] is not None]
        le_vals = [r["value"] for r in le_rows if r["value"] is not None]

        if not gdp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for NY.GDP.PCAP.KD.ZG",
            }

        # Mean GDP per capita growth rate over available window
        avg_gdp_growth = sum(gdp_vals) / len(gdp_vals)

        # Life expectancy trend as wellbeing proxy: annual change
        le_trend = None
        if len(le_vals) >= 2:
            le_trend = (le_vals[0] - le_vals[-1]) / (len(le_vals) - 1)

        # Easterlin gap: strong positive GDP growth with flat/negative LE trend
        # signals divergence. Absence of LE data defaults to GDP-only stress read.
        if le_trend is not None:
            # Normalise: GDP growth in pct, LE change in years/yr
            # A healthy society: GDP +2% pa -> LE +0.25 yr/pa (historical norm)
            expected_le_improvement = avg_gdp_growth * 0.12  # rough coefficient
            gap = expected_le_improvement - le_trend  # positive = worse than expected
            if gap < 0:
                score = 10.0
            elif gap < 0.1:
                score = 15.0 + gap * 150.0
            elif gap < 0.3:
                score = 30.0 + (gap - 0.1) * 125.0
            elif gap < 0.6:
                score = 55.0 + (gap - 0.3) * 100.0
            else:
                score = min(100.0, 85.0 + (gap - 0.6) * 50.0)
        else:
            # No LE data: score based on GDP growth direction alone
            # Very high growth with no wellbeing anchoring = moderate concern
            score = 30.0 + min(20.0, abs(avg_gdp_growth) * 1.5)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "avg_gdp_per_capita_growth_pct": round(avg_gdp_growth, 3),
                "life_expectancy_trend_yr_per_yr": round(le_trend, 4) if le_trend is not None else None,
                "easterlin_gap_proxy": round(
                    (avg_gdp_growth * 0.12 - le_trend) if le_trend is not None else 0.0, 4
                ),
                "n_obs_gdp": len(gdp_vals),
                "n_obs_le": len(le_vals),
            },
        }
