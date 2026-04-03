"""AI productivity dividend: TFP growth trend as proxy for AI-driven productivity.

Total Factor Productivity (TFP) captures output growth not explained by capital
or labor accumulation -- it is the residual attributed to technology, knowledge,
and efficiency gains. Accelerating TFP growth in digitally advanced economies
is a leading indicator that AI and automation are generating a productivity dividend.

Solow (1987) productivity paradox: computers visible everywhere except productivity
statistics. Modern evidence (Brynjolfsson et al. 2018) suggests AI productivity
gains have a J-curve delay before appearing in TFP statistics.

Score: rising TFP trend -> STABLE (dividend materializing), stagnant/declining
TFP -> CRISIS (paradox persists, dividend not realized).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AIProductivityDividend(LayerBase):
    layer_id = "lAI"
    name = "AI Productivity Dividend"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        tfp_code = "NY.GDP.PCAP.KD.ZG"
        rnd_code = "GB.XPD.RSDV.GD.ZS"

        tfp_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (tfp_code, "%GDP per capita growth%"),
        )
        rnd_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rnd_code, "%research and development%"),
        )

        tfp_vals = [r["value"] for r in tfp_rows if r["value"] is not None]
        rnd_vals = [r["value"] for r in rnd_rows if r["value"] is not None]

        if not tfp_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for GDP per capita growth NY.GDP.PCAP.KD.ZG",
            }

        latest_growth = tfp_vals[0]
        # Trend: average recent vs older window
        recent_avg = sum(tfp_vals[:3]) / len(tfp_vals[:3]) if len(tfp_vals) >= 3 else latest_growth
        older_avg = sum(tfp_vals[3:8]) / len(tfp_vals[3:8]) if len(tfp_vals) >= 8 else None
        trend_direction = (
            "accelerating" if older_avg is not None and recent_avg > older_avg + 0.5
            else "decelerating" if older_avg is not None and recent_avg < older_avg - 0.5
            else "stable"
        )

        rnd_gdp = rnd_vals[0] if rnd_vals else None

        # Score inversely related to growth (higher TFP growth = lower stress score)
        # Negative growth -> high stress; strong positive -> low stress
        if latest_growth >= 4.0:
            base = 10.0
        elif latest_growth >= 2.0:
            base = 10.0 + (4.0 - latest_growth) * 7.5
        elif latest_growth >= 0.0:
            base = 25.0 + (2.0 - latest_growth) * 12.5
        elif latest_growth >= -2.0:
            base = 50.0 + abs(latest_growth) * 10.0
        else:
            base = min(95.0, 70.0 + abs(latest_growth + 2.0) * 5.0)

        # R&D investment signals future dividend -- modestly reduces stress
        if rnd_gdp is not None:
            if rnd_gdp >= 3.0:
                base = max(5.0, base - 10.0)
            elif rnd_gdp >= 1.5:
                base = max(5.0, base - 5.0)

        # Accelerating trend reduces stress; decelerating increases it
        if trend_direction == "accelerating":
            base = max(5.0, base - 8.0)
        elif trend_direction == "decelerating":
            base = min(100.0, base + 5.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "gdp_per_capita_growth_pct": round(latest_growth, 2),
                "recent_3yr_avg_growth": round(recent_avg, 2),
                "rnd_gdp_pct": round(rnd_gdp, 2) if rnd_gdp is not None else None,
                "trend_direction": trend_direction,
                "n_obs_growth": len(tfp_vals),
                "n_obs_rnd": len(rnd_vals),
                "dividend_materializing": latest_growth >= 2.0 and trend_direction == "accelerating",
            },
        }
