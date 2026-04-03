"""Income-happiness threshold: diminishing returns at high income levels.

Kahneman and Deaton (2010) identified an approximate income satiation threshold
for emotional wellbeing around $75,000 (2010 USD). Killingsworth (2021) found
continued gains at higher incomes for evaluative wellbeing but with a flattening
curve. This module estimates where a country's GDP per capita sits relative to
an international purchasing-power-parity threshold and scores the marginal
wellbeing return to income growth.

Score: income well below threshold (high marginal return still available) ->
WATCH/STRESS (population welfare gap), income near threshold -> STABLE
(efficient zone), income well above with stagnant wellbeing proxies -> WATCH
(diminishing returns, policy should shift focus).
"""

from __future__ import annotations

import math

from app.layers.base import LayerBase

# International threshold: ~$25,000 PPP (2017 constant), rough midpoint of literature
_THRESHOLD_PPP = 25_000.0


class IncomeHappinessThreshold(LayerBase):
    layer_id = "lHE"
    name = "Income Happiness Threshold"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        gni_code = "NY.GNP.PCAP.PP.KD"
        gdp_code = "NY.GDP.PCAP.PP.KD"

        # Prefer GNI PPP; fall back to GDP PPP
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (gni_code, "%GNI per capita%PPP%"),
        )
        vals = [r["value"] for r in rows if r["value"] is not None]

        if not vals:
            rows = await db.fetch_all(
                "SELECT value FROM data_points WHERE series_id = "
                "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
                "ORDER BY date DESC LIMIT 10",
                (gdp_code, "%GDP per capita%PPP%"),
            )
            vals = [r["value"] for r in rows if r["value"] is not None]

        if not vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no PPP income data (NY.GNP.PCAP.PP.KD or NY.GDP.PCAP.PP.KD)",
            }

        latest = vals[0]
        ratio = latest / _THRESHOLD_PPP

        # Marginal wellbeing return diminishes logarithmically above threshold.
        # Below threshold: welfare gap -> higher stress score.
        # Near threshold (0.8-1.5x): optimal zone -> low stress.
        # Above threshold (>1.5x): diminishing returns zone -> moderate watch.
        if ratio < 0.2:
            # Very low income: major welfare gap
            score = 70.0 + (0.2 - ratio) * 100.0
        elif ratio < 0.5:
            score = 50.0 + (0.5 - ratio) * 66.7
        elif ratio < 0.8:
            score = 30.0 + (0.8 - ratio) * 66.7
        elif ratio <= 1.5:
            # Near/at threshold: lowest stress (welfare system working)
            score = 10.0 + abs(1.0 - ratio) * 13.3
        else:
            # Above threshold: diminishing returns score based on log excess
            excess = math.log(ratio / 1.5) if ratio > 1.5 else 0.0
            score = 18.0 + excess * 15.0

        score = min(100.0, max(0.0, score))

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "income_ppp_usd": round(latest, 0),
                "threshold_ppp_usd": _THRESHOLD_PPP,
                "threshold_ratio": round(ratio, 3),
                "zone": (
                    "welfare_gap"
                    if ratio < 0.8
                    else "optimal"
                    if ratio <= 1.5
                    else "diminishing_returns"
                ),
                "n_obs": len(vals),
            },
        }
