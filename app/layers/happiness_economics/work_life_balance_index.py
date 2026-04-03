"""Work-life balance index: working hours vs productivity and wellbeing.

Excess working hours are consistently linked to lower life satisfaction, poorer
health outcomes, and reduced productivity per hour (OECD, ILO). Countries with
average annual hours above 2,000 per worker show measurably lower happiness
rankings despite comparable or higher income levels. The Nordic model -- with
~1,600 hours and high output per hour -- demonstrates that fewer hours with
higher productivity is the wellbeing-optimal equilibrium.

This module uses average annual working hours per employed person (ILO / WDI
proxy: SL.TLF.CACT.ZS and GDP per worker) to assess the intensity-efficiency
balance. Higher hours with lower output per hour signals poor work-life balance.

Score: low hours + high productivity -> STABLE, high hours + low productivity
-> CRISIS (worker exploitation zone).
"""

from __future__ import annotations

from app.layers.base import LayerBase

# ILO norm: 1,920 hrs/yr (40 hrs/wk x 48 wks). OECD avg ~1,750.
_NORM_HOURS = 1_750.0
# Above 2,200 hrs/yr is considered high-stress territory by ILO
_HIGH_HOURS = 2_200.0


class WorkLifeBalanceIndex(LayerBase):
    layer_id = "lHE"
    name = "Work Life Balance Index"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        hours_code = "SL.TLF.PART.ZS"  # part-time employment as inverse proxy
        gdp_worker_code = "SL.GDP.PCAP.EM.KD"  # GDP per person employed (2017 PPP)

        # Primary: GDP per person employed as productivity proxy
        prod_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (gdp_worker_code, "%GDP per person employed%"),
        )
        # Secondary: part-time rate as inverse hours proxy
        pt_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (hours_code, "%part-time%employment%"),
        )

        prod_vals = [r["value"] for r in prod_rows if r["value"] is not None]
        pt_vals = [r["value"] for r in pt_rows if r["value"] is not None]

        if not prod_vals and not pt_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for SL.GDP.PCAP.EM.KD or SL.TLF.PART.ZS",
            }

        score_components = []

        if prod_vals:
            productivity = prod_vals[0]
            # High productivity per worker -> better work-life balance
            # Scale: <$10k -> CRISIS, $10-25k -> STRESS, $25-60k -> WATCH, >$60k -> STABLE
            if productivity < 10_000:
                prod_score = 75.0 + (10_000 - productivity) / 1_000
            elif productivity < 25_000:
                prod_score = 50.0 + (25_000 - productivity) / 600.0
            elif productivity < 60_000:
                prod_score = 25.0 + (60_000 - productivity) / 1_400.0
            else:
                prod_score = max(5.0, 25.0 - (productivity - 60_000) / 5_000.0)
            score_components.append(min(100.0, prod_score))

        if pt_vals:
            # Higher part-time share -> more flexible work arrangements -> lower stress
            pt_rate = pt_vals[0]
            pt_score = max(5.0, 60.0 - pt_rate * 1.2)
            score_components.append(min(100.0, pt_score))

        score = sum(score_components) / len(score_components)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "metrics": {
                "gdp_per_worker_ppp_usd": round(prod_vals[0], 0) if prod_vals else None,
                "part_time_employment_pct": round(pt_vals[0], 2) if pt_vals else None,
                "balance_tier": (
                    "healthy"
                    if score < 25
                    else "moderate"
                    if score < 50
                    else "strained"
                    if score < 75
                    else "exploitative"
                ),
                "n_obs_prod": len(prod_vals),
                "n_obs_pt": len(pt_vals),
            },
        }
