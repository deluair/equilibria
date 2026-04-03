"""Export market concentration: destination market diversity trend.

Distinct from concentration.py (which measures product-level Herfindahl-
Hirschman Index), this module measures geographic partner concentration
of a country's exports over time. A country exporting to many partners
is more resilient than one depending on a single market.

Approach:
- Query bilateral trade data grouped by partner from the database.
- For each year, compute the HHI of export shares across partners.
- Fit an OLS trend to the HHI time series.
- Worsening concentration (rising HHI trend) increases stress score.

HHI = sum(s_i^2) where s_i = partner share of total exports.
HHI ranges 0 (perfectly diversified) to 1 (single partner).

Score:
- Latest HHI level: 0-1 mapped to 0-70 (higher = more concentrated = stress)
- Trend component: rising HHI trend adds up to 30 additional points
"""

import numpy as np

from app.layers.base import LayerBase


class ExportMarketConcentration(LayerBase):
    layer_id = "l1"
    name = "Export Market Concentration"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Query bilateral export flows by partner and year
        rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('comtrade', 'baci', 'wits', 'gravity')
              AND ds.name LIKE '%export%bilateral%'
            ORDER BY dp.date
            """,
            (country,),
        )

        if not rows or len(rows) < 5:
            # Fallback: query trade concentration data stored as WDI proxies
            rows = await db.fetch_all(
                """
                SELECT dp.date, dp.value, ds.wdi_code
                FROM data_points dp
                JOIN data_series ds ON ds.id = dp.series_id
                WHERE ds.country_iso3 = ?
                  AND ds.wdi_code IN ('TX.VAL.MRCH.XD.WD', 'NE.EXP.GNFS.ZS')
                ORDER BY dp.date
                """,
                (country,),
            )

        if not rows or len(rows) < 3:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient export partner data"}

        import json

        # Attempt to build partner-year HHI from bilateral metadata
        year_partner_flows: dict[str, dict[str, float]] = {}
        for row in rows:
            val = row["value"]
            if val is None or val <= 0:
                continue
            date = row["date"]
            meta_raw = row.get("metadata")
            if not meta_raw:
                continue
            try:
                meta = json.loads(meta_raw)
            except (TypeError, ValueError):
                continue
            partner = meta.get("partner_iso3") or meta.get("partner")
            if not partner:
                continue
            year = date[:4] if date else None
            if not year:
                continue
            year_partner_flows.setdefault(year, {})
            year_partner_flows[year][partner] = year_partner_flows[year].get(partner, 0.0) + float(val)

        if len(year_partner_flows) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "insufficient bilateral export data for HHI computation"}

        years = sorted(year_partner_flows.keys())
        hhi_series = []
        for y in years:
            flows = year_partner_flows[y]
            total = sum(flows.values())
            if total <= 0:
                continue
            shares = np.array([v / total for v in flows.values()])
            hhi = float(np.sum(shares ** 2))
            hhi_series.append((y, hhi))

        if len(hhi_series) < 2:
            return {"score": None, "signal": "UNAVAILABLE", "error": "could not compute HHI series"}

        hhi_years = [h[0] for h in hhi_series]
        hhi_vals = np.array([h[1] for h in hhi_series])
        latest_hhi = float(hhi_vals[-1])

        # OLS trend on HHI
        t = np.arange(len(hhi_vals), dtype=float)
        X = np.column_stack([np.ones(len(t)), t])
        beta = np.linalg.lstsq(X, hhi_vals, rcond=None)[0]
        trend_slope = float(beta[1])

        # Score: level (0-70) + trend (0-30)
        level_score = latest_hhi * 70.0
        trend_score = max(0.0, min(30.0, trend_slope * 500.0)) if trend_slope > 0 else 0.0
        score = float(np.clip(level_score + trend_score, 0.0, 100.0))

        return {
            "score": round(score, 2),
            "country": country,
            "latest_hhi": round(latest_hhi, 4),
            "mean_hhi": round(float(np.mean(hhi_vals)), 4),
            "hhi_trend_slope": round(trend_slope, 6),
            "trend_direction": "worsening" if trend_slope > 0 else "improving",
            "n_years": len(hhi_series),
            "date_range": [hhi_years[0], hhi_years[-1]],
            "concentration_level": self._classify_hhi(latest_hhi),
        }

    @staticmethod
    def _classify_hhi(hhi: float) -> str:
        if hhi < 0.10:
            return "highly diversified"
        elif hhi < 0.25:
            return "moderately diversified"
        elif hhi < 0.50:
            return "concentrated"
        else:
            return "highly concentrated"
