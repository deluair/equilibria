"""Trade Agreements depth index module.

Methodology
-----------
Measures the depth and coverage of Preferential Trade Agreements (PTAs) using
the DESTA (Design of Trade Agreements) database framework. Depth is captured by
counting WTO+ and WTO-X provisions (Horn, Mavroidis & Sapir 2010):

WTO+ provisions: tariffs, services (GATS+), TBT+, SPS+, investment,
    competition, government procurement, IP, labor, environment.
WTO-X provisions: provisions going beyond the WTO mandate that require
    domestic policy changes.

Key analyses:
1. Depth index: weighted count of provisions per agreement (0-100 scale).
2. Agreement proliferation: growth in PTA membership and coverage over time.
3. Overlap measurement: share of trade covered by multiple (overlapping)
   agreements -- the "spaghetti bowl" problem (Bhagwati 1995).
4. Partner diversification across PTA network.

Score (0-100): higher score indicates fragmented, shallow PTA network with
poor overlap management (greater spaghetti bowl vulnerability).

References:
    Horn, H., Mavroidis, P.C. and Sapir, A. (2010). "Beyond the WTO? An
        anatomy of EU and US preferential trade agreements." The World
        Economy, 33(11), 1565-1588.
    Hofmann, C., Osnago, A. and Ruta, M. (2017). "Horizontal Depth: A New
        Database on the Content of Preferential Trade Agreements."
        World Bank Policy Research Working Paper 7981.
    Bhagwati, J. (1995). "US Trade Policy: The Infatuation with Free
        Trade Areas." in The Dangerous Drift to Preferential Trade
        Agreements. AEI Press.
"""

from __future__ import annotations

import json

import numpy as np
from scipy import stats

from app.layers.base import LayerBase

# WTO+ and WTO-X provision categories used in DESTA/WB depth scoring
WTO_PLUS_PROVISIONS = [
    "tariff_elimination", "services_gats_plus", "tbt_plus", "sps_plus",
    "investment_rules", "competition_policy", "government_procurement",
    "intellectual_property", "labor_standards", "environment",
]
WTO_X_PROVISIONS = [
    "capital_flows", "visa_asylum", "consumer_protection",
    "data_protection", "anti_corruption", "civil_aviation",
    "energy", "fisheries", "education",
]

# Maximum possible depth score (WTO+ + WTO-X)
MAX_DEPTH = len(WTO_PLUS_PROVISIONS) + len(WTO_X_PROVISIONS)


class TradeAgreements(LayerBase):
    layer_id = "l1"
    name = "Trade Agreements Depth"

    async def compute(self, db, **kwargs) -> dict:
        """Compute PTA depth index and network characteristics.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country : str - ISO3 country code
            year    : int - reference year (default latest)
        """
        country = kwargs.get("country", "USA")
        year = kwargs.get("year")

        # Fetch PTA data stored as data_series with source='desta'
        year_clause = "AND dp.date <= ?" if year else ""
        params_q: list = [country, country]
        if year:
            params_q.append(str(year))

        rows = await db.execute_fetchall(
            f"""
            SELECT dp.date, dp.value, ds.code, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'desta'
              AND (ds.country_iso3 = ? OR ds.description LIKE '%' || ? || '%')
              {year_clause}
            ORDER BY dp.date DESC
            """,
            tuple(params_q),
        )

        if not rows:
            return self._synthetic_result(country)

        agreements = []
        provision_counts_wto_plus = []
        provision_counts_wto_x = []
        entry_years = []
        partners = set()

        for row in rows:
            meta = json.loads(row[3]) if row[3] else {}
            n_wto_plus = int(meta.get("wto_plus_count", 0))
            n_wto_x = int(meta.get("wto_x_count", 0))
            entry_year = meta.get("entry_into_force_year")
            partner = meta.get("partner_iso3", "")

            provision_counts_wto_plus.append(n_wto_plus)
            provision_counts_wto_x.append(n_wto_x)
            if entry_year:
                entry_years.append(int(entry_year))
            if partner:
                partners.add(partner)

            agreements.append({
                "code": row[2],
                "date": row[0],
                "wto_plus": n_wto_plus,
                "wto_x": n_wto_x,
                "depth_raw": n_wto_plus + n_wto_x,
                "partner": partner,
            })

        n = len(agreements)
        if n == 0:
            return self._synthetic_result(country)

        wtp = np.array(provision_counts_wto_plus, dtype=float)
        wtx = np.array(provision_counts_wto_x, dtype=float)
        depth_raw = wtp + wtx

        # Normalized depth index (0-100)
        mean_depth = float(np.mean(depth_raw))
        depth_index = float(np.clip(mean_depth / MAX_DEPTH * 100, 0, 100))

        # Proliferation: linear trend in entry years if we have enough
        proliferation = {}
        if len(entry_years) >= 3:
            ey = np.array(sorted(entry_years))
            # Count PTAs by decade
            decades = {}
            for y_val in ey:
                decade = (y_val // 10) * 10
                decades[decade] = decades.get(decade, 0) + 1
            # Trend slope (PTAs per year over sample)
            x_t = np.arange(len(ey), dtype=float)
            slope, intercept, r_val, p_val, _ = stats.linregress(x_t, ey)
            proliferation = {
                "total_agreements": n,
                "earliest_entry": int(ey[0]),
                "latest_entry": int(ey[-1]),
                "by_decade": {str(k): v for k, v in sorted(decades.items())},
                "trend_r_squared": round(r_val ** 2, 4),
                "avg_interval_years": round(float(np.mean(np.diff(ey))), 2) if len(ey) > 1 else None,
            }
        else:
            proliferation = {"total_agreements": n, "note": "insufficient history for trend"}

        # Overlap: share of partners covered by 2+ agreements
        from collections import Counter
        partner_counts = Counter(a["partner"] for a in agreements if a["partner"])
        overlapping = sum(1 for v in partner_counts.values() if v > 1)
        overlap_ratio = overlapping / len(partner_counts) if partner_counts else 0.0

        # Partner diversification (HHI of bilateral PTA trade)
        n_partners = len(partners)
        if n_partners > 0:
            hhi_ptas = 1.0 / n_partners  # equal weight proxy
            diversification_score = float(np.clip((1 - hhi_ptas) * 100, 0, 100))
        else:
            diversification_score = 0.0

        # Top agreements by depth
        top_agreements = sorted(agreements, key=lambda x: x["depth_raw"], reverse=True)[:5]

        # Score computation:
        # Low depth index -> higher vulnerability score
        # High overlap (spaghetti bowl) -> higher score
        # Low diversification -> higher score
        depth_penalty = max(0.0, 100 - depth_index) * 0.5
        overlap_penalty = float(overlap_ratio) * 20
        diversification_penalty = max(0.0, 50 - diversification_score) * 0.3

        score = float(np.clip(depth_penalty + overlap_penalty + diversification_penalty, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "depth_index": round(depth_index, 2),
            "mean_wto_plus": round(float(np.mean(wtp)), 2),
            "mean_wto_x": round(float(np.mean(wtx)), 2),
            "n_agreements": n,
            "n_partners": n_partners,
            "overlap_ratio": round(overlap_ratio, 4),
            "diversification_score": round(diversification_score, 2),
            "proliferation": proliferation,
            "top_agreements_by_depth": top_agreements,
        }

    @staticmethod
    def _synthetic_result(country: str) -> dict:
        """Return unavailable result when no DESTA data is loaded."""
        return {
            "score": None,
            "signal": "UNAVAILABLE",
            "error": f"No DESTA PTA data for {country}",
            "country": country,
        }
