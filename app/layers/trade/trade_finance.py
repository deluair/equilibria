"""Trade finance gap estimation and impact analysis.

Methodology:
    Estimate trade finance availability and its impact on trade flows,
    particularly for SMEs in developing countries:

    1. Trade finance gap estimation (ADB methodology):
       Rejected trade finance applications / total applications.
       Estimated unmet demand from survey extrapolation.
       Following ADB Trade Finance Gaps report methodology.

    2. Letter of credit (LC) usage:
       Share of trade transacted via LCs vs open account.
       LC confirmation rates by country risk category.
       Spread analysis: LC fees relative to sovereign risk premium.

    3. Trade credit availability:
       Bank-intermediated trade finance (supply-chain finance, factoring,
       forfaiting). Days payable/receivable in cross-border trade.
       Following ICC Trade Register data patterns.

    4. SME exporter impact:
       Differential rejection rates for SMEs vs large firms.
       Trade finance constraint as binding barrier to export participation.
       Following Chauffour & Malouche (2011).

    Score (0-100): Higher score indicates greater trade finance stress
    (large gap, high rejection rates, limited SME access).

References:
    ADB (2023). "2023 Trade Finance Gaps, Growth, and Jobs Survey."
        Asian Development Bank.
    Chauffour, J.P. & Malouche, M. (2011). "Trade Finance During the
        Great Trade Collapse." World Bank.
    Auboin, M. & DiCaprio, A. (2017). "Why Do Trade Finance Gaps Persist?"
        WTO Staff Working Paper ERSD-2017-01.
"""

from __future__ import annotations

import numpy as np
from scipy import stats

from app.layers.base import LayerBase


class TradeFinance(LayerBase):
    layer_id = "l1"
    name = "Trade Finance"

    async def compute(self, db, **kwargs) -> dict:
        """Estimate trade finance gap and accessibility.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default BGD)
            year : int - reference year
        """
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year", 2022)

        # Fetch trade finance survey data
        survey_rows = await db.fetch_all(
            """
            SELECT ds.name, ds.metadata AS indicator, dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'trade_finance'
              AND ds.country_iso3 = ?
            ORDER BY dp.date DESC
            """,
            (country,),
        )

        # Organize by indicator
        indicators = {}
        for r in survey_rows:
            key = r["indicator"] or r["name"]
            if key not in indicators and r["value"] is not None:
                indicators[key] = float(r["value"])

        # Extract key metrics (with fallback defaults from ADB estimates)
        rejection_rate = indicators.get("rejection_rate")
        gap_usd = indicators.get("trade_finance_gap_usd")
        sme_rejection_rate = indicators.get("sme_rejection_rate")
        large_firm_rejection_rate = indicators.get("large_firm_rejection_rate")
        lc_usage_share = indicators.get("lc_usage_share")
        lc_confirmation_rate = indicators.get("lc_confirmation_rate")
        avg_lc_fee_bps = indicators.get("avg_lc_fee_bps")
        days_receivable = indicators.get("days_receivable_cross_border")

        # Fetch country trade volume for gap/trade ratio
        trade_vol_rows = await db.fetch_all(
            """
            SELECT SUM(dp.value) AS total_trade
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('baci', 'comtrade', 'wdi')
              AND ds.country_iso3 = ?
              AND (ds.name LIKE '%export%' OR ds.name LIKE '%import%')
              AND dp.date = ?
            """,
            (country, str(year)),
        )

        total_trade = None
        if trade_vol_rows and trade_vol_rows[0]["total_trade"]:
            total_trade = float(trade_vol_rows[0]["total_trade"])

        gap_trade_ratio = None
        if gap_usd is not None and total_trade and total_trade > 0:
            gap_trade_ratio = gap_usd / total_trade

        # Fetch banking sector indicators for credit availability
        banking_rows = await db.fetch_all(
            """
            SELECT ds.name, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source IN ('wdi', 'fred')
              AND ds.country_iso3 = ?
              AND (ds.name LIKE '%domestic credit%'
                   OR ds.name LIKE '%bank assets%'
                   OR ds.name LIKE '%interest rate spread%'
                   OR ds.name LIKE '%credit to private%')
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        banking = {}
        for r in banking_rows:
            if r["value"] is not None:
                banking[r["name"]] = float(r["value"])

        credit_to_private_pct = banking.get("credit_to_private_sector_pct_gdp")
        interest_spread = banking.get("interest_rate_spread")

        # SME differential analysis
        sme_gap_ratio = None
        if sme_rejection_rate is not None and large_firm_rejection_rate is not None:
            if large_firm_rejection_rate > 0:
                sme_gap_ratio = sme_rejection_rate / large_firm_rejection_rate

        # Time series trend in gap (if multiple years available)
        gap_trend = None
        gap_series = await db.fetch_all(
            """
            SELECT dp.date, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'trade_finance'
              AND ds.country_iso3 = ?
              AND ds.metadata = 'trade_finance_gap_usd'
            ORDER BY dp.date ASC
            """,
            (country,),
        )

        if len(gap_series) >= 3:
            years_arr = np.array([float(r["date"][:4]) for r in gap_series])
            gaps_arr = np.array([float(r["value"]) for r in gap_series])
            slope, intercept, r_value, p_value, std_err = stats.linregress(years_arr, gaps_arr)
            gap_trend = {
                "slope_usd_per_year": round(float(slope), 2),
                "r_squared": round(float(r_value ** 2), 4),
                "p_value": round(float(p_value), 4),
                "direction": "widening" if slope > 0 else "narrowing",
                "n_observations": len(gap_series),
            }

        # Peer comparison: fetch gap data for comparable countries
        peer_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.value
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'trade_finance'
              AND ds.metadata = 'rejection_rate'
            ORDER BY dp.date DESC
            """,
        )

        peer_rejection = {}
        for r in peer_rows:
            c = r["country_iso3"]
            if c not in peer_rejection and r["value"] is not None:
                peer_rejection[c] = float(r["value"])

        percentile_rank = None
        if country in peer_rejection and len(peer_rejection) > 1:
            vals = np.array(list(peer_rejection.values()))
            country_val = peer_rejection[country]
            percentile_rank = float(np.sum(vals < country_val) / len(vals)) * 100

        # Score computation
        # High rejection rate (max 30 points)
        rej = rejection_rate if rejection_rate is not None else 20.0
        rejection_penalty = min(rej / 50.0, 1.0) * 30.0  # 50% rejection -> max

        # Large gap relative to trade (max 25 points)
        gtr = gap_trade_ratio if gap_trade_ratio is not None else 0.1
        gap_penalty = min(gtr / 0.3, 1.0) * 25.0  # 30% gap/trade -> max

        # SME differential (max 20 points)
        sme_pen = 10.0  # default
        if sme_gap_ratio is not None:
            sme_pen = min((sme_gap_ratio - 1.0) / 3.0, 1.0) * 20.0

        # Financial infrastructure weakness (max 25 points)
        credit_pct = credit_to_private_pct if credit_to_private_pct is not None else 40.0
        infra_penalty = (1.0 - min(credit_pct / 150.0, 1.0)) * 15.0
        spread_pen = min((interest_spread or 5.0) / 15.0, 1.0) * 10.0

        score = float(np.clip(
            rejection_penalty + gap_penalty + sme_pen + infra_penalty + spread_pen,
            0, 100,
        ))

        result = {
            "score": round(score, 2),
            "country": country,
            "year": year,
            "rejection_rate_pct": round(rej, 2),
            "trade_finance_gap_usd": gap_usd,
            "gap_trade_ratio": round(gap_trade_ratio, 4) if gap_trade_ratio else None,
            "total_trade_volume": total_trade,
            "sme_rejection_rate": sme_rejection_rate,
            "large_firm_rejection_rate": large_firm_rejection_rate,
            "sme_gap_ratio": round(sme_gap_ratio, 2) if sme_gap_ratio else None,
            "lc_usage_share": lc_usage_share,
            "lc_confirmation_rate": lc_confirmation_rate,
            "avg_lc_fee_bps": avg_lc_fee_bps,
            "days_receivable_cross_border": days_receivable,
            "credit_to_private_pct_gdp": credit_to_private_pct,
            "interest_rate_spread": interest_spread,
            "percentile_rank": round(percentile_rank, 2) if percentile_rank else None,
        }

        if gap_trend:
            result["gap_trend"] = gap_trend

        return result
