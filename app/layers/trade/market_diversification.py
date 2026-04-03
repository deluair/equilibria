"""Export market diversification scoring.

Methodology:
    Measure geographic concentration risk of a country's export portfolio.
    Combines multiple diversification metrics:

    1. Herfindahl-Hirschman Index (HHI) of export destinations:
       HHI = sum(s_j^2) where s_j is partner j's share of total exports.
    2. Normalized HHI: HHI* = (HHI - 1/N) / (1 - 1/N).
    3. Entropy index: H = -sum(s_j * ln(s_j)).
    4. Effective number of markets: N_eff = 1 / HHI.
    5. Gini coefficient of export distribution across partners.
    6. Top-5 partner concentration ratio (CR5).

    New market entry probability estimated from historical patterns:
    frequency of new partner-product relationships appearing per year.

    Score (0-100): Higher score means higher geographic concentration
    (less diversified, more vulnerable).

References:
    Cadot, O. et al. (2011). "Export diversification: What's behind the
        hump?" Review of Economics and Statistics, 93(2), 590-605.
    Hausmann, R. and Rodrik, D. (2003). "Economic development as
        self-discovery." Journal of Development Economics, 72(2), 603-633.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class MarketDiversification(LayerBase):
    layer_id = "l1"
    name = "Market Diversification"

    async def compute(self, db, **kwargs) -> dict:
        """Compute export market diversification metrics.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            reporter : str - ISO3 country code
            year : int - reference year
            lookback_years : int - years for new market entry analysis
        """
        reporter = kwargs.get("reporter", "USA")
        year = kwargs.get("year", 2022)
        lookback = kwargs.get("lookback_years", 5)

        # Fetch export values by partner
        rows = await db.execute(
            """
            SELECT partner_iso3, SUM(trade_value) as total_exports
            FROM bilateral_trade
            WHERE reporter_iso3 = ? AND year = ? AND flow = 'export'
            GROUP BY partner_iso3
            HAVING total_exports > 0
            ORDER BY total_exports DESC
            """,
            (reporter, year),
        )
        records = await rows.fetchall()

        if not records:
            return {"score": 50.0, "hhi": None,
                    "note": "No export data available"}

        partners = [r["partner_iso3"] for r in records]
        values = np.array([float(r["total_exports"]) for r in records])
        total = values.sum()

        if total <= 0:
            return {"score": 50.0, "hhi": None,
                    "note": "Zero total exports"}

        shares = values / total
        n = len(shares)

        # HHI
        hhi = float(np.sum(shares ** 2))

        # Normalized HHI
        hhi_norm = (hhi - 1 / n) / (1 - 1 / n) if n > 1 else 1.0

        # Entropy
        entropy = float(-np.sum(shares * np.log(shares + 1e-15)))
        max_entropy = np.log(n)
        entropy_normalized = entropy / max_entropy if max_entropy > 0 else 0.0

        # Effective number of markets
        n_effective = 1 / hhi if hhi > 0 else n

        # Gini coefficient
        sorted_shares = np.sort(shares)
        index = np.arange(1, n + 1)
        gini = float(
            (2 * np.sum(index * sorted_shares) / (n * np.sum(sorted_shares))) - (n + 1) / n
        )

        # Concentration ratios
        cr5 = float(np.sum(shares[:5])) if n >= 5 else float(np.sum(shares))
        cr10 = float(np.sum(shares[:10])) if n >= 10 else float(np.sum(shares))

        # Top partners
        top_partners = [
            {"partner": partners[i], "share": float(shares[i]),
             "value": float(values[i])}
            for i in range(min(10, n))
        ]

        # New market entry analysis
        new_entry_rows = await db.execute(
            """
            SELECT year, COUNT(DISTINCT partner_iso3) as n_partners
            FROM bilateral_trade
            WHERE reporter_iso3 = ?
              AND year BETWEEN ? AND ?
              AND flow = 'export'
              AND trade_value > 0
            GROUP BY year
            ORDER BY year
            """,
            (reporter, year - lookback, year),
        )
        entry_records = await new_entry_rows.fetchall()

        new_market_rate = 0.0
        if len(entry_records) >= 2:
            partner_counts = [int(r["n_partners"]) for r in entry_records]
            # Approximate new entry rate as average year-over-year increase
            diffs = [partner_counts[i] - partner_counts[i - 1]
                     for i in range(1, len(partner_counts))]
            new_market_rate = float(np.mean(diffs))

        # Score: high HHI = high concentration = high score (bad)
        # HHI ranges from 1/N (perfectly diversified) to 1 (single partner)
        score = float(np.clip(hhi_norm * 100, 0, 100))

        return {
            "score": score,
            "hhi": hhi,
            "hhi_normalized": float(hhi_norm),
            "entropy": entropy,
            "entropy_normalized": float(entropy_normalized),
            "gini": gini,
            "n_effective_markets": float(n_effective),
            "n_partners": n,
            "cr5": cr5,
            "cr10": cr10,
            "top_partners": top_partners,
            "new_market_entry_rate": new_market_rate,
            "total_exports": float(total),
            "reporter": reporter,
            "year": year,
        }
