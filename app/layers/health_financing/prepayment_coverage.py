"""Prepayment coverage: insurance and tax-funded health financing.

Measures the share of health expenditure financed through prepayment mechanisms
(social health insurance, private insurance, and government tax-funded schemes)
versus direct out-of-pocket payments. Higher prepayment share indicates stronger
financial protection and risk pooling.

The domestic general government health expenditure (GGHE) share of current
health expenditure (CHE) proxies the prepayment rate when social insurance
data is unavailable.

Key references:
    Preker, A.S. et al. (2002). Rich-poor differences in health care financing.
        In: Dror, D.M. & Preker, A.S. (eds.) Social Reinsurance. World Bank.
    WHO (2010). Health systems financing: the path to universal coverage.
    Kutzin, J. (2012). Anything goes on the path to universal health coverage?
        Bulletin of the World Health Organization, 90(11), 867-868.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PrepaymentCoverage(LayerBase):
    layer_id = "lHF"
    name = "Prepayment Coverage"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Compute prepayment coverage rate from government health expenditure share.

        Fetches domestic general government health expenditure as % of CHE
        (SH.XPD.GHED.CH.ZS) as primary prepayment proxy. Also fetches
        OOP as % of CHE (SH.XPD.OOPC.CH.ZS) to derive non-OOP (prepayment) share.
        Cross-validates both approaches and scores by prepayment deficit.

        Returns dict with score, signal, and prepayment coverage metrics.
        """
        gghe_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.GHED.CH.ZS'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        oop_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.OOPC.CH.ZS'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        if not gghe_rows and not oop_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No government health expenditure or OOP data in DB",
            }

        def _latest(rows) -> dict[str, float]:
            out: dict[str, float] = {}
            for row in rows:
                iso = row["country_iso3"]
                if iso not in out and row["value"] is not None:
                    out[iso] = float(row["value"])
            return out

        gghe_data = _latest(gghe_rows)
        oop_data = _latest(oop_rows)

        # Prepayment rate: 100 - OOP% (if OOP data available), else GGHE%
        prepayment_rates: dict[str, float] = {}
        for iso in set(gghe_data.keys()) | set(oop_data.keys()):
            if iso in oop_data:
                # More direct: prepayment = non-OOP share
                prepayment_rates[iso] = max(0.0, 100.0 - oop_data[iso])
            elif iso in gghe_data:
                prepayment_rates[iso] = gghe_data[iso]

        if not prepayment_rates:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid prepayment data after combining sources",
            }

        values = list(prepayment_rates.values())
        mean_prepayment = float(np.mean(values))
        median_prepayment = float(np.median(values))

        # WHO target: >=80% prepayment for adequate financial protection
        who_prepayment_target = 80.0
        low_coverage = [v for v in values if v < 50]
        moderate_coverage = [v for v in values if 50 <= v < 80]
        adequate_coverage = [v for v in values if v >= 80]

        n = len(values)
        # Score: low prepayment = high stress
        deficit_score = (len(low_coverage) * 1.0 + len(moderate_coverage) * 0.4) / n
        score = float(np.clip(deficit_score * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "n_countries": n,
                "mean_prepayment_rate_pct": round(mean_prepayment, 2),
                "median_prepayment_rate_pct": round(median_prepayment, 2),
                "who_target_pct": who_prepayment_target,
                "countries_adequate_coverage_gte80pct": len(adequate_coverage),
                "countries_moderate_coverage_50_80pct": len(moderate_coverage),
                "countries_low_coverage_lt50pct": len(low_coverage),
                "pct_below_who_target": round(100.0 * (n - len(adequate_coverage)) / n, 1),
                "sources_used": {
                    "gghe_countries": len(gghe_data),
                    "oop_derived_countries": len(oop_data),
                },
            },
        }
