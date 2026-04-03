"""Disease financing gap: funding shortfall for priority disease burden.

Estimates the gap between disease burden (measured by mortality and morbidity
indicators) and health financing capacity. Countries with high disease burden
but low health spending face the largest financing gaps for priority areas
including communicable diseases, maternal/child health, and NCDs.

The financing gap is proxied by the ratio of disease burden indicators to
health expenditure, identifying countries where spending is inadequate relative
to need.

Key references:
    Institute for Health Metrics and Evaluation (2019). Global Burden of
        Disease Study 2019.
    Dieleman, J.L. et al. (2018). Estimated global spending on health R&D
        for 29 global health causes, 2000-2017. JAMA, 323(2), 133-145.
    Stenberg, K. et al. (2017). Financing transformative health systems towards
        achievement of the health SDGs. The Lancet Global Health, 5(9), e875-e887.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class DiseaseFinancingGap(LayerBase):
    layer_id = "lHF"
    name = "Disease Financing Gap"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate disease financing gap using burden vs spending indicators.

        Fetches under-5 mortality rate (SH.DYN.MORT), maternal mortality ratio
        (SH.STA.MMRT), HIV prevalence (SH.DYN.AIDS.ZS), and health expenditure
        per capita (SH.XPD.CHEX.PC.CD) to identify countries with high disease
        burden relative to financing.

        Returns dict with score, signal, and disease financing gap metrics.
        """
        u5m_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.DYN.MORT'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        mmr_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.STA.MMRT'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        hiv_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.DYN.AIDS.ZS'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        hepc_rows = await db.fetch_all(
            """
            SELECT ds.country_iso3, dp.date, dp.value
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.series_id = 'SH.XPD.CHEX.PC.CD'
              AND dp.value IS NOT NULL
            ORDER BY ds.country_iso3, dp.date DESC
            """
        )

        if not u5m_rows and not mmr_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No disease burden data (U5M or MMR) in DB",
            }

        def _latest(rows) -> dict[str, float]:
            out: dict[str, float] = {}
            for row in rows:
                iso = row["country_iso3"]
                if iso not in out and row["value"] is not None:
                    out[iso] = float(row["value"])
            return out

        u5m_data = _latest(u5m_rows)
        mmr_data = _latest(mmr_rows)
        hiv_data = _latest(hiv_rows)
        hepc_data = _latest(hepc_rows)

        # Normalize burden indicators to 0-1 scale for composite
        def _normalize(vals: list[float]) -> list[float]:
            if not vals:
                return []
            mn, mx = min(vals), max(vals)
            if mx == mn:
                return [0.5] * len(vals)
            return [(v - mn) / (mx - mn) for v in vals]

        # Build composite burden and financing gap per country
        all_isos = (
            set(u5m_data.keys())
            | set(mmr_data.keys())
            | set(hiv_data.keys())
        ) & set(hepc_data.keys())

        if len(all_isos) < 5:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"Insufficient countries with both burden and financing data: {len(all_isos)}",
            }

        # Normalize each indicator across available countries
        u5m_isos = [iso for iso in all_isos if iso in u5m_data]
        mmr_isos = [iso for iso in all_isos if iso in mmr_data]

        u5m_norm = dict(zip(u5m_isos, _normalize([u5m_data[i] for i in u5m_isos])))
        mmr_norm = dict(zip(mmr_isos, _normalize([mmr_data[i] for i in mmr_isos])))

        # Log-normalize health spending (diminishing returns at high spending)
        hepc_vals = [hepc_data[iso] for iso in all_isos if hepc_data.get(iso, 0) > 0]
        if not hepc_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No valid health expenditure data for financing gap calc",
            }

        log_hepc = {iso: np.log(hepc_data[iso]) for iso in all_isos if hepc_data.get(iso, 0) > 0}
        log_vals = list(log_hepc.values())
        log_min, log_max = min(log_vals), max(log_vals)

        gaps: list[float] = []
        for iso in all_isos:
            if iso not in log_hepc:
                continue
            burden = 0.0
            n_burden = 0
            if iso in u5m_norm:
                burden += u5m_norm[iso]
                n_burden += 1
            if iso in mmr_norm:
                burden += mmr_norm[iso]
                n_burden += 1
            if n_burden == 0:
                continue
            avg_burden = burden / n_burden

            # Spending adequacy: normalized log spending (0=lowest, 1=highest)
            spend_norm = (log_hepc[iso] - log_min) / (log_max - log_min) if log_max > log_min else 0.5

            # Gap = burden - spending (positive = underfinanced relative to burden)
            gap = avg_burden - spend_norm
            gaps.append(gap)

        if not gaps:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "Could not compute financing gaps",
            }

        mean_gap = float(np.mean(gaps))
        high_gap = [g for g in gaps if g > 0.3]
        moderate_gap = [g for g in gaps if 0.1 < g <= 0.3]

        # Score: positive mean gap and high fraction of high-gap countries = stress
        n_gaps = len(gaps)
        stress = (len(high_gap) * 1.0 + len(moderate_gap) * 0.5) / n_gaps
        gap_component = float(np.clip(max(0, mean_gap) * 100, 0, 50))
        burden_component = float(np.clip(stress * 50, 0, 50))
        score = float(np.clip(gap_component + burden_component, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "n_countries_assessed": n_gaps,
                "mean_financing_gap_normalized": round(mean_gap, 4),
                "countries_high_gap_gt0_3": len(high_gap),
                "countries_moderate_gap_0_1_0_3": len(moderate_gap),
                "pct_high_financing_gap": round(100.0 * len(high_gap) / n_gaps, 1),
                "burden_indicators_used": {
                    "u5m_countries": len(u5m_data),
                    "mmr_countries": len(mmr_data),
                    "hiv_countries": len(hiv_data),
                },
                "financing_indicator": "SH.XPD.CHEX.PC.CD",
                "financing_countries": len(hepc_data),
            },
        }
