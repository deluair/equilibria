"""Pandemic medicine preparedness: hospital beds and health expenditure composite.

Pandemic preparedness requires both physical healthcare capacity (hospital beds)
and adequate health financing. This module combines hospital beds per 1,000
people (SH.MED.BEDS.ZS) and health expenditure as % GDP (SH.XPD.CHEX.GD.ZS)
into a preparedness stress score.

Key references:
    Kandel, N. et al. (2020). Health security capacities in the context of
        COVID-19 outbreak. The Lancet, 395(10229), 1047-1053.
    Fan, V.Y. et al. (2018). Pandemic risk: how large are the expected losses?
        Bulletin of the World Health Organization, 96(2), 129-134.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class PandemicMedicinePreparedness(LayerBase):
    layer_id = "lPH"
    name = "Pandemic Medicine Preparedness"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate pandemic medicine preparedness from beds and health spending.

        Uses SH.MED.BEDS.ZS (hospital beds per 1,000 people) and
        SH.XPD.CHEX.GD.ZS (health expenditure % GDP). Low beds and low
        spending jointly indicate poor pandemic medicine preparedness.

        Returns dict with score, signal, and relevant metrics.
        """
        beds_code = "SH.MED.BEDS.ZS"
        beds_name = "hospital beds"
        chex_code = "SH.XPD.CHEX.GD.ZS"
        chex_name = "health expenditure"

        beds_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (beds_code, f"%{beds_name}%"),
        )
        chex_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (chex_code, f"%{chex_name}%"),
        )

        if not beds_rows and not chex_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"No data for {beds_code} or {chex_code} in DB",
            }

        def _extract(rows):
            vals = [float(r["value"]) for r in rows if r["value"] is not None]
            return vals[0] if vals else None

        beds_latest = _extract(beds_rows)
        chex_latest = _extract(chex_rows)

        scores = []
        # Hospital beds: <1 per 1k = very low (score 80+); 1-3 = moderate; >5 = adequate
        if beds_latest is not None:
            beds_score = float(np.clip(((3.0 - beds_latest) / 3.0) * 75, 0, 100))
            scores.append(beds_score)

        # Health expenditure: <3% GDP = very low (score 70+); >6% = adequate
        if chex_latest is not None:
            chex_score = float(np.clip(((6.0 - chex_latest) / 6.0) * 70, 0, 100))
            scores.append(chex_score)

        if not scores:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All fetched rows have NULL value",
            }

        score = float(np.clip(float(np.mean(scores)), 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "hospital_beds_per1k_latest": round(beds_latest, 3) if beds_latest is not None else None,
                "health_exp_pct_gdp_latest": round(chex_latest, 2) if chex_latest is not None else None,
                "indicators": [beds_code, chex_code],
            },
        }
