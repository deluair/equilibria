"""Generic medicine penetration: inverted domestic patent applications.

Low domestic patent filings signal a generic-friendly environment, where
originator drug protections are limited and generic manufacturers face fewer
barriers. This proxy is appropriate for low- and middle-income countries that
rely on generics for pharmaceutical access.

Key references:
    Waning, B. et al. (2010). A lifeline to treatment: the role of Indian
        generic manufacturers in supplying antiretroviral medicines to
        developing countries. Journal of the International AIDS Society, 13(1).
    Kapczynski, A. & Hollis, A. (2009). The innovation divide. PLoS Med, 6.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class GenericMedicinePenetration(LayerBase):
    layer_id = "lPH"
    name = "Generic Medicine Penetration"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        """Estimate generic medicine penetration via inverted patent applications.

        Uses IP.PAT.RESD (patent applications by residents). Few resident
        patents = weaker local patent ecosystem = more favorable for generics.
        Score is inverted: low patent count -> low score (good for generics).

        Returns dict with score, signal, and relevant metrics.
        """
        code = "IP.PAT.RESD"
        name = "patent applications residents"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (code, f"%{name}%"),
        )

        if not rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": f"No data for {code} in DB",
            }

        values = [float(row["value"]) for row in rows if row["value"] is not None]
        if not values:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All fetched rows have NULL value",
            }

        latest = values[0]
        mean_val = float(np.mean(values))

        # Inverted: very high patent filings (e.g., 500k+) = low generic penetration = high score
        # Score = 100 * (patents / 600000), clipped. Low patent country -> low score (favorable).
        score = float(np.clip((latest / 600_000.0) * 100, 0, 100))

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "resident_patents_latest": round(latest, 0),
                "resident_patents_mean_15obs": round(mean_val, 0),
                "n_observations": len(values),
                "indicator": code,
                "interpretation": "Lower score = more generic-friendly patent environment",
            },
        }
