"""Institutional Convergence module.

Measures governance improvement or deterioration over time using the WGI
composite trend across all six dimensions: CC.EST, GE.EST, PV.EST, RL.EST,
RQ.EST, VA.EST. A positive multi-year trend signals institutional convergence
toward better governance; a negative trend signals deterioration.

The stress score reflects the current governance level (latest WGI composite
average) combined with the direction and pace of change. Countries with weak
institutions that are improving get partial credit; those deteriorating from
an already weak base receive maximum stress.

References:
    Kaufmann, D., Kraay, A. & Mastruzzi, M. (2010). WGI 1996-2009. World Bank.
    Rodrik, D., Subramanian, A. & Trebbi, F. (2004). Institutions Rule. JEG 9(2).
    Acemoglu, D. & Robinson, J.A. (2012). Why Nations Fail. Crown Business.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class InstitutionalConvergence(LayerBase):
    layer_id = "lIE"
    name = "Institutional Convergence"

    _WGI_CODES = [
        ("CC.EST", "%control of corruption%"),
        ("GE.EST", "%government effectiveness%"),
        ("PV.EST", "%political stability%"),
        ("RL.EST", "%rule of law%"),
        ("RQ.EST", "%regulatory quality%"),
        ("VA.EST", "%voice and accountability%"),
    ]

    async def compute(self, db, **kwargs) -> dict:
        all_vals: list[float] = []
        # Collect latest values for each WGI dimension
        for code, name_pat in self._WGI_CODES:
            rows = await db.fetch_all(
                "SELECT value FROM data_points WHERE series_id = ("
                "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
                "ORDER BY date DESC LIMIT 15",
                (code, name_pat),
            )
            if rows:
                all_vals.append(float(rows[0]["value"]))

        if not all_vals:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no WGI composite data"}

        # Current governance level: mean of available WGI dimensions
        mean_wgi = sum(all_vals) / len(all_vals)

        # Base stress from current level
        base_stress = 1.0 - (mean_wgi + 2.5) / 5.0
        base_stress = max(0.0, min(1.0, base_stress))

        # Trend: try to get time-series for RL.EST as representative dimension
        trend_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("RL.EST", "%rule of law%"),
        )

        trend_adjustment = 0.0
        trend_dir = None
        trend_magnitude = None

        if len(trend_rows) >= 3:
            ts = [float(r["value"]) for r in trend_rows]
            # rows are DESC (most recent first)
            # slope = (oldest - newest) would be negative if improving
            delta = ts[0] - ts[-1]  # positive if latest > oldest = improving
            span = len(ts) - 1
            slope_per_obs = delta / span if span > 0 else 0.0
            trend_dir = "improving" if slope_per_obs > 0.01 else "deteriorating" if slope_per_obs < -0.01 else "stable"
            trend_magnitude = round(abs(slope_per_obs), 5)
            # Improvement reduces stress by up to 5 points; deterioration adds up to 5
            trend_adjustment = -slope_per_obs * 10.0  # flip: negative delta -> worse
            trend_adjustment = max(-5.0, min(5.0, trend_adjustment))

        score = round(base_stress * 100.0 + trend_adjustment, 2)
        score = max(0.0, min(100.0, score))

        result = {
            "score": score,
            "metrics": {
                "wgi_composite_mean": round(mean_wgi, 4),
                "n_dimensions": len(all_vals),
                "base_stress": round(base_stress, 4),
            },
            "reference": "WGI 6-dimension composite; Rodrik et al. 2004 JEG; Acemoglu & Robinson 2012",
        }
        if trend_dir:
            result["metrics"]["trend"] = trend_dir
            result["metrics"]["trend_slope_per_obs"] = trend_magnitude
            result["metrics"]["trend_adjustment"] = round(trend_adjustment, 2)
        return result
