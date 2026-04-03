"""Institutional Stability module.

Combines two WGI dimensions:
1. PV.EST: Political Stability and Absence of Violence/Terrorism.
   Low values indicate high political instability and violence risk, which
   disrupts institutions, deters investment, and raises transaction costs.
2. VA.EST: Voice and Accountability. Measures political participation,
   civil liberties, and press freedom. Weak accountability enables institutional
   deterioration and reduces checks on executive discretion.

Both are on WGI scale (-2.5 to 2.5). Each is inverted to stress and averaged.

References:
    World Bank. (2023). Worldwide Governance Indicators.
    Alesina, A. & Perotti, R. (1996). Income Distribution, Political Instability,
        and Investment. European Economic Review 40(6), 1203-1228.
    Acemoglu, D. et al. (2019). Democracy Does Cause Growth. JPE 127(1), 47-100.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class InstitutionalStability(LayerBase):
    layer_id = "lIE"
    name = "Institutional Stability"

    async def compute(self, db, **kwargs) -> dict:
        pv_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("PV.EST", "%political stability%"),
        )
        va_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = ("
            "SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            ("VA.EST", "%voice and accountability%"),
        )

        if not pv_rows and not va_rows:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no institutional stability data"}

        def wgi_stress(rows):
            if not rows:
                return None, None
            val = float(rows[0]["value"])
            stress = 1.0 - (val + 2.5) / 5.0
            return max(0.0, min(1.0, stress)), round(val, 4)

        pv_stress, pv_val = wgi_stress(pv_rows)
        va_stress, va_val = wgi_stress(va_rows)

        stresses = [s for s in [pv_stress, va_stress] if s is not None]
        composite_stress = sum(stresses) / len(stresses)
        score = round(composite_stress * 100.0, 2)

        return {
            "score": score,
            "metrics": {
                "pv_est": pv_val,
                "va_est": va_val,
                "pv_stress": round(pv_stress, 4) if pv_stress is not None else None,
                "va_stress": round(va_stress, 4) if va_stress is not None else None,
                "n_indicators": len(stresses),
            },
            "reference": "WB PV.EST + VA.EST; Alesina & Perotti 1996; Acemoglu et al. 2019 JPE",
        }
