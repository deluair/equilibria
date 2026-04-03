"""AI startup ecosystem: new business entry rate and venture capital proxy.

A vibrant AI startup ecosystem requires: (1) ease of business entry so new
AI ventures can form quickly, (2) available risk capital to fund pre-revenue
AI companies, and (3) adequate human capital. New business density (registrations
per 1,000 adults) proxies entrepreneurial dynamism. Domestic credit to private
sector as a share of GDP proxies capital availability for early-stage ventures
in the absence of direct VC data.

Acemoglu and Restrepo (2022): countries with denser startup ecosystems adapt
faster to automation via creative destruction and reallocation.

Score: low business entry + low private credit -> CRISIS (ecosystem absent),
high entry rate + deep financial system -> STABLE (ecosystem enabling AI diffusion).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class AIStartupEcosystem(LayerBase):
    layer_id = "lAI"
    name = "AI Startup Ecosystem"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        bizreg_code = "IC.BUS.NREG"
        credit_code = "FS.AST.PRVT.GD.ZS"

        biz_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (bizreg_code, "%new business registrations%"),
        )
        credit_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (credit_code, "%domestic credit to private sector%"),
        )

        biz_vals = [r["value"] for r in biz_rows if r["value"] is not None]
        credit_vals = [r["value"] for r in credit_rows if r["value"] is not None]

        if not biz_vals and not credit_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for new business registrations IC.BUS.NREG or private credit FS.AST.PRVT.GD.ZS",
            }

        biz_reg = biz_vals[0] if biz_vals else None
        private_credit = credit_vals[0] if credit_vals else None

        # Base score from private credit (financial ecosystem depth)
        if private_credit is not None:
            if private_credit >= 100:
                base = 10.0
            elif private_credit >= 60:
                base = 10.0 + (100.0 - private_credit) * 0.5
            elif private_credit >= 30:
                base = 30.0 + (60.0 - private_credit) * 0.67
            elif private_credit >= 10:
                base = 50.0 + (30.0 - private_credit) * 1.5
            else:
                base = min(90.0, 80.0 + (10.0 - private_credit) * 1.0)
        else:
            base = 55.0

        # Business registration density modifies ecosystem vibrancy
        # Higher new registrations = more entrepreneurial dynamism
        if biz_reg is not None:
            if biz_reg >= 5000:
                base = max(5.0, base - 15.0)
            elif biz_reg >= 1000:
                base = max(5.0, base - 8.0)
            elif biz_reg >= 200:
                base = max(5.0, base - 3.0)
            elif biz_reg < 50:
                base = min(100.0, base + 8.0)

        score = round(base, 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "new_business_registrations": int(biz_reg) if biz_reg is not None else None,
                "private_credit_gdp_pct": round(private_credit, 2) if private_credit is not None else None,
                "n_obs_business": len(biz_vals),
                "n_obs_credit": len(credit_vals),
                "ecosystem_vibrant": score < 30,
                "capital_access": (
                    "deep" if private_credit is not None and private_credit >= 80
                    else "moderate" if private_credit is not None and private_credit >= 40
                    else "shallow"
                ),
            },
        }
