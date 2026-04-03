"""Digital currency geopolitics: CBDC adoption and dollar displacement risk.

Central Bank Digital Currencies (CBDCs) represent the most significant
structural challenge to the post-WWII dollar-centric international monetary
system since Bretton Woods. The PBOC's e-CNY, mBridge, and bilateral CBDC
swap agreements explicitly target dollar displacement in trade settlement.
This module proxies CBDC adoption and digital finance penetration via
WDI financial inclusion indicators (mobile money, digital payments).

Eichengreen (2011) network externalities argument: reserve currency
incumbency is durable, but CBDC interoperability can overcome switching
costs at scale.

Score: low digital financial penetration (limited CBDC risk) -> STABLE;
high mobile money + digital payment penetration -> WATCH/STRESS
(dollar displacement risk rising).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class DigitalCurrencyGeopolitics(LayerBase):
    layer_id = "lMS"
    name = "Digital Currency Geopolitics"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        # Mobile money account ownership: WDI FX.OWN.TOTL.MA.ZS (mobile)
        mobile_code = "FX.OWN.TOTL.MA.ZS"
        mobile_name = "Account ownership at financial institution"
        rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            (mobile_code, f"%{mobile_name}%"),
        )
        # Internet users as digital economy proxy: WDI IT.NET.USER.ZS
        inet_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            ("IT.NET.USER.ZS", "%Internet users%"),
        )
        # Mobile phone subscriptions: IT.CEL.SETS.P2
        mobile_subs_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 10",
            ("IT.CEL.SETS.P2", "%mobile cellular subscriptions%"),
        )

        fin_vals = [r["value"] for r in rows if r["value"] is not None]
        inet_vals = [r["value"] for r in inet_rows if r["value"] is not None]
        mobile_vals = [r["value"] for r in mobile_subs_rows if r["value"] is not None]

        if not fin_vals and not inet_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for digital inclusion proxies",
            }

        # Use whichever proxy is available
        if fin_vals:
            primary = fin_vals[0]
            primary_label = "financial_account_ownership_pct"
        elif inet_vals:
            primary = inet_vals[0]
            primary_label = "internet_users_pct"
        else:
            primary = mobile_vals[0] if mobile_vals else None
            primary_label = "mobile_subs_per100"

        if primary is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "all digital proxies returned null",
            }

        internet_pct = inet_vals[0] if inet_vals else None

        # Score: higher digital penetration = higher CBDC displacement potential
        # This is a structural/long-run risk, so scoring is moderate even at high levels
        if primary < 20:
            base = 8.0
        elif primary < 40:
            base = 8.0 + (primary - 20) * 0.7
        elif primary < 60:
            base = 22.0 + (primary - 40) * 0.8
        elif primary < 80:
            base = 38.0 + (primary - 60) * 0.85
        else:
            base = 55.0 + min(25.0, (primary - 80) * 0.5)

        # Internet penetration amplifies CBDC displacement risk
        if internet_pct is not None and internet_pct > 70:
            base = min(100.0, base + 10.0)

        score = round(base, 2)
        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                primary_label: round(primary, 2),
                "internet_users_pct": round(internet_pct, 2) if internet_pct is not None else None,
                "mobile_subs_per100": round(mobile_vals[0], 2) if mobile_vals else None,
                "n_obs_fin": len(fin_vals),
                "n_obs_inet": len(inet_vals),
                "cbdc_displacement_risk": "high" if score > 50 else "moderate" if score > 25 else "low",
            },
        }
