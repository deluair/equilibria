"""Digital misinformation cost: economic cost proxy via trust in institutions index.

Misinformation imposes measurable economic costs: misallocated health spending
during infodemics, electoral distortions reducing policy quality, financial
market volatility from rumor-driven panic, and reduced social cooperation in
emergencies. These costs correlate inversely with institutional trust. The WDI
governance composite (control of corruption + government effectiveness) proxies
for the baseline institutional trust that constrains misinformation spread and
limits its economic damage.

Score: high institutional trust -> STABLE (low mis-info cost); moderate trust
-> WATCH; low trust -> STRESS (high economic damage from mis-info); failed
governance with open internet -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class DigitalMisinformationCost(LayerBase):
    layer_id = "lMD"
    name = "Digital Misinformation Cost"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        corrupt_code = "CC.EST"  # Control of corruption (WGI)
        goveff_code = "GE.EST"  # Government effectiveness (WGI)
        net_code = "IT.NET.USER.ZS"

        corrupt_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (corrupt_code, "%control of corruption%"),
        )
        goveff_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (goveff_code, "%government effectiveness%"),
        )
        net_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (net_code, "%internet users%"),
        )

        corrupt_vals = [r["value"] for r in corrupt_rows if r["value"] is not None]
        goveff_vals = [r["value"] for r in goveff_rows if r["value"] is not None]
        net_vals = [r["value"] for r in net_rows if r["value"] is not None]

        if not corrupt_vals and not goveff_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for corruption control CC.EST or government effectiveness GE.EST",
            }

        corrupt = corrupt_vals[0] if corrupt_vals else None
        goveff = goveff_vals[0] if goveff_vals else None
        net_pct = net_vals[0] if net_vals else 50.0

        # WGI scores -2.5 to +2.5 -> normalize to 0-100 (higher = better governance)
        if corrupt is not None:
            corrupt_norm = ((corrupt + 2.5) / 5.0) * 100.0
            corrupt_norm = max(0.0, min(100.0, corrupt_norm))
        else:
            corrupt_norm = 40.0

        if goveff is not None:
            goveff_norm = ((goveff + 2.5) / 5.0) * 100.0
            goveff_norm = max(0.0, min(100.0, goveff_norm))
        else:
            goveff_norm = 40.0

        # Trust composite: average of corruption control and government effectiveness
        trust_norm = (corrupt_norm + goveff_norm) / 2.0

        # Misinformation cost = low trust + high internet reach
        # High internet with low trust -> misinformation spreads widely and costs more
        internet_amplifier = net_pct / 100.0

        # Base stress from low trust
        base = 100.0 - trust_norm

        # Amplify cost when internet reaches more people in low-trust environments
        if trust_norm < 50 and net_pct >= 40:
            amplification = (50.0 - trust_norm) * internet_amplifier * 0.3
            base = min(100.0, base + amplification)

        score = round(max(5.0, min(100.0, base)), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "corruption_control_wgi": round(corrupt, 3) if corrupt is not None else None,
                "gov_effectiveness_wgi": round(goveff, 3) if goveff is not None else None,
                "internet_users_pct": round(net_pct, 2),
                "trust_composite_norm": round(trust_norm, 2),
                "internet_amplifier": round(internet_amplifier, 3),
                "n_obs_corruption": len(corrupt_vals),
                "n_obs_goveff": len(goveff_vals),
                "n_obs_internet": len(net_vals),
                "high_cost_risk": trust_norm < 40 and net_pct >= 50,
            },
        }
