"""Information ecosystem health: internet freedom and misinformation risk composite.

A healthy information ecosystem requires open internet infrastructure combined
with institutional capacity to counter misinformation. High internet penetration
in low-governance environments accelerates misinformation spread as social media
amplifies content without editorial gatekeeping. Conversely, authoritarian
control reduces misinformation via censorship but destroys information freedom.
The optimal zone is high access paired with strong institutional credibility.

Score: high internet access + strong governance + high trust -> STABLE; open
internet with weak institutions (misinformation risk) -> WATCH/STRESS; closed
internet with strong censorship -> STRESS; neither access nor governance -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class InformationEcosystemHealth(LayerBase):
    layer_id = "lMD"
    name = "Information Ecosystem Health"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        net_code = "IT.NET.USER.ZS"
        voice_code = "VA.EST"  # Voice and accountability (WGI)
        rule_code = "RL.EST"  # Rule of law (WGI)

        net_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (net_code, "%internet users%"),
        )
        voice_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (voice_code, "%voice and accountability%"),
        )
        rule_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (rule_code, "%rule of law%"),
        )

        net_vals = [r["value"] for r in net_rows if r["value"] is not None]
        voice_vals = [r["value"] for r in voice_rows if r["value"] is not None]
        rule_vals = [r["value"] for r in rule_rows if r["value"] is not None]

        if not net_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for internet users IT.NET.USER.ZS",
            }

        net_pct = net_vals[0]
        voice = voice_vals[0] if voice_vals else None
        rule = rule_vals[0] if rule_vals else None

        # Access score: internet penetration as 0-100
        access_norm = net_pct

        # Governance quality: WGI scores -2.5 to +2.5 -> normalize to 0-100
        if voice is not None:
            voice_norm = ((voice + 2.5) / 5.0) * 100.0
            voice_norm = max(0.0, min(100.0, voice_norm))
        else:
            voice_norm = 40.0

        if rule is not None:
            rule_norm = ((rule + 2.5) / 5.0) * 100.0
            rule_norm = max(0.0, min(100.0, rule_norm))
        else:
            rule_norm = 40.0

        gov_norm = (voice_norm + rule_norm) / 2.0

        # Ecosystem health: high access + high governance = healthy
        # High access + low governance = misinformation amplification risk
        # Low access + high governance = suppression (also unhealthy)
        ecosystem_health = (access_norm * 0.5 + gov_norm * 0.5)

        # Misinformation amplification penalty: high access with low governance
        if access_norm >= 60 and gov_norm < 40:
            penalty = (access_norm - 60) * 0.3  # open internet, weak institutions
        else:
            penalty = 0.0

        # Invert: higher health -> lower stress
        base = 100.0 - ecosystem_health + penalty
        score = round(max(5.0, min(100.0, base)), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "internet_users_pct": round(net_pct, 2),
                "voice_accountability_wgi": round(voice, 3) if voice is not None else None,
                "rule_of_law_wgi": round(rule, 3) if rule is not None else None,
                "governance_norm": round(gov_norm, 2),
                "ecosystem_health_index": round(ecosystem_health, 2),
                "misinformation_penalty": round(penalty, 2),
                "n_obs_internet": len(net_vals),
                "n_obs_voice": len(voice_vals),
                "n_obs_rule": len(rule_vals),
                "misinformation_risk_elevated": access_norm >= 60 and gov_norm < 40,
            },
        }
