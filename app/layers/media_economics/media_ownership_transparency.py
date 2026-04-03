"""Media ownership transparency: regulatory quality and transparency for ownership rules.

Transparent media ownership rules require two institutional prerequisites:
a competent regulatory body with enforcement capacity, and a general environment
of political accountability that resists capture of media regulators by owner
interests. The WGI regulatory quality score and voice and accountability index
together proxy for the institutional conditions under which ownership disclosure
requirements are enacted and enforced. Without them, concentration proceeds
unchecked and cross-ownership of media and political interests goes undisclosed.

Score: strong regulatory quality + high accountability -> STABLE transparent
ownership regime; moderate -> WATCH; weak regulatory capacity -> STRESS opacity
and concentration; captured or absent regulation -> CRISIS.
"""

from __future__ import annotations

from app.layers.base import LayerBase


class MediaOwnershipTransparency(LayerBase):
    layer_id = "lMD"
    name = "Media Ownership Transparency"
    weight = 0.20

    async def compute(self, db, **kwargs) -> dict:
        reg_code = "IQ.REG.QUAL.XQ"  # Regulatory quality (WGI)
        voice_code = "VA.EST"  # Voice and accountability (WGI)
        corrupt_code = "CC.EST"  # Control of corruption (WGI)

        reg_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (reg_code, "%regulatory quality%"),
        )
        voice_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (voice_code, "%voice and accountability%"),
        )
        corrupt_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (corrupt_code, "%control of corruption%"),
        )

        reg_vals = [r["value"] for r in reg_rows if r["value"] is not None]
        voice_vals = [r["value"] for r in voice_rows if r["value"] is not None]
        corrupt_vals = [r["value"] for r in corrupt_rows if r["value"] is not None]

        if not reg_vals and not voice_vals and not corrupt_vals:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no data for regulatory quality, voice accountability, or corruption control",
            }

        reg = reg_vals[0] if reg_vals else None
        voice = voice_vals[0] if voice_vals else None
        corrupt = corrupt_vals[0] if corrupt_vals else None

        # Normalize WGI scores (-2.5 to +2.5) -> 0-100 (higher = better)
        def norm_wgi(v):
            if v is None:
                return 40.0  # neutral fallback
            return max(0.0, min(100.0, ((v + 2.5) / 5.0) * 100.0))

        reg_norm = norm_wgi(reg)
        voice_norm = norm_wgi(voice)
        corrupt_norm = norm_wgi(corrupt)

        # Ownership transparency composite:
        # Regulatory quality (enforcement capacity): 50% weight
        # Voice/accountability (political will): 30% weight
        # Corruption control (capture resistance): 20% weight
        transparency_norm = (
            0.50 * reg_norm +
            0.30 * voice_norm +
            0.20 * corrupt_norm
        )

        # Invert: higher transparency norm -> lower stress
        base = 100.0 - transparency_norm
        score = round(max(5.0, min(95.0, base)), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "regulatory_quality_wgi": round(reg, 3) if reg is not None else None,
                "voice_accountability_wgi": round(voice, 3) if voice is not None else None,
                "corruption_control_wgi": round(corrupt, 3) if corrupt is not None else None,
                "reg_norm": round(reg_norm, 2),
                "voice_norm": round(voice_norm, 2),
                "corrupt_norm": round(corrupt_norm, 2),
                "transparency_composite": round(transparency_norm, 2),
                "n_obs_regulatory": len(reg_vals),
                "n_obs_voice": len(voice_vals),
                "n_obs_corruption": len(corrupt_vals),
                "ownership_regime_adequate": transparency_norm >= 55.0,
            },
        }
