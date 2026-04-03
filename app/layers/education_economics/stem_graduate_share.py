"""STEM graduates as % of total tertiary graduates.

Measures the share of university graduates in Science, Technology, Engineering,
and Mathematics fields. Low STEM share relative to peer countries signals
structural vulnerability to technological change and reduced innovation capacity.

UNESCO classification: ISCED fields 05 (Natural sciences, math, statistics),
06 (Information and communication technologies), 07 (Engineering, manufacturing,
construction).

References:
    UNESCO Institute for Statistics (2023). ISCED Fields of Education and
        Training 2013 (ISCED-F 2013).
    Acemoglu, D. & Restrepo, P. (2018). The race between man and machine:
        implications of technology for growth, factor shares, and employment.
        AER, 108(6), 1488-1542.

Score: very low STEM share -> STRESS (technological absorption constraint).
"""

from __future__ import annotations

from app.layers.base import LayerBase


class StemGraduateShare(LayerBase):
    layer_id = "lED"
    name = "STEM Graduate Share"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # STEM graduates % of total tertiary (UIS SE.TER.GRAD.STEM.ZS or similar)
        stem_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source = 'stem_graduates'
            ORDER BY dp.date DESC
            LIMIT 5
            """,
            (country,),
        )

        # Fallback: UIS WDI series SE.TER.GRAD.FE.SI.ZS (female STEM) + male
        wdi_stem_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN (
                  'SE.TER.GRAD.FE.SI.ZS', 'SE.TER.GRAD.MA.SI.ZS',
                  'SE.TER.GRAD.FE.EN.ZS', 'SE.TER.GRAD.MA.EN.ZS'
              )
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        stem_share = None

        s_vals = [r["value"] for r in stem_rows if r["value"] is not None]
        if s_vals:
            stem_share = s_vals[0]

        if stem_share is None and wdi_stem_rows:
            # Average across available gender-specific STEM fields
            vals = [r["value"] for r in wdi_stem_rows if r["value"] is not None]
            if vals:
                stem_share = sum(vals) / len(vals)

        if stem_share is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no STEM graduate data",
            }

        stem_share = max(0.0, min(100.0, stem_share))

        # Global median STEM share ~25-30%. Below 15% is low, above 35% is strong.
        if stem_share >= 35:
            score = 10.0
        elif stem_share >= 25:
            score = 22.0
        elif stem_share >= 15:
            score = 45.0
        elif stem_share >= 8:
            score = 65.0
        else:
            score = 80.0

        return {
            "score": round(score, 2),
            "country": country,
            "stem_graduate_share_pct": round(stem_share, 2),
            "global_median_benchmark_pct": 27.0,
            "interpretation": "STEM graduates as % of total tertiary; <15% signals structural gap",
        }
