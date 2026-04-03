"""PISA-equivalent learning outcomes index.

Constructs a normalized education quality index from available standardized
test score data (PISA, TIMSS, PIRLS, EGRA). Where multiple scores exist,
takes the unweighted mean after normalizing each to a 0-100 scale anchored
to PISA's 300-600 observed range.

PISA domain subscores (reading, math, science) are each normalized:
    normalized = (score - 300) / 300 * 100

References:
    OECD (2023). PISA 2022 Results. OECD Publishing.
    Hanushek, E.A. & Woessmann, L. (2015). The Knowledge Capital of Nations.
        MIT Press.
    Filmer, D. et al. (2020). Learning-Adjusted Years of Schooling (LAYS):
        defining a new macro measure of education. World Bank Policy Research WP 8591.

Score: low normalized score -> STRESS (poor learning outcomes despite enrollment).
"""

from __future__ import annotations

from app.layers.base import LayerBase


def _normalize_pisa(raw: float) -> float:
    """Map PISA score (300-600 range) to 0-100."""
    return max(0.0, min(100.0, (raw - 300.0) / 300.0 * 100.0))


class EducationQualityScore(LayerBase):
    layer_id = "lED"
    name = "Education Quality Score"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "BGD")

        # Direct quality index if stored (e.g. LAYS or harmonized score)
        quality_rows = await db.fetch_all(
            """
            SELECT dp.value, dp.date, ds.series_id
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.country_iso3 = ?
              AND ds.source IN ('pisa', 'timss', 'education_quality', 'lays')
            ORDER BY dp.date DESC
            LIMIT 10
            """,
            (country,),
        )

        if not quality_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "no PISA/TIMSS/LAYS data",
            }

        scores_normalized = []
        raw_scores = []
        for row in quality_rows:
            v = row["value"]
            if v is None:
                continue
            sid = row["series_id"] or ""
            # PISA scores are typically 300-600
            if "pisa" in sid.lower() or v > 100:
                normalized = _normalize_pisa(v)
            else:
                # Already normalized 0-100
                normalized = max(0.0, min(100.0, v))
            scores_normalized.append(normalized)
            raw_scores.append(v)

        if not scores_normalized:
            return {"score": None, "signal": "UNAVAILABLE", "error": "no valid quality scores"}

        quality_index = sum(scores_normalized) / len(scores_normalized)

        # Stress score: low quality = high stress
        # quality_index = 0 -> score 100 (crisis), quality_index = 100 -> score 0 (stable)
        stress_score = round(100.0 - quality_index, 2)

        return {
            "score": stress_score,
            "country": country,
            "quality_index_0_100": round(quality_index, 2),
            "n_assessments": len(scores_normalized),
            "raw_values": [round(v, 1) for v in raw_scores[:5]],
            "interpretation": "normalized learning outcomes; higher quality_index = lower stress",
        }
