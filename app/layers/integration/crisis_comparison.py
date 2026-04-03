"""Crisis Comparison Module.

Compares current economic conditions to historical crisis episodes:
Asian Financial Crisis (1997), Global Financial Crisis (2008),
European Debt Crisis (2012), COVID-19 (2020).

Uses Mahalanobis distance for multi-dimensional similarity and identifies
which historical crisis the current conditions most resemble.
"""

import json
import logging
from datetime import datetime, timezone

import numpy as np
from scipy.spatial.distance import mahalanobis

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

LAYER_IDS = ["l1", "l2", "l3", "l4", "l5"]

# Historical crisis profiles: stylized layer score signatures (0-100)
# Higher score = more stress in that dimension.
# These represent characteristic stress patterns during each crisis.
CRISIS_PROFILES = {
    "asian_1997": {
        "name": "Asian Financial Crisis (1997-98)",
        "year": 1997,
        "profile": {
            "l1": 85.0,  # Severe trade collapse, capital flight
            "l2": 78.0,  # Currency crisis, GDP contraction
            "l3": 60.0,  # Rising unemployment, especially informal
            "l4": 55.0,  # Institutional weaknesses exposed
            "l5": 40.0,  # Agricultural sector less affected initially
        },
        "description": "Currency crisis, capital account reversal, trade collapse. "
                       "Concentrated in trade and macro layers.",
        "signature": "trade_macro_dominant",
    },
    "gfc_2008": {
        "name": "Global Financial Crisis (2008-09)",
        "year": 2008,
        "profile": {
            "l1": 72.0,  # Trade volumes collapsed ~12%
            "l2": 85.0,  # Financial system near-collapse, deep recession
            "l3": 75.0,  # Severe unemployment spike
            "l4": 50.0,  # Development setback, poverty increase
            "l5": 45.0,  # Food prices volatile but contained
        },
        "description": "Financial system crisis spreading to real economy. "
                       "Macro and labor most affected.",
        "signature": "macro_labor_dominant",
    },
    "euro_2012": {
        "name": "European Debt Crisis (2010-12)",
        "year": 2012,
        "profile": {
            "l1": 55.0,  # Intra-EU trade disruption
            "l2": 80.0,  # Sovereign debt, austerity, double-dip
            "l3": 82.0,  # Extreme unemployment in periphery
            "l4": 60.0,  # Institutional strain, governance questions
            "l5": 35.0,  # Agricultural sector relatively insulated
        },
        "description": "Sovereign debt crisis with severe labor market impact. "
                       "Labor and macro dominant, concentrated in Europe.",
        "signature": "labor_macro_dominant",
    },
    "covid_2020": {
        "name": "COVID-19 Pandemic (2020)",
        "year": 2020,
        "profile": {
            "l1": 78.0,  # Supply chain disruption, trade collapse
            "l2": 70.0,  # GDP shock but fast monetary response
            "l3": 80.0,  # Massive job losses, uneven recovery
            "l4": 65.0,  # Development gains reversed, inequality up
            "l5": 60.0,  # Food supply chain stress, price spikes
        },
        "description": "Exogenous supply shock affecting all layers simultaneously. "
                       "Broad-based stress, high across all dimensions.",
        "signature": "broad_based",
    },
}


class CrisisComparison(LayerBase):
    layer_id = "l6"
    name = "Crisis Comparison"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        custom_profiles = kwargs.get("custom_profiles")

        profiles = CRISIS_PROFILES.copy()
        if custom_profiles:
            profiles.update(custom_profiles)

        # Fetch current layer scores
        current_scores = await self._fetch_current_scores(db, country_iso3)

        if len(current_scores) < 3:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "distances": {},
                "most_similar": None,
                "country_iso3": country_iso3,
                "reason": f"Need at least 3 layers, got {len(current_scores)}",
            }

        # Compute distances to each crisis profile
        distances = self._compute_crisis_distances(current_scores, profiles)

        # Identify most similar crisis
        most_similar = min(distances.items(), key=lambda x: x[1]["distance"])

        # Compute similarity scores (inverse distance, normalized)
        max_dist = max(d["distance"] for d in distances.values()) if distances else 1.0
        for crisis_id, data in distances.items():
            data["similarity"] = round(
                1.0 - (data["distance"] / max(max_dist, 1e-10)), 4
            )

        # Current stress level (average of available scores)
        avg_score = float(np.mean(list(current_scores.values())))

        # Compute composite distance as a "crisis proximity" score
        min_dist = most_similar[1]["distance"]
        # Normalize: closer to a crisis = higher score
        # max theoretical distance is sqrt(5 * 100^2) ~ 223
        crisis_proximity = max(0.0, min(100.0, (1.0 - min_dist / 223.0) * avg_score))

        # Layer-by-layer comparison with most similar crisis
        layer_comparison = self._layer_comparison(
            current_scores, profiles[most_similar[0]]
        )

        # Compute historical trajectory similarity if we have time series
        trajectory_sim = await self._trajectory_similarity(
            db, country_iso3, profiles
        )

        await self._store_result(
            db, country_iso3, crisis_proximity, most_similar[0], distances
        )

        return {
            "score": round(crisis_proximity, 2),
            "signal": self.classify_signal(crisis_proximity),
            "most_similar_crisis": {
                "id": most_similar[0],
                "name": most_similar[1]["name"],
                "distance": round(most_similar[1]["distance"], 4),
                "similarity": most_similar[1]["similarity"],
                "signature": most_similar[1]["signature"],
            },
            "distances": {
                cid: {
                    "name": d["name"],
                    "distance": round(d["distance"], 4),
                    "similarity": d["similarity"],
                    "euclidean": round(d["euclidean"], 4),
                }
                for cid, d in sorted(
                    distances.items(), key=lambda x: x[1]["distance"]
                )
            },
            "layer_comparison": layer_comparison,
            "trajectory_similarity": trajectory_sim,
            "current_scores": {k: round(v, 2) for k, v in current_scores.items()},
            "avg_stress": round(avg_score, 2),
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def _fetch_current_scores(
        self, db, country_iso3: str
    ) -> dict[str, float]:
        scores = {}
        for lid in LAYER_IDS:
            row = await db.fetch_one(
                """
                SELECT score FROM analysis_results
                WHERE layer = ? AND country_iso3 = ? AND score IS NOT NULL
                ORDER BY created_at DESC LIMIT 1
                """,
                (lid, country_iso3),
            )
            if row and row["score"] is not None:
                scores[lid] = float(row["score"])
        return scores

    def _compute_crisis_distances(
        self, current: dict[str, float], profiles: dict
    ) -> dict:
        """Compute Euclidean and Mahalanobis distances to each crisis profile."""
        available = list(current.keys())
        current_vec = np.array([current[lid] for lid in available])

        # Build covariance matrix from crisis profiles for Mahalanobis
        profile_vecs = []
        for cid, cdata in profiles.items():
            prof = cdata["profile"]
            vec = np.array([prof.get(lid, 50.0) for lid in available])
            profile_vecs.append(vec)

        if len(profile_vecs) > 1:
            profile_matrix = np.array(profile_vecs)
            cov = np.cov(profile_matrix.T)
            # Regularize covariance
            cov += np.eye(len(available)) * 1.0
            try:
                cov_inv = np.linalg.inv(cov)
            except np.linalg.LinAlgError:
                cov_inv = np.eye(len(available))
        else:
            cov_inv = np.eye(len(available))

        distances = {}
        for cid, cdata in profiles.items():
            prof = cdata["profile"]
            crisis_vec = np.array([prof.get(lid, 50.0) for lid in available])

            # Euclidean distance
            euclid = float(np.linalg.norm(current_vec - crisis_vec))

            # Mahalanobis distance
            try:
                maha = float(mahalanobis(current_vec, crisis_vec, cov_inv))
            except (ValueError, np.linalg.LinAlgError):
                maha = euclid

            distances[cid] = {
                "name": cdata["name"],
                "distance": maha,
                "euclidean": euclid,
                "year": cdata["year"],
                "signature": cdata["signature"],
                "description": cdata["description"],
            }

        return distances

    def _layer_comparison(
        self, current: dict[str, float], crisis_profile: dict
    ) -> list[dict]:
        """Layer-by-layer comparison with the most similar crisis."""
        profile_scores = crisis_profile["profile"]
        comparison = []

        for lid in LAYER_IDS:
            curr = current.get(lid)
            prof = profile_scores.get(lid)
            if curr is None or prof is None:
                continue

            diff = curr - prof
            comparison.append({
                "layer": lid,
                "current": round(curr, 2),
                "crisis_reference": round(prof, 2),
                "difference": round(diff, 2),
                "direction": "worse" if diff > 0 else "better",
                "severity_ratio": round(curr / max(prof, 1.0), 2),
            })

        return comparison

    async def _trajectory_similarity(
        self, db, country_iso3: str, profiles: dict
    ) -> dict | None:
        """Compare the trajectory (direction of change) with crisis patterns.

        If we have enough history, check whether the current path looks like
        the early stages of a known crisis.
        """
        # Fetch recent composite score trend
        rows = await db.fetch_all(
            """
            SELECT score FROM analysis_results
            WHERE analysis_type = 'composite_score' AND country_iso3 = ?
              AND score IS NOT NULL
            ORDER BY created_at DESC LIMIT 12
            """,
            (country_iso3,),
        )

        if len(rows) < 4:
            return None

        scores = [r["score"] for r in reversed(rows)]
        recent_change = scores[-1] - scores[0]
        acceleration = (scores[-1] - scores[-4]) - (scores[-4] - scores[0]) if len(scores) >= 4 else 0.0

        # Crisis pre-period signatures:
        # Rapid deterioration + acceleration = crisis onset
        trajectory = {
            "recent_change": round(recent_change, 2),
            "acceleration": round(acceleration, 2),
            "periods": len(scores),
        }

        if acceleration > 5.0 and recent_change > 10.0:
            trajectory["pattern"] = "accelerating_deterioration"
            trajectory["warning"] = "Trajectory resembles pre-crisis acceleration"
        elif recent_change > 15.0:
            trajectory["pattern"] = "rapid_deterioration"
            trajectory["warning"] = "Fast deterioration, monitor closely"
        elif recent_change < -10.0:
            trajectory["pattern"] = "improving"
            trajectory["warning"] = None
        else:
            trajectory["pattern"] = "stable"
            trajectory["warning"] = None

        return trajectory

    async def _store_result(
        self, db, country_iso3: str, score: float,
        most_similar: str, distances: dict,
    ):
        await db.execute(
            """
            INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "crisis_comparison",
                country_iso3,
                "l6",
                json.dumps({"method": "mahalanobis_distance"}),
                json.dumps({
                    "most_similar": most_similar,
                    "distances": {
                        cid: round(d["distance"], 4) for cid, d in distances.items()
                    },
                }),
                round(score, 2),
                self.classify_signal(score),
            ),
        )
