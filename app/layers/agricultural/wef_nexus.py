"""Water-Energy-Food nexus scoring and sustainability assessment.

Computes a composite WEF nexus score measuring resource use efficiency,
cross-sector dependencies, and sustainability of the water-energy-food
system. Each sector is scored on efficiency and stress, then
interdependencies are mapped and penalized for fragility.

Methodology:
    1. Water sector indicators:
       - Water use efficiency (WUE): crop output per m3
       - Agricultural water withdrawal share of total
       - Irrigation efficiency (beneficial use / total withdrawal)
       - Water stress index (withdrawal / renewable resources)

    2. Energy sector indicators:
       - Energy intensity of agriculture (MJ per $ agricultural GDP)
       - Share of energy used for irrigation pumping
       - Renewable energy share in agricultural energy use
       - Energy cost share of farm input costs

    3. Food sector indicators:
       - Caloric self-sufficiency ratio
       - Dietary diversity score
       - Food loss and waste percentage
       - Land use efficiency (calories per hectare)

    4. Cross-sector dependencies:
       - Water-Energy: energy for pumping and treatment
       - Energy-Food: fuel/electricity for mechanization and processing
       - Water-Food: irrigation water requirements
       - Feedback loops and cascading risk multiplier

    5. Composite score:
       WEF = w_water * S_water + w_energy * S_energy + w_food * S_food
             + w_nexus * nexus_penalty

    Score: poor resource efficiency + high cross-sector stress = vulnerability.

References:
    Hoff, H. (2011). "Understanding the Nexus." Stockholm Environment
        Institute.
    Bazilian, M. et al. (2011). "Considering the energy, water and food
        nexus." Energy Policy, 39(12), 7896-7906.
    FAO (2014). "The Water-Energy-Food Nexus: A New Approach in Support
        of Food Security and Sustainable Agriculture."
    Ringler, C., Bhaduri, A. & Lawford, R. (2013). "The nexus across
        water, energy, land and food (WELF)." Current Opinion in
        Environmental Sustainability, 5(6), 589-594.
"""

from __future__ import annotations

import numpy as np

from app.layers.base import LayerBase


class WEFNexus(LayerBase):
    layer_id = "l5"
    name = "WEF Nexus"

    # Sector weights in composite
    WEIGHTS = {"water": 0.30, "energy": 0.25, "food": 0.25, "nexus": 0.20}

    async def compute(self, db, **kwargs) -> dict:
        """Compute WEF nexus sustainability score.

        Parameters
        ----------
        db : async database connection
        **kwargs :
            country_iso3 : str - ISO3 country code (default "BGD")
            year : int - reference year
        """
        country = kwargs.get("country_iso3", "BGD")
        year = kwargs.get("year")

        year_clause = "AND dp.date = ?" if year else ""
        params = [country]
        if year:
            params.append(str(year))

        rows = await db.fetch_all(
            f"""
            SELECT dp.value, ds.description, ds.metadata
            FROM data_points dp
            JOIN data_series ds ON ds.id = dp.series_id
            WHERE ds.source = 'wef_nexus'
              AND ds.country_iso3 = ?
              {year_clause}
            ORDER BY ds.description
            """,
            tuple(params),
        )

        if not rows or len(rows) < 6:
            return {"score": None, "signal": "UNAVAILABLE",
                    "error": "insufficient WEF nexus data"}

        import json

        indicators: dict[str, float] = {}
        for row in rows:
            meta = json.loads(row["metadata"]) if row.get("metadata") else {}
            indicator_name = meta.get("indicator") or row["description"]
            if indicator_name and row["value"] is not None:
                indicators[indicator_name] = float(row["value"])

        # Water sector scoring
        water_indicators = self._score_water(indicators)

        # Energy sector scoring
        energy_indicators = self._score_energy(indicators)

        # Food sector scoring
        food_indicators = self._score_food(indicators)

        # Cross-sector nexus scoring
        nexus_indicators = self._score_nexus(indicators, water_indicators,
                                              energy_indicators, food_indicators)

        # Composite WEF score
        w = self.WEIGHTS
        water_stress = water_indicators.get("stress_score", 50.0)
        energy_stress = energy_indicators.get("stress_score", 50.0)
        food_stress = food_indicators.get("stress_score", 50.0)
        nexus_penalty = nexus_indicators.get("nexus_penalty", 50.0)

        composite = (
            w["water"] * water_stress
            + w["energy"] * energy_stress
            + w["food"] * food_stress
            + w["nexus"] * nexus_penalty
        )
        score = float(np.clip(composite, 0, 100))

        return {
            "score": round(score, 2),
            "country": country,
            "n_indicators": len(indicators),
            "sector_scores": {
                "water_stress": round(water_stress, 2),
                "energy_stress": round(energy_stress, 2),
                "food_stress": round(food_stress, 2),
                "nexus_penalty": round(nexus_penalty, 2),
            },
            "water": water_indicators,
            "energy": energy_indicators,
            "food": food_indicators,
            "nexus": nexus_indicators,
            "weights": self.WEIGHTS,
        }

    @staticmethod
    def _score_water(ind: dict[str, float]) -> dict:
        """Score water sector indicators (0 = good, 100 = crisis)."""
        results = {}

        # Water stress index: withdrawal / renewable (Falkenmark)
        water_stress = ind.get("water_stress_index", ind.get("water_withdrawal_pct", 50))
        results["water_stress_index"] = round(float(water_stress), 3)

        # Agricultural water share of total withdrawal
        ag_water_share = ind.get("ag_water_withdrawal_share", 0.7)
        results["ag_water_share"] = round(float(ag_water_share), 3)

        # Irrigation efficiency
        irrig_eff = ind.get("irrigation_efficiency", 0.5)
        results["irrigation_efficiency"] = round(float(irrig_eff), 3)

        # Crop water productivity (kg per m3)
        cwp = ind.get("crop_water_productivity_kg_m3", 0.5)
        results["crop_water_productivity"] = round(float(cwp), 3)

        # Water stress score
        # High withdrawal ratio, low efficiency = stress
        stress_from_withdrawal = float(np.clip(water_stress * 100, 0, 50))
        stress_from_efficiency = float(np.clip((1 - irrig_eff) * 50, 0, 50))
        results["stress_score"] = round(
            float(np.clip(stress_from_withdrawal + stress_from_efficiency, 0, 100)), 2
        )

        return results

    @staticmethod
    def _score_energy(ind: dict[str, float]) -> dict:
        """Score energy sector indicators (0 = good, 100 = crisis)."""
        results = {}

        # Energy intensity of agriculture (MJ per $ GDP)
        energy_intensity = ind.get("ag_energy_intensity_mj_per_usd", 5.0)
        results["energy_intensity_mj_per_usd"] = round(float(energy_intensity), 2)

        # Pumping energy share
        pump_share = ind.get("pumping_energy_share", 0.3)
        results["pumping_energy_share"] = round(float(pump_share), 3)

        # Renewable energy share in agriculture
        re_share = ind.get("renewable_energy_share_ag", 0.1)
        results["renewable_energy_share"] = round(float(re_share), 3)

        # Energy cost share of farm costs
        energy_cost_share = ind.get("energy_cost_share_farm", 0.15)
        results["energy_cost_share"] = round(float(energy_cost_share), 3)

        # Energy stress score
        # High intensity, low RE share, high cost share = stress
        intensity_stress = float(np.clip(energy_intensity / 20 * 40, 0, 40))
        re_stress = float(np.clip((1 - re_share) * 30, 0, 30))
        cost_stress = float(np.clip(energy_cost_share * 100, 0, 30))
        results["stress_score"] = round(
            float(np.clip(intensity_stress + re_stress + cost_stress, 0, 100)), 2
        )

        return results

    @staticmethod
    def _score_food(ind: dict[str, float]) -> dict:
        """Score food sector indicators (0 = good, 100 = crisis)."""
        results = {}

        # Caloric self-sufficiency
        caloric_ssr = ind.get("caloric_self_sufficiency", 0.9)
        results["caloric_self_sufficiency"] = round(float(caloric_ssr), 3)

        # Dietary diversity (0-12 food group score)
        diet_diversity = ind.get("dietary_diversity_score", 6)
        results["dietary_diversity_score"] = round(float(diet_diversity), 1)

        # Food loss and waste (% of production)
        food_loss_pct = ind.get("food_loss_waste_pct", 15)
        results["food_loss_waste_pct"] = round(float(food_loss_pct), 1)

        # Land use efficiency (kcal per hectare)
        land_eff = ind.get("land_use_efficiency_kcal_ha", 5e6)
        results["land_use_efficiency_kcal_ha"] = round(float(land_eff), 0)

        # Food stress score
        ssr_stress = float(np.clip((1 - caloric_ssr) * 100, 0, 40))
        diversity_stress = float(np.clip((12 - diet_diversity) / 12 * 30, 0, 30))
        loss_stress = float(np.clip(food_loss_pct / 30 * 30, 0, 30))
        results["stress_score"] = round(
            float(np.clip(ssr_stress + diversity_stress + loss_stress, 0, 100)), 2
        )

        return results

    @staticmethod
    def _score_nexus(
        ind: dict[str, float],
        water: dict, energy: dict, food: dict
    ) -> dict:
        """Score cross-sector nexus dependencies and fragility.

        Nexus penalty increases when:
        - Multiple sectors are stressed simultaneously (compounding risk)
        - Cross-sector dependencies are high (cascading failure)
        """
        results = {}

        # Cross-sector dependency coefficients
        # Water-Energy coupling: energy needed per m3 of water delivered
        we_coupling = ind.get("energy_per_m3_water_kwh", 0.5)
        results["water_energy_coupling_kwh_m3"] = round(float(we_coupling), 3)

        # Energy-Food coupling: energy input per calorie output
        ef_coupling = ind.get("energy_input_per_food_calorie", 3.0)
        results["energy_food_coupling"] = round(float(ef_coupling), 2)

        # Water-Food coupling: virtual water per calorie
        wf_coupling = ind.get("virtual_water_per_kcal_liters", 1.0)
        results["water_food_coupling_l_kcal"] = round(float(wf_coupling), 3)

        # Compounding risk: geometric mean of sector stress scores
        stresses = [
            water.get("stress_score", 50),
            energy.get("stress_score", 50),
            food.get("stress_score", 50),
        ]
        # If all sectors stressed, penalty amplifies
        mean_stress = float(np.mean(stresses))
        max_stress = float(np.max(stresses))
        float(np.min(stresses))

        # Cascading risk: higher when coupling is high AND sectors are stressed
        coupling_factor = float(np.mean([we_coupling, ef_coupling / 10, wf_coupling]))
        cascade_penalty = float(np.clip(coupling_factor * mean_stress / 50 * 30, 0, 30))

        # Synchronization penalty: all sectors stressed together is worse
        stress_cv = float(np.std(stresses) / mean_stress) if mean_stress > 0 else 0
        sync_penalty = float(np.clip((1 - stress_cv) * max_stress / 100 * 30, 0, 30))

        # Base penalty from average stress
        base_penalty = float(np.clip(mean_stress * 0.4, 0, 40))

        nexus_penalty = float(np.clip(base_penalty + cascade_penalty + sync_penalty, 0, 100))

        results["cascade_penalty"] = round(cascade_penalty, 2)
        results["synchronization_penalty"] = round(sync_penalty, 2)
        results["base_penalty"] = round(base_penalty, 2)
        results["nexus_penalty"] = round(nexus_penalty, 2)
        results["sector_stress_mean"] = round(mean_stress, 2)
        results["sector_stress_max"] = round(max_stress, 2)

        return results
