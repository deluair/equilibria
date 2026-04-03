"""Smart cities: digital infrastructure, IoT adoption, data-driven governance,
and smart mobility indicators.

Key frameworks:

1. Digital infrastructure index (ITU-D, 2022):
   Composite of broadband penetration, 5G coverage, data center density,
   cloud adoption rate, and cybersecurity index.
   Normalized 0-100 using z-score standardization across cities/countries.

2. IoT adoption (Ericsson Mobility Report methodology):
   IoT density = connected_devices / population (devices per 100 persons)
   Industrial IoT share = industrial_iot_connections / total_iot_connections
   Sectors: smart grid, smart water, smart transport, smart buildings.

3. Data-driven governance (Open Data Barometer 2023):
   Open data score: availability, accessibility, machine-readability of
   government datasets. Linked to service efficiency and anti-corruption.
   AI/ML use in public services: % of agencies using predictive analytics.

4. Smart mobility indicators (UITP, Deloitte 2022):
   Modal share shift: walking/cycling/transit vs single-occupancy vehicle.
   MaaS (Mobility-as-a-Service) adoption: shared ride usage per 1000 residents.
   Smart parking and congestion pricing implementation.
   EV charging infrastructure density.

References:
    ITU (2022). Global ICT Development Index. International Telecommunication Union.
    Ericsson (2023). Ericsson Mobility Report. IoT Connections Outlook.
    Open Data Barometer (2023). Global Report. World Wide Web Foundation.
    UITP (2022). Smart Mobility: State of Play. International Association of PT.
    Deloitte (2022). Future of Mobility: Smart City Transport Benchmark.

Sources: ITU, World Bank WDI, OECD Digital Economy Outlook, UN E-Government Survey.
"""

from __future__ import annotations

import json

import numpy as np
from scipy import stats as sp_stats

from app.layers.base import LayerBase


class SmartCities(LayerBase):
    layer_id = "l11"
    name = "Smart Cities"

    async def compute(self, db, **kwargs) -> dict:
        country = kwargs.get("country_iso3", "USA")
        results: dict = {"country": country}

        # --- Digital infrastructure index ---
        digital_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('DIGITAL_INFRA_INDEX', 'ITU_DEVELOPMENT_INDEX',
                                   'IT.NET.BBND.P2')
            ORDER BY dp.date
            """,
            (country,),
        )

        digital_result = None
        if digital_rows:
            latest = digital_rows[-1]
            dii = float(latest["value"])
            meta = json.loads(latest["metadata"]) if latest.get("metadata") else {}

            broadband_per100 = meta.get("broadband_per100", None)
            fiveg_coverage_pct = meta.get("fiveg_coverage_pct", None)
            cloud_adoption = meta.get("cloud_adoption_pct", None)
            cybersecurity_index = meta.get("cybersecurity_index", None)

            if len(digital_rows) >= 3:
                dii_arr = np.array([float(r["value"]) for r in digital_rows[-10:]])
                t_arr = np.arange(len(dii_arr))
                slope_dii, _, _, _, _ = sp_stats.linregress(t_arr, dii_arr)
            else:
                slope_dii = None

            digital_result = {
                "digital_infrastructure_index": round(dii, 2),
                "broadband_per_100": round(float(broadband_per100), 2) if broadband_per100 is not None else None,
                "fiveg_coverage_pct": round(float(fiveg_coverage_pct), 2) if fiveg_coverage_pct is not None else None,
                "cloud_adoption_pct": round(float(cloud_adoption), 2) if cloud_adoption is not None else None,
                "cybersecurity_index": round(float(cybersecurity_index), 2) if cybersecurity_index is not None else None,
                "trend_slope": round(float(slope_dii), 4) if slope_dii is not None else None,
                "tier": (
                    "advanced" if dii > 75
                    else "developing" if dii > 50
                    else "nascent"
                ),
            }
        results["digital_infrastructure"] = digital_result or {"error": "no digital infrastructure data"}

        # --- IoT adoption ---
        iot_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('IOT_DENSITY', 'CONNECTED_DEVICES_PER100')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        iot_result = None
        if iot_rows:
            meta = json.loads(iot_rows[0]["metadata"]) if iot_rows[0].get("metadata") else {}
            iot_density = float(iot_rows[0]["value"])
            industrial_iot_share = meta.get("industrial_iot_share", None)
            smart_grid_coverage = meta.get("smart_grid_coverage", None)
            smart_building_pct = meta.get("smart_building_pct", None)

            iot_result = {
                "iot_devices_per_100": round(iot_density, 2),
                "industrial_iot_share": round(float(industrial_iot_share), 4) if industrial_iot_share is not None else None,
                "smart_grid_coverage": round(float(smart_grid_coverage), 4) if smart_grid_coverage is not None else None,
                "smart_building_pct": round(float(smart_building_pct), 2) if smart_building_pct is not None else None,
                "adoption_level": (
                    "high" if iot_density > 50
                    else "medium" if iot_density > 15
                    else "low"
                ),
            }
        results["iot_adoption"] = iot_result or {"error": "no IoT data"}

        # --- Data-driven governance ---
        gov_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('OPEN_DATA_SCORE', 'UN_EGOVERNMENT_INDEX',
                                   'EGOVERNMENT_DEVELOPMENT_INDEX')
            ORDER BY dp.date
            """,
            (country,),
        )

        governance_result = None
        if gov_rows:
            latest_gov = gov_rows[-1]
            egov_score = float(latest_gov["value"])
            meta = json.loads(latest_gov["metadata"]) if latest_gov.get("metadata") else {}

            open_data_score = meta.get("open_data_score", None)
            ai_gov_adoption = meta.get("ai_in_gov_pct_agencies", None)
            online_services = meta.get("online_services_index", None)

            if len(gov_rows) >= 3:
                gov_arr = np.array([float(r["value"]) for r in gov_rows[-10:]])
                t_arr = np.arange(len(gov_arr))
                slope_gov, _, r_gov, p_gov, _ = sp_stats.linregress(t_arr, gov_arr)
            else:
                slope_gov, r_gov, p_gov = None, None, None

            governance_result = {
                "egovernment_index": round(egov_score, 4),
                "open_data_score": round(float(open_data_score), 2) if open_data_score is not None else None,
                "ai_in_gov_pct_agencies": round(float(ai_gov_adoption), 2) if ai_gov_adoption is not None else None,
                "online_services_index": round(float(online_services), 4) if online_services is not None else None,
                "trend_slope": round(float(slope_gov), 5) if slope_gov is not None else None,
                "r_squared": round(float(r_gov ** 2), 4) if r_gov is not None else None,
                "tier": (
                    "leader" if egov_score > 0.75
                    else "strong" if egov_score > 0.55
                    else "developing" if egov_score > 0.35
                    else "lagging"
                ),
            }
        results["data_driven_governance"] = governance_result or {"error": "no e-government data"}

        # --- Smart mobility ---
        mobility_rows = await db.fetch_all(
            """
            SELECT dp.date, dp.value, ds.metadata
            FROM data_series ds
            JOIN data_points dp ON dp.series_id = ds.id
            WHERE ds.country_iso3 = ?
              AND ds.series_id IN ('SMART_MOBILITY_INDEX', 'TRANSIT_MODAL_SHARE',
                                   'EV_CHARGING_PER_100K')
            ORDER BY dp.date DESC
            LIMIT 1
            """,
            (country,),
        )

        mobility_result = None
        if mobility_rows:
            meta = json.loads(mobility_rows[0]["metadata"]) if mobility_rows[0].get("metadata") else {}
            mobility_idx = float(mobility_rows[0]["value"])
            transit_modal_share = meta.get("transit_modal_share", None)
            ev_chargers_per_100k = meta.get("ev_chargers_per_100k", None)
            maas_users_per_1000 = meta.get("maas_users_per_1000", None)
            congestion_pricing = meta.get("congestion_pricing_active", None)

            mobility_result = {
                "smart_mobility_index": round(mobility_idx, 2),
                "transit_modal_share_pct": round(float(transit_modal_share), 2) if transit_modal_share is not None else None,
                "ev_chargers_per_100k": round(float(ev_chargers_per_100k), 2) if ev_chargers_per_100k is not None else None,
                "maas_users_per_1000": round(float(maas_users_per_1000), 2) if maas_users_per_1000 is not None else None,
                "congestion_pricing_active": bool(congestion_pricing) if congestion_pricing is not None else None,
                "rating": (
                    "advanced" if mobility_idx > 70
                    else "transitioning" if mobility_idx > 45
                    else "car_dependent"
                ),
            }
        results["smart_mobility"] = mobility_result or {"error": "no smart mobility data"}

        # --- Score (low score = advanced smart city = stable) ---
        score = 40.0

        if digital_result and not digital_result.get("error"):
            dii_v = digital_result["digital_infrastructure_index"]
            if dii_v > 75:
                score -= 15.0
            elif dii_v > 50:
                score -= 7.0
            elif dii_v < 30:
                score += 15.0

        if governance_result and not governance_result.get("error"):
            tier = governance_result["tier"]
            if tier == "leader":
                score -= 10.0
            elif tier == "lagging":
                score += 15.0
            elif tier == "developing":
                score += 7.0

        if iot_result and not iot_result.get("error"):
            if iot_result["adoption_level"] == "low":
                score += 10.0
            elif iot_result["adoption_level"] == "high":
                score -= 5.0

        if mobility_result and not mobility_result.get("error"):
            if mobility_result["rating"] == "car_dependent":
                score += 10.0
            elif mobility_result["rating"] == "advanced":
                score -= 5.0

        score = float(np.clip(score, 0.0, 100.0))
        return {"score": round(score, 1), "results": results}
