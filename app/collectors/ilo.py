"""ILO (International Labour Organization) collector.

Fetches labor market indicators from ILO SDMX-JSON 2.0 API.
Endpoint: https://sdmx.ilo.org/rest/data
Series: unemployment rate, labor force participation, employment by sector, wages.
30 major countries. Stores in data_series + data_points with source="ILO".
"""

import asyncio
import json

from app.collectors.base import BaseCollector
from app.db import get_db, release_db

# ILO SDMX-JSON 2.0 base URL
ILO_API = "https://sdmx.ilo.org/rest/data"

# ILO dataflow -> (canonical_name, unit, description, sex/classif filter)
# Format: dataflow/key?format=jsondata
ILO_INDICATORS = {
    "UNE_DEAP_SEX_AGE_RT": {
        "canonical": "unemployment_rate",
        "unit": "percent",
        "description": "Unemployment rate by sex and age, ILO modelled estimates",
        "key": "..SEX_T.AGE_YTHADULT_YGE15",  # total, 15+
    },
    "EAP_DWAP_SEX_AGE_RT": {
        "canonical": "labor_force_participation",
        "unit": "percent",
        "description": "Labour force participation rate by sex and age",
        "key": "..SEX_T.AGE_YTHADULT_YGE15",
    },
    "EMP_TEMP_SEX_ECO_NB": {
        "canonical": "employment_agriculture",
        "unit": "thousands",
        "description": "Employment in agriculture (ISIC Rev.4 A)",
        "key": "..SEX_T.ECO_ISIC4_A",
    },
    "EAR_4MTH_SEX_ECO_CUR_NB": {
        "canonical": "mean_monthly_earnings",
        "unit": "local_currency",
        "description": "Mean nominal monthly earnings of employees",
        "key": "..SEX_T.ECO_ISIC4_TOTAL.CUR_TYPE_LCU",
    },
}

# 30 major countries (ISO3)
COUNTRIES = [
    "USA", "CHN", "JPN", "DEU", "IND", "GBR", "FRA", "ITA", "CAN", "BRA",
    "RUS", "KOR", "AUS", "MEX", "ESP", "IDN", "NLD", "SAU", "TUR", "CHE",
    "POL", "SWE", "ARG", "THA", "ZAF", "BGD", "VNM", "PHL", "MYS", "NGA",
]


class ILOCollector(BaseCollector):
    name = "ilo"
    timeout = 60  # ILO API can be slow

    async def collect(self) -> list[dict]:
        all_points = []
        for dataflow, meta in ILO_INDICATORS.items():
            try:
                points = await self._fetch_dataflow(dataflow, meta)
                all_points.extend(points)
                self.logger.info(
                    f"[ilo] fetched {meta['canonical']} ({dataflow}): {len(points)} points"
                )
            except Exception as e:
                self.logger.warning(f"[ilo] failed {meta['canonical']}: {e}")
            await asyncio.sleep(1.0)  # rate limit
        return all_points

    async def _fetch_dataflow(self, dataflow: str, meta: dict) -> list[dict]:
        """Fetch a single ILO dataflow for all countries."""
        points = []
        country_str = "+".join(COUNTRIES)
        key = meta["key"].replace("..", f".{country_str}.")
        url = f"{ILO_API}/{dataflow}/{key}"
        params = {
            "format": "jsondata",
            "startPeriod": "2000",
            "endPeriod": "2025",
        }

        resp = await self._request("GET", url, params=params)
        body = resp.json()

        # Parse SDMX-JSON 2.0 structure
        datasets = body.get("dataSets", [])
        if not datasets:
            return points

        structure = body.get("structure", {})
        dimensions = structure.get("dimensions", {})
        obs_dims = dimensions.get("observation", [])
        series_dims = dimensions.get("series", [])

        # Build dimension value lookups
        time_values = []
        for dim in obs_dims:
            if dim.get("id") == "TIME_PERIOD":
                time_values = [v.get("id", "") for v in dim.get("values", [])]

        country_dim_idx = None
        country_values = []
        for i, dim in enumerate(series_dims):
            if dim.get("id") == "REF_AREA":
                country_dim_idx = i
                country_values = [v.get("id", "") for v in dim.get("values", [])]

        dataset = datasets[0]
        series_data = dataset.get("series", {})

        for series_key, series_obj in series_data.items():
            obs = series_obj.get("observations", {})
            # Parse series key to get country
            key_parts = series_key.split(":")
            iso3 = ""
            if country_dim_idx is not None and country_dim_idx < len(key_parts):
                idx = int(key_parts[country_dim_idx])
                if idx < len(country_values):
                    iso3 = country_values[idx]

            if not iso3 or iso3 not in COUNTRIES:
                continue

            for time_idx_str, obs_values in obs.items():
                time_idx = int(time_idx_str)
                if time_idx >= len(time_values):
                    continue
                period = time_values[time_idx]
                value = obs_values[0] if obs_values else None
                if value is None:
                    continue

                # Normalize period to date
                date_str = self._period_to_date(period)
                if not date_str:
                    continue

                points.append({
                    "source": "ILO",
                    "series_id": dataflow,
                    "canonical_name": meta["canonical"],
                    "country_iso3": iso3,
                    "date": date_str,
                    "value": float(value),
                    "unit": meta["unit"],
                    "frequency": "annual",
                })

        return points

    @staticmethod
    def _period_to_date(period: str) -> str:
        """Convert ILO time period to date string."""
        if not period:
            return ""
        # Annual: "2020" -> "2020-01-01"
        if len(period) == 4 and period.isdigit():
            return f"{period}-01-01"
        # Quarterly: "2020-Q1" -> "2020-01-01"
        if "Q" in period:
            parts = period.split("-Q")
            if len(parts) == 2:
                year = parts[0]
                q = int(parts[1])
                month = {1: "01", 2: "04", 3: "07", 4: "10"}.get(q, "01")
                return f"{year}-{month}-01"
        # Monthly: "2020-01" -> "2020-01-01"
        if len(period) == 7 and "-" in period:
            return f"{period}-01"
        return ""

    async def validate(self, data: list[dict]) -> list[dict]:
        valid = []
        valid_iso3 = set(COUNTRIES)
        for row in data:
            try:
                v = float(row["value"])
                if v != v:
                    continue
                if row["country_iso3"] not in valid_iso3:
                    continue
                valid.append(row)
            except (ValueError, TypeError):
                continue
        return valid

    async def store(self, data: list[dict]) -> int:
        if not data:
            return 0

        db = await get_db()
        try:
            series_map = {}
            for row in data:
                key = (row["source"], row["series_id"], row["country_iso3"])
                if key not in series_map:
                    series_map[key] = {
                        "source": row["source"],
                        "series_id": row["series_id"],
                        "canonical_name": row["canonical_name"],
                        "country_iso3": row["country_iso3"],
                        "unit": row["unit"],
                        "frequency": row["frequency"],
                        "points": [],
                    }
                series_map[key]["points"].append((row["date"], row["value"]))

            stored = 0
            for key, info in series_map.items():
                desc = ILO_INDICATORS.get(info["series_id"], {}).get("description", "")
                await db.execute(
                    """INSERT INTO data_series
                    (source, series_id, country_iso3, name, description,
                     unit, frequency, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source, series_id, country_iso3) DO UPDATE SET
                        description = excluded.description,
                        unit = excluded.unit,
                        frequency = excluded.frequency""",
                    (
                        info["source"], info["series_id"], info["country_iso3"],
                        info["canonical_name"], desc, info["unit"], info["frequency"],
                        json.dumps({"ilo_dataflow": info["series_id"]}),
                    ),
                )

                row = await db.fetch_one(
                    """SELECT id FROM data_series
                    WHERE source = ? AND series_id = ? AND country_iso3 = ?""",
                    (info["source"], info["series_id"], info["country_iso3"]),
                )
                if row is None:
                    continue
                sid = row["id"]

                for date_str, value in info["points"]:
                    await db.execute(
                        """INSERT INTO data_points (series_id, date, value)
                        VALUES (?, ?, ?)
                        ON CONFLICT(series_id, date) DO UPDATE SET value = excluded.value""",
                        (sid, date_str, value),
                    )
                    stored += 1

            return stored
        finally:
            await release_db(db)
