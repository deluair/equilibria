"""NOAA Climate Data Online collector.

Fetches temperature and precipitation data for major agricultural regions.
Endpoint: https://www.ncdc.noaa.gov/cdo-web/api/v2
Requires NOAA_TOKEN. Rate limited (5 req/sec, 10000 req/day).
Stores in data_series + data_points with source="NOAA".
"""

import asyncio
import json

from app.collectors.base import BaseCollector
from app.config import settings
from app.db import get_db, release_db

NOAA_API = "https://www.ncdc.noaa.gov/cdo-web/api/v2"

# Dataset: Global Summary of the Year (GSOY)
DATASET_ID = "GSOY"

# Data types to fetch
DATATYPES = {
    "TAVG": ("avg_temperature", "celsius_tenths", "Average temperature"),
    "TMAX": ("max_temperature", "celsius_tenths", "Maximum temperature"),
    "TMIN": ("min_temperature", "celsius_tenths", "Minimum temperature"),
    "PRCP": ("precipitation", "mm_tenths", "Total precipitation"),
    "EMXT": ("extreme_max_temp", "celsius_tenths", "Extreme maximum temperature"),
    "EMNT": ("extreme_min_temp", "celsius_tenths", "Extreme minimum temperature"),
}

# Major agricultural regions (FIPS country code -> ISO3)
# NOAA uses FIPS country codes for locationid
LOCATIONS = {
    "FIPS:US": "USA",
    "FIPS:CH": "CHN",
    "FIPS:IN": "IND",
    "FIPS:BR": "BRA",
    "FIPS:RS": "RUS",
    "FIPS:AU": "AUS",
    "FIPS:CA": "CAN",
    "FIPS:AR": "ARG",
    "FIPS:FR": "FRA",
    "FIPS:GM": "DEU",
    "FIPS:ID": "IDN",
    "FIPS:TH": "THA",
    "FIPS:MX": "MEX",
    "FIPS:NI": "NGA",
    "FIPS:PK": "PAK",
    "FIPS:BG": "BGD",
    "FIPS:VM": "VNM",
    "FIPS:SF": "ZAF",
    "FIPS:EG": "EGY",
    "FIPS:TU": "TUR",
}


class NOAACollector(BaseCollector):
    name = "noaa"
    timeout = 60

    def __init__(self):
        super().__init__()
        if not settings.noaa_token:
            raise ValueError("NOAA_TOKEN not set")
        self._token = settings.noaa_token

    async def collect(self) -> list[dict]:
        all_points = []

        for location_id, iso3 in LOCATIONS.items():
            try:
                points = await self._fetch_location(location_id, iso3)
                all_points.extend(points)
                self.logger.info(f"[noaa] fetched {iso3}: {len(points)} points")
            except Exception as e:
                self.logger.warning(f"[noaa] failed {iso3}: {e}")
            await asyncio.sleep(0.3)  # rate limit: 5 req/sec max

        return all_points

    async def _fetch_location(self, location_id: str, iso3: str) -> list[dict]:
        """Fetch climate data for a single country/location."""
        points = []
        headers = {"token": self._token}
        datatype_str = ",".join(DATATYPES.keys())

        # Fetch year by year to stay within NOAA result limits
        for year in range(2000, 2026):
            params = {
                "datasetid": DATASET_ID,
                "locationid": location_id,
                "datatypeid": datatype_str,
                "startdate": f"{year}-01-01",
                "enddate": f"{year}-12-31",
                "limit": 1000,
                "units": "metric",
            }

            try:
                resp = await self._request(
                    "GET", f"{NOAA_API}/data", params=params, headers=headers
                )
                body = resp.json()
            except Exception:
                continue

            results = body.get("results", [])
            for rec in results:
                datatype = rec.get("datatype", "")
                if datatype not in DATATYPES:
                    continue

                value = rec.get("value")
                date_str = rec.get("date", "")
                if value is None or not date_str:
                    continue

                # NOAA dates: "2020-01-01T00:00:00" -> "2020-01-01"
                date_str = date_str[:10]
                canonical, unit, desc = DATATYPES[datatype]
                series_id = f"{DATASET_ID}_{datatype}"

                points.append({
                    "source": "NOAA",
                    "series_id": series_id,
                    "canonical_name": canonical,
                    "country_iso3": iso3,
                    "date": date_str,
                    "value": float(value),
                    "unit": unit,
                    "frequency": "annual",
                })

            await asyncio.sleep(0.3)

        return points

    async def validate(self, data: list[dict]) -> list[dict]:
        valid = []
        valid_iso3 = set(LOCATIONS.values())
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
                desc = DATATYPES.get(
                    info["series_id"].replace(f"{DATASET_ID}_", ""),
                    ("", "", ""),
                )[2]
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
                        json.dumps({"noaa_dataset": DATASET_ID}),
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
