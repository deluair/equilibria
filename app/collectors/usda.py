"""USDA Economic Research Service collector.

Fetches agricultural trade data and commodity prices from USDA PSD Online API.
Endpoint: https://apps.fas.usda.gov/PSDOnlineDataServices/api
Stores in data_series + data_points with source="USDA".
"""

import asyncio
import json

from app.collectors.base import BaseCollector
from app.db import get_db, release_db

PSD_API = "https://apps.fas.usda.gov/PSDOnlineDataServices/api"

# Commodity code -> (canonical_name, description)
PSD_COMMODITIES = {
    "0410000": ("wheat", "Wheat"),
    "0440000": ("rice_milled", "Rice, Milled"),
    "0422110": ("corn", "Corn"),
    "2222000": ("soybeans", "Oilseed, Soybean"),
    "0813100": ("cotton", "Cotton"),
    "0711100": ("sugar_centrifugal", "Sugar, Centrifugal"),
    "0711200": ("sugar_raw", "Sugar, Raw"),
    "2631000": ("palm_oil", "Oil, Palm"),
    "0430000": ("barley", "Barley"),
    "0459100": ("sorghum", "Sorghum"),
}

# Attribute codes -> (canonical_suffix, unit, description)
PSD_ATTRIBUTES = {
    "125": ("production", "thousand_mt", "Production"),
    "088": ("domestic_consumption", "thousand_mt", "Domestic Consumption"),
    "071": ("exports", "thousand_mt", "Exports"),
    "057": ("imports", "thousand_mt", "Imports"),
    "176": ("ending_stocks", "thousand_mt", "Ending Stocks"),
}

# Countries (ISO3 -> PSD country code)
PSD_COUNTRIES = {
    "USA": "US", "CHN": "CH", "IND": "IN", "BRA": "BR", "ARG": "AR",
    "AUS": "AS", "CAN": "CA", "FRA": "FR", "DEU": "GM", "IDN": "ID",
    "JPN": "JA", "MEX": "MX", "NGA": "NI", "PAK": "PK", "RUS": "RS",
    "THA": "TH", "TUR": "TU", "VNM": "VM", "BGD": "BG", "ZAF": "SF",
}


class USDACollector(BaseCollector):
    name = "usda"
    timeout = 60

    async def collect(self) -> list[dict]:
        all_points = []

        for commodity_code, (canonical, desc) in PSD_COMMODITIES.items():
            try:
                points = await self._fetch_commodity(commodity_code, canonical, desc)
                all_points.extend(points)
                self.logger.info(
                    f"[usda] fetched {canonical}: {len(points)} points"
                )
            except Exception as e:
                self.logger.warning(f"[usda] failed {canonical}: {e}")
            await asyncio.sleep(1.0)  # rate limit

        return all_points

    async def _fetch_commodity(
        self, commodity_code: str, canonical: str, desc: str
    ) -> list[dict]:
        """Fetch PSD data for a single commodity across countries."""
        points = []

        for iso3, psd_code in PSD_COUNTRIES.items():
            url = f"{PSD_API}/CommodityData"
            params = {
                "commodityCode": commodity_code,
                "countryCode": psd_code,
                "marketYear": ",".join(str(y) for y in range(2010, 2026)),
            }

            try:
                resp = await self._request("GET", url, params=params)
                records = resp.json()
            except Exception:
                continue

            if not isinstance(records, list):
                continue

            for rec in records:
                attr_id = str(rec.get("attributeId", ""))
                market_year = rec.get("marketYear")
                value = rec.get("value")

                if attr_id not in PSD_ATTRIBUTES or value is None:
                    continue

                suffix, unit, attr_desc = PSD_ATTRIBUTES[attr_id]
                series_id = f"{commodity_code}_{attr_id}"
                can_name = f"{canonical}_{suffix}"

                points.append({
                    "source": "USDA",
                    "series_id": series_id,
                    "canonical_name": can_name,
                    "country_iso3": iso3,
                    "date": f"{market_year}-01-01",
                    "value": float(value),
                    "unit": unit,
                    "frequency": "annual",
                })

            await asyncio.sleep(0.5)

        return points

    async def validate(self, data: list[dict]) -> list[dict]:
        valid = []
        valid_iso3 = set(PSD_COUNTRIES.keys())
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
                await db.execute(
                    """INSERT INTO data_series
                    (source, series_id, country_iso3, name, description,
                     unit, frequency, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source, series_id, country_iso3) DO UPDATE SET
                        unit = excluded.unit,
                        frequency = excluded.frequency""",
                    (
                        info["source"], info["series_id"], info["country_iso3"],
                        info["canonical_name"], "", info["unit"], info["frequency"],
                        json.dumps({"usda_psd": info["series_id"]}),
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
