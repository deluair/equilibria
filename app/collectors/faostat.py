"""FAOSTAT collector.

Fetches agricultural data from FAOSTAT bulk API.
Indicators: production indices, food balance, trade matrix, land use.
Area Code 16 = Bangladesh. Stores in data_series + data_points with source="FAOSTAT".
"""

import asyncio
import csv
import io
import json
import zipfile

from app.collectors.base import BaseCollector
from app.db import get_db, release_db

FAOSTAT_API = "https://bulks-faostat.fao.org/production"

# Dataset code -> (filename in zip, canonical_prefix, description)
FAOSTAT_DATASETS = {
    "QI": {
        "zip": "Production_Indices_E_All_Data_(Normalized).zip",
        "canonical_prefix": "production_index",
        "description": "Production indices",
        "items": {
            "2051": ("gross_production_index", "index_2014-2016=100",
                     "Gross Production Index Number"),
            "2054": ("net_production_index", "index_2014-2016=100",
                     "Net Production Index Number"),
        },
    },
    "FBS": {
        "zip": "FoodBalanceSheets_E_All_Data_(Normalized).zip",
        "canonical_prefix": "food_balance",
        "description": "Food balance sheets",
        "items": {
            "2901": ("food_supply_kcal", "kcal_per_capita_per_day",
                     "Food supply (kcal/capita/day)"),
            "2903": ("protein_supply", "g_per_capita_per_day",
                     "Protein supply quantity"),
        },
    },
    "TM": {
        "zip": "Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip",
        "canonical_prefix": "trade",
        "description": "Detailed trade matrix",
        "items": {
            "5922": ("export_value", "thousands_usd", "Export Value"),
            "5610": ("import_value", "thousands_usd", "Import Value"),
        },
    },
    "RL": {
        "zip": "Inputs_LandUse_E_All_Data_(Normalized).zip",
        "canonical_prefix": "land_use",
        "description": "Land use",
        "items": {
            "6601": ("agricultural_land", "thousand_ha", "Agricultural land"),
            "6621": ("arable_land", "thousand_ha", "Arable land"),
            "6650": ("forest_land", "thousand_ha", "Forest land"),
        },
    },
}

# Focus countries (Area Code -> ISO3). Area Code 16 = Bangladesh.
AREA_CODES = {
    "16": "BGD", "231": "USA", "351": "CHN", "100": "IND", "79": "DEU",
    "21": "BRA", "108": "IDN", "223": "TUR", "203": "THA", "238": "VNM",
    "101": "IRL", "185": "RUS", "114": "JPN", "10": "AUS", "33": "CAN",
    "68": "FRA", "106": "ITA", "84": "GBR", "138": "MEX", "115": "KOR",
    "166": "NGA", "202": "ZAF", "170": "PAK", "171": "PHL", "2": "AFG",
    "75": "EGY", "15": "ARG", "40": "COL", "62": "ETH", "131": "MYS",
}


class FAOSTATCollector(BaseCollector):
    name = "faostat"
    timeout = 120  # bulk downloads can be large

    async def collect(self) -> list[dict]:
        all_points = []
        for ds_code, ds_meta in FAOSTAT_DATASETS.items():
            try:
                points = await self._fetch_dataset(ds_code, ds_meta)
                all_points.extend(points)
                self.logger.info(
                    f"[faostat] fetched {ds_code}: {len(points)} points"
                )
            except Exception as e:
                self.logger.warning(f"[faostat] failed {ds_code}: {e}")
            await asyncio.sleep(2.0)  # rate limit between bulk downloads
        return all_points

    async def _fetch_dataset(self, ds_code: str, ds_meta: dict) -> list[dict]:
        """Download and parse a FAOSTAT bulk zip file."""
        url = f"{FAOSTAT_API}/{ds_meta['zip']}"
        resp = await self._request("GET", url)

        points = []
        zip_bytes = io.BytesIO(resp.content)
        valid_areas = set(AREA_CODES.keys())
        valid_items = set(ds_meta["items"].keys())

        with zipfile.ZipFile(zip_bytes) as zf:
            # Find the CSV file in the zip
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                return points

            with zf.open(csv_names[0]) as csvfile:
                reader = csv.DictReader(io.TextIOWrapper(csvfile, encoding="utf-8"))
                for row in reader:
                    area_code = row.get("Area Code", "").strip()
                    if area_code not in valid_areas:
                        continue

                    element_code = row.get("Element Code", "").strip()
                    year = row.get("Year", "").strip()
                    value_str = row.get("Value", "").strip()

                    if not year or not value_str:
                        continue

                    # Match against our target items by element code
                    if element_code not in valid_items:
                        continue

                    try:
                        value = float(value_str)
                    except ValueError:
                        continue

                    item_meta = ds_meta["items"][element_code]
                    iso3 = AREA_CODES[area_code]
                    canonical = f"{ds_meta['canonical_prefix']}_{item_meta[0]}"
                    series_id = f"{ds_code}_{element_code}"

                    points.append({
                        "source": "FAOSTAT",
                        "series_id": series_id,
                        "canonical_name": canonical,
                        "country_iso3": iso3,
                        "date": f"{year}-01-01",
                        "value": value,
                        "unit": item_meta[1],
                        "frequency": "annual",
                    })

        return points

    async def validate(self, data: list[dict]) -> list[dict]:
        valid = []
        valid_iso3 = set(AREA_CODES.values())
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
                        json.dumps({"faostat_dataset": info["series_id"]}),
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
