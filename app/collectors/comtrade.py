"""UN Comtrade collector.

Fetches bilateral trade flows from UN Comtrade v1 API.
Top bilateral trade flows for major country pairs.
Uses COMTRADE_API_KEY from settings. Rate limited.
Stores in data_series + data_points with source="COMTRADE".
"""

import asyncio
import json

from app.collectors.base import BaseCollector
from app.config import settings
from app.db import get_db, release_db

COMTRADE_API = "https://comtradeapi.un.org/data/v1/get/C/A"

# Reporter countries (ISO3 -> M49 numeric code)
REPORTERS = {
    "USA": "842", "CHN": "156", "JPN": "392", "DEU": "276", "IND": "356",
    "GBR": "826", "FRA": "250", "ITA": "380", "CAN": "124", "BRA": "076",
    "KOR": "410", "AUS": "036", "MEX": "484", "ESP": "724", "IDN": "360",
    "NLD": "528", "SAU": "682", "TUR": "792", "CHE": "756", "POL": "616",
    "SWE": "752", "ARG": "032", "THA": "764", "ZAF": "710", "BGD": "050",
    "VNM": "704", "PHL": "608", "MYS": "458", "NGA": "566", "RUS": "643",
}

# HS commodity codes to track (AG2 level)
COMMODITIES = {
    "TOTAL": ("total_trade", "Total trade"),
    "27": ("mineral_fuels", "Mineral fuels, oils"),
    "84": ("machinery", "Nuclear reactors, boilers, machinery"),
    "85": ("electrical_machinery", "Electrical machinery and equipment"),
    "87": ("vehicles", "Vehicles other than railway"),
    "61": ("knit_apparel", "Articles of apparel, knitted"),
    "62": ("woven_apparel", "Articles of apparel, not knitted"),
    "72": ("iron_steel", "Iron and steel"),
    "71": ("pearls_gems", "Pearls, precious stones, metals"),
    "39": ("plastics", "Plastics and articles thereof"),
}


class ComtradeCollector(BaseCollector):
    name = "comtrade"
    timeout = 60

    def __init__(self):
        super().__init__()
        self._api_key = settings.comtrade_api_key
        if not self._api_key:
            raise ValueError("COMTRADE_API_KEY not set")

    async def collect(self) -> list[dict]:
        all_points = []

        # Fetch recent years for each reporter
        for iso3, m49 in REPORTERS.items():
            try:
                points = await self._fetch_reporter(iso3, m49)
                all_points.extend(points)
                self.logger.info(f"[comtrade] fetched {iso3}: {len(points)} points")
            except Exception as e:
                self.logger.warning(f"[comtrade] failed {iso3}: {e}")
            # Comtrade rate limit: 1 req/sec for free tier
            await asyncio.sleep(1.5)

        return all_points

    async def _fetch_reporter(self, iso3: str, m49: str) -> list[dict]:
        """Fetch trade data for a single reporter country."""
        points = []
        headers = {"Ocp-Apim-Subscription-Key": self._api_key}

        for cmd_code, (canonical, desc) in COMMODITIES.items():
            for flow_code, flow_name in [("M", "import"), ("X", "export")]:
                params = {
                    "reporterCode": m49,
                    "period": "2020,2021,2022,2023,2024",
                    "partnerCode": "0",  # World
                    "flowCode": flow_code,
                    "cmdCode": cmd_code,
                    "partner2Code": "0",
                    "customsCode": "C00",
                    "motCode": "0",
                }

                try:
                    resp = await self._request(
                        "GET", COMTRADE_API, params=params, headers=headers
                    )
                    body = resp.json()
                except Exception:
                    continue

                records = body.get("data", [])
                for rec in records:
                    period = str(rec.get("period", ""))
                    value = rec.get("primaryValue")
                    if value is None or not period:
                        continue

                    series_id = f"{flow_code}_{cmd_code}"
                    can_name = f"{flow_name}_{canonical}"

                    points.append({
                        "source": "COMTRADE",
                        "series_id": series_id,
                        "canonical_name": can_name,
                        "country_iso3": iso3,
                        "date": f"{period}-01-01",
                        "value": float(value),
                        "unit": "usd",
                        "frequency": "annual",
                    })

                await asyncio.sleep(1.5)  # rate limit per request

        return points

    async def validate(self, data: list[dict]) -> list[dict]:
        valid = []
        valid_iso3 = set(REPORTERS.keys())
        for row in data:
            try:
                v = float(row["value"])
                if v != v:
                    continue
                if v < 0:
                    continue  # trade values should be non-negative
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
                        json.dumps({"comtrade_series": info["series_id"]}),
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
