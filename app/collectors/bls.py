"""BLS (Bureau of Labor Statistics) collector.

Fetches US labor and price data from BLS Public Data API v2.
Series: CPI components, employment situation, JOLTS, productivity.
Requires BLS_API_KEY for v2 (higher rate limits).
Stores in data_series + data_points with source="BLS".
"""

import asyncio
import json

from app.collectors.base import BaseCollector
from app.config import settings
from app.db import get_db, release_db

BLS_API = "https://api.bls.gov/publicAPI/v2/timeseries/data/"

# BLS series ID -> (canonical_name, unit, description)
BLS_SERIES = {
    # CPI components
    "CUUR0000SA0": ("cpi_all_urban", "index_1982-84=100",
                    "CPI-U All items, US city average"),
    "CUUR0000SAF1": ("cpi_food", "index_1982-84=100",
                     "CPI-U Food"),
    "CUUR0000SETA01": ("cpi_gasoline", "index_1982-84=100",
                       "CPI-U Gasoline, all types"),
    "CUUR0000SAH1": ("cpi_shelter", "index_1982-84=100",
                     "CPI-U Shelter"),
    "CUUR0000SAM": ("cpi_medical", "index_1982-84=100",
                    "CPI-U Medical care"),
    "CUUR0000SAE1": ("cpi_education", "index_1982-84=100",
                     "CPI-U Education"),
    # Employment situation
    "LNS14000000": ("unemployment_rate", "percent",
                    "Unemployment rate, seasonally adjusted"),
    "CES0000000001": ("total_nonfarm", "thousands",
                      "Total nonfarm employment"),
    "LNS11300000": ("labor_force_participation", "percent",
                    "Labor force participation rate"),
    "LNS12300000": ("employment_population_ratio", "percent",
                    "Employment-population ratio"),
    # JOLTS
    "JTS000000000000000JOL": ("jolts_openings", "thousands",
                              "JOLTS Job openings, total nonfarm"),
    "JTS000000000000000HIL": ("jolts_hires", "thousands",
                              "JOLTS Hires, total nonfarm"),
    "JTS000000000000000QUL": ("jolts_quits", "thousands",
                              "JOLTS Quits, total nonfarm"),
    # Productivity
    "PRS85006092": ("labor_productivity", "percent",
                    "Nonfarm business labor productivity, quarterly % change"),
    "PRS85006112": ("unit_labor_costs", "percent",
                    "Nonfarm business unit labor costs, quarterly % change"),
    # Average hourly earnings
    "CES0500000003": ("avg_hourly_earnings", "usd_per_hour",
                      "Average hourly earnings, total private"),
}


class BLSCollector(BaseCollector):
    name = "bls"

    def __init__(self):
        super().__init__()
        self._api_key = settings.bls_api_key or ""

    async def collect(self) -> list[dict]:
        all_points = []
        series_ids = list(BLS_SERIES.keys())

        # BLS v2 allows up to 50 series per request, 10-year window
        # Split into chunks of 25 and fetch 2000-2025 in two windows
        for start_year, end_year in [(2000, 2012), (2013, 2025)]:
            for i in range(0, len(series_ids), 25):
                chunk = series_ids[i : i + 25]
                try:
                    points = await self._fetch_chunk(chunk, start_year, end_year)
                    all_points.extend(points)
                    self.logger.info(
                        f"[bls] fetched {len(chunk)} series "
                        f"({start_year}-{end_year}): {len(points)} points"
                    )
                except Exception as e:
                    self.logger.warning(
                        f"[bls] failed chunk ({start_year}-{end_year}): {e}"
                    )
                await asyncio.sleep(1.0)  # rate limit

        return all_points

    async def _fetch_chunk(
        self, series_ids: list[str], start_year: int, end_year: int
    ) -> list[dict]:
        """Fetch a chunk of BLS series for a year range."""
        payload = {
            "seriesid": series_ids,
            "startyear": str(start_year),
            "endyear": str(end_year),
            "catalog": False,
            "calculations": False,
            "annualaverage": False,
        }
        if self._api_key:
            payload["registrationkey"] = self._api_key

        resp = await self._request("POST", BLS_API, json=payload)
        body = resp.json()

        if body.get("status") != "REQUEST_SUCCEEDED":
            msg = body.get("message", ["Unknown error"])
            raise RuntimeError(f"BLS API error: {msg}")

        points = []
        for series_result in body.get("Results", {}).get("series", []):
            sid = series_result.get("seriesID", "")
            meta = BLS_SERIES.get(sid)
            if not meta:
                continue

            canonical, unit, description = meta
            for dp in series_result.get("data", []):
                year = dp.get("year", "")
                period = dp.get("period", "")
                value_str = dp.get("value", "")

                if not year or not value_str:
                    continue
                # Skip annual averages (M13)
                if period == "M13":
                    continue

                try:
                    value = float(value_str)
                except ValueError:
                    continue

                date_str = self._period_to_date(year, period)
                if not date_str:
                    continue

                freq = "monthly"
                if period.startswith("Q"):
                    freq = "quarterly"

                points.append({
                    "source": "BLS",
                    "series_id": sid,
                    "canonical_name": canonical,
                    "country_iso3": "USA",
                    "date": date_str,
                    "value": value,
                    "unit": unit,
                    "frequency": freq,
                })

        return points

    @staticmethod
    def _period_to_date(year: str, period: str) -> str:
        """Convert BLS year + period to date string."""
        if period.startswith("M") and len(period) == 3:
            month = period[1:]
            return f"{year}-{month}-01"
        if period.startswith("Q"):
            q = int(period[2:]) if len(period) == 3 else int(period[1:])
            month = {1: "01", 2: "04", 3: "07", 4: "10"}.get(q, "01")
            return f"{year}-{month}-01"
        return ""

    async def validate(self, data: list[dict]) -> list[dict]:
        valid = []
        for row in data:
            try:
                v = float(row["value"])
                if v != v:
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
                desc = BLS_SERIES.get(info["series_id"], ("", "", ""))[2]
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
                        json.dumps({"bls_series_id": info["series_id"]}),
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
