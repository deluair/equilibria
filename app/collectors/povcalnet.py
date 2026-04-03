"""PovcalNet / World Bank Poverty and Inequality Platform (PIP) collector.

Fetches poverty headcounts and Gini coefficients from PIP API.
Endpoint: https://api.worldbank.org/pip/v1
Stores in data_series + data_points with source="PIP".
"""

import asyncio
import json

from app.collectors.base import BaseCollector
from app.db import get_db, release_db

PIP_API = "https://api.worldbank.org/pip/v1"

# Poverty lines (daily, 2017 PPP USD)
POVERTY_LINES = {
    "2.15": ("poverty_215", "Poverty headcount ratio at $2.15/day (2017 PPP)"),
    "3.65": ("poverty_365", "Poverty headcount ratio at $3.65/day (2017 PPP)"),
    "6.85": ("poverty_685", "Poverty headcount ratio at $6.85/day (2017 PPP)"),
}

# 30 major countries (ISO3)
COUNTRIES = [
    "USA", "CHN", "JPN", "DEU", "IND", "GBR", "FRA", "ITA", "CAN", "BRA",
    "RUS", "KOR", "AUS", "MEX", "ESP", "IDN", "NLD", "SAU", "TUR", "CHE",
    "POL", "SWE", "ARG", "THA", "ZAF", "BGD", "VNM", "PHL", "MYS", "NGA",
]


class PovcalNetCollector(BaseCollector):
    name = "povcalnet"
    timeout = 60

    async def collect(self) -> list[dict]:
        all_points = []

        # Fetch poverty data at each poverty line
        for pov_line, (canonical, desc) in POVERTY_LINES.items():
            try:
                points = await self._fetch_poverty_line(pov_line, canonical, desc)
                all_points.extend(points)
                self.logger.info(
                    f"[povcalnet] fetched {canonical}: {len(points)} points"
                )
            except Exception as e:
                self.logger.warning(f"[povcalnet] failed {canonical}: {e}")
            await asyncio.sleep(0.5)

        # Fetch Gini coefficients
        try:
            gini_points = await self._fetch_gini()
            all_points.extend(gini_points)
            self.logger.info(f"[povcalnet] fetched gini: {len(gini_points)} points")
        except Exception as e:
            self.logger.warning(f"[povcalnet] failed gini: {e}")

        return all_points

    async def _fetch_poverty_line(
        self, pov_line: str, canonical: str, desc: str
    ) -> list[dict]:
        """Fetch poverty headcount for a given poverty line."""
        points = []

        for iso3 in COUNTRIES:
            params = {
                "country": iso3,
                "year": "all",
                "povline": pov_line,
                "fill_gaps": "true",
                "welfare_type": "all",
                "reporting_level": "national",
                "format": "json",
            }

            try:
                resp = await self._request("GET", f"{PIP_API}/pip", params=params)
                records = resp.json()
            except Exception:
                continue

            if not isinstance(records, list):
                continue

            for rec in records:
                year = rec.get("reporting_year")
                headcount = rec.get("headcount")

                if year is None or headcount is None:
                    continue

                points.append({
                    "source": "PIP",
                    "series_id": f"headcount_{pov_line}",
                    "canonical_name": canonical,
                    "country_iso3": iso3,
                    "date": f"{year}-01-01",
                    "value": float(headcount) * 100,  # convert ratio to percent
                    "unit": "percent",
                    "frequency": "annual",
                })

            await asyncio.sleep(0.3)

        return points

    async def _fetch_gini(self) -> list[dict]:
        """Fetch Gini coefficients for all countries."""
        points = []

        for iso3 in COUNTRIES:
            params = {
                "country": iso3,
                "year": "all",
                "fill_gaps": "true",
                "welfare_type": "all",
                "reporting_level": "national",
                "format": "json",
            }

            try:
                resp = await self._request("GET", f"{PIP_API}/pip", params=params)
                records = resp.json()
            except Exception:
                continue

            if not isinstance(records, list):
                continue

            # Track seen years to avoid duplicates from different welfare types
            seen_years = set()
            for rec in records:
                year = rec.get("reporting_year")
                gini = rec.get("gini")

                if year is None or gini is None:
                    continue
                if year in seen_years:
                    continue
                seen_years.add(year)

                points.append({
                    "source": "PIP",
                    "series_id": "gini",
                    "canonical_name": "gini_coefficient",
                    "country_iso3": iso3,
                    "date": f"{year}-01-01",
                    "value": float(gini) * 100,  # convert to 0-100 scale
                    "unit": "index_0_100",
                    "frequency": "annual",
                })

            await asyncio.sleep(0.3)

        return points

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
                # Poverty headcount and Gini should be 0-100
                if v < 0 or v > 100:
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
                desc = ""
                if info["series_id"] == "gini":
                    desc = "Gini coefficient (0-100 scale)"
                else:
                    pov_line = info["series_id"].replace("headcount_", "")
                    desc = POVERTY_LINES.get(pov_line, ("", ""))[1]

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
                        json.dumps({"pip_indicator": info["series_id"]}),
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
