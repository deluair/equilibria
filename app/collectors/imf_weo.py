"""IMF World Economic Outlook collector.

Fetches GDP growth forecasts, fiscal indicators, and current account data
from the IMF DataMapper API (NOT SDMX).
Endpoint: https://www.imf.org/external/datamapper/api/v1
Stores in data_series + data_points with source="IMF_WEO".
"""

import asyncio
import json

from app.collectors.base import BaseCollector
from app.db import get_db, release_db

IMF_API = "https://www.imf.org/external/datamapper/api/v1"

# IMF WEO indicator code -> (canonical_name, unit, description)
IMF_INDICATORS = {
    "NGDP_RPCH": ("gdp_growth_real", "percent",
                  "GDP, constant prices, % change"),
    "NGDPD": ("gdp_current_usd", "billions_usd",
              "GDP, current prices, USD billions"),
    "NGDPDPC": ("gdp_per_capita", "usd",
                "GDP per capita, current prices, USD"),
    "PCPIPCH": ("inflation_avg_consumer", "percent",
                "Inflation, average consumer prices, % change"),
    "PCPIEPCH": ("inflation_end_of_period", "percent",
                 "Inflation, end of period consumer prices, % change"),
    "GGR_NGDP": ("govt_revenue_gdp", "percent",
                 "General government revenue, % of GDP"),
    "GGX_NGDP": ("govt_expenditure_gdp", "percent",
                 "General government total expenditure, % of GDP"),
    "GGXCNL_NGDP": ("fiscal_balance_gdp", "percent",
                     "General government net lending/borrowing, % of GDP"),
    "GGXWDG_NGDP": ("govt_gross_debt_gdp", "percent",
                     "General government gross debt, % of GDP"),
    "BCA_NGDPD": ("current_account_gdp", "percent",
                  "Current account balance, % of GDP"),
    "LUR": ("unemployment_rate", "percent",
            "Unemployment rate, % of total labor force"),
    "LP": ("population", "millions",
           "Population, millions"),
}

# 30 major countries (ISO3)
COUNTRIES = [
    "USA", "CHN", "JPN", "DEU", "IND", "GBR", "FRA", "ITA", "CAN", "BRA",
    "RUS", "KOR", "AUS", "MEX", "ESP", "IDN", "NLD", "SAU", "TUR", "CHE",
    "POL", "SWE", "ARG", "THA", "ZAF", "BGD", "VNM", "PHL", "MYS", "NGA",
]


class IMFWEOCollector(BaseCollector):
    name = "imf_weo"
    timeout = 60

    async def collect(self) -> list[dict]:
        all_points = []
        for indicator, meta in IMF_INDICATORS.items():
            try:
                points = await self._fetch_indicator(indicator, meta)
                all_points.extend(points)
                self.logger.info(
                    f"[imf_weo] fetched {meta[0]} ({indicator}): {len(points)} points"
                )
            except Exception as e:
                self.logger.warning(f"[imf_weo] failed {meta[0]}: {e}")
            await asyncio.sleep(0.5)  # rate limit
        return all_points

    async def _fetch_indicator(self, indicator: str, meta: tuple) -> list[dict]:
        """Fetch a single WEO indicator for all countries via DataMapper API."""
        canonical, unit, description = meta
        country_str = ",".join(COUNTRIES)
        url = f"{IMF_API}/{indicator}/{country_str}"

        resp = await self._request("GET", url)
        body = resp.json()

        points = []
        # DataMapper API returns: {"values": {"INDICATOR": {"COUNTRY": {"YEAR": value}}}}
        values = body.get("values", {})
        indicator_data = values.get(indicator, {})

        for iso3, year_data in indicator_data.items():
            if iso3 not in COUNTRIES:
                continue
            if not isinstance(year_data, dict):
                continue

            for year, value in year_data.items():
                if value is None:
                    continue
                try:
                    val = float(value)
                except (ValueError, TypeError):
                    continue

                points.append({
                    "source": "IMF_WEO",
                    "series_id": indicator,
                    "canonical_name": canonical,
                    "country_iso3": iso3,
                    "date": f"{year}-01-01",
                    "value": val,
                    "unit": unit,
                    "frequency": "annual",
                })

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
                desc = IMF_INDICATORS.get(info["series_id"], ("", "", ""))[2]
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
                        json.dumps({"imf_weo_indicator": info["series_id"]}),
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
