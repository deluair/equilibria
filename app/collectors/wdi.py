"""World Bank WDI (World Development Indicators) collector.

Fetches 20 key indicators for 50 major countries.
Uses World Bank API v2: https://api.worldbank.org/v2/country/{iso2}/indicator/{code}?format=json
Stores in data_series + data_points tables with source="WDI".
"""

import asyncio
import json

from app.collectors.base import BaseCollector
from app.db import get_db, release_db

# WDI indicator code -> (canonical name, unit, description)
WDI_INDICATORS = {
    "NY.GDP.PCAP.PP.CD": ("gdp_per_capita_ppp", "current_intl_dollar", "GDP per capita, PPP"),
    "NY.GDP.MKTP.KD.ZG": ("gdp_growth", "percent", "GDP growth (annual %)"),
    "NY.GDP.MKTP.CD": ("gdp_current_usd", "current_usd", "GDP (current US$)"),
    "SI.POV.DDAY": ("poverty_190", "percent", "Poverty headcount ratio at $2.15/day"),
    "SI.POV.GINI": ("gini_index", "index", "Gini index"),
    "NE.TRD.GNFS.ZS": ("trade_pct_gdp", "percent", "Trade (% of GDP)"),
    "SP.DYN.LE00.IN": ("life_expectancy", "years", "Life expectancy at birth"),
    "SP.DYN.TFRT.IN": ("fertility_rate", "births_per_woman", "Fertility rate, total"),
    "SE.PRM.ENRR": ("primary_enrollment", "percent", "School enrollment, primary (% gross)"),
    "SE.TER.ENRR": ("tertiary_enrollment", "percent", "School enrollment, tertiary (% gross)"),
    "FP.CPI.TOTL.ZG": ("inflation_cpi", "percent", "Inflation, consumer prices (annual %)"),
    "BN.CAB.XOKA.GD.ZS": ("current_account_gdp", "percent", "Current account balance (% of GDP)"),
    "GC.DOD.TOTL.GD.ZS": ("govt_debt_gdp", "percent", "Central government debt (% of GDP)"),
    "SL.UEM.TOTL.ZS": ("unemployment_rate", "percent", "Unemployment, total (% of labor force)"),
    "SL.TLF.CACT.ZS": ("labor_participation", "percent", "Labor force participation rate"),
    "BX.KLT.DINV.WD.GD.ZS": (
        "fdi_pct_gdp", "percent", "Foreign direct investment, net inflows (% of GDP)",
    ),
    "IT.NET.USER.ZS": (
        "internet_users", "percent", "Individuals using the Internet (% of population)",
    ),
    "EG.USE.PCAP.KG.OE": ("energy_use_per_capita", "kg_oil_equiv", "Energy use per capita"),
    "EN.ATM.CO2E.PC": ("co2_per_capita", "metric_tons", "CO2 emissions per capita"),
    "SP.POP.TOTL": ("population", "persons", "Population, total"),
}

# Top 50 countries by GDP (ISO2 -> ISO3 mapping)
COUNTRIES = {
    "US": "USA", "CN": "CHN", "JP": "JPN", "DE": "DEU", "IN": "IND",
    "GB": "GBR", "FR": "FRA", "IT": "ITA", "CA": "CAN", "BR": "BRA",
    "RU": "RUS", "KR": "KOR", "AU": "AUS", "MX": "MEX", "ES": "ESP",
    "ID": "IDN", "NL": "NLD", "SA": "SAU", "TR": "TUR", "CH": "CHE",
    "PL": "POL", "SE": "SWE", "BE": "BEL", "TH": "THA", "AR": "ARG",
    "NO": "NOR", "AT": "AUT", "IE": "IRL", "IL": "ISR", "NG": "NGA",
    "ZA": "ZAF", "SG": "SGP", "MY": "MYS", "PH": "PHL", "DK": "DNK",
    "BD": "BGD", "EG": "EGY", "VN": "VNM", "PK": "PAK", "CL": "CHL",
    "CO": "COL", "FI": "FIN", "CZ": "CZE", "RO": "ROU", "PT": "PRT",
    "NZ": "NZL", "PE": "PER", "GR": "GRC", "KZ": "KAZ", "HU": "HUN",
}

# API base
WB_API = "https://api.worldbank.org/v2"
# Fetch 60 years of data (1960-present)
DATE_RANGE = "1960:2025"
# Per-page limit (WB API max is 32500)
PER_PAGE = 10000


class WDICollector(BaseCollector):
    name = "wdi"

    async def collect(self) -> list[dict]:
        """Fetch 20 indicators for 50 countries from World Bank API."""
        all_points = []
        country_codes = ";".join(COUNTRIES.keys())

        for indicator_code, (canonical, unit, description) in WDI_INDICATORS.items():
            try:
                points = await self._fetch_indicator(
                    indicator_code, country_codes, canonical, unit
                )
                all_points.extend(points)
                self.logger.info(
                    f"[wdi] fetched {canonical} ({indicator_code}): {len(points)} points"
                )
            except Exception as e:
                self.logger.warning(f"[wdi] failed {canonical} ({indicator_code}): {e}")

            # rate limit between indicator requests
            await asyncio.sleep(0.3)

        return all_points

    async def _fetch_indicator(
        self, indicator_code: str, country_codes: str, canonical: str, unit: str
    ) -> list[dict]:
        """Fetch a single indicator for all countries."""
        url = f"{WB_API}/country/{country_codes}/indicator/{indicator_code}"
        params = {
            "format": "json",
            "date": DATE_RANGE,
            "per_page": PER_PAGE,
        }

        points = []
        page = 1
        total_pages = 1

        while page <= total_pages:
            params["page"] = page
            resp = await self._request("GET", url, params=params)
            body = resp.json()

            # WB API returns [metadata, data] or [metadata, null]
            if not isinstance(body, list) or len(body) < 2 or body[1] is None:
                break

            meta = body[0]
            total_pages = meta.get("pages", 1)
            records = body[1]

            for rec in records:
                value = rec.get("value")
                if value is None:
                    continue

                # WB API returns ISO3 in countryiso3code field
                iso3 = rec.get("countryiso3code", "")
                if not iso3 or len(iso3) != 3:
                    # fallback: look up from our map
                    country_id = rec.get("country", {}).get("id", "")
                    iso3 = COUNTRIES.get(country_id, country_id)

                year = rec.get("date", "")
                if not year:
                    continue

                points.append(
                    {
                        "source": "WDI",
                        "series_id": indicator_code,
                        "canonical_name": canonical,
                        "country_iso3": iso3.upper(),
                        "date": f"{year}-01-01",  # annual data, use Jan 1
                        "value": float(value),
                        "unit": unit,
                        "frequency": "annual",
                    }
                )

            page += 1

        return points

    async def validate(self, data: list[dict]) -> list[dict]:
        """Drop rows with missing values or invalid countries."""
        valid = []
        valid_iso3 = set(COUNTRIES.values())
        for row in data:
            try:
                v = float(row["value"])
                if v != v:  # NaN check
                    continue
                if row["country_iso3"] not in valid_iso3:
                    continue
                valid.append(row)
            except (ValueError, TypeError):
                continue
        return valid

    async def store(self, data: list[dict]) -> int:
        """Upsert series and data points into SQLite."""
        if not data:
            return 0

        db = await get_db()
        try:
            # Group by (source, series_id, country_iso3)
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
                # Look up description from WDI_INDICATORS
                desc = ""
                for code, (canon, _, d) in WDI_INDICATORS.items():
                    if code == info["series_id"]:
                        desc = d
                        break

                # Upsert data_series
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
                        info["source"],
                        info["series_id"],
                        info["country_iso3"],
                        info["canonical_name"],
                        desc,
                        info["unit"],
                        info["frequency"],
                        json.dumps({"wdi_indicator": info["series_id"]}),
                    ),
                )

                # Get the series row id
                row = await db.fetch_one(
                    """SELECT id FROM data_series
                    WHERE source = ? AND series_id = ? AND country_iso3 = ?""",
                    (info["source"], info["series_id"], info["country_iso3"]),
                )
                if row is None:
                    continue
                sid = row["id"]

                # Upsert data points
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
