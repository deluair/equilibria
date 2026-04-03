"""FRED (Federal Reserve Economic Data) collector.

Fetches 30 key macroeconomic series using fredapi.
Stores in data_series + data_points tables with source="FRED".
Rate limit: 0.5s between series to respect FRED API limits.
"""

import asyncio
import json
from functools import partial

from fredapi import Fred

from app.collectors.base import BaseCollector
from app.config import settings
from app.db import get_db, release_db

# Canonical name -> FRED series ID
FRED_SERIES = {
    # GDP and output
    "real_gdp_growth": "A191RL1Q225SBEA",  # Real GDP growth rate (quarterly, annualized)
    "real_gdp": "GDPC1",  # Real GDP level
    "industrial_production": "INDPRO",  # Industrial production index
    "capacity_utilization": "TCU",  # Total capacity utilization
    # Prices and inflation
    "cpi_all": "CPIAUCSL",  # CPI for all urban consumers
    "core_cpi": "CPILFESL",  # CPI less food and energy
    "pce_price_index": "PCEPI",  # PCE price index
    "producer_price_index": "PPIACO",  # PPI all commodities
    # Labor market
    "unemployment_rate": "UNRATE",  # Civilian unemployment rate
    "nonfarm_payrolls": "PAYEMS",  # Total nonfarm payrolls
    "initial_claims": "ICSA",  # Initial jobless claims
    "labor_force_participation": "CIVPART",  # Labor force participation rate
    "avg_hourly_earnings": "CES0500000003",  # Average hourly earnings
    # Interest rates and monetary
    "fed_funds_rate": "FEDFUNDS",  # Effective federal funds rate
    "treasury_10y": "GS10",  # 10-Year Treasury constant maturity
    "treasury_2y": "GS2",  # 2-Year Treasury constant maturity
    "term_spread_10y2y": "T10Y2Y",  # 10Y-2Y Treasury spread
    "treasury_3m": "TB3MS",  # 3-Month Treasury bill
    "m2_money_supply": "M2SL",  # M2 money stock
    # Financial conditions
    "vix": "VIXCLS",  # CBOE VIX
    "sp500": "SP500",  # S&P 500 index
    "baa_spread": "BAA10Y",  # Moody's Baa - 10Y spread
    # Housing and real estate
    "housing_starts": "HOUST",  # Housing starts
    "case_shiller_national": "CSUSHPINSA",  # Case-Shiller national home price
    # Trade and international
    "trade_balance": "BOPGSTB",  # Trade balance goods and services
    "real_broad_dollar": "RBUSBIS",  # Real broad dollar index (BIS)
    # Commodities and energy
    "oil_price_wti": "DCOILWTICO",  # WTI crude oil price
    "oil_price_brent": "DCOILBRENTEU",  # Brent crude oil price
    # Consumer and business sentiment
    "consumer_sentiment": "UMCSENT",  # U of Michigan consumer sentiment
    "leading_index": "USSLIND",  # Leading economic index
}

# Metadata for each series
SERIES_META = {
    "real_gdp_growth": {"unit": "percent", "frequency": "quarterly"},
    "real_gdp": {"unit": "billions_2017_usd", "frequency": "quarterly"},
    "industrial_production": {"unit": "index_2017=100", "frequency": "monthly"},
    "capacity_utilization": {"unit": "percent", "frequency": "monthly"},
    "cpi_all": {"unit": "index_1982-84=100", "frequency": "monthly"},
    "core_cpi": {"unit": "index_1982-84=100", "frequency": "monthly"},
    "pce_price_index": {"unit": "index_2017=100", "frequency": "monthly"},
    "producer_price_index": {"unit": "index_1982=100", "frequency": "monthly"},
    "unemployment_rate": {"unit": "percent", "frequency": "monthly"},
    "nonfarm_payrolls": {"unit": "thousands", "frequency": "monthly"},
    "initial_claims": {"unit": "claims", "frequency": "weekly"},
    "labor_force_participation": {"unit": "percent", "frequency": "monthly"},
    "avg_hourly_earnings": {"unit": "usd_per_hour", "frequency": "monthly"},
    "fed_funds_rate": {"unit": "percent", "frequency": "monthly"},
    "treasury_10y": {"unit": "percent", "frequency": "monthly"},
    "treasury_2y": {"unit": "percent", "frequency": "monthly"},
    "term_spread_10y2y": {"unit": "percent", "frequency": "daily"},
    "treasury_3m": {"unit": "percent", "frequency": "monthly"},
    "m2_money_supply": {"unit": "billions_usd", "frequency": "monthly"},
    "vix": {"unit": "index", "frequency": "daily"},
    "sp500": {"unit": "index", "frequency": "daily"},
    "baa_spread": {"unit": "percent", "frequency": "daily"},
    "housing_starts": {"unit": "thousands_units", "frequency": "monthly"},
    "case_shiller_national": {"unit": "index_jan2000=100", "frequency": "monthly"},
    "trade_balance": {"unit": "millions_usd", "frequency": "monthly"},
    "real_broad_dollar": {"unit": "index_jan2006=100", "frequency": "monthly"},
    "oil_price_wti": {"unit": "usd_per_barrel", "frequency": "daily"},
    "oil_price_brent": {"unit": "usd_per_barrel", "frequency": "daily"},
    "consumer_sentiment": {"unit": "index_1966q1=100", "frequency": "monthly"},
    "leading_index": {"unit": "index_2016=100", "frequency": "monthly"},
}


class FREDCollector(BaseCollector):
    name = "fred"

    def __init__(self):
        super().__init__()
        if not settings.fred_api_key:
            raise ValueError("FRED_API_KEY not set")
        self._fred = Fred(api_key=settings.fred_api_key)

    async def collect(self) -> list[dict]:
        """Fetch all 30 FRED series. Returns list of data point dicts."""
        all_points = []
        loop = asyncio.get_event_loop()

        for canonical_name, series_id in FRED_SERIES.items():
            try:
                # fredapi is sync, run in executor
                series = await loop.run_in_executor(
                    None, partial(self._fred.get_series, series_id)
                )
                meta = SERIES_META.get(canonical_name, {})
                for date, value in series.items():
                    if value is not None and str(value) != "NaN" and str(value) != ".":
                        all_points.append(
                            {
                                "source": "FRED",
                                "series_id": series_id,
                                "canonical_name": canonical_name,
                                "country_iso3": "USA",
                                "date": date.strftime("%Y-%m-%d"),
                                "value": float(value),
                                "unit": meta.get("unit", ""),
                                "frequency": meta.get("frequency", ""),
                            }
                        )
                self.logger.info(f"[fred] fetched {canonical_name} ({series_id})")
            except Exception as e:
                self.logger.warning(f"[fred] failed to fetch {canonical_name}: {e}")

            # rate limit: 0.5s between series
            await asyncio.sleep(0.5)

        return all_points

    async def validate(self, data: list[dict]) -> list[dict]:
        """Drop rows with missing or non-numeric values."""
        valid = []
        for row in data:
            try:
                v = float(row["value"])
                if v != v:  # NaN check
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
            # Group by series
            series_map = {}
            for row in data:
                key = (row["source"], row["series_id"], row["country_iso3"])
                if key not in series_map:
                    series_map[key] = row
                    series_map[key]["points"] = []
                series_map[key]["points"].append((row["date"], row["value"]))

            stored = 0
            for key, info in series_map.items():
                # Upsert data_series
                await db.execute(
                    """INSERT INTO data_series
                    (source, series_id, country_iso3, name, unit, frequency, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source, series_id, country_iso3) DO UPDATE SET
                        unit = excluded.unit,
                        frequency = excluded.frequency""",
                    (
                        info["source"],
                        info["series_id"],
                        info["country_iso3"],
                        info["canonical_name"],
                        info["unit"],
                        info["frequency"],
                        json.dumps({"fred_series_id": info["series_id"]}),
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
