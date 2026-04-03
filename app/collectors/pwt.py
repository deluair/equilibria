"""Penn World Table (PWT) 10.1 collector.

Downloads PWT 10.1 data from University of Groningen.
Variables: TFP, capital stock, human capital, real GDP PPP.
Stores in data_series + data_points with source="PWT".
"""

import csv
import io
import json
import zipfile

from app.collectors.base import BaseCollector
from app.db import get_db, release_db

# PWT 10.1 download URL (Excel or CSV in zip)
PWT_URL = "https://dataverse.nl/api/access/datafile/354098"

# PWT variable -> (canonical_name, unit, description)
PWT_VARIABLES = {
    "rgdpe": ("real_gdp_expenditure", "millions_2017_usd",
              "Expenditure-side real GDP at chained PPPs"),
    "rgdpo": ("real_gdp_output", "millions_2017_usd",
              "Output-side real GDP at chained PPPs"),
    "pop": ("population", "millions",
            "Population"),
    "emp": ("employment", "millions",
            "Number of persons engaged"),
    "avh": ("avg_hours_worked", "hours_per_year",
            "Average annual hours worked by persons engaged"),
    "hc": ("human_capital_index", "index",
           "Human capital index, based on years of schooling and returns to education"),
    "rnna": ("capital_stock", "millions_2017_usd",
             "Capital stock at constant 2017 national prices"),
    "rtfpna": ("tfp_level", "index_usa=1",
               "TFP level at current PPPs (USA=1)"),
    "ctfp": ("tfp_level_current", "index_usa=1",
             "TFP level at current PPPs (USA=1)"),
    "labsh": ("labor_share", "ratio",
              "Share of labour compensation in GDP at current national prices"),
    "irr": ("internal_rate_return", "ratio",
            "Real internal rate of return"),
    "csh_i": ("investment_share_gdp", "ratio",
              "Share of gross capital formation at current PPPs"),
    "csh_g": ("govt_consumption_share", "ratio",
              "Share of government consumption at current PPPs"),
    "csh_x": ("export_share_gdp", "ratio",
              "Share of merchandise exports at current PPPs"),
    "csh_m": ("import_share_gdp", "ratio",
              "Share of merchandise imports at current PPPs"),
}

# 30 major countries (ISO3)
COUNTRIES = {
    "USA", "CHN", "JPN", "DEU", "IND", "GBR", "FRA", "ITA", "CAN", "BRA",
    "RUS", "KOR", "AUS", "MEX", "ESP", "IDN", "NLD", "SAU", "TUR", "CHE",
    "POL", "SWE", "ARG", "THA", "ZAF", "BGD", "VNM", "PHL", "MYS", "NGA",
}


class PennWorldTableCollector(BaseCollector):
    name = "pwt"
    timeout = 120  # large file download

    async def collect(self) -> list[dict]:
        """Download and parse PWT 10.1 data."""
        self.logger.info("[pwt] downloading PWT 10.1 dataset...")
        resp = await self._request("GET", PWT_URL)

        all_points = []
        content = resp.content

        # PWT comes as a tab-separated or CSV file
        # Try to parse as zip first, then as plain CSV/TSV
        try:
            zip_bytes = io.BytesIO(content)
            with zipfile.ZipFile(zip_bytes) as zf:
                csv_names = [
                    n for n in zf.namelist()
                    if n.endswith(".csv") or n.endswith(".tsv") or n.endswith(".txt")
                ]
                if csv_names:
                    with zf.open(csv_names[0]) as f:
                        all_points = self._parse_csv(
                            io.TextIOWrapper(f, encoding="utf-8")
                        )
        except zipfile.BadZipFile:
            # Plain CSV/TSV
            text = content.decode("utf-8", errors="replace")
            all_points = self._parse_csv(io.StringIO(text))

        self.logger.info(f"[pwt] parsed {len(all_points)} data points")
        return all_points

    def _parse_csv(self, fileobj) -> list[dict]:
        """Parse PWT CSV/TSV into data point dicts."""
        points = []
        # Detect delimiter
        sample = fileobj.read(2048)
        fileobj.seek(0)
        delimiter = "\t" if "\t" in sample else ","

        reader = csv.DictReader(fileobj, delimiter=delimiter)
        # Normalize column names to lowercase
        if reader.fieldnames:
            reader.fieldnames = [f.strip().lower() for f in reader.fieldnames]

        for row in reader:
            iso3 = (row.get("countrycode") or row.get("country_code") or "").strip()
            if iso3 not in COUNTRIES:
                continue

            year = (row.get("year") or "").strip()
            if not year:
                continue

            for var_name, (canonical, unit, desc) in PWT_VARIABLES.items():
                value_str = (row.get(var_name) or "").strip()
                if not value_str or value_str.lower() in ("", "na", "nan", "."):
                    continue
                try:
                    value = float(value_str)
                except ValueError:
                    continue

                points.append({
                    "source": "PWT",
                    "series_id": var_name,
                    "canonical_name": canonical,
                    "country_iso3": iso3,
                    "date": f"{year}-01-01",
                    "value": value,
                    "unit": unit,
                    "frequency": "annual",
                })

        return points

    async def validate(self, data: list[dict]) -> list[dict]:
        valid = []
        for row in data:
            try:
                v = float(row["value"])
                if v != v:
                    continue
                if row["country_iso3"] not in COUNTRIES:
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
                desc = PWT_VARIABLES.get(info["series_id"], ("", "", ""))[2]
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
                        json.dumps({"pwt_variable": info["series_id"]}),
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
