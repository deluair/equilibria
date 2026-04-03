"""V-Dem (Varieties of Democracy) collector.

Downloads V-Dem democracy dataset CSV from v-dem.net.
Key variables: v2x_polyarchy, v2x_libdem, v2x_partipdem, v2x_delibdem.
Stores in data_series + data_points with source="VDEM".
"""

import csv
import io
import json
import zipfile

from app.collectors.base import BaseCollector
from app.db import get_db, release_db

# V-Dem dataset download URL (Country-Year: V-Dem Core)
VDEM_URL = "https://v-dem.net/media/datasets/V-Dem-CY-Core-v14.csv.zip"

# V-Dem variables -> (canonical_name, unit, description)
VDEM_VARIABLES = {
    "v2x_polyarchy": ("electoral_democracy", "index_0_1",
                      "Electoral democracy index"),
    "v2x_libdem": ("liberal_democracy", "index_0_1",
                   "Liberal democracy index"),
    "v2x_partipdem": ("participatory_democracy", "index_0_1",
                      "Participatory democracy index"),
    "v2x_delibdem": ("deliberative_democracy", "index_0_1",
                     "Deliberative democracy index"),
    "v2x_egaldem": ("egalitarian_democracy", "index_0_1",
                    "Egalitarian democracy index"),
    "v2x_rule": ("rule_of_law", "index_0_1",
                 "Rule of law index"),
    "v2x_corr": ("corruption", "index_0_1",
                 "Political corruption index"),
    "v2x_freexp_altinf": ("freedom_expression", "index_0_1",
                          "Freedom of expression and alternative sources of information"),
    "v2x_frassoc_thick": ("freedom_association", "index_0_1",
                          "Freedom of association (thick)"),
    "v2x_suffr": ("suffrage", "index_0_1",
                  "Share of population with suffrage"),
    "v2xcl_rol": ("civil_liberties_rule_of_law", "index_0_1",
                  "Civil liberty rule of law"),
    "v2x_civlib": ("civil_liberties", "index_0_1",
                   "Civil liberties index"),
}

# 30 countries (ISO3 -> V-Dem country_text_id)
COUNTRIES = {
    "USA": "United States of America", "CHN": "China",
    "JPN": "Japan", "DEU": "Germany", "IND": "India",
    "GBR": "United Kingdom", "FRA": "France", "ITA": "Italy",
    "CAN": "Canada", "BRA": "Brazil", "RUS": "Russia",
    "KOR": "South Korea", "AUS": "Australia", "MEX": "Mexico",
    "ESP": "Spain", "IDN": "Indonesia", "NLD": "Netherlands",
    "SAU": "Saudi Arabia", "TUR": "Turkey", "CHE": "Switzerland",
    "POL": "Poland", "SWE": "Sweden", "ARG": "Argentina",
    "THA": "Thailand", "ZAF": "South Africa", "BGD": "Bangladesh",
    "VNM": "Vietnam", "PHL": "Philippines", "MYS": "Malaysia",
    "NGA": "Nigeria",
}

# Reverse: country name -> ISO3
NAME_TO_ISO3 = {v: k for k, v in COUNTRIES.items()}


class VDemCollector(BaseCollector):
    name = "vdem"
    timeout = 180  # large CSV download

    async def collect(self) -> list[dict]:
        """Download and parse V-Dem dataset."""
        self.logger.info("[vdem] downloading V-Dem dataset...")
        resp = await self._request("GET", VDEM_URL)

        all_points = []
        zip_bytes = io.BytesIO(resp.content)

        with zipfile.ZipFile(zip_bytes) as zf:
            csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csv_names:
                self.logger.warning("[vdem] no CSV found in zip")
                return all_points

            with zf.open(csv_names[0]) as csvfile:
                reader = csv.DictReader(
                    io.TextIOWrapper(csvfile, encoding="utf-8")
                )

                for row in reader:
                    country_name = row.get("country_name", "").strip()
                    iso3 = NAME_TO_ISO3.get(country_name)
                    if not iso3:
                        continue

                    year = row.get("year", "").strip()
                    if not year:
                        continue

                    # Only recent data (2000+)
                    try:
                        y = int(year)
                        if y < 2000:
                            continue
                    except ValueError:
                        continue

                    for var_name, (canonical, unit, desc) in VDEM_VARIABLES.items():
                        value_str = (row.get(var_name) or "").strip()
                        if not value_str or value_str.lower() in ("", "na", "nan", "."):
                            continue
                        try:
                            value = float(value_str)
                        except ValueError:
                            continue

                        all_points.append({
                            "source": "VDEM",
                            "series_id": var_name,
                            "canonical_name": canonical,
                            "country_iso3": iso3,
                            "date": f"{year}-01-01",
                            "value": value,
                            "unit": unit,
                            "frequency": "annual",
                        })

        self.logger.info(f"[vdem] parsed {len(all_points)} data points")
        return all_points

    async def validate(self, data: list[dict]) -> list[dict]:
        valid = []
        valid_iso3 = set(COUNTRIES.keys())
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
                desc = VDEM_VARIABLES.get(info["series_id"], ("", "", ""))[2]
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
                        json.dumps({"vdem_variable": info["series_id"]}),
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
