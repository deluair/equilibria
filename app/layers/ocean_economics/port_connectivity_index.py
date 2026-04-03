"""Port connectivity index: tracking quality and shipping volume as connectivity proxies.

LP.LPI.TRAC.XQ (logistics: tracking and tracing quality, scale 1-5) measures port
and freight reliability. IS.SHP.GOOD.TU (goods transported by water, million ton-km)
proxies shipping volume. Together they estimate port system connectivity and efficiency.

Sources: World Bank WDI (LP.LPI.TRAC.XQ, IS.SHP.GOOD.TU)
"""

from __future__ import annotations

from app.layers.base import LayerBase

# LPI tracking scale max
LPI_MAX = 5.0


class PortConnectivityIndex(LayerBase):
    layer_id = "lOE"
    name = "Port Connectivity Index"

    async def compute(self, db, **kwargs) -> dict:
        tracking_code = "LP.LPI.TRAC.XQ"
        tracking_name = "tracking and tracing"
        tracking_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (tracking_code, f"%{tracking_name}%"),
        )

        shipping_code = "IS.SHP.GOOD.TU"
        shipping_name = "goods transported by water"
        shipping_rows = await db.fetch_all(
            "SELECT value FROM data_points WHERE series_id = "
            "(SELECT id FROM data_series WHERE indicator_code = ? OR name LIKE ?) "
            "ORDER BY date DESC LIMIT 15",
            (shipping_code, f"%{shipping_name}%"),
        )

        if not tracking_rows and not shipping_rows:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "No port connectivity data found",
            }

        tracking_vals = [row["value"] for row in tracking_rows if row["value"] is not None]
        shipping_vals = [row["value"] for row in shipping_rows if row["value"] is not None]

        tracking_latest = float(tracking_vals[0]) if tracking_vals else None
        shipping_latest = float(shipping_vals[0]) if shipping_vals else None

        if tracking_latest is None and shipping_latest is None:
            return {
                "score": None,
                "signal": "UNAVAILABLE",
                "error": "All port connectivity rows have null values",
            }

        # Connectivity score: higher LPI tracking = better (lower risk)
        # Score is inverted: poor connectivity = high score (risk signal)
        if tracking_latest is not None:
            lpi_norm = tracking_latest / LPI_MAX  # 0-1, higher = better
            tracking_risk = (1.0 - lpi_norm) * 60.0
        else:
            tracking_risk = 30.0

        # Shipping volume: lower volume may indicate under-developed port system
        if shipping_latest is not None:
            # Normalise: 0 ton-km = high risk, 500B+ = low risk
            shipping_norm = min(shipping_latest / 5e5, 1.0)
            shipping_risk = (1.0 - shipping_norm) * 40.0
        else:
            shipping_risk = 20.0

        score = round(min(100.0, tracking_risk + shipping_risk), 2)

        return {
            "score": score,
            "signal": self.classify_signal(score),
            "metrics": {
                "lpi_tracking_score": round(tracking_latest, 3) if tracking_latest else None,
                "lpi_max_scale": LPI_MAX,
                "shipping_volume_m_ton_km": round(shipping_latest, 2) if shipping_latest else None,
                "tracking_risk_component": round(tracking_risk, 2),
                "shipping_risk_component": round(shipping_risk, 2),
                "n_tracking_obs": len(tracking_vals),
                "n_shipping_obs": len(shipping_vals),
            },
        }
