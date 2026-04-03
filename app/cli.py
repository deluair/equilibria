"""Equilibria CLI.

Usage:
    python -m app.cli <command> [options]
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_serve(args: argparse.Namespace) -> None:
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


async def _run_collector(name: str) -> dict:
    from app.collectors.bls import BLSCollector
    from app.collectors.comtrade import ComtradeCollector
    from app.collectors.faostat import FAOSTATCollector
    from app.collectors.fred import FREDCollector
    from app.collectors.ilo import ILOCollector
    from app.collectors.imf_weo import IMFWEOCollector
    from app.collectors.noaa import NOAACollector
    from app.collectors.povcalnet import PovcalNetCollector
    from app.collectors.pwt import PennWorldTableCollector
    from app.collectors.usda import USDACollector
    from app.collectors.vdem import VDemCollector
    from app.collectors.wdi import WDICollector

    _MAP = {
        "fred": FREDCollector,
        "wdi": WDICollector,
        "ilo": ILOCollector,
        "faostat": FAOSTATCollector,
        "bls": BLSCollector,
        "imf_weo": IMFWEOCollector,
        "pwt": PennWorldTableCollector,
        "comtrade": ComtradeCollector,
        "usda": USDACollector,
        "noaa": NOAACollector,
        "vdem": VDemCollector,
        "povcalnet": PovcalNetCollector,
    }

    cls = _MAP[name]
    collector = cls()
    return await collector.run()


async def _collect_all() -> None:
    from app.db import close_db, init_db

    await init_db()
    sources = [
        "fred", "wdi", "ilo", "faostat", "bls",
        "imf_weo", "pwt", "comtrade", "usda", "noaa", "vdem", "povcalnet",
    ]
    try:
        for src in sources:
            logger.info("Collecting: %s", src)
            result = await _run_collector(src)
            status = result.get("status", "unknown")
            if status == "success":
                collected = result.get("collected", result.get("series", "?"))
                stored = result.get("stored", "?")
                logger.info("  %s: collected=%s stored=%s", src, collected, stored)
            else:
                logger.warning("  %s: FAILED - %s", src, result.get("error", "unknown error"))
    finally:
        await close_db()


async def _collect_one(name: str) -> None:
    from app.db import close_db, init_db

    await init_db()
    try:
        logger.info("Collecting: %s", name)
        result = await _run_collector(name)
        status = result.get("status", "unknown")
        if status == "success":
            logger.info("Done: %s", result)
        else:
            logger.error("Failed: %s", result.get("error", "unknown error"))
            sys.exit(1)
    finally:
        await close_db()


async def _generate_briefing(briefing_type: str, country: str | None) -> None:
    from app.briefings.country_deep_dive import CountryDeepDiveBriefing
    from app.briefings.economic_conditions import EconomicConditionsBriefing
    from app.briefings.trade_flash import TradeFlashBriefing
    from app.db import close_db, get_db, init_db, release_db

    _MAP = {
        "economic_conditions": EconomicConditionsBriefing,
        "trade_flash": TradeFlashBriefing,
        "country_deep_dive": CountryDeepDiveBriefing,
    }

    await init_db()
    db = await get_db()
    try:
        cls = _MAP[briefing_type]
        briefing = cls()
        kwargs = {}
        if country:
            kwargs["country"] = country.upper()
        result = await briefing.generate(db, **kwargs)
        await briefing.save(result, db, country_iso3=country.upper() if country else "GLOBAL")
        logger.info(
            "Generated briefing: type=%s title=%s",
            result["briefing_type"],
            result["title"],
        )
    finally:
        await release_db(db)
        await close_db()


async def _status() -> None:
    from app.db import close_db, fetch_all, fetch_one, init_db

    await init_db()
    try:
        series_row = await fetch_one("SELECT COUNT(*) AS n FROM data_series")
        points_row = await fetch_one("SELECT COUNT(*) AS n FROM data_points")
        briefings_row = await fetch_one("SELECT COUNT(*) AS n FROM briefings")
        last_collections = await fetch_all(
            "SELECT source, status, finished_at "
            "FROM collection_log "
            "ORDER BY started_at DESC "
            "LIMIT 20"
        )

        series_count = series_row["n"] if series_row else 0
        points_count = points_row["n"] if points_row else 0
        briefings_count = briefings_row["n"] if briefings_row else 0

        print("Equilibria Platform Status")
        print("=" * 40)
        print(f"  Data series : {series_count:,}")
        print(f"  Data points : {points_count:,}")
        print(f"  Briefings   : {briefings_count:,}")
        print()

        if last_collections:
            # Deduplicate: keep latest per source
            seen: set[str] = set()
            unique = []
            for row in last_collections:
                if row["source"] not in seen:
                    seen.add(row["source"])
                    unique.append(row)

            print("Last collection per source:")
            for row in sorted(unique, key=lambda r: r["source"]):
                ts = row["finished_at"] or "—"
                print(f"  {row['source']:<14} {row['status']:<10} {ts}")
        else:
            print("No collection runs recorded yet.")

        # Module counts
        import importlib.util
        import pathlib

        app_dir = pathlib.Path(__file__).parent
        def _count(subdir: str) -> int:
            d = app_dir / subdir
            if not d.exists():
                return 0
            return sum(
                1 for f in d.glob("*.py")
                if f.name not in ("__init__.py", "base.py")
            )

        print()
        print("Modules:")
        print(f"  Collectors  : {_count('collectors')}")
        print(f"  Briefings   : {_count('briefings')}")
        print(f"  Layers      : {_count('layers')}")
        print(f"  Estimators  : {_count('estimation')}")
    finally:
        await close_db()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m app.cli",
        description="Equilibria CLI",
    )
    sub = parser.add_subparsers(dest="command", metavar="<command>")

    # serve
    serve_p = sub.add_parser("serve", help="Start API server")
    serve_p.add_argument("--host", default="127.0.0.1")
    serve_p.add_argument("--port", type=int, default=8003)
    serve_p.add_argument("--reload", action="store_true", help="Enable auto-reload (dev)")

    # collect-all
    sub.add_parser("collect-all", help="Run all data collectors sequentially")

    # collect <source>
    collect_p = sub.add_parser("collect", help="Run a specific collector")
    collect_p.add_argument(
        "source",
        choices=["fred", "wdi", "ilo", "faostat", "bls", "imf_weo", "pwt",
                 "comtrade", "usda", "noaa", "vdem", "povcalnet"],
    )

    # generate-briefing <type>
    gen_p = sub.add_parser("generate-briefing", help="Generate a briefing document")
    gen_p.add_argument(
        "type",
        choices=["economic_conditions", "trade_flash", "country_deep_dive"],
    )
    gen_p.add_argument("--country", default=None, help="ISO3 country code (e.g. USA)")

    # status
    sub.add_parser("status", help="Show platform status")

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    if args.command == "serve":
        cmd_serve(args)
    elif args.command == "collect-all":
        asyncio.run(_collect_all())
    elif args.command == "collect":
        asyncio.run(_collect_one(args.source))
    elif args.command == "generate-briefing":
        asyncio.run(_generate_briefing(args.type, args.country))
    elif args.command == "status":
        asyncio.run(_status())
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
