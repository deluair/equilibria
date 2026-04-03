"""Briefing Orchestrator.

Coordinates all briefing generators. Manages generation schedules based on
cadence, maintains a priority queue for alert-driven briefings, and tracks
generation history to avoid redundant work.
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from enum import Enum

from app.layers.base import LayerBase

logger = logging.getLogger(__name__)

LAYER_IDS = ["l1", "l2", "l3", "l4", "l5"]


class BriefingType(str, Enum):
    ECONOMIC_CONDITIONS = "economic_conditions"
    TRADE_FLASH = "trade_flash"
    LABOR_PULSE = "labor_pulse"
    DEVELOPMENT_TRACKER = "development_tracker"
    AGRICULTURAL_OUTLOOK = "agricultural_outlook"
    POLICY_ALERT = "policy_alert"
    COUNTRY_DEEP_DIVE = "country_deep_dive"


class Priority(int, Enum):
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4


# Cadence definitions (in hours)
BRIEFING_CADENCE = {
    BriefingType.ECONOMIC_CONDITIONS: 168,     # Weekly
    BriefingType.TRADE_FLASH: 168,             # Weekly
    BriefingType.LABOR_PULSE: 720,             # Monthly (30 days)
    BriefingType.DEVELOPMENT_TRACKER: 2160,    # Quarterly (90 days)
    BriefingType.AGRICULTURAL_OUTLOOK: 720,    # Monthly
    BriefingType.POLICY_ALERT: 0,              # Event-driven (no cadence)
    BriefingType.COUNTRY_DEEP_DIVE: 0,         # On demand
}

# Signal thresholds that trigger policy alerts
ALERT_THRESHOLDS = {
    "CRISIS": Priority.CRITICAL,
    "STRESS": Priority.HIGH,
    "WATCH": Priority.NORMAL,
}

# Cooldown: minimum hours between same-type briefings for same country
ALERT_COOLDOWN_HOURS = 24


class BriefingOrchestrator(LayerBase):
    layer_id = "l6"
    name = "Briefing Orchestrator"

    async def compute(self, db, **kwargs) -> dict:
        country_iso3 = kwargs.get("country_iso3", "USA")
        force = kwargs.get("force", False)
        briefing_type = kwargs.get("briefing_type")
        check_only = kwargs.get("check_only", False)

        # Get generation history
        history = await self._get_generation_history(db, country_iso3)

        # Check for alert conditions
        alerts = await self._check_alert_conditions(db, country_iso3)

        # Build priority queue
        queue = self._build_priority_queue(
            history, alerts, country_iso3, force, briefing_type
        )

        if check_only:
            return {
                "score": 0.0,
                "signal": "STABLE",
                "queue": [self._queue_item_to_dict(item) for item in queue],
                "alerts": alerts,
                "history_summary": self._summarize_history(history),
                "country_iso3": country_iso3,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

        # Execute briefings in priority order
        results = []
        for item in queue:
            result = await self._generate_briefing(
                db, country_iso3, item
            )
            results.append(result)

        # Track generation
        await self._record_generation(db, country_iso3, results)

        n_generated = len([r for r in results if r.get("success")])
        n_alerts = len(alerts)
        score = min(n_alerts * 25.0, 100.0)

        return {
            "score": round(score, 2),
            "signal": self.classify_signal(score),
            "generated": results,
            "n_generated": n_generated,
            "queue_size": len(queue),
            "alerts": alerts,
            "history_summary": self._summarize_history(history),
            "country_iso3": country_iso3,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    async def _get_generation_history(
        self, db, country_iso3: str
    ) -> list[dict]:
        """Fetch recent briefing generation records."""
        return await db.fetch_all(
            """
            SELECT id, title, signal, created_at FROM briefings
            WHERE country_iso3 = ?
            ORDER BY created_at DESC LIMIT 50
            """,
            (country_iso3,),
        )

    async def _check_alert_conditions(
        self, db, country_iso3: str
    ) -> list[dict]:
        """Check if any layer signals warrant an alert briefing."""
        alerts = []

        # Check composite score
        composite = await db.fetch_one(
            """
            SELECT score, signal FROM analysis_results
            WHERE analysis_type = 'composite_score' AND country_iso3 = ?
            ORDER BY created_at DESC LIMIT 1
            """,
            (country_iso3,),
        )

        if composite and composite["signal"] in ALERT_THRESHOLDS:
            alerts.append({
                "type": "composite_signal",
                "signal": composite["signal"],
                "score": composite["score"],
                "priority": ALERT_THRESHOLDS[composite["signal"]].value,
                "message": f"Composite score at {composite['signal']} level ({composite['score']})",
            })

        # Check individual layer signals
        for lid in LAYER_IDS:
            row = await db.fetch_one(
                """
                SELECT score, signal FROM analysis_results
                WHERE layer = ? AND country_iso3 = ? AND score IS NOT NULL
                ORDER BY created_at DESC LIMIT 1
                """,
                (lid, country_iso3),
            )
            if row and row["signal"] in ("CRISIS", "STRESS"):
                alerts.append({
                    "type": f"layer_{lid}_signal",
                    "layer": lid,
                    "signal": row["signal"],
                    "score": row["score"],
                    "priority": ALERT_THRESHOLDS.get(row["signal"], Priority.NORMAL).value,
                    "message": f"Layer {lid} at {row['signal']} ({row['score']})",
                })

        # Check for structural breaks
        breaks = await db.fetch_one(
            """
            SELECT result FROM analysis_results
            WHERE analysis_type = 'cross_layer_break' AND country_iso3 = ?
            ORDER BY created_at DESC LIMIT 1
            """,
            (country_iso3,),
        )
        if breaks and breaks["result"]:
            try:
                break_data = json.loads(breaks["result"])
                if break_data.get("n_joint_breaks", 0) > 0:
                    alerts.append({
                        "type": "structural_break",
                        "signal": "WATCH",
                        "priority": Priority.HIGH.value,
                        "message": f"Joint structural break detected across {break_data.get('n_joint_breaks')} points",
                    })
            except (json.JSONDecodeError, TypeError):
                pass

        return sorted(alerts, key=lambda x: x["priority"])

    def _build_priority_queue(
        self, history: list[dict], alerts: list[dict],
        country_iso3: str, force: bool, specific_type: str | None,
    ) -> list[dict]:
        """Build ordered queue of briefings to generate."""
        now = datetime.now(timezone.utc)
        queue = []

        # Parse last generation time per briefing type
        last_gen = {}
        for h in history:
            title = h.get("title", "")
            for bt in BriefingType:
                if bt.value in title.lower().replace(" ", "_"):
                    if bt not in last_gen:
                        created = h.get("created_at", "")
                        try:
                            last_gen[bt] = datetime.fromisoformat(
                                created.replace("Z", "+00:00") if "Z" in created
                                else created
                            )
                        except (ValueError, TypeError):
                            pass

        # If specific type requested
        if specific_type:
            try:
                bt = BriefingType(specific_type)
                queue.append({
                    "type": bt,
                    "priority": Priority.NORMAL,
                    "reason": "requested",
                    "country_iso3": country_iso3,
                })
                return queue
            except ValueError:
                pass

        # Add alert-driven briefings
        if alerts:
            # Check cooldown
            last_alert = last_gen.get(BriefingType.POLICY_ALERT)
            cooldown_ok = (
                last_alert is None or
                force or
                (now - last_alert.replace(tzinfo=timezone.utc if last_alert.tzinfo is None else last_alert.tzinfo))
                > timedelta(hours=ALERT_COOLDOWN_HOURS)
            )
            if cooldown_ok:
                top_priority = min(a["priority"] for a in alerts)
                queue.append({
                    "type": BriefingType.POLICY_ALERT,
                    "priority": Priority(top_priority),
                    "reason": "alert_triggered",
                    "alerts": alerts,
                    "country_iso3": country_iso3,
                })

        # Add cadence-based briefings
        for bt, cadence_hours in BRIEFING_CADENCE.items():
            if cadence_hours == 0:
                continue  # event-driven or on-demand

            last = last_gen.get(bt)
            if force or last is None:
                due = True
            else:
                last_tz = last.replace(tzinfo=timezone.utc) if last.tzinfo is None else last
                hours_since = (now - last_tz).total_seconds() / 3600
                due = hours_since >= cadence_hours

            if due:
                queue.append({
                    "type": bt,
                    "priority": Priority.NORMAL,
                    "reason": "cadence_due" if not force else "forced",
                    "country_iso3": country_iso3,
                })

        # Sort by priority
        queue.sort(key=lambda x: x["priority"].value if isinstance(x["priority"], Priority) else x["priority"])

        return queue

    async def _generate_briefing(
        self, db, country_iso3: str, queue_item: dict
    ) -> dict:
        """Generate a single briefing.

        This is a coordination point: the actual content generation is handled
        by dedicated briefing modules in app/briefings/. This method tracks
        the generation attempt and stores metadata.
        """
        bt = queue_item["type"]
        now = datetime.now(timezone.utc)

        try:
            # Fetch data needed for the briefing
            data = await self._fetch_briefing_data(db, country_iso3, bt)

            # Build briefing content stub (actual AI generation is in app/briefings/)
            content = self._build_briefing_stub(bt, country_iso3, data, queue_item)

            # Store briefing record
            composite_score = data.get("composite_score")
            signal = data.get("signal", "UNAVAILABLE")

            await db.execute(
                """
                INSERT INTO briefings (country_iso3, title, content, layer_scores, composite_score, signal)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    country_iso3,
                    f"{bt.value}_{country_iso3}_{now.strftime('%Y%m%d')}",
                    json.dumps(content),
                    json.dumps(data.get("layer_scores", {})),
                    composite_score,
                    signal,
                ),
            )

            return {
                "type": bt.value,
                "success": True,
                "country_iso3": country_iso3,
                "timestamp": now.isoformat(),
                "data_summary": {
                    "composite": composite_score,
                    "signal": signal,
                },
            }

        except Exception as e:
            logger.exception("Briefing generation failed: %s", bt.value)
            return {
                "type": bt.value,
                "success": False,
                "error": str(e),
                "country_iso3": country_iso3,
                "timestamp": now.isoformat(),
            }

    async def _fetch_briefing_data(
        self, db, country_iso3: str, bt: BriefingType
    ) -> dict:
        """Fetch relevant analysis data for a briefing type."""
        data = {"layer_scores": {}}

        # Always fetch composite
        composite = await db.fetch_one(
            """
            SELECT score, signal FROM analysis_results
            WHERE analysis_type = 'composite_score' AND country_iso3 = ?
            ORDER BY created_at DESC LIMIT 1
            """,
            (country_iso3,),
        )
        if composite:
            data["composite_score"] = composite["score"]
            data["signal"] = composite["signal"]

        # Fetch layer scores
        for lid in LAYER_IDS:
            row = await db.fetch_one(
                """
                SELECT score FROM analysis_results
                WHERE layer = ? AND country_iso3 = ? AND score IS NOT NULL
                ORDER BY created_at DESC LIMIT 1
                """,
                (lid, country_iso3),
            )
            if row:
                data["layer_scores"][lid] = row["score"]

        # Type-specific data
        if bt == BriefingType.POLICY_ALERT:
            breaks = await db.fetch_one(
                """
                SELECT result FROM analysis_results
                WHERE analysis_type = 'cross_layer_break' AND country_iso3 = ?
                ORDER BY created_at DESC LIMIT 1
                """,
                (country_iso3,),
            )
            if breaks and breaks["result"]:
                try:
                    data["breaks"] = json.loads(breaks["result"])
                except (json.JSONDecodeError, TypeError):
                    pass

        elif bt == BriefingType.COUNTRY_DEEP_DIVE:
            profile = await db.fetch_one(
                """
                SELECT result FROM analysis_results
                WHERE analysis_type = 'country_profile' AND country_iso3 = ?
                ORDER BY created_at DESC LIMIT 1
                """,
                (country_iso3,),
            )
            if profile and profile["result"]:
                try:
                    data["profile"] = json.loads(profile["result"])
                except (json.JSONDecodeError, TypeError):
                    pass

        return data

    def _build_briefing_stub(
        self, bt: BriefingType, country_iso3: str,
        data: dict, queue_item: dict,
    ) -> dict:
        """Build briefing content structure.

        The actual narrative is generated by the AI briefing modules.
        This provides the data scaffold.
        """
        return {
            "type": bt.value,
            "country_iso3": country_iso3,
            "composite_score": data.get("composite_score"),
            "signal": data.get("signal"),
            "layer_scores": data.get("layer_scores", {}),
            "generation_reason": queue_item.get("reason", "unknown"),
            "alerts": queue_item.get("alerts", []),
            "status": "data_ready",
            "narrative": None,  # populated by AI briefing generators
        }

    async def _record_generation(
        self, db, country_iso3: str, results: list[dict]
    ):
        """Record generation batch in analysis_results for tracking."""
        successful = [r for r in results if r.get("success")]
        failed = [r for r in results if not r.get("success")]

        await db.execute(
            """
            INSERT INTO analysis_results (analysis_type, country_iso3, layer, parameters, result, score, signal)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "briefing_generation",
                country_iso3,
                "l6",
                json.dumps({
                    "n_requested": len(results),
                    "n_successful": len(successful),
                    "n_failed": len(failed),
                }),
                json.dumps({
                    "types_generated": [r["type"] for r in successful],
                    "errors": [{"type": r["type"], "error": r.get("error")} for r in failed],
                }),
                0.0,
                "STABLE",
            ),
        )

    def _summarize_history(self, history: list[dict]) -> dict:
        """Summarize recent generation history."""
        if not history:
            return {"total": 0, "most_recent": None, "types": {}}

        type_counts = {}
        for h in history:
            title = h.get("title", "")
            for bt in BriefingType:
                if bt.value in title:
                    type_counts[bt.value] = type_counts.get(bt.value, 0) + 1
                    break

        return {
            "total": len(history),
            "most_recent": history[0].get("created_at") if history else None,
            "types": type_counts,
        }

    @staticmethod
    def _queue_item_to_dict(item: dict) -> dict:
        """Convert queue item to JSON-serializable dict."""
        return {
            "type": item["type"].value if isinstance(item["type"], BriefingType) else item["type"],
            "priority": item["priority"].value if isinstance(item["priority"], Priority) else item["priority"],
            "reason": item.get("reason", "unknown"),
        }
