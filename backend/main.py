"""
main.py — Single consolidated FastAPI application for the F1 Live Race Strategy Copilot.

Architecture (no Kafka, no Redis — zero external messaging/cache dependencies):
  - ReplayProducer: reads raw_laps from Neon Postgres in lap order,
                    notifies via Postgres LISTEN/NOTIFY (pg_notify).
  - StrategyConsumer: receives LISTEN notifications via asyncpg,
                      runs strategy_engine, writes to in-process RACE_STATE dict.
  - WSBroadcaster: reads RACE_STATE every 2s and pushes to all WebSocket clients.

All state lives in RACE_STATE — a plain asyncio-safe Python dict. No Redis needed.
One Render web service, single process, one wake-up ping covers everything.

HTTP endpoints:
  GET  /health  → wake-up ping target
  GET  /ws      → WebSocket: broadcasts race state to all connected frontends
  POST /agent   → LangGraph natural-language strategy agent
"""
from __future__ import annotations
import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from strategy_engine import (
    DriverState, compute_strategy_options, TOTAL_LAPS
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("main")

# ── Config ────────────────────────────────────────────────────────────────────
DATABASE_URL   = os.environ["DATABASE_URL"]          # postgresql+asyncpg://...
GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
SESSION_KEY    = int(os.environ.get("SESSION_KEY", "9158"))
FOCUS_DRIVERS  = [int(x) for x in os.environ.get("FOCUS_DRIVERS", "1,11,16").split(",")]
REPLAY_SPEED   = float(os.environ.get("REPLAY_SPEED", "1.0"))  # seconds per simulated lap
ENVIRONMENT    = os.environ.get("ENVIRONMENT", "production")

# asyncpg wants plain postgresql:// (not postgresql+asyncpg://)
_PG_URL = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")

PG_NOTIFY_CHANNEL = "telemetry_replay"

# ── Shared in-memory race state (replaces Redis) ──────────────────────────────
# Keyed structure:
#   RACE_STATE["meta"]            → {current_lap, total_laps, session_key, ts}
#   RACE_STATE["drivers"][dn]     → per-driver state dict
#   RACE_STATE["strategies"][dn]  → StrategyResult dict (focus drivers only)
RACE_STATE: dict = {
    "meta": {},
    "drivers": {},
    "strategies": {},
}

# ── Shared WebSocket client list ──────────────────────────────────────────────
connected_clients: list[WebSocket] = []
replay_current_lap: int = 1


# ── Background task: Replay Producer ─────────────────────────────────────────
async def replay_producer_task():
    """
    Reads lap rows from Neon Postgres in replay order and publishes
    one lap_event JSON payload per driver per lap via pg_notify().
    Runs at REPLAY_SPEED seconds per simulated lap.
    Loops back to lap 1 after race completes.
    """
    global replay_current_lap

    log.info("ReplayProducer: connecting to Neon Postgres...")
    pool = await asyncpg.create_pool(_PG_URL, min_size=1, max_size=3)
    log.info("ReplayProducer: started ✓")

    try:
        while True:
            async with pool.acquire() as conn:
                laps = await conn.fetch("""
                    SELECT l.driver_number, l.lap_number, l.lap_duration,
                           l.is_pit_out_lap, l.i1_speed, l.st_speed,
                           s.compound, s.tyre_age_at_start,
                           s.lap_start as stint_lap_start,
                           p.pit_duration,
                           pos.position,
                           iv.gap_to_leader, iv.interval_gap,
                           d.abbreviation, d.team_name, d.team_colour
                    FROM raw_laps l
                    LEFT JOIN raw_stints s ON (
                        s.session_key = l.session_key
                        AND s.driver_number = l.driver_number
                        AND l.lap_number >= s.lap_start
                        AND l.lap_number <= COALESCE(s.lap_end, 999)
                    )
                    LEFT JOIN raw_pits p ON (
                        p.session_key = l.session_key
                        AND p.driver_number = l.driver_number
                        AND p.lap_number = l.lap_number
                    )
                    LEFT JOIN raw_positions pos ON (
                        pos.session_key = l.session_key
                        AND pos.driver_number = l.driver_number
                        AND pos.lap_number = l.lap_number
                    )
                    LEFT JOIN raw_intervals iv ON (
                        iv.session_key = l.session_key
                        AND iv.driver_number = l.driver_number
                        AND iv.lap_number = l.lap_number
                    )
                    LEFT JOIN raw_drivers d ON (
                        d.session_key = l.session_key
                        AND d.driver_number = l.driver_number
                    )
                    WHERE l.session_key = $1
                    ORDER BY l.lap_number ASC, l.driver_number ASC
                """, SESSION_KEY)

            total_laps = max((r["lap_number"] for r in laps), default=TOTAL_LAPS)
            log.info(f"ReplayProducer: {len(laps)} rows, {total_laps} laps — starting replay")

            # Separate connection for NOTIFY (cannot share with pool acquire easily)
            notify_conn = await asyncpg.connect(_PG_URL)
            try:
                current_lap = 1
                for row in laps:
                    lap_num = row["lap_number"]

                    if lap_num > current_lap:
                        await asyncio.sleep(REPLAY_SPEED)
                        current_lap = lap_num
                        replay_current_lap = current_lap

                    stint_start = row["stint_lap_start"] or 1
                    tyre_age_at_start = row["tyre_age_at_start"] or 0
                    tyre_age = tyre_age_at_start + (lap_num - stint_start)

                    event = {
                        "session_key": SESSION_KEY,
                        "lap_number": lap_num,
                        "total_laps": total_laps,
                        "driver_number": row["driver_number"],
                        "abbreviation": row["abbreviation"] or str(row["driver_number"]),
                        "team_name": row["team_name"] or "Unknown",
                        "team_colour": row["team_colour"] or "FFFFFF",
                        "lap_duration": row["lap_duration"],
                        "is_pit_out_lap": row["is_pit_out_lap"] or False,
                        "is_pit_lap": row["pit_duration"] is not None,
                        "pit_duration": row["pit_duration"],
                        "compound": row["compound"] or "UNKNOWN",
                        "tyre_age": tyre_age,
                        "position": row["position"],
                        "gap_to_leader": row["gap_to_leader"],
                        "interval_gap": row["interval_gap"],
                        "st_speed": row["st_speed"],
                        "ts": time.time(),
                    }

                    # Publish via Postgres NOTIFY — StrategyConsumer picks this up
                    await notify_conn.execute(
                        f"SELECT pg_notify('{PG_NOTIFY_CHANNEL}', $1)",
                        json.dumps(event)
                    )
            finally:
                await notify_conn.close()

            log.info(f"ReplayProducer: race complete (lap {total_laps}). Restarting in 5s...")
            await broadcast_all_clients({"type": "race_finished", "restart_in": 5})
            await asyncio.sleep(5)

    except asyncio.CancelledError:
        log.info("ReplayProducer: shutting down")
    finally:
        await pool.close()


# ── Background task: Strategy Consumer ───────────────────────────────────────
async def strategy_consumer_task():
    """
    LISTENs on the Postgres NOTIFY channel. For each lap_event received,
    updates the in-process RACE_STATE dict and recomputes strategy options
    for focus drivers. No Redis — no external calls needed.
    """
    log.info("StrategyConsumer: connecting to Neon Postgres (LISTEN)...")
    conn = await asyncpg.connect(_PG_URL)

    # rolling driver state keyed by driver_number
    driver_states: dict[int, dict] = {}

    async def _on_notification(conn, pid, channel, payload):
        try:
            event: dict = json.loads(payload)
            dn = event["driver_number"]
            lap = event["lap_number"]
            driver_states[dn] = event

            # Update shared in-memory race meta
            RACE_STATE["meta"] = {
                "current_lap": lap,
                "total_laps": event.get("total_laps", TOTAL_LAPS),
                "session_key": SESSION_KEY,
                "ts": event["ts"],
            }

            # Update per-driver state
            RACE_STATE["drivers"][dn] = {
                "lap_number": lap,
                "position": event.get("position") or 99,
                "compound": event["compound"],
                "tyre_age": event["tyre_age"],
                "gap_to_leader": event.get("gap_to_leader") if event.get("gap_to_leader", -1) and event.get("gap_to_leader", -1) >= 0 else -1,
                "interval_gap": event.get("interval_gap") if event.get("interval_gap", -1) and event.get("interval_gap", -1) >= 0 else -1,
                "lap_duration": event.get("lap_duration") or -1,
                "is_pit_lap": int(event.get("is_pit_lap", False)),
                "abbreviation": event.get("abbreviation", str(dn)),
                "team_colour": event.get("team_colour", "FFFFFF"),
                "team_name": event.get("team_name", "Unknown"),
                "driver_number": dn,
            }

            # Compute strategy only for focus drivers once we have enough state
            if dn in FOCUS_DRIVERS and len(driver_states) >= 3:
                _run_strategy(dn, lap, event, driver_states)

        except Exception as e:
            log.warning(f"StrategyConsumer notification error: {e}")

    await conn.add_listener(PG_NOTIFY_CHANNEL, _on_notification)
    log.info("StrategyConsumer: listening on channel '%s' ✓", PG_NOTIFY_CHANNEL)

    try:
        # Keep connection alive indefinitely; asyncpg fires callbacks on notifications
        while True:
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        log.info("StrategyConsumer: shutting down")
    finally:
        await conn.remove_listener(PG_NOTIFY_CHANNEL, _on_notification)
        await conn.close()


# ── Strategy helpers ──────────────────────────────────────────────────────────

def _make_driver_state(dn: int, event: dict, lap: int, driver_states: dict) -> DriverState:
    total = event.get("total_laps", TOTAL_LAPS)
    my_pos = event.get("position") or 99
    gaps_ahead = [
        s.get("interval_gap", 9999) for d, s in driver_states.items()
        if s.get("position") and s["position"] == my_pos - 1 and s.get("interval_gap")
    ]
    return DriverState(
        driver_number=dn,
        name=event.get("abbreviation", str(dn)),
        team=event.get("team_name", "Unknown"),
        team_colour=event.get("team_colour", "FFFFFF"),
        position=my_pos,
        current_lap=lap,
        compound=event.get("compound", "HARD"),
        tyre_age=event.get("tyre_age", 0),
        gap_to_leader=event.get("gap_to_leader") if event.get("gap_to_leader", -1) and event.get("gap_to_leader", -1) >= 0 else None,
        gap_ahead=gaps_ahead[0] if gaps_ahead else None,
        gap_behind=None,
        last_lap_time=event.get("lap_duration") if event.get("lap_duration", -1) and event.get("lap_duration", -1) > 0 else None,
        laps_remaining=max(1, total - lap),
    )


def _run_strategy(dn: int, lap: int, event: dict, driver_states: dict):
    """Compute strategy options and update RACE_STATE["strategies"] in-process."""
    try:
        focus_states = [
            _make_driver_state(d, driver_states[d], lap, driver_states)
            for d in FOCUS_DRIVERS if d in driver_states
        ]
        target = _make_driver_state(dn, event, lap, driver_states)
        result = compute_strategy_options(target, focus_states)
        RACE_STATE["strategies"][dn] = result.to_dict()
    except Exception as e:
        log.warning(f"Strategy compute failed for driver {dn}: {e}")


# ── WebSocket broadcaster ─────────────────────────────────────────────────────
async def broadcast_all_clients(message: dict):
    dead: list[WebSocket] = []
    for ws in connected_clients:
        try:
            await ws.send_json(message)
        except Exception:
            dead.append(ws)
    for ws in dead:
        connected_clients.remove(ws)


async def ws_broadcaster_task():
    """
    Reads RACE_STATE from memory every 2 seconds and broadcasts
    a consolidated frame to all connected WebSocket clients.
    """
    log.info("WSBroadcaster: started")
    try:
        while True:
            await asyncio.sleep(2)
            if not connected_clients:
                continue
            if not RACE_STATE["meta"]:
                continue

            drivers_frame = sorted(
                RACE_STATE["drivers"].values(),
                key=lambda d: int(d.get("position", 99))
            )

            frame = {
                "type": "race_update",
                "race": RACE_STATE["meta"],
                "drivers": drivers_frame,
                "strategies": {str(k): v for k, v in RACE_STATE["strategies"].items()},
                "focus_drivers": FOCUS_DRIVERS,
            }
            await broadcast_all_clients(frame)
    except asyncio.CancelledError:
        log.info("WSBroadcaster: shutting down")


# ── App lifespan ──────────────────────────────────────────────────────────────
background_tasks: list[asyncio.Task] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Starting F1 Race Strategy Copilot backend...")

    missing = [v for v in ["DATABASE_URL", "GOOGLE_API_KEY"] if not os.environ.get(v)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {missing}")

    tasks = [
        asyncio.create_task(replay_producer_task(), name="ReplayProducer"),
        asyncio.create_task(strategy_consumer_task(), name="StrategyConsumer"),
        asyncio.create_task(ws_broadcaster_task(), name="WSBroadcaster"),
    ]
    background_tasks.extend(tasks)

    yield  # app is running

    log.info("Shutting down background tasks...")
    for task in background_tasks:
        task.cancel()
    await asyncio.gather(*background_tasks, return_exceptions=True)
    log.info("Shutdown complete.")


# ── FastAPI app ───────────────────────────────────────────────────────────────
app = FastAPI(title="F1 Race Strategy Copilot", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # tighten to your Vercel domain in production
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    """Wake-up ping target — silent fetch on frontend mount wakes Render before visitor notices."""
    return {
        "status": "ok",
        "session_key": SESSION_KEY,
        "focus_drivers": FOCUS_DRIVERS,
        "replay_speed": REPLAY_SPEED,
        "current_lap": replay_current_lap,
        "clients": len(connected_clients),
    }


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    """
    WebSocket connection for real-time race state + strategy updates.
    Frontend connects here; broadcaster task pushes frames every 2s.
    """
    await ws.accept()
    connected_clients.append(ws)
    log.info(f"WebSocket client connected. Total: {len(connected_clients)}")
    try:
        await ws.send_json({"type": "connected", "focus_drivers": FOCUS_DRIVERS})
        while True:
            await asyncio.sleep(30)
            await ws.send_json({"type": "ping"})
    except WebSocketDisconnect:
        connected_clients.remove(ws)
        log.info(f"WebSocket client disconnected. Remaining: {len(connected_clients)}")
    except Exception as e:
        if ws in connected_clients:
            connected_clients.remove(ws)
        log.warning(f"WebSocket error: {e}")


# ── Agent endpoint (LangGraph) ────────────────────────────────────────────────

class AgentRequest(BaseModel):
    question: str
    driver_number: Optional[int] = None


class AgentResponse(BaseModel):
    answer: str
    sim_table: Optional[dict] = None
    grounded: bool = True
    quota_fallback: bool = False


@app.post("/agent", response_model=AgentResponse)
async def agent_endpoint(req: AgentRequest):
    """
    Natural-language strategy agent powered by LangGraph + Gemini 3.6 Flash.
    Reads live race state from in-process RACE_STATE, runs what-if simulation,
    and returns a grounded answer — not a canned LLM response.
    """
    from agent import run_agent  # lazy import to avoid slowing startup
    try:
        result = await run_agent(req.question, req.driver_number)
        return AgentResponse(**result)
    except Exception as e:
        log.error(f"Agent error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), reload=False)
