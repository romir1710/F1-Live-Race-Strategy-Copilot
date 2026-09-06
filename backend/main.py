"""
main.py — Single consolidated FastAPI application for the F1 Live Race Strategy Copilot.

Background asyncio tasks (started on startup, no separate services needed):
  - ReplayProducer: reads raw_laps from Neon Postgres row-by-row, publishes to Aiven Kafka
  - StrategyConsumer: consumes lap_events from Kafka, runs strategy_engine, writes to Upstash Redis
  - Keepalive: sends a heartbeat to Aiven Kafka every 5min to prevent free-tier sleep

HTTP endpoints:
  GET  /health          → wake-up ping target (replicate FlowState pattern)
  GET  /ws              → WebSocket: broadcasts race state to all connected frontends
  POST /agent           → LangGraph natural-language strategy agent

One Render web service. One wake-up ping covers everything.
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
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from upstash_redis import AsyncRedis

from strategy_engine import (
    DriverState, compute_strategy_options, TOTAL_LAPS
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("main")

# ── Config ────────────────────────────────────────────────────────────────────
DATABASE_URL       = os.environ["DATABASE_URL"]          # asyncpg format
KAFKA_SERVERS      = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_USERNAME     = os.environ["KAFKA_SASL_USERNAME"]
KAFKA_PASSWORD     = os.environ["KAFKA_SASL_PASSWORD"]
KAFKA_CA_CERT_PATH = os.environ.get("KAFKA_CA_CERT_PATH", "./kafka-ca.pem")
REDIS_URL          = os.environ["UPSTASH_REDIS_REST_URL"]
REDIS_TOKEN        = os.environ["UPSTASH_REDIS_REST_TOKEN"]
GOOGLE_API_KEY     = os.environ["GOOGLE_API_KEY"]
SESSION_KEY        = int(os.environ.get("SESSION_KEY", "9158"))
FOCUS_DRIVERS      = [int(x) for x in os.environ.get("FOCUS_DRIVERS", "1,11,16").split(",")]
REPLAY_SPEED       = float(os.environ.get("REPLAY_SPEED", "1.0"))  # seconds per simulated lap
ENVIRONMENT        = os.environ.get("ENVIRONMENT", "production")

KAFKA_TOPIC        = "lap_events"
KAFKA_KEEPALIVE    = "keepalive"
REDIS_RACE_KEY     = "race:live"
REDIS_STRATEGY_KEY = "strategy:{dn}"
REDIS_DRIVER_KEY   = "drivers:meta"

# ── Shared state ──────────────────────────────────────────────────────────────
connected_clients: list[WebSocket] = []
replay_paused = False
replay_current_lap = 1


# ── Kafka SSL helper ──────────────────────────────────────────────────────────
def kafka_ssl_context():
    import ssl
    ctx = ssl.create_default_context()
    if os.path.exists(KAFKA_CA_CERT_PATH):
        ctx.load_verify_locations(KAFKA_CA_CERT_PATH)
    return ctx


# ── Background task: Replay Producer ─────────────────────────────────────────
async def replay_producer_task():
    """
    Reads lap rows from Neon Postgres in replay order and publishes
    one lap_event message per driver per lap to Kafka.
    Runs at REPLAY_SPEED seconds per simulated lap.
    Loops back to lap 1 after race completes.
    """
    global replay_current_lap, replay_paused

    log.info("ReplayProducer: connecting to Neon Postgres...")
    pool = await asyncpg.create_pool(
        DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://"),
        min_size=1, max_size=3
    )

    log.info("ReplayProducer: connecting to Aiven Kafka...")
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        ssl_context=kafka_ssl_context(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    await producer.start()
    log.info("ReplayProducer: started ✓")

    try:
        while True:
            # Fetch all laps for this session, ordered by lap_number then driver
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
            log.info(f"ReplayProducer: {len(laps)} rows fetched, {total_laps} laps — starting replay")

            current_lap = 1
            for row in laps:
                while replay_paused:
                    await asyncio.sleep(0.1)

                lap_num = row["lap_number"]

                # When lap number advances, sleep to simulate real time
                if lap_num > current_lap:
                    await asyncio.sleep(REPLAY_SPEED)
                    current_lap = lap_num
                    replay_current_lap = current_lap

                # Compute tyre age for this lap
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

                await producer.send(KAFKA_TOPIC, event)

            # Race finished — signal restart
            log.info(f"ReplayProducer: race complete (lap {total_laps}). Restarting in 5s...")
            await broadcast_all_clients({"type": "race_finished", "restart_in": 5})
            await asyncio.sleep(5)
            # Loop back to top → next iteration restarts from lap 1

    except asyncio.CancelledError:
        log.info("ReplayProducer: shutting down")
    finally:
        await producer.stop()
        await pool.close()


# ── Background task: Strategy Consumer ───────────────────────────────────────
async def strategy_consumer_task():
    """
    Consumes lap_events from Kafka, runs strategy_engine.py per focus driver,
    writes results to Upstash Redis for the WebSocket broadcaster to serve.
    """
    log.info("StrategyConsumer: connecting to Aiven Kafka...")
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        ssl_context=kafka_ssl_context(),
        group_id="strategy-consumer",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode()),
    )
    await consumer.start()
    log.info("StrategyConsumer: started ✓")

    redis = AsyncRedis(url=REDIS_URL, token=REDIS_TOKEN)

    # rolling state: driver_number → latest event
    driver_states: dict[int, dict] = {}

    try:
        async for msg in consumer:
            event: dict = msg.value
            dn = event["driver_number"]
            lap = event["lap_number"]
            driver_states[dn] = event

            # Write raw race state to Redis every lap, every driver
            await redis.hset(REDIS_RACE_KEY, mapping={
                "current_lap": lap,
                "total_laps": event.get("total_laps", TOTAL_LAPS),
                "session_key": SESSION_KEY,
                "ts": event["ts"],
            })

            # Write per-driver state
            await redis.hset(f"driver:{dn}", mapping={
                "lap_number": lap,
                "position": event.get("position") or 99,
                "compound": event["compound"],
                "tyre_age": event["tyre_age"],
                "gap_to_leader": event.get("gap_to_leader") or -1,
                "interval_gap": event.get("interval_gap") or -1,
                "lap_duration": event.get("lap_duration") or -1,
                "is_pit_lap": int(event.get("is_pit_lap", False)),
                "abbreviation": event.get("abbreviation", str(dn)),
                "team_colour": event.get("team_colour", "FFFFFF"),
                "team_name": event.get("team_name", "Unknown"),
            })

            # Compute strategy options only for focus drivers
            if dn in FOCUS_DRIVERS and len(driver_states) >= 3:
                _run_strategy(dn, lap, event, driver_states, redis)

    except asyncio.CancelledError:
        log.info("StrategyConsumer: shutting down")
    finally:
        await consumer.stop()


def _make_driver_state(dn: int, event: dict, lap: int, driver_states: dict) -> DriverState:
    total = event.get("total_laps", TOTAL_LAPS)
    # Find who's directly ahead/behind from current snapshot
    all_positions = {d: s.get("position", 99) for d, s in driver_states.items() if s.get("position")}
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
        gap_to_leader=event.get("gap_to_leader") if event.get("gap_to_leader", -1) >= 0 else None,
        gap_ahead=gaps_ahead[0] if gaps_ahead else None,
        gap_behind=None,
        last_lap_time=event.get("lap_duration") if event.get("lap_duration", -1) > 0 else None,
        laps_remaining=max(1, total - lap),
    )


def _run_strategy(dn: int, lap: int, event: dict, driver_states: dict, redis):
    """Compute strategy options and schedule Redis write (non-blocking)."""
    try:
        focus_states = [
            _make_driver_state(d, driver_states[d], lap, driver_states)
            for d in FOCUS_DRIVERS if d in driver_states
        ]
        target = _make_driver_state(dn, event, lap, driver_states)
        result = compute_strategy_options(target, focus_states)
        asyncio.ensure_future(
            redis.set(
                REDIS_STRATEGY_KEY.format(dn=dn),
                json.dumps(result.to_dict()),
                ex=300  # expire after 5 min safety
            )
        )
    except Exception as e:
        log.warning(f"Strategy compute failed for driver {dn}: {e}")


# ── Background task: Aiven Keepalive ─────────────────────────────────────────
async def kafka_keepalive_task():
    """
    Sends a keepalive heartbeat to Aiven Kafka every 5 minutes.
    Prevents Aiven free-tier auto-power-off during idle periods (e.g. between replay loops).
    """
    log.info("KafkaKeepalive: started")
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        security_protocol="SASL_SSL",
        sasl_mechanism="PLAIN",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        ssl_context=kafka_ssl_context(),
        value_serializer=lambda v: json.dumps(v).encode(),
    )
    await producer.start()
    try:
        while True:
            await asyncio.sleep(300)  # 5 minutes
            await producer.send(KAFKA_KEEPALIVE, {"type": "keepalive", "ts": time.time()})
            log.debug("KafkaKeepalive: heartbeat sent")
    except asyncio.CancelledError:
        log.info("KafkaKeepalive: shutting down")
    finally:
        await producer.stop()


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
    Reads race state from Upstash Redis every 2 seconds and broadcasts
    a consolidated frame to all connected WebSocket clients.
    """
    redis = AsyncRedis(url=REDIS_URL, token=REDIS_TOKEN)
    log.info("WSBroadcaster: started")

    try:
        while True:
            await asyncio.sleep(2)
            if not connected_clients:
                continue

            # Build broadcast frame
            race_meta = await redis.hgetall(REDIS_RACE_KEY)
            if not race_meta:
                continue

            # Fetch all driver states
            drivers_frame = []
            all_driver_keys = await redis.keys("driver:*")
            for key in all_driver_keys:
                d = await redis.hgetall(key)
                if d:
                    dn_str = key.split(":")[-1]
                    d["driver_number"] = dn_str
                    drivers_frame.append(d)

            # Fetch strategy for focus drivers
            strategies = {}
            for dn in FOCUS_DRIVERS:
                raw = await redis.get(REDIS_STRATEGY_KEY.format(dn=dn))
                if raw:
                    strategies[dn] = json.loads(raw)

            frame = {
                "type": "race_update",
                "race": race_meta,
                "drivers": sorted(drivers_frame, key=lambda d: int(d.get("position", 99))),
                "strategies": strategies,
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

    # Validate required env vars before starting tasks
    missing = [v for v in ["DATABASE_URL", "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_SASL_USERNAME",
                            "KAFKA_SASL_PASSWORD", "UPSTASH_REDIS_REST_URL",
                            "UPSTASH_REDIS_REST_TOKEN", "GOOGLE_API_KEY"] if not os.environ.get(v)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {missing}")

    tasks = [
        asyncio.create_task(replay_producer_task(), name="ReplayProducer"),
        asyncio.create_task(strategy_consumer_task(), name="StrategyConsumer"),
        asyncio.create_task(ws_broadcaster_task(), name="WSBroadcaster"),
        asyncio.create_task(kafka_keepalive_task(), name="KafkaKeepalive"),
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
    """Wake-up ping target — same pattern as FlowState producer health endpoint."""
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
        # Send a "connected" ack immediately so frontend can drop "Connecting..." overlay
        await ws.send_json({"type": "connected", "focus_drivers": FOCUS_DRIVERS})
        while True:
            # Keep connection alive; actual data is pushed by broadcaster
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
    driver_number: Optional[int] = None  # context hint

class AgentResponse(BaseModel):
    answer: str
    sim_table: Optional[dict] = None
    grounded: bool = True  # always True unless quota fallback
    quota_fallback: bool = False


@app.post("/agent", response_model=AgentResponse)
async def agent_endpoint(req: AgentRequest):
    """
    Natural-language strategy agent powered by LangGraph + Gemini 3.1 Flash-Lite.
    Reads live race state from Upstash Redis, runs what-if simulation if needed,
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
