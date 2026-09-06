# F1 Live Race Strategy Copilot

> Real-time Formula 1 race strategy simulation with a natural-language AI agent. Built as a portfolio project for placement applications to Mercedes-AMG (F1/HPP), Cadillac F1, McLaren, and Aston Martin Aramco.

[![Vercel](https://img.shields.io/badge/Frontend-Vercel-black)](https://vercel.com)
[![Render](https://img.shields.io/badge/Backend-Render-blue)](https://render.com)
[![Kafka](https://img.shields.io/badge/Broker-Aiven_Kafka-red)](https://aiven.io)
[![Redis](https://img.shields.io/badge/Cache-Upstash_Redis-green)](https://upstash.com)
[![Neon](https://img.shields.io/badge/Database-Neon_Postgres-teal)](https://neon.tech)

---

## What it does

### Part 1 — Live Strategy Engine
Replays real 2024 Bahrain GP telemetry (lap times, tyre stints, pit stops, gaps) at accelerated speed through a Kafka pipeline, and **continuously computes a branching comparison of 3 strategy options** per focus driver:

| Option | Description |
|--------|-------------|
| **Stay Out** | Projects degradation cost of extending current stint to race end |
| **Undercut** | Simulates pitting this lap — models pit lane time loss vs fresh-tyre pace gain and potential position jump |
| **Overcut** | Stays out N laps then pits — models track position retention vs rivals pitting first |

Each option shows: projected finish position, time delta vs stay-out, tyre life risk (LOW/MEDIUM/HIGH), and a 1-sentence human-readable reasoning. The recommended option is highlighted. All projections update in real time as the replay advances.

### Part 2 — Natural Language Agent
A LangGraph agent answers questions like *"what if Perez had pitted a lap earlier?"* by **actually re-running the strategy engine** with modified parameters — not a canned LLM response. Powered by Gemini 3.1 Flash-Lite with a graceful quota fallback that returns raw simulation data when the daily free limit is hit.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  Neon Postgres (free-forever)                                       │
│  Pre-fetched 2024 Bahrain Race data:                                │
│  raw_laps, raw_stints, raw_pits, raw_intervals, raw_positions       │
└────────────────────────────┬────────────────────────────────────────┘
                             │ reads rows in lap order
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Render (single web service — one wake-up ping covers all)          │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  FastAPI app (main.py)                                      │   │
│  │                                                             │   │
│  │  AsyncIO background tasks:                                  │   │
│  │  ├── ReplayProducer  → publishes to Aiven Kafka             │   │
│  │  ├── StrategyConsumer ← consumes Kafka → strategy_engine   │   │
│  │  │                    → writes to Upstash Redis             │   │
│  │  ├── WSBroadcaster   → reads Redis → WebSocket push        │   │
│  │  └── KafkaKeepalive  → heartbeat every 5min (Aiven wakeup) │   │
│  │                                                             │   │
│  │  HTTP routes:                                               │   │
│  │  GET  /health   ← wake-up ping from frontend               │   │
│  │  GET  /ws       ← WebSocket for frontend                   │   │
│  │  POST /agent    ← LangGraph what-if agent                  │   │
│  └─────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────┘
                             │ Kafka topic: lap_events (~300B/msg)
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Aiven Kafka (free tier — 5 topics, 250 KiB/s throughput)          │
│  Throughput at 1×: ~20 drivers × 300B/lap × 1 lap/sec = 6 KB/s    │
│  Well within limits.                                                │
└─────────────────────────────────────────────────────────────────────┘
                             │ reads every 2s
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Upstash Redis (serverless, free tier)                              │
│  race:live HASH  — current_lap, total_laps, session_key, ts        │
│  driver:{n} HASH — position, compound, tyre_age, gap, lap_time     │
│  strategy:{n} JSON — 3 StrategyOptions + recommended + model note  │
└─────────────────────────────────────────────────────────────────────┘
                             │ WebSocket push every 2s
                             ▼
┌─────────────────────────────────────────────────────────────────────┐
│  Vercel (Next.js 15, Tailwind CSS)                                  │
│  ├── Race Tower — live standings, tyre badges, gaps                 │
│  ├── Strategy Cards — 3-way branching comparison, updates live     │
│  ├── Agent Chat — natural language, grounded in simulation         │
│  └── /api/agent — thin proxy to Render /agent                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Agent path (natural language)
```
User question → /api/agent (Vercel proxy)
  → POST /agent (Render)
    → read_race_state node  (reads Upstash Redis)
    → parse_intent node     (Gemini: classify what-if/explain/compare)
    → run_simulation node   (re-runs strategy_engine with modified params)
    → compose_answer node   (Gemini: grounded answer from sim data)
  → response: { answer, sim_table, grounded, quota_fallback }
```

---

## Strategy model

The engine uses a **first-order linear tyre degradation model**, fit from actual 2024 Bahrain lap time distributions in the fetched data:

| Compound | Pace delta vs Hard | Degradation rate |
|----------|-------------------|-----------------|
| SOFT     | −1.2 s/lap        | 0.085 s/lap/lap |
| MEDIUM   | −0.5 s/lap        | 0.040 s/lap/lap |
| HARD     | baseline          | 0.018 s/lap/lap |

Position simulation for undercuts: models which cars within 1.5× pit-delta gap can be jumped if their tyres are significantly older. This is labelled as a model in the UI — the goal is directional accuracy and legibility, not a Monte Carlo simulation.

---

## Quick start (local)

### Prerequisites
- Python 3.12+ (managed by uv)
- Node 18+
- Accounts: Neon, Aiven, Upstash, Google AI Studio

### Backend
```bash
cd backend
cp .env.example .env
# Fill in all credentials in .env

# Install uv (if not already)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Create venv and install deps
uv venv --python 3.12
source .venv/bin/activate
uv pip install -r requirements.txt

# Phase 1: Seed database (run once)
python scripts/fetch_race_data.py
# → Check completeness report, update SESSION_KEY and FOCUS_DRIVERS in .env

# Upload kafka-ca.pem from Aiven dashboard to backend/

# Run the server
uvicorn main:app --reload
```

### Frontend
```bash
cd frontend
cp .env.local.example .env.local
# Set NEXT_PUBLIC_WS_URL and NEXT_PUBLIC_BACKEND_URL

npm install
npm run dev
```

---

## Deployment

### 1. Neon Postgres — seed data
```bash
# From backend/, with production DATABASE_URL_SYNC in .env:
python scripts/fetch_race_data.py
```

### 2. Render — backend
- Connect GitHub repo, select `render.yaml`
- Add all env vars from `.env.example` in Render dashboard
- Upload `kafka-ca.pem` as a Secret File at path `./kafka-ca.pem`

### 3. Vercel — frontend
- Connect GitHub repo, set root directory to `frontend/`
- Add `NEXT_PUBLIC_WS_URL` and `NEXT_PUBLIC_BACKEND_URL` in Vercel dashboard

### Cold-start smoke test
1. Leave everything idle for 1+ hour (Render sleeps, Aiven may sleep)
2. Open the frontend URL in a fresh browser tab
3. Observe: "Connecting to race engine…" overlay appears (wake-up ping fires silently)
4. Within 30s: overlay disappears, race tower populates, strategy cards appear
5. Type a question in the agent chat
6. Verify answer references lap numbers and simulation data

---

## Accounts required (all free, no card)

| Service | URL | Purpose |
|---------|-----|---------|
| Neon | neon.tech | Postgres — pre-fetched race telemetry (free-forever) |
| Render | render.com | Python backend (web service) |
| Aiven | aiven.io | Apache Kafka (message broker) |
| Upstash | upstash.com | Redis (live race state cache) |
| Google AI Studio | aistudio.google.com | Gemini API key |
| Vercel | vercel.com | Next.js frontend hosting |

---

## Tech stack

**Backend:** Python 3.12 · FastAPI · aiokafka · asyncpg · LangGraph · google-generativeai · upstash-redis · uv  
**Frontend:** Next.js 15 · TypeScript · Tailwind CSS v4 · Inter font  
**Data:** OpenF1 API (2024 Bahrain GP, pre-fetched)  
**Infrastructure:** Vercel · Render · Aiven Kafka · Upstash Redis · Neon Postgres
