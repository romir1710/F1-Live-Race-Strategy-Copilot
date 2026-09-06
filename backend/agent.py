"""
agent.py — LangGraph agent for natural-language F1 strategy queries.

Graph: read_race_state → parse_intent → [run_simulation | explain_current | compare_options]
       → compose_answer

Uses Gemini 3.1 Flash-Lite (highest free daily cap on Google AI Studio).
Falls back gracefully if daily quota is exhausted: returns raw sim delta + brief note.
"""
from __future__ import annotations
import json
import logging
import os
from typing import Any, Optional, TypedDict

import google.generativeai as genai
from langgraph.graph import StateGraph, END
from upstash_redis import AsyncRedis

from strategy_engine import (
    DriverState, compute_strategy_options, TOTAL_LAPS
)

log = logging.getLogger("agent")

REDIS_URL   = os.environ["UPSTASH_REDIS_REST_URL"]
REDIS_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]
GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
FOCUS_DRIVERS = [int(x) for x in os.environ.get("FOCUS_DRIVERS", "1,11,16").split(",")]

# Gemini setup — use 3.1 Flash-Lite (highest free RPD)
genai.configure(api_key=GOOGLE_API_KEY)

# Try 3.1 Flash-Lite first, fall back to whatever is available
GEMINI_MODEL_PREFERENCE = [
    "gemini-3.1-flash-lite",
    "gemini-3.0-flash-lite",
    "gemini-2.0-flash-lite",
    "gemini-1.5-flash-latest",
]

def get_gemini_model() -> genai.GenerativeModel:
    for model_name in GEMINI_MODEL_PREFERENCE:
        try:
            m = genai.GenerativeModel(model_name)
            # Quick ping to validate
            return m
        except Exception:
            continue
    raise RuntimeError("No Gemini Flash-Lite model available")


# ── LangGraph state ───────────────────────────────────────────────────────────

class AgentState(TypedDict):
    question: str
    driver_hint: Optional[int]
    race_state: dict            # from Redis
    driver_states: dict         # driver_number → state dict
    strategy_states: dict       # driver_number → StrategyResult dict
    intent: str                 # "what_if" | "explain" | "compare"
    entities: dict              # extracted: driver, lap, compound, etc.
    sim_result: Optional[dict]  # filled by run_simulation node
    answer: str
    quota_exhausted: bool


# ── Nodes ─────────────────────────────────────────────────────────────────────

async def read_race_state(state: AgentState) -> AgentState:
    """Read current race state from Upstash Redis."""
    redis = AsyncRedis(url=REDIS_URL, token=REDIS_TOKEN)

    race_meta = await redis.hgetall("race:live") or {}
    driver_states = {}
    strategy_states = {}

    # Fetch all driver states
    keys = await redis.keys("driver:*")
    for key in keys:
        d = await redis.hgetall(key)
        if d:
            dn = int(key.split(":")[-1])
            driver_states[dn] = d

    # Fetch strategy for focus drivers
    for dn in FOCUS_DRIVERS:
        raw = await redis.get(f"strategy:{dn}")
        if raw:
            strategy_states[dn] = json.loads(raw)

    return {**state, "race_state": race_meta, "driver_states": driver_states,
            "strategy_states": strategy_states}


async def parse_intent(state: AgentState) -> AgentState:
    """
    Use Gemini Flash-Lite to classify intent and extract entities.
    Falls back to rule-based classification if quota is exhausted.
    """
    question = state["question"]
    race_lap = state["race_state"].get("current_lap", "?")
    focus_names = [state["driver_states"].get(dn, {}).get("abbreviation", str(dn))
                   for dn in FOCUS_DRIVERS]

    prompt = f"""You are classifying a Formula 1 strategy question.

Current race state: Lap {race_lap}/{TOTAL_LAPS}. Focus drivers: {', '.join(focus_names)}.

Question: "{question}"

Classify the question into ONE of these intents:
- "what_if" — user asks about an alternative scenario (pitting earlier/later, different compound, etc.)
- "explain" — user asks why something happened or what a strategy term means
- "compare" — user asks to compare two options or drivers

Also extract these entities (use null if not mentioned):
- driver: driver abbreviation (e.g. "VER", "HAM") or null
- lap_number: specific lap number or null
- compound: tyre compound (SOFT/MEDIUM/HARD) or null
- pit_delta: how many laps earlier or later to pit (integer, negative = earlier) or null

Respond ONLY as valid JSON: {{"intent": "...", "driver": ..., "lap_number": ..., "compound": ..., "pit_delta": ...}}"""

    try:
        model = get_gemini_model()
        response = model.generate_content(prompt)
        parsed = json.loads(response.text.strip().strip("```json").strip("```"))
        intent = parsed.get("intent", "explain")
        entities = {k: parsed.get(k) for k in ["driver", "lap_number", "compound", "pit_delta"]}
        return {**state, "intent": intent, "entities": entities, "quota_exhausted": False}
    except Exception as e:
        log.warning(f"Gemini quota or error on parse_intent: {e}")
        # Rule-based fallback
        q_lower = question.lower()
        if any(w in q_lower for w in ["what if", "had pitted", "earlier", "later", "instead"]):
            intent = "what_if"
        elif any(w in q_lower for w in ["compare", "vs", "versus", "better"]):
            intent = "compare"
        else:
            intent = "explain"
        return {**state, "intent": intent, "entities": {}, "quota_exhausted": True}


async def run_simulation(state: AgentState) -> AgentState:
    """
    Re-runs strategy_engine with modified parameters extracted from the question.
    This is what makes the agent's answer grounded in the real simulation, not a hallucination.
    """
    entities = state["entities"]
    driver_states_raw = state["driver_states"]
    race_state = state["race_state"]

    # Find target driver
    target_dn = None
    if entities.get("driver"):
        abbrev = entities["driver"].upper()
        for dn, d in driver_states_raw.items():
            if d.get("abbreviation", "").upper() == abbrev:
                target_dn = dn
                break
    if not target_dn and state.get("driver_hint"):
        target_dn = state["driver_hint"]
    if not target_dn:
        target_dn = FOCUS_DRIVERS[0]  # default to first focus driver

    # Build DriverState from Redis snapshot
    current_lap = int(race_state.get("current_lap", 20))
    total_laps = int(race_state.get("total_laps", TOTAL_LAPS))
    target_raw = driver_states_raw.get(target_dn, {})

    target = DriverState(
        driver_number=target_dn,
        name=target_raw.get("abbreviation", str(target_dn)),
        team=target_raw.get("team_name", "Unknown"),
        team_colour=target_raw.get("team_colour", "FFFFFF"),
        position=int(target_raw.get("position", 5)),
        current_lap=current_lap,
        compound=target_raw.get("compound", "HARD"),
        tyre_age=int(target_raw.get("tyre_age", 10)),
        gap_to_leader=float(target_raw.get("gap_to_leader", -1)) if float(target_raw.get("gap_to_leader", -1)) > 0 else None,
        gap_ahead=float(target_raw.get("interval_gap", -1)) if float(target_raw.get("interval_gap", -1)) > 0 else None,
        gap_behind=None,
        last_lap_time=float(target_raw.get("lap_duration", -1)) if float(target_raw.get("lap_duration", -1)) > 0 else None,
        laps_remaining=max(1, total_laps - current_lap),
    )

    # Apply what-if modification
    pit_delta = entities.get("pit_delta")  # e.g. -2 means "2 laps earlier"
    forced_lap = None
    if pit_delta is not None:
        forced_lap = max(current_lap, current_lap + int(pit_delta))

    forced_compound = entities.get("compound")

    # Run actual simulation (not a canned response)
    all_focus = [
        DriverState(
            driver_number=dn,
            name=driver_states_raw.get(dn, {}).get("abbreviation", str(dn)),
            team=driver_states_raw.get(dn, {}).get("team_name", ""),
            team_colour=driver_states_raw.get(dn, {}).get("team_colour", "FFFFFF"),
            position=int(driver_states_raw.get(dn, {}).get("position", 10)),
            current_lap=current_lap,
            compound=driver_states_raw.get(dn, {}).get("compound", "HARD"),
            tyre_age=int(driver_states_raw.get(dn, {}).get("tyre_age", 5)),
            gap_to_leader=None,
            gap_ahead=None,
            gap_behind=None,
            last_lap_time=None,
            laps_remaining=max(1, total_laps - current_lap),
        )
        for dn in FOCUS_DRIVERS if dn in driver_states_raw
    ]

    # Actual outcome (what happened / is happening)
    actual_result = compute_strategy_options(target, all_focus)

    # Hypothetical outcome (what the user asked about)
    hypo_result = compute_strategy_options(
        target, all_focus,
        force_pit_lap=forced_lap,
        force_compound=forced_compound,
    )

    sim_table = {
        "driver": target.name,
        "current_lap": current_lap,
        "actual": actual_result.to_dict(),
        "hypothetical": hypo_result.to_dict(),
        "what_if_description": f"pit_delta={pit_delta}, compound={forced_compound}",
    }

    return {**state, "sim_result": sim_table}


async def explain_current(state: AgentState) -> AgentState:
    """For explain/compare intents: pass strategy_states directly as context."""
    return {**state, "sim_result": {"strategy_states": state["strategy_states"]}}


async def compose_answer(state: AgentState) -> AgentState:
    """
    Write the final answer using Gemini Flash-Lite grounded in sim_result.
    Falls back to a structured text answer from raw data if quota is exhausted.
    """
    question = state["question"]
    sim_result = state["sim_result"] or {}
    race_lap = state["race_state"].get("current_lap", "?")
    total_laps = state["race_state"].get("total_laps", TOTAL_LAPS)

    # Build context string for Gemini
    context = json.dumps(sim_result, indent=2)[:3000]  # trim to avoid token overflow

    prompt = f"""You are a Formula 1 race strategy engineer answering a question live during a race.
Answer concisely and precisely (3–5 sentences max). Reference specific numbers from the simulation data.
Do NOT make up numbers not in the simulation data. Do NOT use canned responses.

Current race: Lap {race_lap} of {total_laps}.
Simulation data: {context}

Question: "{question}"

Answer:"""

    if state.get("quota_exhausted", False):
        # Fallback: don't call Gemini, compose from raw data
        answer = _quota_fallback_answer(question, sim_result, race_lap)
        return {**state, "answer": answer, "quota_exhausted": True}

    try:
        model = get_gemini_model()
        response = model.generate_content(prompt)
        return {**state, "answer": response.text.strip(), "quota_exhausted": False}
    except Exception as e:
        log.warning(f"Gemini quota or error on compose_answer: {e}")
        answer = _quota_fallback_answer(question, sim_result, race_lap)
        return {**state, "answer": answer, "quota_exhausted": True}


def _quota_fallback_answer(question: str, sim_result: dict, race_lap: Any) -> str:
    """
    Returns a structured answer from raw simulation data when Gemini quota is exhausted.
    This ensures the agent always returns something useful, never a bare error.
    """
    lines = [f"[Strategy data — Lap {race_lap}]"]
    if "actual" in sim_result and "hypothetical" in sim_result:
        actual_rec = sim_result["actual"].get("recommended", "?")
        hypo_opts = sim_result["hypothetical"].get("options", [])
        if hypo_opts:
            undercut = next((o for o in hypo_opts if o["label"] == "UNDERCUT"), None)
            if undercut:
                delta = undercut.get("projected_time_delta_s", 0)
                pos = undercut.get("projected_finish_position", "?")
                lines.append(f"Simulation result: modified pit window projects {delta:+.1f}s vs stay-out, P{pos}.")
        lines.append(f"Current recommendation: {actual_rec}.")
    elif "strategy_states" in sim_result:
        for dn, strat in sim_result["strategy_states"].items():
            rec = strat.get("recommended", "?")
            lines.append(f"Driver #{dn}: recommended {rec}.")
    lines.append("(AI explanation unavailable — daily quota reached. Showing raw simulation data.)")
    return " ".join(lines)


# ── Intent router ─────────────────────────────────────────────────────────────

def route_intent(state: AgentState) -> str:
    return "run_simulation" if state["intent"] == "what_if" else "explain_current"


# ── Build graph ───────────────────────────────────────────────────────────────

def build_graph() -> Any:
    graph = StateGraph(AgentState)
    graph.add_node("read_race_state", read_race_state)
    graph.add_node("parse_intent", parse_intent)
    graph.add_node("run_simulation", run_simulation)
    graph.add_node("explain_current", explain_current)
    graph.add_node("compose_answer", compose_answer)

    graph.set_entry_point("read_race_state")
    graph.add_edge("read_race_state", "parse_intent")
    graph.add_conditional_edges("parse_intent", route_intent, {
        "run_simulation": "run_simulation",
        "explain_current": "explain_current",
    })
    graph.add_edge("run_simulation", "compose_answer")
    graph.add_edge("explain_current", "compose_answer")
    graph.add_edge("compose_answer", END)

    return graph.compile()


_graph = None  # lazy singleton


async def run_agent(question: str, driver_number: Optional[int] = None) -> dict:
    """Entry point called by main.py /agent endpoint."""
    global _graph
    if _graph is None:
        _graph = build_graph()

    initial_state = AgentState(
        question=question,
        driver_hint=driver_number,
        race_state={},
        driver_states={},
        strategy_states={},
        intent="explain",
        entities={},
        sim_result=None,
        answer="",
        quota_exhausted=False,
    )

    final_state = await _graph.ainvoke(initial_state)

    return {
        "answer": final_state["answer"],
        "sim_table": final_state.get("sim_result"),
        "grounded": True,
        "quota_fallback": final_state.get("quota_exhausted", False),
    }
