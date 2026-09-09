"""
agent.py — LangGraph agent for natural-language F1 strategy queries.

Graph: read_race_state → parse_intent → [run_simulation | explain_current]
       → compose_answer

Reads live race state from the in-process RACE_STATE dict in main.py.
No Redis, no external cache — all state is in-memory, always up to date.

Uses Gemini 3.6 Flash (highest free daily cap on Google AI Studio, Sep 2026).
Falls back gracefully if daily quota is exhausted: returns raw sim delta + brief note.
"""
from __future__ import annotations
import json
import logging
import os
from typing import Any, Optional, TypedDict

import google.generativeai as genai
from langgraph.graph import StateGraph, END

from strategy_engine import (
    DriverState, compute_strategy_options, TOTAL_LAPS
)

log = logging.getLogger("agent")

GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
FOCUS_DRIVERS = [int(x) for x in os.environ.get("FOCUS_DRIVERS", "1,11,16").split(",")]

# Gemini setup — primary model: gemini-3.6-flash (free tier, AI Studio, Sep 2026)
genai.configure(api_key=GOOGLE_API_KEY)

# gemini-3.6-flash: current free-tier Flash model as of September 2026 (released July 21 2026).
# Fallback chain tries newer → older in case of regional availability gaps.
GEMINI_MODEL_PREFERENCE = [
    "gemini-3.6-flash",   # primary — free tier, AI Studio, Sep 2026
    "gemini-3.8-flash",   # newer but may have stricter rate limits
    "gemini-3.7-flash",
    "gemini-2.5-flash",   # last resort — older generation
]


def get_gemini_model() -> genai.GenerativeModel:
    for model_name in GEMINI_MODEL_PREFERENCE:
        try:
            m = genai.GenerativeModel(model_name)
            return m
        except Exception:
            continue
    raise RuntimeError("No Gemini Flash model available")


# ── LangGraph state ───────────────────────────────────────────────────────────

class AgentState(TypedDict):
    question: str
    driver_hint: Optional[int]
    race_state: dict            # from in-process RACE_STATE["meta"]
    driver_states: dict         # driver_number → state dict
    strategy_states: dict       # driver_number → StrategyResult dict
    intent: str                 # "what_if" | "explain" | "compare"
    entities: dict              # extracted: driver, lap, compound, etc.
    sim_result: Optional[dict]  # filled by run_simulation node
    answer: str
    quota_exhausted: bool


# ── Nodes ─────────────────────────────────────────────────────────────────────

async def read_race_state(state: AgentState) -> AgentState:
    """Read current race state from the shared in-process RACE_STATE dict."""
    # Import here to avoid circular import at module load time
    from main import RACE_STATE

    race_meta = dict(RACE_STATE.get("meta", {}))
    driver_states = {int(k): dict(v) for k, v in RACE_STATE.get("drivers", {}).items()}
    strategy_states = {int(k): dict(v) for k, v in RACE_STATE.get("strategies", {}).items()}

    return {**state, "race_state": race_meta, "driver_states": driver_states,
            "strategy_states": strategy_states}


async def parse_intent(state: AgentState) -> AgentState:
    """
    Use Gemini Flash to classify intent and extract entities.
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

    # Build DriverState from in-memory snapshot
    current_lap = int(race_state.get("current_lap", 20))
    total_laps = int(race_state.get("total_laps", TOTAL_LAPS))
    target_raw = driver_states_raw.get(target_dn, {})

    def _safe_float(val, default=-1):
        try:
            return float(val) if val is not None else default
        except (ValueError, TypeError):
            return default

    target = DriverState(
        driver_number=target_dn,
        name=target_raw.get("abbreviation", str(target_dn)),
        team=target_raw.get("team_name", "Unknown"),
        team_colour=target_raw.get("team_colour", "FFFFFF"),
        position=int(target_raw.get("position", 5)),
        current_lap=current_lap,
        compound=target_raw.get("compound", "HARD"),
        tyre_age=int(target_raw.get("tyre_age", 10)),
        gap_to_leader=_safe_float(target_raw.get("gap_to_leader")) if _safe_float(target_raw.get("gap_to_leader")) > 0 else None,
        gap_ahead=_safe_float(target_raw.get("interval_gap")) if _safe_float(target_raw.get("interval_gap")) > 0 else None,
        gap_behind=None,
        last_lap_time=_safe_float(target_raw.get("lap_duration")) if _safe_float(target_raw.get("lap_duration")) > 0 else None,
        laps_remaining=max(1, total_laps - current_lap),
    )

    # Apply what-if modification
    pit_delta = entities.get("pit_delta")
    forced_lap = None
    if pit_delta is not None:
        forced_lap = max(current_lap, current_lap + int(pit_delta))

    forced_compound = entities.get("compound")

    # Run actual simulation
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

    actual_result = compute_strategy_options(target, all_focus)
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
    """
    For explain/compare intents: start with pre-computed strategy_states for
    focus drivers, then compute strategy on-the-fly for any other driver
    mentioned in the question so the agent can answer about all 20 drivers.
    """
    driver_states_raw = state["driver_states"]
    race_state = state["race_state"]
    entities = state["entities"]
    current_lap = int(race_state.get("current_lap", 20))
    total_laps = int(race_state.get("total_laps", TOTAL_LAPS))

    # Start with pre-computed focus driver strategies
    all_strategies = dict(state["strategy_states"])

    def _safe_float(val, default=-1):
        try:
            return float(val) if val is not None else default
        except (ValueError, TypeError):
            return default

    def _build_driver_state(dn: int) -> "DriverState | None":
        raw = driver_states_raw.get(dn)
        if not raw:
            return None
        return DriverState(
            driver_number=dn,
            name=raw.get("abbreviation", str(dn)),
            team=raw.get("team_name", "Unknown"),
            team_colour=raw.get("team_colour", "FFFFFF"),
            position=int(raw.get("position", 99)),
            current_lap=current_lap,
            compound=raw.get("compound", "HARD"),
            tyre_age=int(raw.get("tyre_age", 10)),
            gap_to_leader=_safe_float(raw.get("gap_to_leader")) if _safe_float(raw.get("gap_to_leader")) > 0 else None,
            gap_ahead=_safe_float(raw.get("interval_gap")) if _safe_float(raw.get("interval_gap")) > 0 else None,
            gap_behind=None,
            last_lap_time=_safe_float(raw.get("lap_duration")) if _safe_float(raw.get("lap_duration")) > 0 else None,
            laps_remaining=max(1, total_laps - current_lap),
        )

    # ── Find ALL drivers mentioned anywhere in the question ─────────────────
    # Don't rely on Gemini's single-entity extraction — scan question text
    # directly against every known abbreviation (e.g. "Compare Perez and Russell"
    # should find both PER and RUS).
    question_upper = state["question"].upper()
    mentioned_dns: list[int] = [
        dn for dn, raw in driver_states_raw.items()
        if raw.get("abbreviation", "").upper() in question_upper
        # also accept full surnames written in the question
        or raw.get("abbreviation", "").upper() in [
            w for w in question_upper.split() if len(w) >= 3
        ]
    ]

    # For any mentioned driver not already in all_strategies, compute now
    all_focus_states = [_build_driver_state(dn) for dn in FOCUS_DRIVERS if dn in driver_states_raw]
    all_focus_states = [s for s in all_focus_states if s is not None]

    for dn in mentioned_dns:
        if dn not in all_strategies and dn in driver_states_raw:
            target = _build_driver_state(dn)
            if target:
                try:
                    result = compute_strategy_options(target, all_focus_states or [target])
                    all_strategies[dn] = result.to_dict()
                except Exception:
                    pass

    # Broad compare ("all drivers", "everyone", etc.) — compute up to 10
    question_lower = state["question"].lower()
    is_broad = any(w in question_lower for w in ["all drivers", "everyone", "field", "full grid"])
    if is_broad:
        for dn in list(driver_states_raw.keys())[:10]:
            if dn not in all_strategies:
                target = _build_driver_state(dn)
                if target:
                    try:
                        result = compute_strategy_options(target, all_focus_states or [target])
                        all_strategies[dn] = result.to_dict()
                    except Exception:
                        pass

    return {**state, "sim_result": {"strategy_states": all_strategies}}


def _compact_strategy_context(sim_result: dict, state: dict) -> str:
    """
    Render strategy data as compact plain text so ALL drivers fit in the
    context window. Each driver takes ~150 chars vs ~1000 chars for indented JSON.

    Format per driver:
      [ABV] #dn | Lap L | COMPOUND age A | P_pos
        STAY: delta=Xs, proj=Pp, risk=R, conf=C%
        UNDERCUT: pit now -> COMPOUND, delta=Xs, proj=Pp, risk=R, conf=C%
        OVERCUT: pit in N -> COMPOUND, delta=Xs, proj=Pp, risk=R, conf=C%
        recommended=LABEL
    """
    strategy_states = sim_result.get("strategy_states", {})
    driver_states_raw = state.get("driver_states", {})
    lines: list[str] = []

    for dn_key, strat in strategy_states.items():
        dn = int(dn_key)
        raw = driver_states_raw.get(dn, {})
        abbrev = raw.get("abbreviation") or strat.get("driver", {}).get("name", str(dn))
        lap = raw.get("lap_number", "?")
        compound = raw.get("compound", "?")
        tyre_age = raw.get("tyre_age", "?")
        pos = raw.get("position", "?")
        lines.append(f"[{abbrev}] #{dn} | Lap {lap} | {compound} age {tyre_age}L | P{pos}")
        for opt in strat.get("options", []):
            label = opt.get("label", "?")
            delta = opt.get("projected_time_delta_s", 0)
            proj_p = opt.get("projected_finish_position", "?")
            risk = opt.get("tyre_life_risk", "?")
            conf = int((opt.get("confidence", 0)) * 100)
            tgt = opt.get("target_compound", "?")
            pit_n = opt.get("pit_in_n_laps", 0)
            rec = " [RECOMMENDED]" if opt.get("label") == strat.get("recommended") else ""
            if label == "STAY":
                lines.append(f"  STAY: delta={delta:+.1f}s, proj=P{proj_p}, risk={risk}, conf={conf}%{rec}")
            elif label == "UNDERCUT":
                lines.append(f"  UNDERCUT: pit now->{tgt}, delta={delta:+.1f}s, proj=P{proj_p}, risk={risk}, conf={conf}%{rec}")
            else:
                lines.append(f"  OVERCUT: pit in {pit_n} laps->{tgt}, delta={delta:+.1f}s, proj=P{proj_p}, risk={risk}, conf={conf}%{rec}")
        lines.append("")

    return "\n".join(lines)


async def compose_answer(state: AgentState) -> AgentState:
    """
    Write the final answer using Gemini grounded in sim_result.
    Falls back to a structured text answer from raw data if quota is exhausted.
    """
    question = state["question"]
    sim_result = state["sim_result"] or {}
    race_lap = state["race_state"].get("current_lap", "?")
    total_laps = state["race_state"].get("total_laps", TOTAL_LAPS)

    context = _compact_strategy_context(sim_result, state)

    prompt = f"""You are a Formula 1 race strategy engineer answering a question live during a race.
Answer concisely and precisely (3–5 sentences max). Reference specific numbers from the simulation data.
Do NOT make up numbers not in the simulation data. Do NOT use canned responses.

Current race: Lap {race_lap} of {total_laps}.
Simulation data: {context}

Question: "{question}"

Answer:"""

    if state.get("quota_exhausted", False):
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
