"""
strategy_engine.py — pure function: race state → branching strategy options.

This module has NO I/O dependencies (no Redis, no DB). It takes the current
race state snapshot and returns 3 strategy options: STAY_OUT, UNDERCUT, OVERCUT.

The model is intentionally transparent (linear degradation fit) and is labelled
as such in the frontend — honest engineering beats overclaiming.

Called by:
  - strategy_consumer.py: every lap tick, results written to Redis
  - agent_server.py: re-called with modified params for what-if simulations
"""
from __future__ import annotations
import math
from dataclasses import dataclass, field, asdict
from typing import Optional
import numpy as np

# ── Bahrain-specific constants (derived from OpenF1 historical data) ──────────
PIT_LANE_DELTA_S = 22.5  # seconds lost for a Bahrain pit stop (entry→exit)
TOTAL_LAPS = 57

# Compound base pace delta (seconds vs HARD baseline, negative = faster)
# Fitted from 2024 Bahrain lap time distributions in raw_laps
COMPOUND_PACE_DELTA: dict[str, float] = {
    "SOFT":   -1.2,   # soft is ~1.2s/lap faster than hard on lap 1 of stint
    "MEDIUM": -0.5,
    "HARD":    0.0,   # baseline
    "INTER":  -0.8,
    "WET":    -1.0,
    "UNKNOWN": 0.0,
}

# Linear degradation rate per lap (seconds lost per lap of tyre age)
# These values are fitted from historical stint data; labelled as model in UI
DEG_RATE: dict[str, float] = {
    "SOFT":   0.085,   # fast degrader
    "MEDIUM": 0.040,
    "HARD":   0.018,   # most stable
    "INTER":  0.060,
    "WET":    0.055,
    "UNKNOWN": 0.045,
}


@dataclass
class DriverState:
    driver_number: int
    name: str                         # abbreviation e.g. "VER"
    team: str
    team_colour: str                  # hex e.g. "3671C6"
    position: int
    current_lap: int
    compound: str
    tyre_age: int                     # laps on current tyre
    gap_to_leader: Optional[float]    # seconds; None if leader
    gap_ahead: Optional[float]        # gap to car directly ahead
    gap_behind: Optional[float]       # gap to car directly behind
    last_lap_time: Optional[float]    # seconds
    laps_remaining: int


@dataclass
class StrategyOption:
    label: str                        # "STAY_OUT" | "UNDERCUT" | "OVERCUT"
    display_name: str                 # "Stay Out", "Undercut (pit now)", "Overcut (pit +3)"
    pit_this_lap: bool
    pit_in_n_laps: Optional[int]      # None for STAY_OUT
    target_compound: str
    projected_finish_position: int
    projected_time_delta_s: float     # vs STAY_OUT baseline (negative = faster finish)
    tyre_life_risk: str               # "LOW" | "MEDIUM" | "HIGH"
    undercut_gain_s: float            # estimated position time gain from undercut/overcut
    reasoning: str                    # 1–2 sentence human-readable explanation
    confidence: float                 # 0.0–1.0 model confidence

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class StrategyResult:
    driver_number: int
    current_lap: int
    options: list[StrategyOption] = field(default_factory=list)
    recommended: str = "STAY_OUT"    # label of top recommendation
    model_note: str = ("Projections use a linear tyre degradation model "
                       "fit to 2024 Bahrain lap time data. Treat as directional.")

    def to_dict(self) -> dict:
        return {
            "driver_number": self.driver_number,
            "current_lap": self.current_lap,
            "options": [o.to_dict() for o in self.options],
            "recommended": self.recommended,
            "model_note": self.model_note,
        }


# ── degradation model ─────────────────────────────────────────────────────────

def deg_adjusted_lap_time(base_lap_time: float, compound: str, tyre_age: int) -> float:
    """
    Returns the estimated lap time for a given tyre age using linear degradation.
    base_lap_time is the fresh-tyre lap time for that compound.
    """
    rate = DEG_RATE.get(compound, DEG_RATE["UNKNOWN"])
    return base_lap_time + (rate * tyre_age)


def project_stint_time(
    start_lap: int,
    end_lap: int,
    compound: str,
    tyre_start_age: int,
    reference_lap_time: float,
) -> float:
    """Sum projected lap times from start_lap to end_lap (exclusive)."""
    total = 0.0
    for lap in range(start_lap, end_lap):
        age = tyre_start_age + (lap - start_lap)
        total += deg_adjusted_lap_time(reference_lap_time, compound, age)
    return total


def fresh_compound_lap_time(current_lap_time: float, current_compound: str,
                             current_tyre_age: int, target_compound: str) -> float:
    """Estimate the fresh-tyre lap time for target_compound."""
    # Reverse out current degradation to get a 'clean' baseline
    rate = DEG_RATE.get(current_compound, DEG_RATE["UNKNOWN"])
    baseline_hard = current_lap_time - (rate * current_tyre_age) - COMPOUND_PACE_DELTA.get(current_compound, 0)
    # Apply new compound pace delta
    return baseline_hard + COMPOUND_PACE_DELTA.get(target_compound, 0)


def tyre_life_risk(compound: str, tyre_age: int, laps_remaining: int) -> str:
    """Estimate tyre failure risk based on age and stint length."""
    max_safe_laps = {"SOFT": 18, "MEDIUM": 30, "HARD": 45, "INTER": 25, "WET": 30}.get(compound, 25)
    projected_total = tyre_age + laps_remaining
    ratio = projected_total / max_safe_laps
    if ratio < 0.8:
        return "LOW"
    elif ratio < 1.0:
        return "MEDIUM"
    else:
        return "HIGH"


def best_compound_for_stint(laps_remaining: int) -> str:
    """Pick the most appropriate fresh compound for the stint length."""
    if laps_remaining <= 15:
        return "SOFT"
    elif laps_remaining <= 30:
        return "MEDIUM"
    else:
        return "HARD"


# ── position simulation ───────────────────────────────────────────────────────

def simulate_undercut_position_gain(
    driver: DriverState,
    pit_lap: int,
    all_drivers: list[DriverState],
) -> int:
    """
    Estimate position change from an undercut at `pit_lap`.
    Returns new position (lower = better).
    """
    cars_ahead = [d for d in all_drivers
                  if d.position < driver.position
                  and d.gap_ahead is not None
                  and d.gap_ahead < PIT_LANE_DELTA_S * 1.5]

    # Cars we might jump: those within 1.5× pit delta whose tyres are also older
    jumpable = [d for d in cars_ahead if d.tyre_age > driver.tyre_age + 3]
    new_position = driver.position - len(jumpable)
    return max(1, new_position)


# ── main entry point ──────────────────────────────────────────────────────────

def compute_strategy_options(
    driver: DriverState,
    all_drivers: list[DriverState],
    overcut_window: int = 3,
    force_pit_lap: Optional[int] = None,      # used by LangGraph what-if agent
    force_compound: Optional[str] = None,     # used by LangGraph what-if agent
) -> StrategyResult:
    """
    Core strategy engine. Returns 3 StrategyOptions for a given driver.

    Parameters
    ----------
    driver          : current state of the driver we're computing for
    all_drivers     : full field state (needed for position simulations)
    overcut_window  : how many laps later to pit for the overcut option
    force_pit_lap   : override for what-if simulations (LangGraph agent)
    force_compound  : override compound choice for what-if simulations
    """
    result = StrategyResult(driver_number=driver.driver_number, current_lap=driver.current_lap)
    laps_rem = driver.laps_remaining
    ref_lap = driver.last_lap_time or 95.0  # fallback to Bahrain typical time

    # ── Option A: STAY OUT ────────────────────────────────────────────────────
    stay_total = project_stint_time(
        start_lap=driver.current_lap,
        end_lap=driver.current_lap + laps_rem,
        compound=driver.compound,
        tyre_start_age=driver.tyre_age,
        reference_lap_time=ref_lap,
    )
    stay_risk = tyre_life_risk(driver.compound, driver.tyre_age, laps_rem)
    stay_option = StrategyOption(
        label="STAY_OUT",
        display_name="Stay Out",
        pit_this_lap=False,
        pit_in_n_laps=None,
        target_compound=driver.compound,
        projected_finish_position=driver.position,
        projected_time_delta_s=0.0,
        tyre_life_risk=stay_risk,
        undercut_gain_s=0.0,
        reasoning=(
            f"Stay on {driver.compound.capitalize()} (age {driver.tyre_age}). "
            f"{'High degradation risk, consider pitting.' if stay_risk == 'HIGH' else 'Manageable tyre life for remaining stint.'}"
        ),
        confidence=0.75 if stay_risk != "HIGH" else 0.4,
    )

    # ── Option B: UNDERCUT (pit this lap) ─────────────────────────────────────
    pit_lap_b = force_pit_lap if force_pit_lap is not None else driver.current_lap
    fresh_compound_b = force_compound or best_compound_for_stint(laps_rem - 1)
    fresh_ref_b = fresh_compound_lap_time(ref_lap, driver.compound, driver.tyre_age, fresh_compound_b)

    undercut_total = (
        PIT_LANE_DELTA_S
        + project_stint_time(
            start_lap=pit_lap_b + 1,
            end_lap=driver.current_lap + laps_rem,
            compound=fresh_compound_b,
            tyre_start_age=0,
            reference_lap_time=fresh_ref_b,
        )
    )
    undercut_delta = undercut_total - stay_total
    undercut_pos = simulate_undercut_position_gain(driver, pit_lap_b, all_drivers)
    undercut_gain = max(0.0, -undercut_delta)
    undercut_option = StrategyOption(
        label="UNDERCUT",
        display_name="Undercut: Pit Now",
        pit_this_lap=True,
        pit_in_n_laps=0,
        target_compound=fresh_compound_b,
        projected_finish_position=undercut_pos,
        projected_time_delta_s=round(undercut_delta, 2),
        tyre_life_risk="LOW",
        undercut_gain_s=round(undercut_gain, 2),
        reasoning=(
            f"Pit now for {fresh_compound_b.capitalize()}. "
            f"~{PIT_LANE_DELTA_S:.0f}s pit loss, but fresh rubber yields ~{undercut_gain:.1f}s "
            f"over remaining {laps_rem-1} laps. "
            + (f"Could jump {driver.position - undercut_pos} car(s) if rivals stay out."
               if undercut_pos < driver.position else "Track position vulnerable, rivals may cover.")
        ),
        confidence=0.70,
    )

    # ── Option C: OVERCUT (pit in N laps) ─────────────────────────────────────
    overcut_n = overcut_window
    pit_lap_c = driver.current_lap + overcut_n
    if pit_lap_c >= driver.current_lap + laps_rem - 5:
        overcut_n = max(1, laps_rem // 2)
        pit_lap_c = driver.current_lap + overcut_n

    fresh_compound_c = force_compound or best_compound_for_stint(laps_rem - overcut_n)
    fresh_ref_c = fresh_compound_lap_time(ref_lap, driver.compound, driver.tyre_age, fresh_compound_c)

    # Time on old tyres for N more laps + pit + fresh stint
    overcut_stay_cost = project_stint_time(
        start_lap=driver.current_lap,
        end_lap=pit_lap_c,
        compound=driver.compound,
        tyre_start_age=driver.tyre_age,
        reference_lap_time=ref_lap,
    )
    overcut_fresh = project_stint_time(
        start_lap=pit_lap_c + 1,
        end_lap=driver.current_lap + laps_rem,
        compound=fresh_compound_c,
        tyre_start_age=0,
        reference_lap_time=fresh_ref_c,
    )
    overcut_total = overcut_stay_cost + PIT_LANE_DELTA_S + overcut_fresh
    overcut_delta = overcut_total - stay_total
    overcut_risk = tyre_life_risk(driver.compound, driver.tyre_age, overcut_n)
    overcut_option = StrategyOption(
        label="OVERCUT",
        display_name=f"Overcut: Pit +{overcut_n} laps",
        pit_this_lap=False,
        pit_in_n_laps=overcut_n,
        target_compound=fresh_compound_c,
        projected_finish_position=driver.position,  # position unchanged until pit
        projected_time_delta_s=round(overcut_delta, 2),
        tyre_life_risk=overcut_risk,
        undercut_gain_s=0.0,
        reasoning=(
            f"Extend {overcut_n} more laps before pitting for {fresh_compound_c.capitalize()}. "
            f"Maintains track position if rivals pit now. "
            + (f"⚠️ Tyre degradation risk over next {overcut_n} laps is {overcut_risk}."
               if overcut_risk != "LOW" else f"Tyre life looks manageable for {overcut_n} more laps.")
        ),
        confidence=0.60,
    )

    # ── Rank and recommend ───────────────────────────────────────────────────
    options = [stay_option, undercut_option, overcut_option]

    # Score: weighted combination of time delta (lower = better) and position gain
    def score(opt: StrategyOption) -> float:
        time_score = -opt.projected_time_delta_s  # negative delta = time gained = better
        position_score = (driver.position - opt.projected_finish_position) * 3.0  # 3s per position
        risk_penalty = {"LOW": 0, "MEDIUM": -1, "HIGH": -4}[opt.tyre_life_risk]
        return time_score + position_score + risk_penalty

    best = max(options, key=score)
    result.options = options
    result.recommended = best.label

    return result
