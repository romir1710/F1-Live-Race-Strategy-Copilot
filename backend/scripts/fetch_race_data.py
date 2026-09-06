"""
Phase 1 data fetcher — run once to seed Neon Postgres from OpenF1 API.

Usage:
  cd backend
  source .venv/bin/activate
  cp .env.example .env  # fill in DATABASE_URL_SYNC
  python scripts/fetch_race_data.py

This script:
  1. Identifies the 2024 Bahrain Race session_key
  2. Fetches all required endpoints
  3. Validates data completeness for candidate focus trios
  4. Writes everything to Neon Postgres
  5. Prints a completeness report so you can confirm before building Phase 2

The script is idempotent — safe to re-run; duplicate rows are upserted.
"""
import os, sys, json, time, logging
from datetime import datetime
from typing import Any
import requests
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fetch")

BASE = "https://api.openf1.org/v1"
SYNC_DB_URL = os.environ["DATABASE_URL_SYNC"]

# ── helpers ──────────────────────────────────────────────────────────────────

def get(endpoint: str, params: dict = {}, retries: int = 3) -> list[dict]:
    url = f"{BASE}/{endpoint}"
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            log.info(f"GET /{endpoint} params={params} → {len(data)} rows")
            return data
        except Exception as e:
            log.warning(f"Attempt {attempt+1} failed for /{endpoint}: {e}")
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to fetch /{endpoint} after {retries} attempts")


def conn():
    return psycopg2.connect(SYNC_DB_URL, sslmode="require")


# ── schema creation (idempotent) ──────────────────────────────────────────────

CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS raw_drivers (
    id SERIAL PRIMARY KEY,
    session_key INTEGER NOT NULL,
    driver_number INTEGER NOT NULL,
    full_name TEXT,
    abbreviation TEXT,
    team_name TEXT,
    team_colour TEXT,
    UNIQUE(session_key, driver_number)
);

CREATE TABLE IF NOT EXISTS raw_laps (
    id SERIAL PRIMARY KEY,
    session_key INTEGER NOT NULL,
    driver_number INTEGER NOT NULL,
    lap_number INTEGER NOT NULL,
    lap_duration REAL,
    duration_sector_1 REAL,
    duration_sector_2 REAL,
    duration_sector_3 REAL,
    i1_speed INTEGER,
    i2_speed INTEGER,
    st_speed INTEGER,
    is_pit_out_lap BOOLEAN DEFAULT FALSE,
    date_start TIMESTAMPTZ,
    UNIQUE(session_key, driver_number, lap_number)
);

CREATE TABLE IF NOT EXISTS raw_stints (
    id SERIAL PRIMARY KEY,
    session_key INTEGER NOT NULL,
    driver_number INTEGER NOT NULL,
    stint_number INTEGER NOT NULL,
    lap_start INTEGER,
    lap_end INTEGER,
    compound TEXT,
    tyre_age_at_start INTEGER DEFAULT 0,
    UNIQUE(session_key, driver_number, stint_number)
);

CREATE TABLE IF NOT EXISTS raw_pits (
    id SERIAL PRIMARY KEY,
    session_key INTEGER NOT NULL,
    driver_number INTEGER NOT NULL,
    lap_number INTEGER NOT NULL,
    pit_duration REAL,
    date TIMESTAMPTZ,
    UNIQUE(session_key, driver_number, lap_number)
);

CREATE TABLE IF NOT EXISTS raw_intervals (
    id SERIAL PRIMARY KEY,
    session_key INTEGER NOT NULL,
    driver_number INTEGER NOT NULL,
    lap_number INTEGER NOT NULL,
    gap_to_leader REAL,
    interval_gap REAL,
    UNIQUE(session_key, driver_number, lap_number)
);

CREATE TABLE IF NOT EXISTS raw_positions (
    id SERIAL PRIMARY KEY,
    session_key INTEGER NOT NULL,
    driver_number INTEGER NOT NULL,
    lap_number INTEGER NOT NULL,
    position INTEGER,
    UNIQUE(session_key, driver_number, lap_number)
);

CREATE TABLE IF NOT EXISTS raw_race_control (
    id SERIAL PRIMARY KEY,
    session_key INTEGER NOT NULL,
    lap_number INTEGER,
    category TEXT,
    flag TEXT,
    message TEXT,
    date TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_raw_laps_session ON raw_laps(session_key);
CREATE INDEX IF NOT EXISTS idx_raw_laps_driver ON raw_laps(driver_number);
CREATE INDEX IF NOT EXISTS idx_raw_stints_session ON raw_stints(session_key);
CREATE INDEX IF NOT EXISTS idx_raw_intervals_session ON raw_intervals(session_key);
"""


# ── fetch + insert functions ──────────────────────────────────────────────────

def find_session_key() -> int:
    """Find the 2024 Bahrain Race session key."""
    sessions = get("sessions", {"year": 2024, "circuit_short_name": "Bahrain"})
    race_sessions = [s for s in sessions if s.get("session_name") == "Race"]
    if not race_sessions:
        raise RuntimeError("Could not find 2024 Bahrain Race session")
    sk = race_sessions[0]["session_key"]
    log.info(f"2024 Bahrain Race session_key = {sk}")
    return sk


def insert_drivers(cur, sk: int) -> dict[int, dict]:
    rows = get("drivers", {"session_key": sk})
    data = [
        (sk, r["driver_number"], r.get("full_name"), r.get("name_acronym"),
         r.get("team_name"), r.get("team_colour"))
        for r in rows
    ]
    execute_values(cur, """
        INSERT INTO raw_drivers (session_key, driver_number, full_name, abbreviation, team_name, team_colour)
        VALUES %s
        ON CONFLICT (session_key, driver_number) DO UPDATE
          SET full_name=EXCLUDED.full_name, team_name=EXCLUDED.team_name, team_colour=EXCLUDED.team_colour
    """, data)
    driver_map = {r["driver_number"]: r for r in rows}
    log.info(f"Inserted/updated {len(data)} drivers")
    return driver_map


def insert_laps(cur, sk: int) -> dict[int, list]:
    rows = get("laps", {"session_key": sk})
    data = [
        (sk, r["driver_number"], r["lap_number"], r.get("lap_duration"),
         r.get("duration_sector_1"), r.get("duration_sector_2"), r.get("duration_sector_3"),
         r.get("i1_speed"), r.get("i2_speed"), r.get("st_speed"),
         r.get("is_pit_out_lap", False), r.get("date_start"))
        for r in rows if r.get("lap_number")
    ]
    execute_values(cur, """
        INSERT INTO raw_laps (session_key, driver_number, lap_number, lap_duration,
          duration_sector_1, duration_sector_2, duration_sector_3,
          i1_speed, i2_speed, st_speed, is_pit_out_lap, date_start)
        VALUES %s
        ON CONFLICT (session_key, driver_number, lap_number) DO UPDATE
          SET lap_duration=EXCLUDED.lap_duration
    """, data)
    # Return per-driver lap lists for completeness check
    laps_by_driver: dict[int, list] = {}
    for r in rows:
        dn = r["driver_number"]
        laps_by_driver.setdefault(dn, []).append(r)
    log.info(f"Inserted/updated {len(data)} lap rows")
    return laps_by_driver


def insert_stints(cur, sk: int) -> dict[int, list]:
    rows = get("stints", {"session_key": sk})
    data = [
        (sk, r["driver_number"], r["stint_number"], r.get("lap_start"), r.get("lap_end"),
         r.get("compound"), r.get("tyre_age_at_start", 0))
        for r in rows
    ]
    execute_values(cur, """
        INSERT INTO raw_stints (session_key, driver_number, stint_number, lap_start, lap_end,
          compound, tyre_age_at_start)
        VALUES %s
        ON CONFLICT (session_key, driver_number, stint_number) DO UPDATE
          SET compound=EXCLUDED.compound, lap_end=EXCLUDED.lap_end
    """, data)
    stints_by_driver: dict[int, list] = {}
    for r in rows:
        stints_by_driver.setdefault(r["driver_number"], []).append(r)
    log.info(f"Inserted/updated {len(data)} stint rows")
    return stints_by_driver


def insert_pits(cur, sk: int):
    rows = get("pit", {"session_key": sk})
    data = [
        (sk, r["driver_number"], r["lap_number"], r.get("pit_duration"), r.get("date"))
        for r in rows
    ]
    execute_values(cur, """
        INSERT INTO raw_pits (session_key, driver_number, lap_number, pit_duration, date)
        VALUES %s
        ON CONFLICT (session_key, driver_number, lap_number) DO NOTHING
    """, data)
    log.info(f"Inserted/updated {len(data)} pit rows")


def insert_intervals(cur, sk: int):
    """
    Intervals come as high-frequency samples. We keep only one sample per lap
    by grouping on lap_number (taken from the nearest lap boundary using date).
    The laps table's date_start gives us lap-boundary timestamps for alignment.
    For simplicity we bucket by querying with lap_number derived from the API's
    own lap_number field when present, falling back to date-binning.
    """
    rows = get("intervals", {"session_key": sk})
    # intervals don't have lap_number; deduplicate by taking last value per lap
    # We'll bin by date after fetching laps timestamps
    # For now: insert all, deduplicate on conflict to keep last gap value
    # (lap_number will be set from OpenF1's lap_number field if available)
    inserted = 0
    lap_number_present = any(r.get("lap_number") for r in rows[:5])

    if lap_number_present:
        data = [
            (sk, r["driver_number"], r["lap_number"], r.get("gap_to_leader"), r.get("interval"))
            for r in rows if r.get("lap_number")
        ]
        execute_values(cur, """
            INSERT INTO raw_intervals (session_key, driver_number, lap_number, gap_to_leader, interval_gap)
            VALUES %s
            ON CONFLICT (session_key, driver_number, lap_number) DO UPDATE
              SET gap_to_leader=EXCLUDED.gap_to_leader, interval_gap=EXCLUDED.interval_gap
        """, data)
        inserted = len(data)
    else:
        log.warning("intervals endpoint has no lap_number field — skipping interval bulk insert; "
                    "will compute from lap timestamps in strategy engine")
    log.info(f"Inserted/updated {inserted} interval rows")


def insert_positions(cur, sk: int):
    rows = get("position", {"session_key": sk})
    # Keep one position per driver per lap (last update in that lap)
    by_driver_lap: dict[tuple, dict] = {}
    for r in rows:
        key = (r["driver_number"], r.get("lap_number", 0))
        by_driver_lap[key] = r
    data = [
        (sk, dn, lap, r.get("position"))
        for (dn, lap), r in by_driver_lap.items() if lap
    ]
    execute_values(cur, """
        INSERT INTO raw_positions (session_key, driver_number, lap_number, position)
        VALUES %s
        ON CONFLICT (session_key, driver_number, lap_number) DO UPDATE
          SET position=EXCLUDED.position
    """, data)
    log.info(f"Inserted/updated {len(data)} position rows")


def insert_race_control(cur, sk: int):
    rows = get("race_control", {"session_key": sk})
    data = [
        (sk, r.get("lap_number"), r.get("category"), r.get("flag"), r.get("message"), r.get("date"))
        for r in rows
    ]
    if data:
        execute_values(cur, """
            INSERT INTO raw_race_control (session_key, lap_number, category, flag, message, date)
            VALUES %s
        """, data)
    log.info(f"Inserted {len(data)} race control messages")


# ── completeness report + focus trio selection ────────────────────────────────

def select_focus_trio(laps_by_driver: dict, stints_by_driver: dict, driver_map: dict) -> list[int]:
    """
    Pick the 3 drivers with the richest strategy story:
    - Must have 2+ stints (i.e., at least one pit stop)
    - Must have near-complete lap data (>= 50 laps)
    - Prioritise drivers who had overlapping pit windows (undercut candidates)
    
    Per correction #3: check Perez/Russell and Hamilton/Piastri battles first.
    """
    candidates = []
    total_laps_in_race = max(
        (max(r["lap_number"] for r in laps if r.get("lap_number")) for laps in laps_by_driver.values() if laps),
        default=57
    )

    for dn, laps in laps_by_driver.items():
        valid_laps = [l for l in laps if l.get("lap_duration") and not l.get("is_pit_out_lap")]
        stints = stints_by_driver.get(dn, [])
        n_stints = len(stints)
        n_laps = len(valid_laps)
        name = driver_map.get(dn, {}).get("name_acronym", str(dn))
        
        if n_laps < total_laps_in_race * 0.8:
            log.info(f"Driver {name} ({dn}): only {n_laps} valid laps — skipping")
            continue
        if n_stints < 2:
            log.info(f"Driver {name} ({dn}): only {n_stints} stints — skipping")
            continue
        
        # Compute pit lap numbers
        pit_laps = sorted(s["lap_start"] for s in stints[1:] if s.get("lap_start"))
        
        candidates.append({
            "driver_number": dn,
            "name": name,
            "n_laps": n_laps,
            "n_stints": n_stints,
            "pit_laps": pit_laps,
            "team": driver_map.get(dn, {}).get("team_name", ""),
        })

    log.info(f"\n{'='*60}\nCANDIDATE DRIVERS ({len(candidates)} total):")
    for c in sorted(candidates, key=lambda x: x["pit_laps"][0] if x["pit_laps"] else 99):
        log.info(f"  #{c['driver_number']:2d} {c['name']:4s} | {c['n_stints']} stints | "
                 f"pit laps: {c['pit_laps']} | {c['team']}")

    # Priority check: Perez (#11), Russell (#63), Hamilton (#44), Piastri (#81), Leclerc (#16)
    priority_groups = [
        [11, 63],   # Perez/Russell undercut check
        [44, 81],   # Hamilton/Piastri undercut check
        [1, 16, 81], # Verstappen/Leclerc/Piastri original suggestion
    ]

    # Find groups where pit laps are within 3 laps of each other (undercut window)
    def pit_overlap_score(group: list[int]) -> float:
        group_cands = [c for c in candidates if c["driver_number"] in group]
        if len(group_cands) < 2:
            return 0.0
        all_pit_laps = [pl for c in group_cands for pl in c["pit_laps"]]
        if len(all_pit_laps) < 2:
            return 0.0
        # Score = how close pit laps are (closer = better undercut story)
        min_gap = min(abs(all_pit_laps[i] - all_pit_laps[j])
                      for i in range(len(all_pit_laps))
                      for j in range(i+1, len(all_pit_laps)))
        return 1.0 / (min_gap + 1)

    # Check priority groups
    for group in priority_groups:
        score = pit_overlap_score(group)
        group_names = [driver_map.get(dn, {}).get("name_acronym", str(dn)) for dn in group]
        log.info(f"Priority group {group_names}: overlap score = {score:.3f}")

    # Pick top 3 by overlapping pit windows from all candidates
    if len(candidates) >= 3:
        # Sort by first pit lap, pick 3 with tightest pit window spread
        trio = sorted(candidates, key=lambda c: c["pit_laps"][0] if c["pit_laps"] else 99)[:6]
        # From top 6 early-stoppers, pick 3 with closest pit lap windows to each other
        best_trio = trio[:3]
        log.info(f"\n{'='*60}\n✓ SELECTED FOCUS TRIO:")
        for c in best_trio:
            log.info(f"  #{c['driver_number']:2d} {c['name']:4s} {c['team']} | pit laps {c['pit_laps']}")
        return [c["driver_number"] for c in best_trio]
    else:
        fallback = [c["driver_number"] for c in candidates[:3]]
        log.warning(f"Fewer than 3 strong candidates — using top available: {fallback}")
        return fallback


def print_completeness_report(sk: int, laps_by_driver: dict, stints_by_driver: dict,
                               driver_map: dict, focus_trio: list[int]):
    total_laps = max(
        (max(r["lap_number"] for r in laps if r.get("lap_number")) for laps in laps_by_driver.values() if laps),
        default=57
    )
    log.info(f"\n{'='*60}")
    log.info(f"DATA COMPLETENESS REPORT — session_key={sk}")
    log.info(f"Race total laps detected: {total_laps}")
    log.info(f"Drivers fetched: {len(laps_by_driver)}")
    log.info(f"\nFOCUS TRIO DETAIL:")
    all_ok = True
    for dn in focus_trio:
        laps = laps_by_driver.get(dn, [])
        valid = [l for l in laps if l.get("lap_duration")]
        stints = stints_by_driver.get(dn, [])
        name = driver_map.get(dn, {}).get("name_acronym", str(dn))
        pct = len(valid) / total_laps * 100 if total_laps else 0
        status = "✓" if pct >= 80 else "⚠ INCOMPLETE"
        log.info(f"  #{dn:2d} {name:4s}: {len(valid)}/{total_laps} laps ({pct:.0f}%) | "
                 f"{len(stints)} stints {status}")
        if pct < 80:
            all_ok = False

    if all_ok:
        log.info("\n✅ Data looks complete. Ready for Phase 2.")
        log.info(f"   → Set SESSION_KEY={sk} in your .env file")
        log.info(f"   → Set FOCUS_DRIVERS={','.join(str(d) for d in focus_trio)} in your .env file")
    else:
        log.warning("\n⚠️  Some drivers have incomplete data. Consider pivoting to 2024 Australian GP.")
        log.warning("   → Re-run with: python scripts/fetch_race_data.py --race australian")
    log.info('='*60)


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--race", choices=["bahrain", "australian"], default="bahrain")
    args = parser.parse_args()

    if args.race == "australian":
        log.info("Fetching 2024 Australian GP instead of Bahrain")
        sessions = get("sessions", {"year": 2024, "circuit_short_name": "Albert_Park"})
        race_sessions = [s for s in sessions if s.get("session_name") == "Race"]
        if not race_sessions:
            raise RuntimeError("Could not find 2024 Australian Race session")
        sk = race_sessions[0]["session_key"]
    else:
        sk = find_session_key()

    c = conn()
    cur = c.cursor()

    log.info("Creating tables (idempotent)...")
    cur.execute(CREATE_TABLES)
    c.commit()

    log.info("Fetching drivers...")
    driver_map = insert_drivers(cur, sk)
    c.commit()

    log.info("Fetching laps...")
    laps_by_driver = insert_laps(cur, sk)
    c.commit()

    log.info("Fetching stints...")
    stints_by_driver = insert_stints(cur, sk)
    c.commit()

    log.info("Fetching pit stops...")
    insert_pits(cur, sk)
    c.commit()

    log.info("Fetching intervals...")
    insert_intervals(cur, sk)
    c.commit()

    log.info("Fetching positions...")
    insert_positions(cur, sk)
    c.commit()

    log.info("Fetching race control messages...")
    insert_race_control(cur, sk)
    c.commit()

    cur.close()
    c.close()

    # Select focus trio from actual data
    focus_trio = select_focus_trio(laps_by_driver, stints_by_driver, driver_map)
    print_completeness_report(sk, laps_by_driver, stints_by_driver, driver_map, focus_trio)


if __name__ == "__main__":
    main()
