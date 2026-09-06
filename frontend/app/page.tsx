/**
 * page.tsx — Main race dashboard.
 *
 * Layout:
 *   Left column:   Lap progress bar | Race tower | Agent chat
 *   Right column:  Session header | Strategy cards
 *
 * Overlay:  ConnectionOverlay while WebSocket is not yet connected.
 */
"use client";
import { useMemo } from "react";
import { useRaceSocket } from "@/hooks/useRaceSocket";
import { ConnectionOverlay } from "@/components/ConnectionOverlay";
import { RaceTower } from "@/components/RaceTower";
import { StrategyCards } from "@/components/StrategyCards";
import { AgentChat } from "@/components/AgentChat";
import { LapProgress } from "@/components/LapProgress";

export default function RaceDashboard() {
  const { frame, status, raceFinished } = useRaceSocket();

  const race = frame?.race;
  const drivers = frame?.drivers ?? [];
  const strategies = frame?.strategies ?? {};
  const focusDrivers = frame?.focus_drivers ?? [];

  // Build driver meta lookup from tower data
  const driverMeta = useMemo(() => {
    const map: Record<number, typeof drivers[0]> = {};
    for (const d of drivers) map[Number(d.driver_number)] = d;
    return map;
  }, [drivers]);

  return (
    <>
      <ConnectionOverlay status={status} />

      <div className="min-h-screen bg-zinc-950 text-white font-[family-name:var(--font-inter)]">
        {/* ── Header bar ──────────────────────────────────────────────────── */}
        <header className="border-b border-white/8 px-6 py-3 flex items-center justify-between
                           bg-zinc-950/80 backdrop-blur-sm sticky top-0 z-40">
          <div className="flex items-center gap-3">
            {/* F1 badge */}
            <div className="w-8 h-8 rounded-full bg-red-600 flex items-center justify-center">
              <span className="text-white text-xs font-black leading-none">F1</span>
            </div>
            <div>
              <h1 className="text-sm font-bold text-white leading-tight">
                Live Race Strategy Copilot
              </h1>
              <p className="text-[10px] text-zinc-500">2024 Bahrain Grand Prix · Race Replay</p>
            </div>
          </div>

          {/* Live indicator */}
          <div className="flex items-center gap-2">
            <div
              className={`w-2 h-2 rounded-full ${
                status === "connected"
                  ? "bg-emerald-400 animate-pulse"
                  : "bg-amber-400 animate-pulse"
              }`}
            />
            <span className="text-[11px] text-zinc-400 uppercase tracking-widest">
              {status === "connected" ? "Live" : "Connecting"}
            </span>
            {race && (
              <span className="text-[11px] text-zinc-600 ml-2">
                Lap {race.current_lap} / {race.total_laps}
              </span>
            )}
          </div>
        </header>

        {/* ── Main grid ───────────────────────────────────────────────────── */}
        <main className="max-w-screen-2xl mx-auto px-4 py-6 grid grid-cols-[380px_1fr] gap-6 xl:gap-8">

          {/* ── Left column ─────────────────────────────────────────────── */}
          <div className="flex flex-col gap-6">

            {/* Lap progress */}
            <div className="rounded-2xl border border-white/8 bg-zinc-900/50 p-4">
              <LapProgress
                currentLap={race?.current_lap ?? 0}
                totalLaps={race?.total_laps ?? 57}
                raceFinished={raceFinished}
              />
            </div>

            {/* Race tower */}
            <div className="rounded-2xl border border-white/8 bg-zinc-900/50 p-4">
              <SectionHeader
                label="Race Tower"
                subtitle="Live standings · all 20 drivers"
              />
              <div className="mt-3">
                <RaceTower
                  drivers={drivers}
                  focusDrivers={focusDrivers}
                  currentLap={race?.current_lap ?? 0}
                  totalLaps={race?.total_laps ?? 57}
                />
              </div>
            </div>

            {/* Agent chat */}
            <div className="rounded-2xl border border-white/8 bg-zinc-900/50 p-4 flex-1">
              <SectionHeader
                label="Strategy Copilot"
                subtitle="Natural language · grounded in live simulation"
              />
              <div className="mt-3">
                <AgentChat focusDrivers={focusDrivers} />
              </div>
            </div>
          </div>

          {/* ── Right column ────────────────────────────────────────────── */}
          <div className="flex flex-col gap-6">

            {/* Strategy analysis panel */}
            <div className="rounded-2xl border border-white/8 bg-zinc-900/50 p-6">
              <SectionHeader
                label="Live Strategy Analysis"
                subtitle="3-way branching comparison · updates every lap"
              />
              <div className="mt-5">
                <StrategyCards
                  strategies={strategies}
                  driverMeta={driverMeta}
                  focusDrivers={focusDrivers}
                />
              </div>
            </div>
          </div>
        </main>

        {/* ── Footer ──────────────────────────────────────────────────────── */}
        <footer className="border-t border-white/5 px-6 py-4 text-center">
          <p className="text-[10px] text-zinc-700">
            Telemetry via{" "}
            <a href="https://openf1.org" className="hover:text-zinc-500 transition-colors">
              OpenF1
            </a>{" "}
            · Strategy engine: linear degradation model (see source for assumptions) ·
            Agent: Gemini 3.1 Flash-Lite + LangGraph
          </p>
        </footer>
      </div>
    </>
  );
}

function SectionHeader({ label, subtitle }: { label: string; subtitle: string }) {
  return (
    <div>
      <h2 className="text-xs font-bold uppercase tracking-widest text-zinc-400">{label}</h2>
      <p className="text-[10px] text-zinc-600 mt-0.5">{subtitle}</p>
    </div>
  );
}
