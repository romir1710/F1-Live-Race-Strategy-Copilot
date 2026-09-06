/**
 * RaceTower.tsx — Live race standings tower.
 * Shows all drivers sorted by position with live gaps, tyre badges, and team colours.
 */
"use client";
import { DriverFrame } from "@/hooks/useRaceSocket";
import {
  teamColour,
  COMPOUND_COLOURS,
  COMPOUND_ICONS,
  formatGap,
  formatLapTime,
} from "@/lib/constants";

interface Props {
  drivers: DriverFrame[];
  focusDrivers: number[];
  currentLap: number;
  totalLaps: number;
}

function TyreBadge({ compound, age }: { compound: string; age: number }) {
  const bg = COMPOUND_COLOURS[compound] || COMPOUND_COLOURS.UNKNOWN;
  const icon = COMPOUND_ICONS[compound] || "?";
  const textColor =
    compound === "MEDIUM" || compound === "HARD" ? "#000" : "#fff";
  return (
    <span
      className="inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-bold"
      style={{ backgroundColor: bg, color: textColor }}
    >
      {icon}
      <span className="font-normal opacity-75">{age}L</span>
    </span>
  );
}

export function RaceTower({ drivers, focusDrivers, currentLap, totalLaps }: Props) {
  return (
    <div className="flex flex-col gap-0 rounded-xl overflow-hidden border border-white/10">
      {/* Header */}
      <div className="grid grid-cols-[2rem_3rem_1fr_4rem_5rem_5rem] gap-2 px-3 py-2
                      bg-white/5 text-[10px] font-medium uppercase tracking-widest text-zinc-500">
        <span>P</span>
        <span></span>
        <span>Driver</span>
        <span>Tyre</span>
        <span className="text-right">Gap</span>
        <span className="text-right">Last Lap</span>
      </div>

      {drivers.slice(0, 20).map((d, i) => {
        const isFocus = focusDrivers.includes(Number(d.driver_number));
        const colour = teamColour(d.team_name, d.team_colour);
        const isPit = d.is_pit_lap;

        return (
          <div
            key={d.driver_number}
            className={`grid grid-cols-[2rem_3rem_1fr_4rem_5rem_5rem] gap-2 px-3 py-2.5
                        items-center text-sm transition-colors duration-300
                        ${isFocus ? "bg-white/[0.06] border-l-2" : "bg-white/[0.02]"}
                        ${i % 2 === 0 ? "" : "bg-white/[0.01]"}
                        hover:bg-white/10`}
            style={isFocus ? { borderLeftColor: colour } : {}}
          >
            {/* Position */}
            <span className="text-zinc-400 text-xs font-mono">{d.position || i + 1}</span>

            {/* Driver chip */}
            <span
              className="text-xs font-bold px-1.5 py-0.5 rounded"
              style={{ color: colour }}
            >
              {d.abbreviation || `#${d.driver_number}`}
            </span>

            {/* Team + pit indicator */}
            <span className="text-zinc-300 text-xs truncate flex items-center gap-1.5">
              {d.team_name?.split(" ")[0] || "—"}
              {isPit && (
                <span className="px-1.5 py-0.5 rounded bg-amber-400/20 text-amber-300 text-[9px] font-bold uppercase tracking-wide animate-pulse">
                  PIT
                </span>
              )}
            </span>

            {/* Tyre badge */}
            <TyreBadge compound={d.compound || "UNKNOWN"} age={d.tyre_age || 0} />

            {/* Gap */}
            <span className="text-right text-zinc-400 font-mono text-xs">
              {d.position === 1 ? (
                <span className="text-white font-semibold">Leader</span>
              ) : (
                formatGap(d.gap_to_leader)
              )}
            </span>

            {/* Last lap */}
            <span className="text-right text-zinc-400 font-mono text-xs">
              {formatLapTime(d.lap_duration)}
            </span>
          </div>
        );
      })}
    </div>
  );
}
