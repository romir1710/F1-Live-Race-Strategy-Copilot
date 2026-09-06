/**
 * LapProgress.tsx — Race progress bar + lap counter + replay speed control.
 * Also shows the "Race Finished" overlay and restart button.
 */
"use client";
import { useState } from "react";

interface Props {
  currentLap: number;
  totalLaps: number;
  raceFinished: boolean;
  onSpeedChange?: (speed: number) => void;
}

const SPEED_OPTIONS = [0.5, 1, 5, 10];

export function LapProgress({ currentLap, totalLaps, raceFinished, onSpeedChange }: Props) {
  const [speed, setSpeed] = useState(1);
  const progress = totalLaps > 0 ? (currentLap / totalLaps) * 100 : 0;

  const handleSpeed = (s: number) => {
    setSpeed(s);
    onSpeedChange?.(s);
  };

  return (
    <div className="space-y-2">
      {/* Race finished banner */}
      {raceFinished && (
        <div className="flex items-center justify-between px-4 py-2 rounded-xl
                        bg-checkered-flag border border-white/20 animate-pulse">
          <span className="text-sm font-bold text-white">
            🏁 Race finished — restarting in 5s
          </span>
          <button
            onClick={() => window.location.reload()}
            className="text-xs px-3 py-1 rounded-lg bg-white/20 hover:bg-white/30 text-white transition"
          >
            Restart now
          </button>
        </div>
      )}

      <div className="flex items-center gap-4">
        {/* Lap counter */}
        <div className="flex items-baseline gap-1 min-w-[90px]">
          <span className="text-2xl font-black text-white font-mono">{currentLap}</span>
          <span className="text-sm text-zinc-500">/ {totalLaps}</span>
          <span className="text-xs text-zinc-600 ml-1">LAPS</span>
        </div>

        {/* Progress bar */}
        <div className="flex-1 h-2 bg-white/10 rounded-full overflow-hidden">
          <div
            className="h-full bg-gradient-to-r from-red-600 to-red-400 rounded-full
                       transition-all duration-500"
            style={{ width: `${progress}%` }}
          />
        </div>

        {/* Speed control */}
        <div className="flex items-center gap-1">
          <span className="text-[10px] text-zinc-600 uppercase tracking-widest mr-1">Speed</span>
          {SPEED_OPTIONS.map((s) => (
            <button
              key={s}
              onClick={() => handleSpeed(s)}
              className={`text-xs px-2 py-1 rounded-md transition-all duration-150
                          ${speed === s
                            ? "bg-white/20 text-white font-semibold"
                            : "text-zinc-500 hover:text-zinc-300 hover:bg-white/5"
                          }`}
            >
              {s}×
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}
