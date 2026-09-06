/**
 * StrategyCards.tsx — Branching strategy comparison panel.
 *
 * Shows 3 options (STAY_OUT / UNDERCUT / OVERCUT) per focus driver,
 * updating live every replay tick. The recommended option is highlighted.
 * Includes the model transparency note so reviewers understand it's a real engine.
 */
"use client";
import { StrategyResult, StrategyOption } from "@/hooks/useRaceSocket";
import { DriverFrame } from "@/hooks/useRaceSocket";
import { teamColour, riskColour, COMPOUND_COLOURS } from "@/lib/constants";

interface Props {
  strategies: Record<number, StrategyResult>;
  driverMeta: Record<number, DriverFrame>;
  focusDrivers: number[];
}

const OPTION_ICONS = { STAY_OUT: "⏱", UNDERCUT: "⬇", OVERCUT: "⬆" };
const OPTION_ACCENT = {
  STAY_OUT: "border-zinc-700 bg-zinc-900/50",
  UNDERCUT: "border-blue-500/30 bg-blue-950/30",
  OVERCUT: "border-purple-500/30 bg-purple-950/30",
};
const OPTION_ACCENT_ACTIVE = {
  STAY_OUT: "border-white/40 bg-white/5",
  UNDERCUT: "border-blue-400/60 bg-blue-900/40",
  OVERCUT: "border-purple-400/60 bg-purple-900/40",
};

function StrategyOptionCard({ opt, isRecommended }: { opt: StrategyOption; isRecommended: boolean }) {
  const accent = isRecommended
    ? OPTION_ACCENT_ACTIVE[opt.label as keyof typeof OPTION_ACCENT_ACTIVE]
    : OPTION_ACCENT[opt.label as keyof typeof OPTION_ACCENT];

  const deltaColor =
    opt.projected_time_delta_s < -0.5
      ? "text-emerald-400"
      : opt.projected_time_delta_s > 0.5
      ? "text-red-400"
      : "text-zinc-300";

  return (
    <div
      className={`relative rounded-xl border p-4 transition-all duration-500 ${accent}
                  ${isRecommended ? "ring-1 ring-white/20 shadow-lg shadow-white/5" : ""}`}
    >
      {isRecommended && (
        <span className="absolute top-2 right-2 text-[9px] font-bold uppercase tracking-widest
                         px-2 py-0.5 rounded-full bg-white/10 text-white/80">
          Recommended
        </span>
      )}

      {/* Title row */}
      <div className="flex items-center gap-2 mb-3">
        <span className="text-lg">{OPTION_ICONS[opt.label as keyof typeof OPTION_ICONS]}</span>
        <div>
          <div className="text-sm font-semibold text-white">{opt.display_name}</div>
          {opt.pit_in_n_laps != null && !opt.pit_this_lap && (
            <div className="text-[10px] text-zinc-500">Pit in {opt.pit_in_n_laps} laps</div>
          )}
        </div>
      </div>

      {/* Metrics grid */}
      <div className="grid grid-cols-2 gap-x-4 gap-y-1.5 mb-3">
        <Metric label="Target tyre">
          <span
            className="font-bold text-xs px-1.5 py-0.5 rounded"
            style={{
              backgroundColor: COMPOUND_COLOURS[opt.target_compound] || "#666",
              color: opt.target_compound === "MEDIUM" || opt.target_compound === "HARD" ? "#000" : "#fff",
            }}
          >
            {opt.target_compound}
          </span>
        </Metric>
        <Metric label="Time delta">
          <span className={`font-mono text-sm font-semibold ${deltaColor}`}>
            {opt.projected_time_delta_s >= 0 ? "+" : ""}
            {opt.projected_time_delta_s.toFixed(2)}s
          </span>
        </Metric>
        <Metric label="Projected P">
          <span className="font-mono text-sm text-white font-semibold">
            P{opt.projected_finish_position}
          </span>
        </Metric>
        <Metric label="Tyre risk">
          <span
            className="text-xs font-bold"
            style={{ color: riskColour(opt.tyre_life_risk) }}
          >
            {opt.tyre_life_risk}
          </span>
        </Metric>
      </div>

      {/* Confidence bar */}
      <div className="mb-3">
        <div className="flex justify-between text-[10px] text-zinc-500 mb-1">
          <span>Model confidence</span>
          <span>{Math.round(opt.confidence * 100)}%</span>
        </div>
        <div className="h-1 bg-white/10 rounded-full overflow-hidden">
          <div
            className="h-full rounded-full transition-all duration-1000"
            style={{
              width: `${opt.confidence * 100}%`,
              backgroundColor: isRecommended ? "#27F4D2" : "#555",
            }}
          />
        </div>
      </div>

      {/* Reasoning */}
      <p className="text-[11px] text-zinc-400 leading-relaxed">{opt.reasoning}</p>
    </div>
  );
}

function Metric({
  label,
  children,
}: {
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div>
      <div className="text-[9px] uppercase tracking-widest text-zinc-600 mb-0.5">{label}</div>
      {children}
    </div>
  );
}

export function StrategyCards({ strategies, driverMeta, focusDrivers }: Props) {
  if (!Object.keys(strategies).length) {
    return (
      <div className="flex items-center justify-center h-48 text-zinc-600 text-sm">
        Waiting for strategy data…
      </div>
    );
  }

  return (
    <div className="space-y-8">
      {focusDrivers.map((dn) => {
        const strat = strategies[dn];
        const meta = driverMeta[dn];
        if (!strat) return null;

        const colour = teamColour(meta?.team_name || "", meta?.team_colour);

        return (
          <div key={dn}>
            {/* Driver header */}
            <div className="flex items-center gap-3 mb-4">
              <div
                className="w-1 h-8 rounded-full"
                style={{ backgroundColor: colour }}
              />
              <div>
                <div className="flex items-center gap-2">
                  <span
                    className="text-base font-black tracking-tight"
                    style={{ color: colour }}
                  >
                    {meta?.abbreviation || `#${dn}`}
                  </span>
                  <span className="text-sm text-zinc-400">{meta?.team_name}</span>
                </div>
                <div className="text-[10px] text-zinc-600">
                  Lap {strat.current_lap} · {meta?.compound} age {meta?.tyre_age}L
                </div>
              </div>
            </div>

            {/* 3 option cards */}
            <div className="grid grid-cols-3 gap-3">
              {strat.options.map((opt) => (
                <StrategyOptionCard
                  key={opt.label}
                  opt={opt}
                  isRecommended={opt.label === strat.recommended}
                />
              ))}
            </div>

            {/* Model note */}
            <p className="mt-2 text-[9px] text-zinc-700 italic">{strat.model_note}</p>
          </div>
        );
      })}
    </div>
  );
}
