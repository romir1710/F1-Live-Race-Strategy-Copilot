/**
 * ConnectionOverlay.tsx — "Connecting to race engine..." overlay.
 *
 * Covers the full viewport until WebSocket connects.
 * Shown during: initial load, Render cold-start wake-up, and reconnection.
 * Covers the worst-case fully-cold scenario (Render + Aiven both sleeping after >1hr idle).
 */
"use client";
import { ConnectionStatus } from "@/hooks/useRaceSocket";

const STATUS_MESSAGES: Record<ConnectionStatus, { title: string; subtitle: string }> = {
  connecting: {
    title: "Connecting to race engine…",
    subtitle:
      "Waking up the live telemetry pipeline. If this is a cold start, allow up to 30 seconds.",
  },
  reconnecting: {
    title: "Reconnecting…",
    subtitle: "Connection dropped — attempting to reconnect automatically.",
  },
  error: {
    title: "Connection error",
    subtitle: "The race engine is unreachable. Please refresh the page.",
  },
  connected: {
    title: "",
    subtitle: "",
  },
};

interface Props {
  status: ConnectionStatus;
}

export function ConnectionOverlay({ status }: Props) {
  if (status === "connected") return null;

  const { title, subtitle } = STATUS_MESSAGES[status];

  return (
    <div className="fixed inset-0 z-50 flex flex-col items-center justify-center
                    bg-zinc-950/95 backdrop-blur-sm">
      {/* F1 logo-style animated spinner */}
      <div className="relative w-16 h-16 mb-8">
        <div className="absolute inset-0 rounded-full border-2 border-white/10" />
        <div
          className="absolute inset-0 rounded-full border-2 border-transparent
                     border-t-red-500 animate-spin"
          style={{ animationDuration: "0.8s" }}
        />
        <div className="absolute inset-3 rounded-full bg-red-600 flex items-center justify-center">
          <span className="text-white text-xs font-black">F1</span>
        </div>
      </div>

      <h2 className="text-white text-xl font-bold mb-2">{title}</h2>
      <p className="text-zinc-400 text-sm max-w-sm text-center leading-relaxed">{subtitle}</p>

      {/* Dot pulse */}
      <div className="flex gap-2 mt-8">
        {[0, 1, 2].map((i) => (
          <div
            key={i}
            className="w-2 h-2 rounded-full bg-red-500 animate-bounce"
            style={{ animationDelay: `${i * 150}ms` }}
          />
        ))}
      </div>
    </div>
  );
}
