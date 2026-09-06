/**
 * useRaceSocket.ts — WebSocket hook for the F1 Race Strategy Copilot.
 *
 * Matches the FlowState pattern exactly:
 * - Silent wake-up ping to /health on mount (fires before WebSocket connect)
 * - Auto-reconnect with exponential backoff on drop
 * - "Connecting to race engine..." state managed here
 * - Returns race state, driver states, strategy options, and connection status
 */
"use client";
import { useEffect, useRef, useState, useCallback } from "react";

const WS_URL = process.env.NEXT_PUBLIC_WS_URL || "ws://localhost:8000/ws";
const BACKEND_URL = process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:8000";

export type DriverFrame = {
  driver_number: string;
  abbreviation: string;
  team_name: string;
  team_colour: string;
  position: number;
  compound: string;
  tyre_age: number;
  gap_to_leader: number;
  interval_gap: number;
  lap_duration: number;
  is_pit_lap: boolean;
  lap_number: number;
};

export type StrategyOption = {
  label: "STAY_OUT" | "UNDERCUT" | "OVERCUT";
  display_name: string;
  pit_this_lap: boolean;
  pit_in_n_laps: number | null;
  target_compound: string;
  projected_finish_position: number;
  projected_time_delta_s: number;
  tyre_life_risk: "LOW" | "MEDIUM" | "HIGH";
  undercut_gain_s: number;
  reasoning: string;
  confidence: number;
};

export type StrategyResult = {
  driver_number: number;
  current_lap: number;
  options: StrategyOption[];
  recommended: string;
  model_note: string;
};

export type RaceMeta = {
  current_lap: number;
  total_laps: number;
  session_key: number;
  ts: number;
};

export type RaceFrame = {
  race: RaceMeta;
  drivers: DriverFrame[];
  strategies: Record<number, StrategyResult>;
  focus_drivers: number[];
};

export type ConnectionStatus = "connecting" | "connected" | "reconnecting" | "error";

export type RaceSocketReturn = {
  frame: RaceFrame | null;
  status: ConnectionStatus;
  raceFinished: boolean;
};

export function useRaceSocket(): RaceSocketReturn {
  const [frame, setFrame] = useState<RaceFrame | null>(null);
  const [status, setStatus] = useState<ConnectionStatus>("connecting");
  const [raceFinished, setRaceFinished] = useState(false);

  const wsRef = useRef<WebSocket | null>(null);
  const retryCount = useRef(0);
  const retryTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;

    const ws = new WebSocket(WS_URL);
    wsRef.current = ws;

    ws.onopen = () => {
      retryCount.current = 0;
      setStatus("connected");
      setRaceFinished(false);
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === "race_update") {
          setFrame(msg as RaceFrame);
        } else if (msg.type === "race_finished") {
          setRaceFinished(true);
          setTimeout(() => setRaceFinished(false), 6000);
        }
        // "connected" ack and "ping" are silently ignored
      } catch {}
    };

    ws.onclose = () => {
      wsRef.current = null;
      const delay = Math.min(1000 * 2 ** retryCount.current, 30000);
      retryCount.current += 1;
      setStatus(retryCount.current > 1 ? "reconnecting" : "connecting");
      retryTimer.current = setTimeout(connect, delay);
    };

    ws.onerror = () => {
      ws.close();
      setStatus("error");
    };
  }, []);

  useEffect(() => {
    // ── Wake-up ping (exact FlowState pattern) ────────────────────────────────
    // Silently ping Render health endpoint so the service is awake before
    // the visitor notices any delay. mode: no-cors because Render is cross-origin.
    fetch(`${BACKEND_URL}/health`, { mode: "no-cors" }).catch(() => {});

    // Brief delay so Render has a moment to wake before we open WS
    const wsTimer = setTimeout(connect, 800);

    return () => {
      clearTimeout(wsTimer);
      if (retryTimer.current) clearTimeout(retryTimer.current);
      wsRef.current?.close();
    };
  }, [connect]);

  return { frame, status, raceFinished };
}
