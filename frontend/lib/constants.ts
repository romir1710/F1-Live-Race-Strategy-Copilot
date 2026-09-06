/**
 * Team colour lookup — hex codes for all current F1 teams.
 * Used to colour tyre stint bars, driver name chips, and gap chart lines.
 */
export const TEAM_COLOURS: Record<string, string> = {
  "Red Bull Racing": "#3671C6",
  "Ferrari": "#E8002D",
  "Mercedes": "#27F4D2",
  "McLaren": "#FF8000",
  "Aston Martin": "#229971",
  "Alpine": "#FF87BC",
  "Williams": "#64C4FF",
  "RB": "#6692FF",
  "Haas F1 Team": "#B6BABD",
  "Kick Sauber": "#52E252",
  "Unknown": "#888888",
};

export function teamColour(teamName: string, hexOverride?: string): string {
  if (hexOverride && hexOverride !== "FFFFFF") return `#${hexOverride}`;
  return TEAM_COLOURS[teamName] || "#888888";
}

export const COMPOUND_COLOURS: Record<string, string> = {
  SOFT: "#E8002D",
  MEDIUM: "#FFF200",
  HARD: "#FFFFFF",
  INTER: "#39B54A",
  WET: "#0067FF",
  UNKNOWN: "#666666",
};

export const COMPOUND_ICONS: Record<string, string> = {
  SOFT: "S",
  MEDIUM: "M",
  HARD: "H",
  INTER: "I",
  WET: "W",
  UNKNOWN: "?",
};

export function formatLapTime(seconds: number): string {
  if (!seconds || seconds < 0) return "—";
  const mins = Math.floor(seconds / 60);
  const secs = (seconds % 60).toFixed(3).padStart(6, "0");
  return `${mins}:${secs}`;
}

export function formatGap(gap: number): string {
  if (!gap || gap < 0) return "—";
  return `+${gap.toFixed(3)}s`;
}

export function riskColour(risk: string): string {
  return { LOW: "#27F4D2", MEDIUM: "#FFF200", HIGH: "#E8002D" }[risk] || "#888";
}
