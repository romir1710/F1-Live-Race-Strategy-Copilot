import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";

const inter = Inter({ subsets: ["latin"], variable: "--font-inter" });

export const metadata: Metadata = {
  title: "F1 Live Race Strategy Copilot",
  description:
    "Real-time Formula 1 race strategy simulation with a natural-language AI agent. Branching undercut/overcut/stay-out analysis on live 2024 Bahrain GP telemetry.",
  keywords: ["Formula 1", "F1 strategy", "race strategy", "telemetry", "undercut", "LangGraph"],
  openGraph: {
    title: "F1 Live Race Strategy Copilot",
    description: "Live F1 race strategy simulation with real telemetry and AI agent",
    type: "website",
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className={inter.variable}>
      <body className="antialiased">{children}</body>
    </html>
  );
}
