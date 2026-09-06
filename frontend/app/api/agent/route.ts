/**
 * /api/agent/route.ts — Thin proxy from Vercel to the Render backend agent endpoint.
 *
 * Keeps LangGraph running on Render (stateful, long-lived process) while
 * the frontend calls a same-origin Vercel API route — no CORS issues,
 * no LangGraph-in-serverless problems.
 */
import { NextRequest, NextResponse } from "next/server";

const BACKEND_URL =
  process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:8000";

export async function POST(req: NextRequest) {
  try {
    const body = await req.json();

    const upstream = await fetch(`${BACKEND_URL}/agent`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
      // 55s timeout — generous enough for what-if simulation + Gemini call
      // Vercel max is 60s on Pro, 10s on Hobby — adjust if on Hobby plan
      signal: AbortSignal.timeout(55000),
    });

    if (!upstream.ok) {
      const errorText = await upstream.text();
      return NextResponse.json(
        { error: `Backend error: ${errorText}` },
        { status: upstream.status }
      );
    }

    const data = await upstream.json();
    return NextResponse.json(data);
  } catch (err: unknown) {
    if (err instanceof Error && err.name === "TimeoutError") {
      return NextResponse.json(
        {
          answer: "The strategy engine took too long to respond. Please try again.",
          quota_fallback: false,
          grounded: false,
        },
        { status: 504 }
      );
    }
    return NextResponse.json(
      {
        answer: "Could not reach the strategy engine. Is the backend awake?",
        quota_fallback: false,
        grounded: false,
      },
      { status: 502 }
    );
  }
}
