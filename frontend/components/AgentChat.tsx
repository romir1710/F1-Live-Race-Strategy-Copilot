/**
 * AgentChat.tsx — Natural language strategy query panel.
 * Sends questions to /api/agent → Render backend → LangGraph → Gemini.
 * Shows a "Grounded in live simulation data" badge on all real answers.
 * Shows quota fallback notice when daily limit is hit.
 */
"use client";
import { useState, useRef, useEffect } from "react";

interface Message {
  role: "user" | "assistant";
  content: string;
  simTable?: object | null;
  quotaFallback?: boolean;
}

const SUGGESTED_PROMPTS = [
  "What if Verstappen had pitted a lap earlier?",
  "Why is the undercut window closing?",
  "Compare Perez and Russell's current strategies",
  "What's the risk of staying out on old tyres?",
];

export function AgentChat({ focusDrivers }: { focusDrivers: number[] }) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const sendMessage = async (question: string) => {
    if (!question.trim() || loading) return;
    const userMsg: Message = { role: "user", content: question };
    setMessages((m) => [...m, userMsg]);
    setInput("");
    setLoading(true);

    try {
      const res = await fetch("/api/agent", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ question, driver_number: focusDrivers[0] }),
      });
      const data = await res.json();
      const assistantMsg: Message = {
        role: "assistant",
        content: data.answer || "No response.",
        simTable: data.sim_table,
        quotaFallback: data.quota_fallback,
      };
      setMessages((m) => [...m, assistantMsg]);
    } catch {
      setMessages((m) => [
        ...m,
        { role: "assistant", content: "Connection error — please try again." },
      ]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex flex-col h-full min-h-[460px] max-h-[640px]">
      {/* Suggested prompts (show only if no messages yet) */}
      {messages.length === 0 && (
        <div className="flex flex-col gap-2 mb-4">
          <p className="text-xs text-zinc-500 uppercase tracking-widest mb-1">Try asking:</p>
          {SUGGESTED_PROMPTS.map((p) => (
            <button
              key={p}
              onClick={() => sendMessage(p)}
              className="text-left text-sm text-zinc-400 px-3 py-2.5 rounded-lg border border-white/8
                         hover:border-white/20 hover:text-white hover:bg-white/5 transition-all duration-200"
            >
              {p}
            </button>
          ))}
        </div>
      )}

      {/* Message list */}
      <div className="flex-1 overflow-y-auto space-y-3 pr-1 scrollbar-thin scrollbar-thumb-white/10">
        {messages.map((msg, i) => (
          <div
            key={i}
            className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
          >
            <div
              className={`max-w-[85%] rounded-2xl px-4 py-3 text-sm leading-relaxed
                          ${msg.role === "user"
                            ? "bg-white/10 text-white rounded-br-sm"
                            : "bg-zinc-900 border border-white/10 text-zinc-200 rounded-bl-sm"
                          }`}
            >
              {msg.content}

              {/* Grounded badge / quota fallback notice */}
              {msg.role === "assistant" && (
                <div className="mt-2 flex items-center gap-2">
                  {msg.quotaFallback ? (
                    <span className="text-[9px] text-amber-500/70 border border-amber-500/20
                                     px-2 py-0.5 rounded-full">
                      ⚠ Gemini quota reached · Raw simulation data shown
                    </span>
                  ) : (
                    <span className="text-[9px] text-emerald-500/70 border border-emerald-500/20
                                     px-2 py-0.5 rounded-full">
                      ✓ Grounded in live simulation
                    </span>
                  )}
                </div>
              )}
            </div>
          </div>
        ))}

        {loading && (
          <div className="flex justify-start">
            <div className="bg-zinc-900 border border-white/10 rounded-2xl rounded-bl-sm px-4 py-3">
              <div className="flex gap-1.5 items-center h-4">
                {[0, 1, 2].map((i) => (
                  <div
                    key={i}
                    className="w-1.5 h-1.5 rounded-full bg-zinc-500 animate-bounce"
                    style={{ animationDelay: `${i * 120}ms` }}
                  />
                ))}
              </div>
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <form
        onSubmit={(e) => {
          e.preventDefault();
          sendMessage(input);
        }}
        className="mt-4 flex gap-2"
      >
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Ask a strategy question…"
          disabled={loading}
          className="flex-1 bg-zinc-900 border border-white/10 rounded-xl px-4 py-2.5 text-sm
                     text-white placeholder:text-zinc-600 focus:outline-none focus:border-white/30
                     disabled:opacity-50 transition-colors"
        />
        <button
          type="submit"
          disabled={loading || !input.trim()}
          className="px-4 py-2.5 rounded-xl bg-white/10 hover:bg-white/20 text-white text-sm
                     font-medium disabled:opacity-30 transition-colors"
        >
          Ask
        </button>
      </form>
    </div>
  );
}
