"use client";

import { useState, useRef, useEffect } from "react";

interface Message {
  role: "user" | "assistant";
  content: string;
  tool_calls?: { name: string; status: string }[];
  timestamp: Date;
}

export default function ChatPage() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!input.trim() || loading) return;

    const userMsg: Message = {
      role: "user",
      content: input.trim(),
      timestamp: new Date(),
    };
    setMessages((prev) => [...prev, userMsg]);
    setInput("");
    setLoading(true);

    try {
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: userMsg.content }),
      });

      if (res.ok) {
        const data = await res.json();
        const assistantMsg: Message = {
          role: "assistant",
          content: data.response ?? data.content ?? "No response",
          tool_calls: data.tool_calls,
          timestamp: new Date(),
        };
        setMessages((prev) => [...prev, assistantMsg]);
      } else {
        setMessages((prev) => [
          ...prev,
          {
            role: "assistant",
            content: "Error: could not reach the analysis backend. Ensure the FastAPI server is running on port 8003.",
            timestamp: new Date(),
          },
        ]);
      }
    } catch {
      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          content: "Connection failed. Start the backend with: uv run python -m app.cli serve",
          timestamp: new Date(),
        },
      ]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex flex-col h-[calc(100vh-4rem)]">
      <div className="mb-4">
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)]">
          AI Analysis Chat
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Query economic data, run analyses, generate briefings
        </p>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto mb-4 space-y-4">
        {messages.length === 0 && (
          <div className="flex items-center justify-center h-full">
            <div className="text-center max-w-md">
              <p className="text-sm text-[var(--text-muted)] mb-4">
                Ask questions about economic indicators, request analysis, or generate briefings.
              </p>
              <div className="space-y-2">
                {[
                  "What is the current trade openness ratio?",
                  "Analyze GDP growth trends for the past 5 years",
                  "Generate a flash briefing on labor market conditions",
                  "Compare inflation across South Asian economies",
                ].map((prompt) => (
                  <button
                    key={prompt}
                    onClick={() => setInput(prompt)}
                    className="block w-full text-left px-4 py-2.5 rounded-lg text-xs text-[var(--text-secondary)] bg-[var(--bg-card)] border border-[var(--border)] hover:border-[var(--accent-primary)] transition-colors"
                  >
                    {prompt}
                  </button>
                ))}
              </div>
            </div>
          </div>
        )}

        {messages.map((msg, i) => (
          <div
            key={i}
            className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
          >
            <div
              className={`max-w-[75%] rounded-xl px-4 py-3 ${
                msg.role === "user"
                  ? "bg-[var(--accent-primary)] text-white"
                  : "glass-card"
              }`}
            >
              <p className="text-sm whitespace-pre-wrap">{msg.content}</p>
              {msg.tool_calls && msg.tool_calls.length > 0 && (
                <div className="mt-2 pt-2 border-t border-[var(--border)]/30">
                  <span className="text-[10px] font-mono text-[var(--text-muted)] uppercase tracking-wider">
                    Tool Calls
                  </span>
                  <div className="mt-1 space-y-1">
                    {msg.tool_calls.map((tc, j) => (
                      <div key={j} className="flex items-center gap-2">
                        <span className={`w-1.5 h-1.5 rounded-full ${
                          tc.status === "success" ? "bg-emerald-500" : "bg-amber-500"
                        }`} />
                        <span className="text-xs font-mono text-[var(--text-secondary)]">
                          {tc.name}
                        </span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              <span className="text-[10px] text-[var(--text-muted)] mt-1 block">
                {msg.timestamp.toLocaleTimeString()}
              </span>
            </div>
          </div>
        ))}

        {loading && (
          <div className="flex justify-start">
            <div className="glass-card px-4 py-3">
              <div className="flex items-center gap-2">
                <div className="flex gap-1">
                  <span className="w-1.5 h-1.5 bg-[var(--accent-primary)] rounded-full animate-bounce" />
                  <span className="w-1.5 h-1.5 bg-[var(--accent-primary)] rounded-full animate-bounce [animation-delay:0.15s]" />
                  <span className="w-1.5 h-1.5 bg-[var(--accent-primary)] rounded-full animate-bounce [animation-delay:0.3s]" />
                </div>
                <span className="text-xs text-[var(--text-muted)]">Analyzing...</span>
              </div>
            </div>
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Input */}
      <form onSubmit={handleSubmit} className="flex gap-3">
        <input
          type="text"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          placeholder="Ask about economic indicators, analysis, or briefings..."
          className="flex-1 px-4 py-3 rounded-xl bg-[var(--bg-card)] border border-[var(--border)] text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--accent-primary)] transition-colors"
          disabled={loading}
        />
        <button
          type="submit"
          disabled={loading || !input.trim()}
          className="px-5 py-3 rounded-xl bg-[var(--accent-primary)] text-white text-sm font-medium hover:bg-[var(--accent-primary)]/90 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          Send
        </button>
      </form>
    </div>
  );
}
