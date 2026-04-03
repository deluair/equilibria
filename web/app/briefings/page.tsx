"use client";

import { useEffect, useState } from "react";

interface Briefing {
  id: string;
  title: string;
  type: string;
  layer: string | null;
  created_at: string;
  summary: string | null;
}

const typeLabels: Record<string, { label: string; color: string }> = {
  flash: { label: "Flash", color: "bg-rose-50 text-rose-700" },
  weekly: { label: "Weekly", color: "bg-blue-50 text-blue-700" },
  deep_dive: { label: "Deep Dive", color: "bg-purple-50 text-purple-700" },
  signal: { label: "Signal", color: "bg-amber-50 text-amber-700" },
};

export default function BriefingsPage() {
  const [briefings, setBriefings] = useState<Briefing[]>([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState<string>("all");

  useEffect(() => {
    fetch("/api/briefings")
      .then((r) => (r.ok ? r.json() : []))
      .then((d) => setBriefings(Array.isArray(d) ? d : []))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const types = ["all", ...new Set(briefings.map((b) => b.type))];
  const filtered = filter === "all" ? briefings : briefings.filter((b) => b.type === filter);

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)]">
          Briefings
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          AI-generated analysis briefings across all economic layers
        </p>
      </div>

      {/* Filter Tabs */}
      <div className="flex gap-2 mb-6">
        {types.map((t) => (
          <button
            key={t}
            onClick={() => setFilter(t)}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${
              filter === t
                ? "bg-[var(--accent-primary)] text-white"
                : "bg-[var(--bg-card)] text-[var(--text-secondary)] border border-[var(--border)] hover:border-[var(--border-hover)]"
            }`}
          >
            {t === "all" ? "All" : typeLabels[t]?.label ?? t}
          </button>
        ))}
      </div>

      {/* Briefing List */}
      {loading ? (
        <div className="glass-card p-8 text-center">
          <p className="text-sm text-[var(--text-muted)]">Loading briefings...</p>
        </div>
      ) : filtered.length > 0 ? (
        <div className="space-y-3">
          {filtered.map((b) => {
            const typeConfig = typeLabels[b.type] ?? { label: b.type, color: "bg-gray-50 text-gray-700" };
            return (
              <div key={b.id} className="glass-card p-5">
                <div className="flex items-start justify-between mb-2">
                  <div className="flex items-center gap-2">
                    <span className={`px-2 py-0.5 rounded text-[10px] font-medium ${typeConfig.color}`}>
                      {typeConfig.label}
                    </span>
                    {b.layer && (
                      <span className="text-[10px] font-mono text-[var(--text-muted)]">{b.layer}</span>
                    )}
                  </div>
                  <span className="text-xs text-[var(--text-muted)]">
                    {new Date(b.created_at).toLocaleDateString()}
                  </span>
                </div>
                <h3 className="text-sm font-medium text-[var(--text-primary)]">{b.title}</h3>
                {b.summary && (
                  <p className="text-xs text-[var(--text-secondary)] mt-1.5 leading-relaxed">
                    {b.summary}
                  </p>
                )}
              </div>
            );
          })}
        </div>
      ) : (
        <div className="glass-card p-8 text-center">
          <p className="text-sm text-[var(--text-muted)]">
            No briefings yet. Run the analysis pipeline to generate briefings.
          </p>
          <p className="text-xs text-[var(--text-muted)] mt-2">
            <code className="font-mono bg-[var(--bg-primary)] px-1.5 py-0.5 rounded">
              python -m app.cli briefings generate
            </code>
          </p>
        </div>
      )}
    </div>
  );
}
