"use client";

import { useEffect, useState } from "react";

interface PoliticalScore {
  score: number | null;
  signal: string | null;
  module_count: number | null;
}

const MODULES = [
  { name: "Political Business Cycles", desc: "Election-driven fiscal expansion, monetary accommodation, opportunistic cycles" },
  { name: "Lobbying", desc: "Campaign finance effects, rent-seeking costs, industry concentration and policy bias" },
  { name: "Corruption", desc: "Bribery incidence, institutional quality, growth drag, ICRG and CPI decomposition" },
  { name: "Conflict Economics", desc: "Civil conflict onset, resource curse, war costs, post-conflict reconstruction" },
  { name: "Sanctions", desc: "Trade impact estimation, third-party effects, evasion channels, welfare costs" },
  { name: "Trade Wars", desc: "Tariff retaliation dynamics, consumer burden, welfare decomposition under retaliation" },
  { name: "Media and Information", desc: "Press freedom-growth link, propaganda detection, media capture indices" },
  { name: "Electoral Economics", desc: "Voting behavior, redistribution preferences, income inequality and polarization" },
  { name: "State Capacity", desc: "Tax collection efficiency, bureaucratic quality, public service delivery scores" },
  { name: "Regulatory Capture", desc: "Revolving door effects, industry-regulator proximity, capture measurement" },
];

export default function PoliticalPage() {
  const [data, setData] = useState<PoliticalScore | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/political/score")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setData(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 12
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Political Economy
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Business cycles, lobbying, corruption, conflict, sanctions, state capacity, regulatory capture
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Composite Score", value: data?.score != null ? data.score.toFixed(1) : null },
          { label: "Signal", value: data?.signal ?? null },
          { label: "Modules", value: data?.module_count != null ? String(data.module_count) : String(MODULES.length) },
        ].map((m) => (
          <div key={m.label} className="glass-card p-5">
            <span className="text-xs text-[var(--text-muted)]">{m.label}</span>
            <div className="mt-1">
              {m.value !== null ? (
                <span className="text-xl font-semibold font-mono">{m.value}</span>
              ) : (
                <span className="text-sm text-[var(--text-muted)]">{loading ? "Loading..." : "Awaiting data"}</span>
              )}
            </div>
          </div>
        ))}
      </div>

      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Analytical Modules ({MODULES.length})
        </h2>
        <div className="space-y-0">
          {MODULES.map((mod, i) => (
            <div
              key={mod.name}
              className={`flex items-start gap-4 py-3 ${i < MODULES.length - 1 ? "border-b border-[var(--border)]/50" : ""}`}
            >
              <span className="text-xs font-mono text-[var(--text-muted)] w-5 pt-0.5 shrink-0">
                {String(i + 1).padStart(2, "0")}
              </span>
              <div>
                <p className="text-sm font-medium text-[var(--text-primary)]">{mod.name}</p>
                <p className="text-xs text-[var(--text-secondary)] mt-0.5">{mod.desc}</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
