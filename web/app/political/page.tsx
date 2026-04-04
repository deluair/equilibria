"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine,
} from "recharts";

interface PoliticalStats {
  democracy_index: number | null;
  cpi_score: number | null;
  state_fragility: number | null;
  wgi_indicators: { indicator: string; value: number }[];
}

export default function PoliticalPage() {
  const [stats, setStats] = useState<PoliticalStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/political/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setStats(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const placeholderWGI = [
    { indicator: "Voice & Accountability", value: -0.42 },
    { indicator: "Political Stability", value: -0.85 },
    { indicator: "Govt Effectiveness", value: -0.68 },
    { indicator: "Regulatory Quality", value: -0.72 },
    { indicator: "Rule of Law", value: -0.61 },
    { indicator: "Control of Corruption", value: -0.93 },
  ];

  const wgiData = stats?.wgi_indicators ?? placeholderWGI;

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

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Democracy Index", value: stats?.democracy_index, unit: "/10" },
          { label: "CPI Score", value: stats?.cpi_score, unit: "/100" },
          { label: "State Fragility", value: stats?.state_fragility, unit: "/120" },
        ].map((m) => (
          <div key={m.label} className="glass-card p-5">
            <span className="text-xs text-[var(--text-muted)]">{m.label}</span>
            <div className="mt-1">
              {m.value !== null && m.value !== undefined ? (
                <span className="text-xl font-semibold font-mono">
                  {m.value.toFixed(2)}<span className="text-sm text-[var(--text-muted)] ml-1">{m.unit}</span>
                </span>
              ) : (
                <span className="text-sm text-[var(--text-muted)]">{loading ? "Loading..." : "Awaiting data"}</span>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* WGI Governance Indicators Chart */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          World Governance Indicators (WGI score, -2.5 to +2.5)
        </h2>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={wgiData} layout="vertical" margin={{ left: 150 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis
                type="number"
                tick={{ fontSize: 12, fill: "var(--text-secondary)" }}
                domain={[-2.5, 2.5]}
              />
              <YAxis
                type="category"
                dataKey="indicator"
                tick={{ fontSize: 11, fill: "var(--text-secondary)" }}
                width={145}
              />
              <Tooltip
                contentStyle={{
                  background: "var(--bg-card)",
                  border: "1px solid var(--border)",
                  borderRadius: "0.5rem",
                  fontSize: "0.75rem",
                }}
                formatter={(v: number) => [`${v.toFixed(2)}`, "WGI Score"]}
              />
              <ReferenceLine x={0} stroke="var(--border)" strokeWidth={1} />
              <Bar dataKey="value" fill="var(--accent-secondary)" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Source: World Bank Worldwide Governance Indicators. Scale: -2.5 (weak) to +2.5 (strong).
        </p>
      </div>

      {/* Political Economy Model Table */}
      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
          Political Economy Model Estimates
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--border)]">
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Metric</th>
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Model</th>
                <th className="text-right py-2 px-3 font-medium text-[var(--text-secondary)]">Estimate</th>
              </tr>
            </thead>
            <tbody className="text-[var(--text-primary)]">
              {[
                ["Pre-election fiscal expansion", "Nordhaus PBC", "--"],
                ["Democracy-growth elasticity", "Acemoglu 2019", "--"],
                ["Corruption drag (% GDP/yr)", "Mauro 1995", "--"],
                ["Conflict GDP loss (% per yr)", "Collier 2007", "--"],
                ["Sanctions trade reduction", "Hufbauer", "--"],
                ["Lobbying ROI ($ per $ spent)", "Richter 2009", "--"],
              ].map(([metric, model, est]) => (
                <tr key={metric} className="border-b border-[var(--border)]/50">
                  <td className="py-2 px-3 font-mono text-xs">{metric}</td>
                  <td className="py-2 px-3 text-xs text-[var(--text-secondary)]">{model}</td>
                  <td className="text-right py-2 px-3 font-mono text-xs text-[var(--text-muted)]">{est}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Estimates populate after running the political economy analysis pipeline.
        </p>
      </div>
    </div>
  );
}
