"use client";

import { useEffect, useState } from "react";
import {
  LineChart, Line, AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend,
} from "recharts";

interface MacroSummary {
  gdp_growth: number | null;
  inflation: number | null;
  unemployment: number | null;
  gdp_decomposition: { year: string; consumption: number; investment: number; government: number; net_exports: number }[];
  phillips_data: { inflation: number; unemployment: number; year: string }[];
}

const placeholderGDP = [
  { year: "2019", consumption: 68, investment: 18, government: 14, net_exports: 0 },
  { year: "2020", consumption: 67, investment: 16, government: 16, net_exports: 1 },
  { year: "2021", consumption: 69, investment: 17, government: 14, net_exports: 0 },
  { year: "2022", consumption: 68, investment: 18, government: 13, net_exports: 1 },
  { year: "2023", consumption: 69, investment: 17, government: 14, net_exports: 0 },
];

export default function MacroPage() {
  const [data, setData] = useState<MacroSummary | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/macro/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setData(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const gdpData = data?.gdp_decomposition ?? placeholderGDP;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 2
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Macroeconomic Analysis
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          GDP decomposition, Phillips curve dynamics, business cycle indicators
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "GDP Growth", value: data?.gdp_growth, unit: "%" },
          { label: "Inflation Rate", value: data?.inflation, unit: "%" },
          { label: "Unemployment", value: data?.unemployment, unit: "%" },
        ].map((m) => (
          <div key={m.label} className="glass-card p-5">
            <span className="text-xs text-[var(--text-muted)]">{m.label}</span>
            <div className="mt-1">
              {m.value !== null && m.value !== undefined ? (
                <span className="text-xl font-semibold font-mono">
                  {m.value.toFixed(1)}<span className="text-sm text-[var(--text-muted)] ml-1">{m.unit}</span>
                </span>
              ) : (
                <span className="text-sm text-[var(--text-muted)]">{loading ? "Loading..." : "Awaiting data"}</span>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* GDP Decomposition */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          GDP Expenditure Decomposition (% of GDP)
        </h2>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <AreaChart data={gdpData} margin={{ left: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis dataKey="year" tick={{ fontSize: 12, fill: "var(--text-secondary)" }} />
              <YAxis tick={{ fontSize: 12, fill: "var(--text-secondary)" }} />
              <Tooltip
                contentStyle={{
                  background: "var(--bg-card)",
                  border: "1px solid var(--border)",
                  borderRadius: "0.5rem",
                  fontSize: "0.75rem",
                }}
              />
              <Legend wrapperStyle={{ fontSize: "0.75rem" }} />
              <Area type="monotone" dataKey="consumption" stackId="1" fill="#0891b2" stroke="#0891b2" fillOpacity={0.6} />
              <Area type="monotone" dataKey="investment" stackId="1" fill="#d97706" stroke="#d97706" fillOpacity={0.6} />
              <Area type="monotone" dataKey="government" stackId="1" fill="#7c3aed" stroke="#7c3aed" fillOpacity={0.6} />
              <Area type="monotone" dataKey="net_exports" stackId="1" fill="#059669" stroke="#059669" fillOpacity={0.6} />
            </AreaChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Phillips Curve & Cycle Indicators */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="glass-card p-5">
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
            Phillips Curve
          </h2>
          <p className="text-sm text-[var(--text-secondary)] mb-4">
            Inflation-unemployment tradeoff. Scatter plot will populate with historical data
            from the analysis pipeline.
          </p>
          <div className="h-48 flex items-center justify-center">
            <span className="text-sm text-[var(--text-muted)]">
              {loading ? "Loading..." : "Run macro analysis to generate Phillips curve"}
            </span>
          </div>
        </div>

        <div className="glass-card p-5">
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
            Cycle Indicators
          </h2>
          <div className="space-y-3">
            {[
              { name: "Output Gap", desc: "HP-filtered deviation from trend" },
              { name: "Leading Index", desc: "Composite of yield curve, PMI, claims" },
              { name: "Financial Conditions", desc: "Credit spreads, equity vol, rates" },
            ].map((ind) => (
              <div key={ind.name} className="flex items-center justify-between py-2 border-b border-[var(--border)]/50 last:border-0">
                <div>
                  <p className="text-sm text-[var(--text-primary)]">{ind.name}</p>
                  <p className="text-xs text-[var(--text-muted)]">{ind.desc}</p>
                </div>
                <span className="text-sm font-mono text-[var(--text-muted)]">--</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
