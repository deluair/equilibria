"use client";

import { useEffect, useState } from "react";
import {
  ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, ZAxis,
} from "recharts";

interface DevSummary {
  hdi: number | null;
  gini: number | null;
  poverty_rate: number | null;
  convergence_data: { country: string; initial_gdp: number; growth: number }[];
}

const placeholderConvergence = [
  { country: "Bangladesh", initial_gdp: 1200, growth: 6.5 },
  { country: "India", initial_gdp: 2100, growth: 5.8 },
  { country: "Vietnam", initial_gdp: 2800, growth: 6.2 },
  { country: "Philippines", initial_gdp: 3400, growth: 4.1 },
  { country: "Indonesia", initial_gdp: 4100, growth: 4.8 },
  { country: "Thailand", initial_gdp: 7200, growth: 3.2 },
  { country: "Malaysia", initial_gdp: 11500, growth: 4.0 },
  { country: "South Korea", initial_gdp: 31800, growth: 2.5 },
];

export default function DevelopmentPage() {
  const [data, setData] = useState<DevSummary | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/development/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setData(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const convergenceData = data?.convergence_data ?? placeholderConvergence;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 4
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Development Economics
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Income convergence, poverty dynamics, institutional quality, human development
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Human Development Index", value: data?.hdi, unit: "" },
          { label: "Gini Coefficient", value: data?.gini, unit: "" },
          { label: "Poverty Rate ($2.15/day)", value: data?.poverty_rate, unit: "%" },
        ].map((m) => (
          <div key={m.label} className="glass-card p-5">
            <span className="text-xs text-[var(--text-muted)]">{m.label}</span>
            <div className="mt-1">
              {m.value !== null && m.value !== undefined ? (
                <span className="text-xl font-semibold font-mono">
                  {m.value.toFixed(m.unit === "%" ? 1 : 3)}
                  {m.unit && <span className="text-sm text-[var(--text-muted)] ml-1">{m.unit}</span>}
                </span>
              ) : (
                <span className="text-sm text-[var(--text-muted)]">{loading ? "Loading..." : "Awaiting data"}</span>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Convergence Scatter */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Income Convergence: Initial GDP per Capita vs. Growth Rate
        </h2>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart margin={{ left: 10, bottom: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis
                type="number"
                dataKey="initial_gdp"
                name="Initial GDP/cap"
                tick={{ fontSize: 12, fill: "var(--text-secondary)" }}
                label={{ value: "Initial GDP per Capita (USD)", position: "bottom", fontSize: 11, fill: "var(--text-muted)" }}
              />
              <YAxis
                type="number"
                dataKey="growth"
                name="Growth %"
                tick={{ fontSize: 12, fill: "var(--text-secondary)" }}
                label={{ value: "Avg. Growth Rate (%)", angle: -90, position: "insideLeft", fontSize: 11, fill: "var(--text-muted)" }}
              />
              <ZAxis range={[60, 60]} />
              <Tooltip
                contentStyle={{
                  background: "var(--bg-card)",
                  border: "1px solid var(--border)",
                  borderRadius: "0.5rem",
                  fontSize: "0.75rem",
                }}
                formatter={(value, name) => {
                  const v = Number(value);
                  return [
                    name === "Initial GDP/cap" ? `$${v.toLocaleString()}` : `${v}%`,
                    String(name),
                  ];
                }}
                labelFormatter={() => ""}
              />
              <Scatter data={convergenceData} fill="var(--accent-primary)" />
            </ScatterChart>
          </ResponsiveContainer>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Negative slope indicates beta-convergence (Barro and Sala-i-Martin, 1992).
          Placeholder data shown until analysis pipeline runs.
        </p>
      </div>

      {/* Institutional Indicators */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="glass-card p-5">
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
            Institutional Quality
          </h2>
          <div className="space-y-3">
            {[
              { name: "Rule of Law", source: "WGI" },
              { name: "Regulatory Quality", source: "WGI" },
              { name: "Government Effectiveness", source: "WGI" },
              { name: "Control of Corruption", source: "WGI" },
              { name: "Voice and Accountability", source: "WGI" },
            ].map((ind) => (
              <div key={ind.name} className="flex items-center justify-between py-1.5 border-b border-[var(--border)]/50 last:border-0">
                <div>
                  <p className="text-sm text-[var(--text-primary)]">{ind.name}</p>
                  <p className="text-xs text-[var(--text-muted)]">{ind.source}</p>
                </div>
                <span className="text-sm font-mono text-[var(--text-muted)]">--</span>
              </div>
            ))}
          </div>
        </div>

        <div className="glass-card p-5">
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
            Poverty Dynamics
          </h2>
          <div className="space-y-3">
            {[
              { name: "Headcount Ratio ($2.15)", desc: "International poverty line" },
              { name: "Headcount Ratio ($3.65)", desc: "Lower-middle-income line" },
              { name: "Poverty Gap Index", desc: "Depth of poverty" },
              { name: "Multidimensional Poverty", desc: "OPHI MPI" },
            ].map((ind) => (
              <div key={ind.name} className="flex items-center justify-between py-1.5 border-b border-[var(--border)]/50 last:border-0">
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
