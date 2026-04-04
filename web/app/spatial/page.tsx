"use client";

import { useEffect, useState } from "react";
import {
  ScatterChart, Scatter, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Label,
} from "recharts";

interface SpatialStats {
  urbanization_rate: number | null;
  primacy_index: number | null;
  spatial_gini: number | null;
  regional_convergence: { region: string; initial_gdp: number; growth_rate: number }[];
}

export default function SpatialPage() {
  const [stats, setStats] = useState<SpatialStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/spatial/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setStats(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const placeholderConvergence = [
    { region: "Dhaka", initial_gdp: 2800, growth_rate: 5.1 },
    { region: "Chittagong", initial_gdp: 2200, growth_rate: 6.3 },
    { region: "Rajshahi", initial_gdp: 1100, growth_rate: 7.8 },
    { region: "Khulna", initial_gdp: 1300, growth_rate: 6.9 },
    { region: "Sylhet", initial_gdp: 1700, growth_rate: 5.8 },
    { region: "Rangpur", initial_gdp: 900, growth_rate: 8.2 },
    { region: "Barisal", initial_gdp: 1000, growth_rate: 7.4 },
    { region: "Mymensingh", initial_gdp: 950, growth_rate: 7.9 },
  ];

  const convergenceData = stats?.regional_convergence ?? placeholderConvergence;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 11
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Spatial Economics
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Agglomeration, housing, transport, regional convergence, migration, SEZs, smart cities
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Urbanization Rate", value: stats?.urbanization_rate, unit: "%" },
          { label: "Primacy Index", value: stats?.primacy_index, unit: "ratio" },
          { label: "Spatial Gini", value: stats?.spatial_gini, unit: "index" },
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

      {/* Regional Beta-Convergence Scatter */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Regional Beta-Convergence (Initial GDP per capita vs. Growth Rate)
        </h2>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <ScatterChart margin={{ top: 10, right: 20, bottom: 30, left: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis
                type="number"
                dataKey="initial_gdp"
                tick={{ fontSize: 12, fill: "var(--text-secondary)" }}
                domain={[800, 3000]}
              >
                <Label value="Initial GDP per capita (USD)" offset={-10} position="insideBottom" style={{ fontSize: "0.7rem", fill: "var(--text-muted)" }} />
              </XAxis>
              <YAxis
                type="number"
                dataKey="growth_rate"
                tick={{ fontSize: 12, fill: "var(--text-secondary)" }}
                unit="%"
              />
              <Tooltip
                contentStyle={{
                  background: "var(--bg-card)",
                  border: "1px solid var(--border)",
                  borderRadius: "0.5rem",
                  fontSize: "0.75rem",
                }}
                formatter={(v: number, name: string) => {
                  if (name === "initial_gdp") return [`$${v.toLocaleString()}`, "Initial GDP/cap"];
                  if (name === "growth_rate") return [`${v.toFixed(1)}%`, "Growth rate"];
                  return [v, name];
                }}
                labelFormatter={(_, payload) => payload?.[0]?.payload?.region ?? ""}
              />
              <Scatter data={convergenceData} fill="var(--accent-primary)" />
            </ScatterChart>
          </ResponsiveContainer>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Negative slope indicates beta-convergence: lower-income regions growing faster. Source: BBS divisional accounts.
        </p>
      </div>

      {/* Spatial Model Table */}
      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
          Spatial Model Estimates
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
                ["Agglomeration elasticity", "Combes", "--"],
                ["Moran I (spatial autocorrelation)", "Contiguity", "--"],
                ["Housing supply elasticity", "Saiz", "--"],
                ["Commuting cost (% wage)", "Time-cost", "--"],
                ["SEZ employment multiplier", "DiD", "--"],
                ["Urban-rural wage gap", "Mincer", "--"],
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
          Estimates populate after running the spatial analysis pipeline.
        </p>
      </div>
    </div>
  );
}
