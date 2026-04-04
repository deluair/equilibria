"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";

interface HealthStats {
  health_expenditure_gdp: number | null;
  oop_share: number | null;
  uhc_index: number | null;
  expenditure_by_source: { source: string; share: number }[];
}

export default function HealthPage() {
  const [stats, setStats] = useState<HealthStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/health/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setStats(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const placeholderSources = [
    { source: "Government", share: 51.3 },
    { source: "Out-of-pocket", share: 28.7 },
    { source: "Private insurance", share: 12.4 },
    { source: "External aid", share: 4.1 },
    { source: "Other private", share: 3.5 },
  ];

  const sourceData = stats?.expenditure_by_source ?? placeholderSources;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 8
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Health Economics
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Health expenditure, disease burden, pharmaceutical markets, pandemic shocks, workforce
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Health Expenditure", value: stats?.health_expenditure_gdp, unit: "% GDP" },
          { label: "Out-of-Pocket Share", value: stats?.oop_share, unit: "%" },
          { label: "UHC Service Index", value: stats?.uhc_index, unit: "/100" },
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

      {/* Expenditure by Source Chart */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Health Expenditure by Source (% of total)
        </h2>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={sourceData} layout="vertical" margin={{ left: 120 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis type="number" tick={{ fontSize: 12, fill: "var(--text-secondary)" }} unit="%" domain={[0, 60]} />
              <YAxis
                type="category"
                dataKey="source"
                tick={{ fontSize: 12, fill: "var(--text-secondary)" }}
                width={115}
              />
              <Tooltip
                contentStyle={{
                  background: "var(--bg-card)",
                  border: "1px solid var(--border)",
                  borderRadius: "0.5rem",
                  fontSize: "0.75rem",
                }}
                formatter={(v) => [`${Number(v).toFixed(1)}%`, "Share"]}
              />
              <Bar dataKey="share" fill="var(--accent-primary)" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Source: WHO Global Health Expenditure Database. Values update after running the health analysis pipeline.
        </p>
      </div>

      {/* Health Model Table */}
      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
          Health Model Estimates
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
                ["Catastrophic expenditure threshold", "WHO 10%", "--"],
                ["DALYs per 100,000", "GBD 2021", "--"],
                ["Physician density (per 1,000)", "WHO 2.3/1000", "--"],
                ["DPT3 vaccine coverage", "Vaccine coverage", "--"],
                ["AMR economic burden (% GDP)", "O'Neill", "--"],
                ["Pandemic GDP loss", "Event study", "--"],
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
          Estimates populate after running the health analysis pipeline.
        </p>
      </div>
    </div>
  );
}
