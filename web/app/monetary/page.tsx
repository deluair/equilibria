"use client";

import { useEffect, useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine,
} from "recharts";

interface MonetaryStats {
  policy_rate: number | null;
  inflation_gap: number | null;
  real_rate: number | null;
  rate_path: { period: string; rate: number }[];
}

export default function MonetaryPage() {
  const [stats, setStats] = useState<MonetaryStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/monetary/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setStats(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const placeholderPath = [
    { period: "2020Q1", rate: 1.75 },
    { period: "2020Q2", rate: 0.25 },
    { period: "2021Q1", rate: 0.25 },
    { period: "2021Q4", rate: 0.25 },
    { period: "2022Q2", rate: 1.75 },
    { period: "2022Q4", rate: 4.25 },
    { period: "2023Q2", rate: 5.25 },
    { period: "2023Q4", rate: 5.50 },
    { period: "2024Q2", rate: 5.25 },
    { period: "2024Q4", rate: 4.50 },
  ];

  const ratePath = stats?.rate_path ?? placeholderPath;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 9
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Monetary Economics
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Money demand, central bank policy, inflation targeting, exchange rates, digital currency, capital controls
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Policy Rate", value: stats?.policy_rate, unit: "%" },
          { label: "Inflation Gap", value: stats?.inflation_gap, unit: "pp" },
          { label: "Ex-ante Real Rate", value: stats?.real_rate, unit: "%" },
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

      {/* Policy Rate Path */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Policy Rate Path
        </h2>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={ratePath} margin={{ left: 10, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis dataKey="period" tick={{ fontSize: 11, fill: "var(--text-secondary)" }} />
              <YAxis tick={{ fontSize: 12, fill: "var(--text-secondary)" }} unit="%" />
              <Tooltip
                contentStyle={{
                  background: "var(--bg-card)",
                  border: "1px solid var(--border)",
                  borderRadius: "0.5rem",
                  fontSize: "0.75rem",
                }}
              />
              <ReferenceLine y={2} stroke="var(--accent-secondary)" strokeDasharray="4 4" label={{ value: "Target", fontSize: 11, fill: "var(--text-muted)" }} />
              <Line dataKey="rate" stroke="var(--accent-primary)" dot={false} strokeWidth={2} />
            </LineChart>
          </ResponsiveContainer>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Dashed line: 2% inflation target. Policy rate vs. neutral rate gap drives Taylor rule residuals.
        </p>
      </div>

      {/* Monetary Metrics Table */}
      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
          Monetary Model Estimates
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--border)]">
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Metric</th>
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Framework</th>
                <th className="text-right py-2 px-3 font-medium text-[var(--text-secondary)]">Estimate</th>
              </tr>
            </thead>
            <tbody className="text-[var(--text-primary)]">
              {[
                ["Money demand income elasticity", "Baumol-Tobin", "--"],
                ["Taylor rule coefficient (pi)", "Inflation targeting", "--"],
                ["ERPT coefficient", "Exchange rate pass-through", "--"],
                ["Dollarization ratio", "Currency substitution", "--"],
                ["CBDC adoption elasticity", "Digital currency", "--"],
                ["Capital flow sensitivity (UIP)", "Capital controls", "--"],
              ].map(([metric, framework, est]) => (
                <tr key={metric} className="border-b border-[var(--border)]/50">
                  <td className="py-2 px-3 font-mono text-xs">{metric}</td>
                  <td className="py-2 px-3 text-xs text-[var(--text-secondary)]">{framework}</td>
                  <td className="text-right py-2 px-3 font-mono text-xs text-[var(--text-muted)]">{est}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Estimates populate after running the monetary analysis pipeline.
        </p>
      </div>
    </div>
  );
}
