"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";

interface FinancialStats {
  banking_zscore: number | null;
  npl_ratio: number | null;
  financial_depth: number | null;
  stability_indicators: { indicator: string; value: number }[];
}

export default function FinancialPage() {
  const [stats, setStats] = useState<FinancialStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/financial/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setStats(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const placeholderIndicators = [
    { indicator: "Capital adequacy", value: 15.2 },
    { indicator: "Liquidity ratio", value: 28.4 },
    { indicator: "NPL ratio", value: 3.1 },
    { indicator: "ROA", value: 1.2 },
    { indicator: "Credit-GDP gap", value: 4.7 },
    { indicator: "Leverage ratio", value: 6.3 },
  ];

  const indicatorData = stats?.stability_indicators ?? placeholderIndicators;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 7
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Financial Economics
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Asset pricing, risk measurement, banking stability, sovereign debt, fintech adoption
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Banking Z-Score", value: stats?.banking_zscore, unit: "index" },
          { label: "NPL Ratio", value: stats?.npl_ratio, unit: "%" },
          { label: "Financial Depth", value: stats?.financial_depth, unit: "% GDP" },
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

      {/* Financial Stability Chart */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Financial Stability Indicators
        </h2>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={indicatorData} layout="vertical" margin={{ left: 120 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis type="number" tick={{ fontSize: 12, fill: "var(--text-secondary)" }} />
              <YAxis
                type="category"
                dataKey="indicator"
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
                formatter={(v: number) => [`${v.toFixed(1)}`, "Value"]}
              />
              <Bar dataKey="value" fill="var(--accent-primary)" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Source: IMF FSIs, BIS. Values update after running the financial analysis pipeline.
        </p>
      </div>

      {/* Financial Model Table */}
      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
          Financial Model Estimates
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
                ["Credit-GDP gap", "BIS", "--"],
                ["Systemic risk", "CoVaR", "--"],
                ["Interest rate pass-through", "ECM", "--"],
                ["Financial inclusion index", "Sarma", "--"],
                ["Shadow banking exposure", "FSB", "--"],
                ["Fintech adoption rate", "EIU", "--"],
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
          Estimates populate after running the financial analysis pipeline.
        </p>
      </div>
    </div>
  );
}
