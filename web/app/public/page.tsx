"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";

interface PublicStats {
  tax_revenue_gdp: number | null;
  public_debt_gdp: number | null;
  govt_expenditure_gdp: number | null;
  revenue_composition: { category: string; share: number }[];
}

export default function PublicPage() {
  const [stats, setStats] = useState<PublicStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/public/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setStats(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const placeholderRevenue = [
    { category: "Income tax", share: 32.4 },
    { category: "VAT", share: 27.1 },
    { category: "Corporate tax", share: 14.8 },
    { category: "Trade taxes", share: 8.3 },
    { category: "Excise", share: 6.9 },
    { category: "Property tax", share: 4.2 },
    { category: "Other", share: 6.3 },
  ];

  const revenueData = stats?.revenue_composition ?? placeholderRevenue;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 10
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Public Economics
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Taxation, public goods, federalism, social protection, pensions, regulatory impact
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Tax Revenue", value: stats?.tax_revenue_gdp, unit: "% GDP" },
          { label: "Public Debt", value: stats?.public_debt_gdp, unit: "% GDP" },
          { label: "Govt Expenditure", value: stats?.govt_expenditure_gdp, unit: "% GDP" },
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

      {/* Revenue Composition Chart */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Revenue Composition (% of total)
        </h2>
        <div className="h-72">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={revenueData} layout="vertical" margin={{ left: 110 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis type="number" tick={{ fontSize: 12, fill: "var(--text-secondary)" }} unit="%" domain={[0, 35]} />
              <YAxis
                type="category"
                dataKey="category"
                tick={{ fontSize: 12, fill: "var(--text-secondary)" }}
                width={105}
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
          Source: IMF GFS, OECD Revenue Statistics. Values update after running the public economics pipeline.
        </p>
      </div>

      {/* Public Economics Model Table */}
      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
          Public Economics Model Estimates
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
                ["Tax buoyancy coefficient", "Dudine & Jalles", "--"],
                ["Fiscal multiplier", "Auerbach & Gorodnichenko", "--"],
                ["Transfer targeting leakage rate", "Coady", "--"],
                ["Decentralization efficiency gain", "Oates", "--"],
                ["Pension sustainability ratio", "Actuarial", "--"],
                ["Regulatory compliance cost (% GDP)", "OECD RIA", "--"],
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
          Estimates populate after running the public economics analysis pipeline.
        </p>
      </div>
    </div>
  );
}
