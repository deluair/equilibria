"use client";

import { useEffect, useState } from "react";
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend,
} from "recharts";

interface LaborSummary {
  unemployment_rate: number | null;
  labor_force_participation: number | null;
  median_wage_growth: number | null;
  wage_data: { year: string; nominal: number; real: number }[];
  employment_data: { sector: string; employed: number; change: number }[];
}

const placeholderWages = [
  { year: "2019", nominal: 2.8, real: 0.9 },
  { year: "2020", nominal: 3.1, real: 1.8 },
  { year: "2021", nominal: 4.2, real: -1.5 },
  { year: "2022", nominal: 5.1, real: -1.8 },
  { year: "2023", nominal: 4.3, real: 0.8 },
];

export default function LaborPage() {
  const [data, setData] = useState<LaborSummary | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/labor/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setData(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const wageData = data?.wage_data ?? placeholderWages;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 3
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Labor Market Analysis
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Wage dynamics, employment trends, labor force participation, sectoral shifts
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Unemployment Rate", value: data?.unemployment_rate, unit: "%" },
          { label: "LFPR", value: data?.labor_force_participation, unit: "%" },
          { label: "Median Wage Growth", value: data?.median_wage_growth, unit: "% YoY" },
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

      {/* Wage Growth Chart */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Wage Growth: Nominal vs. Real (% YoY)
        </h2>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={wageData}>
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
              <Line type="monotone" dataKey="nominal" stroke="#0891b2" strokeWidth={2} dot={false} name="Nominal" />
              <Line type="monotone" dataKey="real" stroke="#d97706" strokeWidth={2} dot={false} name="Real" />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Employment by Sector */}
      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
          Employment Trends by Sector
        </h2>
        <p className="text-sm text-[var(--text-secondary)] mb-4">
          Sectoral employment distribution and change. Data sourced from BLS Current Employment Statistics.
        </p>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--border)]">
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Sector</th>
                <th className="text-right py-2 px-3 font-medium text-[var(--text-secondary)]">Employment (000s)</th>
                <th className="text-right py-2 px-3 font-medium text-[var(--text-secondary)]">12-mo Change</th>
              </tr>
            </thead>
            <tbody>
              {(data?.employment_data ?? [
                { sector: "Manufacturing", employed: 12800, change: -0.3 },
                { sector: "Services", employed: 108500, change: 1.8 },
                { sector: "Construction", employed: 7900, change: 0.5 },
                { sector: "Government", employed: 22700, change: 0.9 },
                { sector: "Mining & Logging", employed: 640, change: -1.2 },
              ]).map((row) => (
                <tr key={row.sector} className="border-b border-[var(--border)]/50">
                  <td className="py-2 px-3 text-[var(--text-primary)]">{row.sector}</td>
                  <td className="text-right py-2 px-3 font-mono text-xs">
                    {row.employed.toLocaleString()}
                  </td>
                  <td className={`text-right py-2 px-3 font-mono text-xs ${
                    row.change >= 0 ? "text-emerald-600" : "text-rose-600"
                  }`}>
                    {row.change >= 0 ? "+" : ""}{row.change.toFixed(1)}%
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
