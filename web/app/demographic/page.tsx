"use client";

import { useEffect, useState } from "react";
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from "recharts";

interface DemographicStats {
  tfr: number | null;
  old_age_dependency: number | null;
  hci: number | null;
  age_structure: { cohort: string; male: number; female: number }[];
}

export default function DemographicPage() {
  const [stats, setStats] = useState<DemographicStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/demographic/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setStats(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const placeholderTFR = [
    { period: "1960", global: 4.98, developed: 2.73, developing: 5.98 },
    { period: "1970", global: 4.47, developed: 2.15, developing: 5.44 },
    { period: "1980", global: 3.68, developed: 1.82, developing: 4.39 },
    { period: "1990", global: 3.13, developed: 1.67, developing: 3.71 },
    { period: "2000", global: 2.74, developed: 1.57, developing: 3.18 },
    { period: "2010", global: 2.55, developed: 1.68, developing: 2.86 },
    { period: "2020", global: 2.32, developed: 1.53, developing: 2.58 },
  ];

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 11
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Demographic Economics
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Fertility, aging, human capital investment, population dynamics, gender, child development
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Total Fertility Rate", value: stats?.tfr, unit: "births/woman" },
          { label: "Old-Age Dependency", value: stats?.old_age_dependency, unit: "ratio" },
          { label: "Human Capital Index", value: stats?.hci, unit: "0-1 scale" },
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

      {/* TFR Trends */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Total Fertility Rate Trends
        </h2>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={placeholderTFR} margin={{ left: 10, right: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis dataKey="period" tick={{ fontSize: 11, fill: "var(--text-secondary)" }} />
              <YAxis tick={{ fontSize: 12, fill: "var(--text-secondary)" }} unit="" />
              <Tooltip
                contentStyle={{
                  background: "var(--bg-card)",
                  border: "1px solid var(--border)",
                  borderRadius: "0.5rem",
                  fontSize: "0.75rem",
                }}
              />
              <Legend wrapperStyle={{ fontSize: "0.75rem" }} />
              <Line dataKey="global" name="Global" stroke="var(--accent-primary)" dot={false} strokeWidth={2} />
              <Line dataKey="developed" name="Developed" stroke="var(--accent-secondary)" dot={false} strokeWidth={2} strokeDasharray="4 2" />
              <Line dataKey="developing" name="Developing" stroke="#6b7280" dot={false} strokeWidth={2} strokeDasharray="2 2" />
            </LineChart>
          </ResponsiveContainer>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Replacement fertility: 2.1 births per woman. Source: World Bank WDI.
        </p>
      </div>

      {/* Demographic Model Table */}
      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
          Demographic Model Estimates
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
                ["Returns to schooling (Mincer)", "Human capital", "--"],
                ["Demographic dividend growth boost", "Population economics", "--"],
                ["Child penalty (women earnings)", "Gender economics", "--"],
                ["Early childhood ROI", "Child development", "--"],
                ["Pension system replacement rate", "Aging", "--"],
                ["Fertility-income elasticity", "Quantity-quality tradeoff", "--"],
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
          Estimates populate after running the demographic analysis pipeline.
        </p>
      </div>
    </div>
  );
}
