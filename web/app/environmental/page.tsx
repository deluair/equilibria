"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";

interface EnvironmentalStats {
  carbon_intensity: number | null;
  renewable_share: number | null;
  ekc_turning_point: number | null;
  ghg_by_sector: { sector: string; share: number }[];
}

export default function EnvironmentalPage() {
  const [stats, setStats] = useState<EnvironmentalStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/environmental/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setStats(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const placeholderSectors = [
    { sector: "Energy", share: 73.2 },
    { sector: "Agriculture", share: 11.8 },
    { sector: "Industry", share: 5.6 },
    { sector: "Waste", share: 3.2 },
    { sector: "Land use change", share: 6.2 },
  ];

  const sectorData = stats?.ghg_by_sector ?? placeholderSectors;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 9
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Environmental Economics
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Carbon pricing, EKC, climate damage, green growth, biodiversity, circular economy
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Carbon Intensity", value: stats?.carbon_intensity, unit: "tCO2/M USD" },
          { label: "Renewable Share", value: stats?.renewable_share, unit: "%" },
          { label: "EKC Turning Point", value: stats?.ekc_turning_point, unit: "USD/cap" },
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

      {/* GHG Emissions by Sector Chart */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          GHG Emissions by Sector (% of total)
        </h2>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={sectorData} layout="vertical" margin={{ left: 120 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis type="number" tick={{ fontSize: 12, fill: "var(--text-secondary)" }} unit="%" domain={[0, 80]} />
              <YAxis
                type="category"
                dataKey="sector"
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
                formatter={(v: number) => [`${v.toFixed(1)}%`, "Share"]}
              />
              <Bar dataKey="share" fill="var(--accent-primary)" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Source: IEA, IPCC. Values update after running the environmental analysis pipeline.
        </p>
      </div>

      {/* Environmental Model Table */}
      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
          Environmental Model Estimates
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
                ["Social cost of carbon (USD/tCO2)", "EPA", "--"],
                ["EKC turning point income", "Panel FE", "--"],
                ["Carbon leakage rate", "CBAM", "--"],
                ["Green TFP growth", "Acemoglu", "--"],
                ["Ecosystem service value (% GDP)", "TEEB", "--"],
                ["Just transition cost (% GDP)", "ILO", "--"],
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
          Estimates populate after running the environmental analysis pipeline.
        </p>
      </div>
    </div>
  );
}
