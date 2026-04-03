"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";

interface EnergyStats {
  fossil_subsidy_gdp: number | null;
  renewable_share: number | null;
  energy_intensity: number | null;
  generation_mix: { source: string; share: number }[];
}

export default function EnergyPage() {
  const [stats, setStats] = useState<EnergyStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/energy/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setStats(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const placeholderMix = [
    { source: "Coal", share: 36.4 },
    { source: "Natural gas", share: 22.7 },
    { source: "Oil", share: 2.9 },
    { source: "Nuclear", share: 9.2 },
    { source: "Hydro", share: 15.3 },
    { source: "Wind", share: 7.8 },
    { source: "Solar", share: 4.5 },
    { source: "Other renewables", share: 1.2 },
  ];

  const mixData = stats?.generation_mix ?? placeholderMix;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 10
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Energy Economics
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Oil market, energy security, electricity, efficiency, fossil subsidies, transition, hydrogen, nuclear, carbon capture
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Fossil Fuel Subsidies", value: stats?.fossil_subsidy_gdp, unit: "% GDP" },
          { label: "Renewable Share", value: stats?.renewable_share, unit: "% generation" },
          { label: "Energy Intensity", value: stats?.energy_intensity, unit: "MJ / USD" },
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

      {/* Generation Mix Chart */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Electricity Generation Mix (% of total)
        </h2>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={mixData} layout="vertical" margin={{ left: 120 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis type="number" tick={{ fontSize: 12, fill: "var(--text-secondary)" }} unit="%" domain={[0, 40]} />
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
                formatter={(v: number) => [`${v.toFixed(1)}%`, "Share"]}
              />
              <Bar dataKey="share" fill="var(--accent-primary)" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Source: IEA World Energy Outlook. Values update after running the energy analysis pipeline.
        </p>
      </div>

      {/* Energy Model Table */}
      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
          Energy Model Estimates
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
                ["Oil price elasticity of demand", "Oil market", "--"],
                ["Rebound effect (efficiency)", "Energy efficiency", "--"],
                ["Carbon capture cost (USD/tCO2)", "CCS", "--"],
                ["Hydrogen levelized cost (USD/kg)", "Green hydrogen", "--"],
                ["Nuclear LCOE (USD/MWh)", "Nuclear economics", "--"],
                ["Fossil subsidy removal welfare loss", "Transition economics", "--"],
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
          Estimates populate after running the energy analysis pipeline.
        </p>
      </div>
    </div>
  );
}
