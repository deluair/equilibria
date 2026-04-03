"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";

interface IndustrialStats {
  hhi_median: number | null;
  markup_median: number | null;
  merger_approval_rate: number | null;
  market_concentration: { sector: string; hhi: number }[];
}

export default function IndustrialPage() {
  const [stats, setStats] = useState<IndustrialStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/industrial/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setStats(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const placeholderHHI = [
    { sector: "Search", hhi: 8200 },
    { sector: "Social media", hhi: 6100 },
    { sector: "Cloud compute", hhi: 4800 },
    { sector: "Airlines", hhi: 2600 },
    { sector: "Pharma", hhi: 1400 },
    { sector: "Retail", hhi: 820 },
  ];

  const hhi_data = stats?.market_concentration ?? placeholderHHI;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 8
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Industrial Organization
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Market structure, mergers, price discrimination, innovation, platforms, antitrust, digital markets
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Median HHI", value: stats?.hhi_median, unit: "index" },
          { label: "Median Markup", value: stats?.markup_median, unit: "ratio" },
          { label: "Merger Approval Rate", value: stats?.merger_approval_rate, unit: "%" },
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

      {/* HHI Chart */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Market Concentration by Sector (Herfindahl-Hirschman Index)
        </h2>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={hhi_data} layout="vertical" margin={{ left: 100 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis type="number" tick={{ fontSize: 12, fill: "var(--text-secondary)" }} domain={[0, 10000]} />
              <YAxis
                type="category"
                dataKey="sector"
                tick={{ fontSize: 12, fill: "var(--text-secondary)" }}
                width={95}
              />
              <Tooltip
                contentStyle={{
                  background: "var(--bg-card)",
                  border: "1px solid var(--border)",
                  borderRadius: "0.5rem",
                  fontSize: "0.75rem",
                }}
              />
              <Bar dataKey="hhi" fill="var(--accent-primary)" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          HHI &gt; 2500: highly concentrated. DOJ/FTC threshold for antitrust scrutiny.
        </p>
      </div>

      {/* IO Metrics Table */}
      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
          IO Model Estimates
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--border)]">
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Model / Metric</th>
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Application</th>
                <th className="text-right py-2 px-3 font-medium text-[var(--text-secondary)]">Estimate</th>
              </tr>
            </thead>
            <tbody className="text-[var(--text-primary)]">
              {[
                ["Lerner index (markup)", "Price-cost margin", "--"],
                ["Network effect elasticity", "Platform economics", "--"],
                ["Innovation spillover rate", "R&D / creative destruction", "--"],
                ["Price discrimination welfare gain", "Third-degree PD", "--"],
                ["Startup entry rate", "Entrepreneurship / IO", "--"],
                ["Merger price effect (GUPPI)", "Antitrust simulation", "--"],
              ].map(([model, app, est]) => (
                <tr key={model} className="border-b border-[var(--border)]/50">
                  <td className="py-2 px-3 font-mono text-xs">{model}</td>
                  <td className="py-2 px-3 text-xs text-[var(--text-secondary)]">{app}</td>
                  <td className="text-right py-2 px-3 font-mono text-xs text-[var(--text-muted)]">{est}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Estimates populate after running the industrial organization analysis pipeline.
        </p>
      </div>
    </div>
  );
}
