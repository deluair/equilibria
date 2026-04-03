"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";

interface TradeStats {
  openness: number | null;
  exports_gdp: number | null;
  imports_gdp: number | null;
  top_partners: { country: string; value: number }[];
  rca_sectors: { sector: string; rca: number }[];
}

export default function TradePage() {
  const [stats, setStats] = useState<TradeStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/trade/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setStats(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const placeholderRCA = [
    { sector: "Textiles", rca: 4.2 },
    { sector: "Apparel", rca: 3.8 },
    { sector: "Machinery", rca: 0.4 },
    { sector: "Electronics", rca: 0.3 },
    { sector: "Chemicals", rca: 0.6 },
    { sector: "Agriculture", rca: 1.5 },
  ];

  const rcaData = stats?.rca_sectors ?? placeholderRCA;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 1
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Trade Analysis
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Gravity model estimates, revealed comparative advantage, trade openness indicators
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Trade Openness", value: stats?.openness, unit: "% GDP" },
          { label: "Export Ratio", value: stats?.exports_gdp, unit: "% GDP" },
          { label: "Import Ratio", value: stats?.imports_gdp, unit: "% GDP" },
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

      {/* RCA Chart */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Revealed Comparative Advantage (RCA)
        </h2>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={rcaData} layout="vertical" margin={{ left: 80 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis type="number" tick={{ fontSize: 12, fill: "var(--text-secondary)" }} />
              <YAxis
                type="category"
                dataKey="sector"
                tick={{ fontSize: 12, fill: "var(--text-secondary)" }}
                width={75}
              />
              <Tooltip
                contentStyle={{
                  background: "var(--bg-card)",
                  border: "1px solid var(--border)",
                  borderRadius: "0.5rem",
                  fontSize: "0.75rem",
                }}
              />
              <Bar dataKey="rca" fill="var(--accent-primary)" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          RCA &gt; 1 indicates comparative advantage. Based on Balassa (1965) index.
        </p>
      </div>

      {/* Gravity Model */}
      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
          Gravity Model Estimates
        </h2>
        <p className="text-sm text-[var(--text-secondary)] mb-4">
          Standard gravity equation: log(Trade_ij) = a + b1*log(GDP_i) + b2*log(GDP_j) - b3*log(Distance_ij) + controls + e
        </p>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--border)]">
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Variable</th>
                <th className="text-right py-2 px-3 font-medium text-[var(--text-secondary)]">Coefficient</th>
                <th className="text-right py-2 px-3 font-medium text-[var(--text-secondary)]">Std. Error</th>
                <th className="text-right py-2 px-3 font-medium text-[var(--text-secondary)]">Significance</th>
              </tr>
            </thead>
            <tbody className="text-[var(--text-primary)]">
              <tr className="border-b border-[var(--border)]/50">
                <td className="py-2 px-3 font-mono text-xs">log(GDP_exporter)</td>
                <td className="text-right py-2 px-3 font-mono text-xs text-[var(--text-muted)]">--</td>
                <td className="text-right py-2 px-3 font-mono text-xs text-[var(--text-muted)]">--</td>
                <td className="text-right py-2 px-3 text-[var(--text-muted)]">--</td>
              </tr>
              <tr className="border-b border-[var(--border)]/50">
                <td className="py-2 px-3 font-mono text-xs">log(GDP_importer)</td>
                <td className="text-right py-2 px-3 font-mono text-xs text-[var(--text-muted)]">--</td>
                <td className="text-right py-2 px-3 font-mono text-xs text-[var(--text-muted)]">--</td>
                <td className="text-right py-2 px-3 text-[var(--text-muted)]">--</td>
              </tr>
              <tr>
                <td className="py-2 px-3 font-mono text-xs">log(Distance)</td>
                <td className="text-right py-2 px-3 font-mono text-xs text-[var(--text-muted)]">--</td>
                <td className="text-right py-2 px-3 font-mono text-xs text-[var(--text-muted)]">--</td>
                <td className="text-right py-2 px-3 text-[var(--text-muted)]">--</td>
              </tr>
            </tbody>
          </table>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Estimates will populate after running the trade analysis pipeline.
        </p>
      </div>
    </div>
  );
}
