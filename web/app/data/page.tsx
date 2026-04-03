"use client";

import { useEffect, useState } from "react";

interface DataSource {
  name: string;
  provider: string;
  frequency: string;
  coverage: string;
  layers: string[];
  status: string;
}

const defaultSources: DataSource[] = [
  {
    name: "Federal Reserve Economic Data",
    provider: "FRED / St. Louis Fed",
    frequency: "Daily to Annual",
    coverage: "1950-present",
    layers: ["L2", "L3"],
    status: "configured",
  },
  {
    name: "World Development Indicators",
    provider: "World Bank",
    frequency: "Annual",
    coverage: "1960-present",
    layers: ["L1", "L2", "L4"],
    status: "configured",
  },
  {
    name: "Current Employment Statistics",
    provider: "BLS",
    frequency: "Monthly",
    coverage: "1939-present",
    layers: ["L3"],
    status: "configured",
  },
  {
    name: "International Trade Data",
    provider: "UN Comtrade",
    frequency: "Annual / Monthly",
    coverage: "1962-present",
    layers: ["L1"],
    status: "configured",
  },
  {
    name: "World Economic Outlook",
    provider: "IMF",
    frequency: "Biannual",
    coverage: "1980-present",
    layers: ["L2", "L4"],
    status: "configured",
  },
  {
    name: "FAOSTAT",
    provider: "FAO",
    frequency: "Annual",
    coverage: "1961-present",
    layers: ["L5"],
    status: "configured",
  },
  {
    name: "Worldwide Governance Indicators",
    provider: "World Bank",
    frequency: "Annual",
    coverage: "1996-present",
    layers: ["L4"],
    status: "configured",
  },
  {
    name: "Global Food Security Index",
    provider: "Economist Intelligence Unit",
    frequency: "Annual",
    coverage: "2012-present",
    layers: ["L5"],
    status: "configured",
  },
  {
    name: "Penn World Table",
    provider: "Groningen Growth and Development Centre",
    frequency: "Annual",
    coverage: "1950-2019",
    layers: ["L2", "L4"],
    status: "configured",
  },
  {
    name: "Consumer Price Index",
    provider: "BLS",
    frequency: "Monthly",
    coverage: "1913-present",
    layers: ["L2", "L3"],
    status: "configured",
  },
];

export default function DataPage() {
  const [sources, setSources] = useState<DataSource[]>(defaultSources);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/data/sources")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => {
        if (d && Array.isArray(d)) setSources(d);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)]">
          Data Sources
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Primary data sources, collection status, and coverage across layers
        </p>
      </div>

      {/* Summary */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        <div className="glass-card p-5">
          <span className="text-xs text-[var(--text-muted)]">Total Sources</span>
          <p className="text-2xl font-semibold font-mono text-[var(--text-primary)] mt-1">
            {sources.length}
          </p>
        </div>
        <div className="glass-card p-5">
          <span className="text-xs text-[var(--text-muted)]">Active</span>
          <p className="text-2xl font-semibold font-mono text-emerald-600 mt-1">
            {sources.filter((s) => s.status === "configured" || s.status === "active").length}
          </p>
        </div>
        <div className="glass-card p-5">
          <span className="text-xs text-[var(--text-muted)]">Layers Covered</span>
          <p className="text-2xl font-semibold font-mono text-[var(--text-primary)] mt-1">
            {new Set(sources.flatMap((s) => s.layers)).size}
          </p>
        </div>
      </div>

      {/* Sources Table */}
      <div className="glass-card overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--border)] bg-[var(--bg-primary)]">
                <th className="text-left py-3 px-4 font-medium text-[var(--text-secondary)]">Source</th>
                <th className="text-left py-3 px-4 font-medium text-[var(--text-secondary)]">Provider</th>
                <th className="text-left py-3 px-4 font-medium text-[var(--text-secondary)]">Frequency</th>
                <th className="text-left py-3 px-4 font-medium text-[var(--text-secondary)]">Coverage</th>
                <th className="text-left py-3 px-4 font-medium text-[var(--text-secondary)]">Layers</th>
                <th className="text-right py-3 px-4 font-medium text-[var(--text-secondary)]">Status</th>
              </tr>
            </thead>
            <tbody>
              {sources.map((s) => (
                <tr key={s.name} className="border-b border-[var(--border)]/50 hover:bg-[var(--bg-primary)]/50">
                  <td className="py-2.5 px-4 text-[var(--text-primary)] font-medium">{s.name}</td>
                  <td className="py-2.5 px-4 text-[var(--text-secondary)]">{s.provider}</td>
                  <td className="py-2.5 px-4 text-[var(--text-secondary)]">{s.frequency}</td>
                  <td className="py-2.5 px-4 font-mono text-xs text-[var(--text-muted)]">{s.coverage}</td>
                  <td className="py-2.5 px-4">
                    <div className="flex gap-1">
                      {s.layers.map((l) => (
                        <span
                          key={l}
                          className="px-1.5 py-0.5 rounded text-[10px] font-mono bg-[var(--bg-primary)] text-[var(--text-muted)]"
                        >
                          {l}
                        </span>
                      ))}
                    </div>
                  </td>
                  <td className="text-right py-2.5 px-4">
                    <span className="inline-flex items-center gap-1.5 text-xs text-emerald-600">
                      <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
                      {s.status}
                    </span>
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
