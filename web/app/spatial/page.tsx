"use client";

import { useEffect, useState } from "react";

interface SpatialScore {
  score: number | null;
  signal: string | null;
  module_count: number | null;
}

const MODULES = [
  { name: "Agglomeration", desc: "Urban density elasticity, Marshall externalities, cluster identification" },
  { name: "Housing Markets", desc: "Rent-price ratios, zoning constraints, hedonic pricing, affordability" },
  { name: "Transport Economics", desc: "Infrastructure cost-benefit, commute elasticity, modal choice" },
  { name: "Regional Convergence", desc: "Spatial autocorrelation, club convergence, Moran I, sigma-beta tests" },
  { name: "Migration", desc: "Push-pull determinants, wage equalization, network effects, welfare" },
  { name: "Special Economic Zones", desc: "SEZ causal impact, employment effects, FDI attraction, spillovers" },
  { name: "Smart Cities", desc: "Digital infrastructure productivity, sensor data analysis, 15-minute city metrics" },
  { name: "Land Value Capture", desc: "Infrastructure-induced appreciation, tax increment financing, LVC mechanisms" },
  { name: "Gentrification", desc: "Displacement estimation, amenity capitalization, neighborhood change indices" },
  { name: "Rural-Urban Linkages", desc: "Market integration, remittance flows, structural transformation spillovers" },
];

export default function SpatialPage() {
  const [data, setData] = useState<SpatialScore | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/spatial/score")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setData(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 11
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Spatial Economics
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Agglomeration, housing, transport, regional convergence, migration, SEZs, smart cities
        </p>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Composite Score", value: data?.score != null ? data.score.toFixed(1) : null },
          { label: "Signal", value: data?.signal ?? null },
          { label: "Modules", value: data?.module_count != null ? String(data.module_count) : String(MODULES.length) },
        ].map((m) => (
          <div key={m.label} className="glass-card p-5">
            <span className="text-xs text-[var(--text-muted)]">{m.label}</span>
            <div className="mt-1">
              {m.value !== null ? (
                <span className="text-xl font-semibold font-mono">{m.value}</span>
              ) : (
                <span className="text-sm text-[var(--text-muted)]">{loading ? "Loading..." : "Awaiting data"}</span>
              )}
            </div>
          </div>
        ))}
      </div>

      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Analytical Modules ({MODULES.length})
        </h2>
        <div className="space-y-0">
          {MODULES.map((mod, i) => (
            <div
              key={mod.name}
              className={`flex items-start gap-4 py-3 ${i < MODULES.length - 1 ? "border-b border-[var(--border)]/50" : ""}`}
            >
              <span className="text-xs font-mono text-[var(--text-muted)] w-5 pt-0.5 shrink-0">
                {String(i + 1).padStart(2, "0")}
              </span>
              <div>
                <p className="text-sm font-medium text-[var(--text-primary)]">{mod.name}</p>
                <p className="text-xs text-[var(--text-secondary)] mt-0.5">{mod.desc}</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
