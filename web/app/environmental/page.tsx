"use client";

import { useEffect, useState } from "react";

interface EnvironmentalScore {
  score: number | null;
  signal: string | null;
  module_count: number | null;
}

const MODULES = [
  { name: "Carbon Pricing", desc: "ETS design, carbon tax incidence, abatement cost curves, leakage rates" },
  { name: "Pollution Haven", desc: "Regulatory arbitrage, emissions-intensive FDI flows, race-to-bottom tests" },
  { name: "Environmental Kuznets Curve", desc: "EKC estimation, turning point income, decoupling evidence" },
  { name: "Green Growth", desc: "Green TFP, clean investment multipliers, just transition costs" },
  { name: "Renewables", desc: "LCOE trends, intermittency costs, grid integration, subsidy efficiency" },
  { name: "Climate Damage", desc: "GDP temperature response, Burke et al. damage functions, tail risk" },
  { name: "Biodiversity", desc: "Ecosystem service valuation, habitat loss costs, IPBES metrics" },
  { name: "Water Economics", desc: "Water scarcity pricing, irrigation efficiency, transboundary allocation" },
  { name: "Circular Economy", desc: "Material productivity, waste-to-resource conversion rates, policy impact" },
  { name: "Ocean Economics", desc: "Blue economy valuation, fisheries depletion, plastic pollution costs" },
  { name: "Urban Heat", desc: "UHI economic burden, cooling cost escalation, heat mortality valuation" },
  { name: "Air Quality", desc: "Pollution-productivity link, health cost of PM2.5, regulatory cost-benefit" },
];

export default function EnvironmentalPage() {
  const [data, setData] = useState<EnvironmentalScore | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/environmental/score")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setData(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

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
