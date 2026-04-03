"use client";

import { useEffect, useState } from "react";

interface HealthScore {
  score: number | null;
  signal: string | null;
  module_count: number | null;
}

const MODULES = [
  { name: "Health Expenditure", desc: "Public vs. private spending, out-of-pocket burden, catastrophic payment risk" },
  { name: "Disease Burden", desc: "DALYs, YLLs, YLDs, cause-of-death decomposition by age and sex" },
  { name: "Insurance Coverage", desc: "Population coverage rates, benefit package depth, moral hazard estimation" },
  { name: "Pharmaceutical", desc: "Drug pricing, patent cliffs, biosimilar entry, medicine access indices" },
  { name: "Nutrition", desc: "Stunting, wasting, overweight prevalence, dietary diversity scores" },
  { name: "Pandemic Economics", desc: "GDP impact of disease shocks, preparedness investment, spillover costs" },
  { name: "Mental Health", desc: "Depression and anxiety burden, treatment gap, economic productivity loss" },
  { name: "Health Workforce", desc: "Physician and nurse density, wage premiums, rural-urban maldistribution" },
  { name: "Antimicrobial Resistance", desc: "AMR economic burden, antibiotic consumption trends, resistance transmission" },
  { name: "Telemedicine", desc: "Digital health adoption, teleconsultation cost-effectiveness, access equity" },
];

export default function HealthPage() {
  const [data, setData] = useState<HealthScore | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/health/score")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setData(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 8
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Health Economics
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Health expenditure, disease burden, pharmaceutical markets, pandemic shocks, workforce
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
