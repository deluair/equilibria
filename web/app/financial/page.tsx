"use client";

import { useEffect, useState } from "react";

interface FinancialScore {
  score: number | null;
  signal: string | null;
  module_count: number | null;
}

const MODULES = [
  { name: "CAPM", desc: "Capital asset pricing model, factor exposures, alpha estimation" },
  { name: "Value at Risk", desc: "VaR, CVaR, expected shortfall under historical and parametric methods" },
  { name: "Credit Risk", desc: "Probability of default, loss given default, credit spreads" },
  { name: "Term Structure", desc: "Yield curve fitting, Nelson-Siegel, forward rate extraction" },
  { name: "Volatility", desc: "GARCH, realized volatility, VIX decomposition, vol clustering" },
  { name: "Contagion", desc: "Cross-market spillovers, DCC-GARCH, network transmission channels" },
  { name: "Banking Stability", desc: "Z-score, capital adequacy, NPL ratios, systemic risk indicators" },
  { name: "Fintech", desc: "Digital payments penetration, mobile money adoption, platform lending" },
  { name: "Insurance", desc: "Insurance depth, penetration ratios, catastrophe risk pricing" },
  { name: "Sovereign Debt", desc: "Debt sustainability, spread determinants, rollover risk assessment" },
  { name: "Bank Runs", desc: "Diamond-Dybvig model, deposit insurance effectiveness, liquidity spirals" },
];

export default function FinancialPage() {
  const [data, setData] = useState<FinancialScore | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/financial/score")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setData(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 7
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Financial Economics
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Asset pricing, risk measurement, banking stability, sovereign debt, fintech adoption
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
