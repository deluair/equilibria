"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";

interface MethodsStats {
  estimators_available: number | null;
  datasets_loaded: number | null;
  last_run: string | null;
  method_usage: { method: string; runs: number }[];
}

export default function MethodsPage() {
  const [stats, setStats] = useState<MethodsStats | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/methods/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setStats(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const placeholderUsage = [
    { method: "Synthetic control", runs: 0 },
    { method: "Bunching estimator", runs: 0 },
    { method: "RKD", runs: 0 },
    { method: "Spatial econometrics", runs: 0 },
    { method: "Quantile regression", runs: 0 },
    { method: "Mixture models", runs: 0 },
    { method: "Survival analysis", runs: 0 },
    { method: "Bayesian VAR", runs: 0 },
    { method: "Panel cointegration", runs: 0 },
    { method: "Threshold regression", runs: 0 },
    { method: "Stochastic frontier", runs: 0 },
    { method: "Meta-analysis", runs: 0 },
  ];

  const usageData = stats?.method_usage ?? placeholderUsage;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 12
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Econometric Methods
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Synthetic control, bunching, RKD, spatial, quantile, mixture, survival, Bayesian, panel cointegration, threshold, stochastic frontier, meta-analysis
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Estimators Available", value: stats?.estimators_available ?? 12, unit: "methods", isInt: true },
          { label: "Datasets Loaded", value: stats?.datasets_loaded, unit: "series", isInt: true },
          { label: "Last Pipeline Run", value: null, raw: stats?.last_run ?? (loading ? null : "Not yet run") },
        ].map((m) => (
          <div key={m.label} className="glass-card p-5">
            <span className="text-xs text-[var(--text-muted)]">{m.label}</span>
            <div className="mt-1">
              {"raw" in m ? (
                <span className="text-sm text-[var(--text-secondary)]">
                  {m.raw ?? (loading ? "Loading..." : "Awaiting data")}
                </span>
              ) : m.value !== null && m.value !== undefined ? (
                <span className="text-xl font-semibold font-mono">
                  {m.isInt ? m.value : (m.value as number).toFixed(1)}
                  <span className="text-sm text-[var(--text-muted)] ml-1">{m.unit}</span>
                </span>
              ) : (
                <span className="text-sm text-[var(--text-muted)]">{loading ? "Loading..." : "Awaiting data"}</span>
              )}
            </div>
          </div>
        ))}
      </div>

      {/* Method Usage Chart */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Method Run Count
        </h2>
        <div className="h-80">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={usageData} layout="vertical" margin={{ left: 140 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis type="number" tick={{ fontSize: 12, fill: "var(--text-secondary)" }} allowDecimals={false} />
              <YAxis
                type="category"
                dataKey="method"
                tick={{ fontSize: 11, fill: "var(--text-secondary)" }}
                width={135}
              />
              <Tooltip
                contentStyle={{
                  background: "var(--bg-card)",
                  border: "1px solid var(--border)",
                  borderRadius: "0.5rem",
                  fontSize: "0.75rem",
                }}
              />
              <Bar dataKey="runs" fill="var(--accent-primary)" radius={[0, 4, 4, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Run counts populate after executing estimation pipelines via the analysis API.
        </p>
      </div>

      {/* Method Reference Table */}
      <div className="glass-card p-5">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
          Method Reference
        </h2>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--border)]">
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Method</th>
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Identification / Use</th>
                <th className="text-right py-2 px-3 font-medium text-[var(--text-secondary)]">Key Reference</th>
              </tr>
            </thead>
            <tbody className="text-[var(--text-primary)]">
              {[
                ["Synthetic control", "ATT for single treated unit", "Abadie et al. (2010)"],
                ["Bunching estimator", "Kink/notch at budget constraints", "Kleven (2016)"],
                ["Regression kink design (RKD)", "Kink in treatment assignment", "Card et al. (2015)"],
                ["Spatial econometrics", "Cross-section spillovers", "Anselin (1988)"],
                ["Quantile regression", "Heterogeneous treatment effects", "Koenker & Bassett (1978)"],
                ["Mixture models / EM", "Latent class estimation", "Dempster et al. (1977)"],
                ["Survival / duration models", "Time-to-event data", "Cox (1972)"],
                ["Bayesian VAR", "Macro forecasting with priors", "Sims & Zha (1998)"],
                ["Panel cointegration", "Long-run equilibrium", "Pedroni (1999)"],
                ["Threshold regression", "Regime changes", "Hansen (1999)"],
                ["Stochastic frontier", "Technical efficiency", "Aigner et al. (1977)"],
                ["Meta-analysis / PET-PEESE", "Publication bias correction", "Stanley & Doucouliagos (2012)"],
              ].map(([method, use, ref]) => (
                <tr key={method} className="border-b border-[var(--border)]/50">
                  <td className="py-2 px-3 font-mono text-xs">{method}</td>
                  <td className="py-2 px-3 text-xs text-[var(--text-secondary)]">{use}</td>
                  <td className="text-right py-2 px-3 text-xs text-[var(--text-muted)]">{ref}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
