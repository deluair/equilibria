"use client";

import { useEffect, useState } from "react";
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  ResponsiveContainer, Legend,
} from "recharts";

interface AgSummary {
  food_security_index: number | null;
  ag_gdp_share: number | null;
  price_volatility: number | null;
  price_data: { month: string; world: number; domestic: number }[];
}

const placeholderPrices = [
  { month: "Jul", world: 100, domestic: 102 },
  { month: "Aug", world: 103, domestic: 105 },
  { month: "Sep", world: 98, domestic: 103 },
  { month: "Oct", world: 95, domestic: 100 },
  { month: "Nov", world: 97, domestic: 101 },
  { month: "Dec", world: 101, domestic: 104 },
  { month: "Jan", world: 105, domestic: 108 },
  { month: "Feb", world: 108, domestic: 110 },
  { month: "Mar", world: 104, domestic: 107 },
];

export default function AgriculturalPage() {
  const [data, setData] = useState<AgSummary | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/layers/agricultural/summary")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setData(d))
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const priceData = data?.price_data ?? placeholderPrices;

  return (
    <div>
      <div className="mb-8">
        <span className="text-xs font-mono tracking-wider text-[var(--accent-primary)] uppercase">
          Layer 5
        </span>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)] mt-1">
          Agricultural Economics
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Food security indices, price transmission analysis, agricultural productivity
        </p>
      </div>

      {/* Key Metrics */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        {[
          { label: "Food Security Index", value: data?.food_security_index, unit: "/100" },
          { label: "Agriculture (% GDP)", value: data?.ag_gdp_share, unit: "%" },
          { label: "Price Volatility (CV)", value: data?.price_volatility, unit: "" },
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

      {/* Price Transmission */}
      <div className="glass-card p-5 mb-6">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
          Price Transmission: World vs. Domestic (Index, base=100)
        </h2>
        <div className="h-64">
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={priceData}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
              <XAxis dataKey="month" tick={{ fontSize: 12, fill: "var(--text-secondary)" }} />
              <YAxis tick={{ fontSize: 12, fill: "var(--text-secondary)" }} domain={[90, 115]} />
              <Tooltip
                contentStyle={{
                  background: "var(--bg-card)",
                  border: "1px solid var(--border)",
                  borderRadius: "0.5rem",
                  fontSize: "0.75rem",
                }}
              />
              <Legend wrapperStyle={{ fontSize: "0.75rem" }} />
              <Line type="monotone" dataKey="world" stroke="#0891b2" strokeWidth={2} dot={false} name="World Price" />
              <Line type="monotone" dataKey="domestic" stroke="#d97706" strokeWidth={2} dot={false} name="Domestic Price" />
            </LineChart>
          </ResponsiveContainer>
        </div>
        <p className="text-xs text-[var(--text-muted)] mt-3">
          Measures pass-through of international commodity price shocks to domestic markets.
          Based on Minot (2011) VECM framework.
        </p>
      </div>

      {/* Food Security Components */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="glass-card p-5">
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
            Food Security Dimensions
          </h2>
          <div className="space-y-3">
            {[
              { name: "Availability", desc: "Domestic production, imports, stocks" },
              { name: "Access", desc: "Income, prices, infrastructure" },
              { name: "Utilization", desc: "Nutrition, sanitation, health" },
              { name: "Stability", desc: "Price volatility, supply shocks" },
            ].map((dim) => (
              <div key={dim.name} className="flex items-center justify-between py-1.5 border-b border-[var(--border)]/50 last:border-0">
                <div>
                  <p className="text-sm text-[var(--text-primary)]">{dim.name}</p>
                  <p className="text-xs text-[var(--text-muted)]">{dim.desc}</p>
                </div>
                <span className="text-sm font-mono text-[var(--text-muted)]">--</span>
              </div>
            ))}
          </div>
        </div>

        <div className="glass-card p-5">
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
            Key Commodities
          </h2>
          <div className="space-y-3">
            {[
              { name: "Rice", desc: "Primary staple, largest caloric contribution" },
              { name: "Wheat", desc: "Second staple, high import dependence" },
              { name: "Jute", desc: "Major export crop" },
              { name: "Fish/Shrimp", desc: "Key protein source, aquaculture growth" },
              { name: "Tea", desc: "Traditional export, Sylhet/Chittagong" },
            ].map((c) => (
              <div key={c.name} className="flex items-center justify-between py-1.5 border-b border-[var(--border)]/50 last:border-0">
                <div>
                  <p className="text-sm text-[var(--text-primary)]">{c.name}</p>
                  <p className="text-xs text-[var(--text-muted)]">{c.desc}</p>
                </div>
                <span className="text-sm font-mono text-[var(--text-muted)]">--</span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
