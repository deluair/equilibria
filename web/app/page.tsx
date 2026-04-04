"use client";

import { useEffect, useState } from "react";
import LayerCard from "@/components/LayerCard";

interface LayerScore {
  layer: string;
  code: string;
  score: number | null;
  signal: string | null;
  description: string;
}

interface CompositeData {
  ceas_score: number | null;
  layers: LayerScore[];
  timestamp: string | null;
}

interface Briefing {
  id: string;
  title: string;
  type: string;
  created_at: string;
  layer: string | null;
}

const defaultLayers: LayerScore[] = [
  { layer: "Trade", code: "L1", score: null, signal: null, description: "Gravity model, RCA, trade openness" },
  { layer: "Macro", code: "L2", score: null, signal: null, description: "GDP decomposition, Phillips curve, cycles" },
  { layer: "Labor", code: "L3", score: null, signal: null, description: "Wage analysis, employment trends" },
  { layer: "Development", code: "L4", score: null, signal: null, description: "Convergence, poverty, institutions" },
  { layer: "Agricultural", code: "L5", score: null, signal: null, description: "Food security, price transmission" },
  { layer: "Integration", code: "L6", score: null, signal: null, description: "Composite scores, cross-layer analysis" },
  { layer: "Financial", code: "L7", score: null, signal: null, description: "Banking stability, credit cycles" },
  { layer: "Health", code: "L8", score: null, signal: null, description: "Health expenditure, disease burden" },
  { layer: "Environmental", code: "L9", score: null, signal: null, description: "Carbon pricing, emissions, green growth" },
  { layer: "Public", code: "L10", score: null, signal: null, description: "Taxation, fiscal federalism, pensions" },
  { layer: "Spatial", code: "L11", score: null, signal: null, description: "Agglomeration, housing, transport" },
  { layer: "Political", code: "L12", score: null, signal: null, description: "Governance, conflict, corruption" },
  { layer: "Behavioral", code: "L13", score: null, signal: null, description: "Nudges, biases, time preferences" },
  { layer: "Industrial", code: "L14", score: null, signal: null, description: "Market concentration, firm dynamics" },
  { layer: "Monetary", code: "L15", score: null, signal: null, description: "Money demand, inflation targeting" },
  { layer: "Energy", code: "L16", score: null, signal: null, description: "Oil markets, renewables, efficiency" },
  { layer: "Demographic", code: "L17", score: null, signal: null, description: "Aging, fertility, human capital" },
  { layer: "Methods", code: "L18", score: null, signal: null, description: "Econometric methods toolkit" },
];

const layerHrefs: Record<string, string> = {
  L1: "/trade", L2: "/macro", L3: "/labor", L4: "/development", L5: "/agricultural",
  L7: "/financial", L8: "/health", L9: "/environmental", L10: "/public", L11: "/spatial",
  L12: "/political", L13: "/behavioral", L14: "/industrial", L15: "/monetary",
  L16: "/energy", L17: "/demographic", L18: "/methods",
};

export default function Dashboard() {
  const [composite, setComposite] = useState<CompositeData | null>(null);
  const [briefings, setBriefings] = useState<Briefing[]>([]);
  const [kbStats, setKbStats] = useState<{ total_facts: number; total_articles: number; last_compile: string | null } | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    async function fetchData() {
      try {
        const [compRes, briefRes] = await Promise.allSettled([
          fetch("/api/integration/composite"),
          fetch("/api/briefings"),
        ]);

        if (compRes.status === "fulfilled" && compRes.value.ok) {
          setComposite(await compRes.value.json());
        }

        if (briefRes.status === "fulfilled" && briefRes.value.ok) {
          const data = await briefRes.value.json();
          setBriefings(Array.isArray(data) ? data.slice(0, 5) : []);
        }
        try {
          const kbRes = await fetch("/api/kb/stats");
          if (kbRes.ok) setKbStats(await kbRes.json());
        } catch {}
      } catch (e) {
        setError("Backend unavailable. Start the FastAPI server on port 8003.");
      } finally {
        setLoading(false);
      }
    }

    fetchData();
  }, []);

  const layers = composite?.layers ?? defaultLayers;

  return (
    <div>
      {/* Hero */}
      <div className="mb-8">
        <h1 className="text-3xl font-semibold tracking-tight text-[var(--text-primary)]">
          Equilibria
        </h1>
        <p className="text-base text-[var(--text-secondary)] mt-1">
          Applied Economics Analysis Platform
        </p>
      </div>

      {/* CEAS Composite Score */}
      <div className="glass-card p-6 mb-6">
        <div className="flex items-center justify-between">
          <div>
            <span className="text-xs font-mono tracking-wider text-[var(--text-muted)] uppercase">
              Composite Equilibrium Assessment Score
            </span>
            <div className="flex items-end gap-2 mt-2">
              {composite?.ceas_score !== null && composite?.ceas_score !== undefined ? (
                <>
                  <span className="text-4xl font-semibold font-mono text-[var(--text-primary)]">
                    {composite.ceas_score.toFixed(1)}
                  </span>
                  <span className="text-sm text-[var(--text-muted)] mb-1">/ 100</span>
                </>
              ) : (
                <span className="text-sm text-[var(--text-muted)]">
                  {loading ? "Loading..." : "No composite score available"}
                </span>
              )}
            </div>
          </div>
          {composite?.timestamp && (
            <span className="text-xs text-[var(--text-muted)]">
              Updated {new Date(composite.timestamp).toLocaleDateString()}
            </span>
          )}
        </div>
      </div>

      {/* Error Banner */}
      {error && (
        <div className="glass-card p-4 mb-6 border-l-4 border-l-[var(--accent-secondary)]">
          <p className="text-sm text-[var(--text-secondary)]">{error}</p>
        </div>
      )}

      {/* Layer Score Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4 mb-8">
        {layers.map((layer) => (
          <LayerCard
            key={layer.code}
            name={layer.layer}
            code={layer.code}
            score={layer.score}
            signal={layer.signal as "STABLE" | "WATCH" | "STRESS" | "CRISIS" | null}
            description={layer.description}
            href={layerHrefs[layer.code]}
          />
        ))}
      </div>

      {/* Recent Briefings */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="glass-card p-5">
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
            Recent Briefings
          </h2>
          {briefings.length > 0 ? (
            <ul className="space-y-3">
              {briefings.map((b) => (
                <li key={b.id} className="flex items-start justify-between">
                  <div>
                    <p className="text-sm text-[var(--text-primary)]">{b.title}</p>
                    <p className="text-xs text-[var(--text-muted)] mt-0.5">
                      {b.type}{b.layer ? ` / ${b.layer}` : ""}
                    </p>
                  </div>
                  <span className="text-xs text-[var(--text-muted)] whitespace-nowrap ml-4">
                    {new Date(b.created_at).toLocaleDateString()}
                  </span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-sm text-[var(--text-muted)]">
              {loading ? "Loading..." : "No briefings yet. Run the analysis pipeline to generate briefings."}
            </p>
          )}
        </div>

        {/* Data Source Status */}
        <div className="glass-card p-5">
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
            Data Sources
          </h2>
          <div className="space-y-2">
            {[
              { name: "FRED (Federal Reserve)", status: "configured" },
              { name: "World Bank WDI", status: "configured" },
              { name: "BLS (Bureau of Labor Statistics)", status: "configured" },
              { name: "UN Comtrade", status: "configured" },
              { name: "IMF WEO", status: "configured" },
            ].map((source) => (
              <div key={source.name} className="flex items-center justify-between py-1.5">
                <span className="text-sm text-[var(--text-secondary)]">{source.name}</span>
                <span className="inline-flex items-center gap-1.5 text-xs text-emerald-600">
                  <span className="w-1.5 h-1.5 rounded-full bg-emerald-500" />
                  {source.status}
                </span>
              </div>
            ))}
          </div>
        </div>

        <div className="glass-card p-5">
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
            Knowledge Base
          </h2>
          {kbStats ? (
            <div className="space-y-3">
              <div className="flex items-center justify-between">
                <span className="text-sm text-[var(--text-secondary)]">Articles</span>
                <span className="text-sm font-mono font-semibold text-[var(--text-primary)]">{kbStats.total_articles}</span>
              </div>
              <div className="flex items-center justify-between">
                <span className="text-sm text-[var(--text-secondary)]">Facts</span>
                <span className="text-sm font-mono font-semibold text-[var(--text-primary)]">{kbStats.total_facts}</span>
              </div>
              {kbStats.last_compile && (
                <p className="text-xs text-[var(--text-muted)]">
                  Last compiled {new Date(kbStats.last_compile).toLocaleDateString()}
                </p>
              )}
            </div>
          ) : (
            <p className="text-sm text-[var(--text-muted)]">{loading ? "Loading..." : "Not yet compiled"}</p>
          )}
        </div>
      </div>
    </div>
  );
}
