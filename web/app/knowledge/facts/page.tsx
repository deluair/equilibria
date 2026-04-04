"use client";

import { useEffect, useState } from "react";
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
} from "recharts";

interface Fact {
  id: number;
  claim: string;
  topic: string;
  subtopic: string | null;
  country_iso3: string | null;
  confidence: number;
  is_stale: number;
  updated_at: string;
}

interface KBStats {
  total_facts: number;
  total_articles: number;
  stale_count: number;
  fresh_count: number;
  last_compile: string | null;
  facts_by_topic: { topic: string; count: number }[];
}

const TOPICS = [
  "trade", "macro", "labor", "development", "agricultural", "financial",
  "health", "environmental", "public", "spatial", "political", "behavioral",
  "industrial", "monetary", "energy", "demographic", "methods",
];

const topicColors: Record<string, string> = {
  trade: "bg-cyan-50 text-cyan-700",
  macro: "bg-blue-50 text-blue-700",
  labor: "bg-violet-50 text-violet-700",
  development: "bg-emerald-50 text-emerald-700",
  agricultural: "bg-lime-50 text-lime-700",
  financial: "bg-amber-50 text-amber-700",
  health: "bg-rose-50 text-rose-700",
  environmental: "bg-green-50 text-green-700",
  public: "bg-indigo-50 text-indigo-700",
  spatial: "bg-teal-50 text-teal-700",
  political: "bg-red-50 text-red-700",
  behavioral: "bg-orange-50 text-orange-700",
  industrial: "bg-slate-100 text-slate-700",
  monetary: "bg-purple-50 text-purple-700",
  energy: "bg-yellow-50 text-yellow-700",
  demographic: "bg-pink-50 text-pink-700",
  methods: "bg-gray-100 text-gray-700",
};

function confidenceColor(c: number): string {
  if (c >= 0.7) return "text-emerald-600";
  if (c >= 0.4) return "text-amber-600";
  return "text-rose-600";
}

export default function FactExplorer() {
  const [stats, setStats] = useState<KBStats | null>(null);
  const [facts, setFacts] = useState<Fact[]>([]);
  const [loading, setLoading] = useState(true);
  const [topicFilter, setTopicFilter] = useState("");
  const [staleFilter, setStaleFilter] = useState("");
  const [expandedId, setExpandedId] = useState<number | null>(null);
  const [page, setPage] = useState(1);
  const [total, setTotal] = useState(0);
  const perPage = 20;

  useEffect(() => {
    fetch("/api/kb/stats")
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setStats(d))
      .catch(() => {});
  }, []);

  useEffect(() => {
    setLoading(true);
    const params = new URLSearchParams({ page: String(page), per_page: String(perPage) });
    if (topicFilter) params.set("topic", topicFilter);
    if (staleFilter === "stale") params.set("is_stale", "true");
    if (staleFilter === "fresh") params.set("is_stale", "false");

    fetch(`/api/kb/facts?${params}`)
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => {
        if (d) {
          setFacts(d.facts ?? []);
          setTotal(d.total ?? 0);
        }
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [topicFilter, staleFilter, page]);

  // Reset page when filters change
  useEffect(() => {
    setPage(1);
  }, [topicFilter, staleFilter]);

  const totalPages = Math.ceil(total / perPage);

  return (
    <div>
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)]">
          Fact Explorer
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          {stats
            ? `${stats.total_facts} facts total, ${stats.stale_count} stale`
            : loading ? "Loading..." : "Browse and filter extracted economic facts"}
        </p>
      </div>

      {/* Facts by Topic Chart */}
      {stats && stats.facts_by_topic.length > 0 && (
        <div className="glass-card p-5 mb-6">
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-4">
            Facts by Topic
          </h2>
          <div className="h-64">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={stats.facts_by_topic}
                layout="vertical"
                margin={{ left: 100, right: 20 }}
              >
                <CartesianGrid strokeDasharray="3 3" stroke="var(--border)" />
                <XAxis
                  type="number"
                  tick={{ fontSize: 12, fill: "var(--text-secondary)" }}
                />
                <YAxis
                  type="category"
                  dataKey="topic"
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
                  formatter={(v) => [Number(v).toLocaleString(), "Facts"]}
                />
                <Bar dataKey="count" fill="var(--accent-primary)" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* Filters */}
      <div className="flex gap-3 mb-6">
        <select
          value={topicFilter}
          onChange={(e) => setTopicFilter(e.target.value)}
          className="px-3 py-2 rounded-lg border border-[var(--border)] bg-[var(--bg-card)] text-sm text-[var(--text-primary)] focus:outline-none focus:border-[var(--accent-primary)]"
        >
          <option value="">All topics</option>
          {TOPICS.map((t) => (
            <option key={t} value={t}>{t.charAt(0).toUpperCase() + t.slice(1)}</option>
          ))}
        </select>
        <select
          value={staleFilter}
          onChange={(e) => setStaleFilter(e.target.value)}
          className="px-3 py-2 rounded-lg border border-[var(--border)] bg-[var(--bg-card)] text-sm text-[var(--text-primary)] focus:outline-none focus:border-[var(--accent-primary)]"
        >
          <option value="">All status</option>
          <option value="fresh">Fresh</option>
          <option value="stale">Stale</option>
        </select>
      </div>

      {/* Facts Table */}
      {loading ? (
        <div className="glass-card p-8 text-center">
          <p className="text-sm text-[var(--text-muted)]">Loading facts...</p>
        </div>
      ) : facts.length > 0 ? (
        <>
          <div className="glass-card overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-[var(--border)]">
                    <th className="text-left py-2.5 px-4 font-medium text-[var(--text-secondary)]">Claim</th>
                    <th className="text-left py-2.5 px-4 font-medium text-[var(--text-secondary)]">Topic</th>
                    <th className="text-left py-2.5 px-4 font-medium text-[var(--text-secondary)]">Country</th>
                    <th className="text-right py-2.5 px-4 font-medium text-[var(--text-secondary)]">Confidence</th>
                    <th className="text-center py-2.5 px-4 font-medium text-[var(--text-secondary)]">Status</th>
                    <th className="text-right py-2.5 px-4 font-medium text-[var(--text-secondary)]">Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {facts.map((f) => {
                    const isExpanded = expandedId === f.id;
                    const truncated = f.claim.length > 80 && !isExpanded;
                    return (
                      <tr
                        key={f.id}
                        className="border-b border-[var(--border)]/50 hover:bg-[var(--bg-primary)]/50 transition-colors"
                      >
                        <td className="py-2.5 px-4 max-w-xs">
                          <button
                            onClick={() => setExpandedId(isExpanded ? null : f.id)}
                            className="text-left text-xs text-[var(--text-primary)] leading-relaxed hover:text-[var(--accent-primary)] transition-colors"
                          >
                            {truncated ? f.claim.slice(0, 80) + "..." : f.claim}
                          </button>
                        </td>
                        <td className="py-2.5 px-4">
                          <div className="flex flex-col gap-0.5">
                            <span className={`inline-block px-2 py-0.5 rounded text-[10px] font-medium capitalize w-fit ${topicColors[f.topic] ?? "bg-gray-100 text-gray-700"}`}>
                              {f.topic}
                            </span>
                            {f.subtopic && (
                              <span className="text-[10px] text-[var(--text-muted)]">{f.subtopic}</span>
                            )}
                          </div>
                        </td>
                        <td className="py-2.5 px-4">
                          <span className="text-xs font-mono text-[var(--text-muted)]">
                            {f.country_iso3 ?? "--"}
                          </span>
                        </td>
                        <td className="py-2.5 px-4 text-right">
                          <span className={`text-xs font-mono font-medium ${confidenceColor(f.confidence)}`}>
                            {(f.confidence * 100).toFixed(0)}%
                          </span>
                        </td>
                        <td className="py-2.5 px-4 text-center">
                          <span
                            className={`inline-block w-2 h-2 rounded-full ${f.is_stale ? "bg-[var(--accent-secondary)]" : "bg-emerald-500"}`}
                            title={f.is_stale ? "Stale" : "Fresh"}
                          />
                        </td>
                        <td className="py-2.5 px-4 text-right">
                          <span className="text-xs text-[var(--text-muted)]">
                            {new Date(f.updated_at).toLocaleDateString()}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between mt-4">
              <span className="text-xs text-[var(--text-muted)]">
                Page {page} of {totalPages} ({total} facts)
              </span>
              <div className="flex gap-2">
                <button
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={page <= 1}
                  className="px-3 py-1.5 rounded-lg text-xs font-medium bg-[var(--bg-card)] text-[var(--text-secondary)] border border-[var(--border)] hover:border-[var(--border-hover)] disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                >
                  Previous
                </button>
                <button
                  onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
                  disabled={page >= totalPages}
                  className="px-3 py-1.5 rounded-lg text-xs font-medium bg-[var(--bg-card)] text-[var(--text-secondary)] border border-[var(--border)] hover:border-[var(--border-hover)] disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
                >
                  Next
                </button>
              </div>
            </div>
          )}
        </>
      ) : (
        <div className="glass-card p-8 text-center">
          <p className="text-sm text-[var(--text-muted)]">
            No facts found. Run the analysis pipeline and KB extractor to populate facts.
          </p>
          <p className="text-xs text-[var(--text-muted)] mt-2">
            <code className="font-mono bg-[var(--bg-primary)] px-1.5 py-0.5 rounded">
              python -m app.cli kb extract
            </code>
          </p>
        </div>
      )}
    </div>
  );
}
