"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";

interface Article {
  id: number;
  slug: string;
  title: string;
  topic: string;
  country_iso3: string | null;
  summary: string;
  fact_count: number;
  updated_at: string;
}

interface KBStats {
  total_facts: number;
  total_articles: number;
  stale_count: number;
  fresh_count: number;
  last_compile: string | null;
}

interface SearchFact {
  id: number;
  claim: string;
  topic: string;
  subtopic: string | null;
  confidence: number;
  is_stale: number;
}

interface SearchResult {
  facts: SearchFact[];
  articles: Article[];
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
  if (c >= 0.7) return "bg-emerald-50 text-emerald-700";
  if (c >= 0.4) return "bg-amber-50 text-amber-700";
  return "bg-rose-50 text-rose-700";
}

export default function KnowledgeIndex() {
  const [stats, setStats] = useState<KBStats | null>(null);
  const [articles, setArticles] = useState<Article[]>([]);
  const [loading, setLoading] = useState(true);
  const [activeTopic, setActiveTopic] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<SearchResult | null>(null);
  const [searching, setSearching] = useState(false);

  useEffect(() => {
    async function load() {
      try {
        const [statsRes, articlesRes] = await Promise.allSettled([
          fetch("/api/kb/stats"),
          fetch(activeTopic ? `/api/kb/articles?topic=${activeTopic}` : "/api/kb/articles"),
        ]);
        if (statsRes.status === "fulfilled" && statsRes.value.ok) {
          setStats(await statsRes.value.json());
        }
        if (articlesRes.status === "fulfilled" && articlesRes.value.ok) {
          const data = await articlesRes.value.json();
          setArticles(data.articles ?? []);
        }
      } catch {
        // backend unavailable
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [activeTopic]);

  const doSearch = useCallback((q: string) => {
    if (!q.trim()) {
      setSearchResults(null);
      return;
    }
    setSearching(true);
    const params = new URLSearchParams({ q });
    if (activeTopic) params.set("topic", activeTopic);
    fetch(`/api/kb/search?${params}`)
      .then((r) => (r.ok ? r.json() : null))
      .then((d) => setSearchResults(d))
      .catch(() => setSearchResults(null))
      .finally(() => setSearching(false));
  }, [activeTopic]);

  // Debounce search
  useEffect(() => {
    const timer = setTimeout(() => doSearch(searchQuery), 300);
    return () => clearTimeout(timer);
  }, [searchQuery, doSearch]);

  return (
    <div>
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)]">
          Knowledge Base
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          {stats
            ? `${stats.total_articles} articles, ${stats.total_facts} facts (${stats.fresh_count} fresh, ${stats.stale_count} stale)`
            : loading ? "Loading..." : "AI-compiled economic knowledge from analysis results"}
        </p>
      </div>

      {/* Search */}
      <div className="mb-6">
        <input
          type="text"
          placeholder="Search facts and articles..."
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          className="w-full px-4 py-2.5 rounded-lg border border-[var(--border)] bg-[var(--bg-card)] text-sm text-[var(--text-primary)] placeholder:text-[var(--text-muted)] focus:outline-none focus:border-[var(--accent-primary)] transition-colors"
        />
      </div>

      {/* Topic Chips */}
      <div className="flex flex-wrap gap-2 mb-6">
        <button
          onClick={() => setActiveTopic(null)}
          className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${
            activeTopic === null
              ? "bg-[var(--accent-primary)] text-white"
              : "bg-[var(--bg-card)] text-[var(--text-secondary)] border border-[var(--border)] hover:border-[var(--border-hover)]"
          }`}
        >
          All
        </button>
        {TOPICS.map((t) => (
          <button
            key={t}
            onClick={() => setActiveTopic(activeTopic === t ? null : t)}
            className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors capitalize ${
              activeTopic === t
                ? "bg-[var(--accent-primary)] text-white"
                : "bg-[var(--bg-card)] text-[var(--text-secondary)] border border-[var(--border)] hover:border-[var(--border-hover)]"
            }`}
          >
            {t}
          </button>
        ))}
      </div>

      {/* Search Results */}
      {searchQuery.trim() && (
        <div className="mb-8">
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
            Search Results
          </h2>
          {searching ? (
            <div className="glass-card p-4">
              <p className="text-sm text-[var(--text-muted)]">Searching...</p>
            </div>
          ) : searchResults && (searchResults.facts.length > 0 || searchResults.articles.length > 0) ? (
            <div className="space-y-2">
              {searchResults.facts.map((f) => (
                <div key={`f-${f.id}`} className="glass-card p-4 flex items-start justify-between gap-4">
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-[var(--text-primary)] leading-relaxed">{f.claim}</p>
                    <div className="flex items-center gap-2 mt-1.5">
                      <span className={`px-2 py-0.5 rounded text-[10px] font-medium capitalize ${topicColors[f.topic] ?? "bg-gray-100 text-gray-700"}`}>
                        {f.topic}
                      </span>
                      {f.subtopic && (
                        <span className="text-[10px] text-[var(--text-muted)]">{f.subtopic}</span>
                      )}
                    </div>
                  </div>
                  <span className={`px-2 py-0.5 rounded text-[10px] font-medium shrink-0 ${confidenceColor(f.confidence)}`}>
                    {(f.confidence * 100).toFixed(0)}%
                  </span>
                </div>
              ))}
              {searchResults.articles.map((a) => (
                <Link key={`a-${a.id}`} href={`/knowledge/${a.slug}`} className="block no-underline">
                  <div className="glass-card p-4">
                    <div className="flex items-center gap-2 mb-1">
                      <span className={`px-2 py-0.5 rounded text-[10px] font-medium capitalize ${topicColors[a.topic] ?? "bg-gray-100 text-gray-700"}`}>
                        {a.topic}
                      </span>
                      <span className="text-[10px] text-[var(--text-muted)]">Article</span>
                    </div>
                    <p className="text-sm font-medium text-[var(--text-primary)]">{a.title}</p>
                  </div>
                </Link>
              ))}
            </div>
          ) : (
            <div className="glass-card p-4">
              <p className="text-sm text-[var(--text-muted)]">No results found for &quot;{searchQuery}&quot;</p>
            </div>
          )}
        </div>
      )}

      {/* Article Grid */}
      {!searchQuery.trim() && (
        <>
          <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
            Articles
          </h2>
          {loading ? (
            <div className="glass-card p-8 text-center">
              <p className="text-sm text-[var(--text-muted)]">Loading articles...</p>
            </div>
          ) : articles.length > 0 ? (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {articles.map((a) => (
                <Link key={a.id} href={`/knowledge/${a.slug}`} className="block no-underline">
                  <div className="glass-card p-5 h-full flex flex-col">
                    <div className="flex items-center gap-2 mb-2">
                      <span className={`px-2 py-0.5 rounded text-[10px] font-medium capitalize ${topicColors[a.topic] ?? "bg-gray-100 text-gray-700"}`}>
                        {a.topic}
                      </span>
                      {a.country_iso3 && (
                        <span className="text-[10px] font-mono text-[var(--text-muted)]">{a.country_iso3}</span>
                      )}
                    </div>
                    <h3 className="text-sm font-medium text-[var(--text-primary)] mb-1.5">
                      {a.title}
                    </h3>
                    <p className="text-xs text-[var(--text-secondary)] leading-relaxed flex-1">
                      {a.summary.length > 200 ? a.summary.slice(0, 200) + "..." : a.summary}
                    </p>
                    <div className="flex items-center justify-between mt-3 pt-3 border-t border-[var(--border)]">
                      <span className="text-[10px] text-[var(--text-muted)]">
                        {a.fact_count} fact{a.fact_count !== 1 ? "s" : ""}
                      </span>
                      <span className="text-[10px] text-[var(--text-muted)]">
                        {new Date(a.updated_at).toLocaleDateString()}
                      </span>
                    </div>
                  </div>
                </Link>
              ))}
            </div>
          ) : (
            <div className="glass-card p-8 text-center">
              <p className="text-sm text-[var(--text-muted)]">
                No articles yet. Run the KB compiler to generate articles from analysis results.
              </p>
              <p className="text-xs text-[var(--text-muted)] mt-2">
                <code className="font-mono bg-[var(--bg-primary)] px-1.5 py-0.5 rounded">
                  python -m app.cli kb compile
                </code>
              </p>
            </div>
          )}
        </>
      )}
    </div>
  );
}
