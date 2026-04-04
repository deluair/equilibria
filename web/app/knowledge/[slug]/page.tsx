"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";

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

interface ArticleData {
  id: number;
  slug: string;
  title: string;
  topic: string;
  country_iso3: string | null;
  content: string;
  summary: string;
  fact_count: number;
  created_at: string;
  updated_at: string;
  facts: Fact[];
}

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

function confidenceBadge(c: number) {
  if (c >= 0.7) return { bg: "bg-emerald-50 text-emerald-700", label: "high" };
  if (c >= 0.4) return { bg: "bg-amber-50 text-amber-700", label: "medium" };
  return { bg: "bg-rose-50 text-rose-700", label: "low" };
}

function renderMarkdown(text: string) {
  const lines = text.split("\n");
  const elements: React.ReactNode[] = [];
  let listItems: string[] = [];

  function flushList() {
    if (listItems.length > 0) {
      elements.push(
        <ul key={`ul-${elements.length}`} className="list-disc list-inside space-y-1 mb-4 text-sm text-[var(--text-secondary)] leading-relaxed">
          {listItems.map((item, i) => (
            <li key={i}>{item}</li>
          ))}
        </ul>
      );
      listItems = [];
    }
  }

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const trimmed = line.trim();

    if (!trimmed) {
      flushList();
      continue;
    }

    if (trimmed.startsWith("### ")) {
      flushList();
      elements.push(
        <h3 key={`h3-${i}`} className="text-base font-semibold text-[var(--text-primary)] mt-6 mb-2">
          {trimmed.slice(4)}
        </h3>
      );
    } else if (trimmed.startsWith("## ")) {
      flushList();
      elements.push(
        <h2 key={`h2-${i}`} className="text-lg font-semibold text-[var(--text-primary)] mt-8 mb-3">
          {trimmed.slice(3)}
        </h2>
      );
    } else if (trimmed.startsWith("# ")) {
      flushList();
      elements.push(
        <h1 key={`h1-${i}`} className="text-xl font-semibold text-[var(--text-primary)] mt-8 mb-3">
          {trimmed.slice(2)}
        </h1>
      );
    } else if (trimmed.startsWith("- ") || trimmed.startsWith("* ")) {
      listItems.push(trimmed.slice(2));
    } else {
      flushList();
      elements.push(
        <p key={`p-${i}`} className="text-sm text-[var(--text-secondary)] leading-relaxed mb-3">
          {trimmed}
        </p>
      );
    }
  }
  flushList();
  return elements;
}

export default function ArticleDetail() {
  const params = useParams();
  const slug = params?.slug as string | undefined;
  const [article, setArticle] = useState<ArticleData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!slug) return;
    fetch(`/api/kb/articles/${slug}`)
      .then((r) => {
        if (!r.ok) throw new Error(r.status === 404 ? "Article not found" : "Failed to load");
        return r.json();
      })
      .then((d) => setArticle(d))
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, [slug]);

  if (loading) {
    return (
      <div className="glass-card p-8 text-center">
        <p className="text-sm text-[var(--text-muted)]">Loading article...</p>
      </div>
    );
  }

  if (error || !article) {
    return (
      <div>
        <Link href="/knowledge" className="text-sm text-[var(--accent-primary)] hover:underline mb-4 inline-block">
          &larr; Back to Knowledge Base
        </Link>
        <div className="glass-card p-8 text-center">
          <p className="text-sm text-[var(--text-muted)]">{error ?? "Article not found"}</p>
        </div>
      </div>
    );
  }

  const staleFacts = article.facts.filter((f) => f.is_stale === 1);
  const staleRatio = article.facts.length > 0 ? staleFacts.length / article.facts.length : 0;

  return (
    <div>
      {/* Back link */}
      <Link href="/knowledge" className="text-sm text-[var(--accent-primary)] hover:underline mb-6 inline-block">
        &larr; Back to Knowledge Base
      </Link>

      {/* Header */}
      <div className="mb-6">
        <div className="flex items-center gap-2 mb-2">
          <span className={`px-2 py-0.5 rounded text-[10px] font-medium capitalize ${topicColors[article.topic] ?? "bg-gray-100 text-gray-700"}`}>
            {article.topic}
          </span>
          {article.country_iso3 && (
            <span className="text-xs font-mono text-[var(--text-muted)]">{article.country_iso3}</span>
          )}
        </div>
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)]">
          {article.title}
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-2">{article.summary}</p>
        <p className="text-xs text-[var(--text-muted)] mt-2">
          Last compiled {new Date(article.updated_at).toLocaleDateString()}
          {" "}&middot;{" "}
          {article.fact_count} fact{article.fact_count !== 1 ? "s" : ""}
        </p>
      </div>

      {/* Staleness Warning */}
      {staleRatio > 0.5 && (
        <div className="glass-card p-4 mb-6 border-l-4 border-l-[var(--accent-secondary)]">
          <p className="text-sm text-[var(--text-secondary)]">
            {staleFacts.length} of {article.facts.length} source facts are stale. This article may need recompilation.
          </p>
        </div>
      )}

      {/* Content + Sidebar */}
      <div className="flex gap-6">
        {/* Main body */}
        <div className="flex-1 min-w-0">
          <div className="glass-card p-6">
            {renderMarkdown(article.content)}
          </div>
        </div>

        {/* Sidebar: source facts */}
        {article.facts.length > 0 && (
          <div className="hidden lg:block w-72 shrink-0">
            <div className="sticky top-8">
              <h2 className="text-xs font-semibold text-[var(--text-muted)] uppercase tracking-wider mb-3">
                Source Facts ({article.facts.length})
              </h2>
              <div className="space-y-2 max-h-[calc(100vh-8rem)] overflow-y-auto">
                {article.facts.map((f) => {
                  const badge = confidenceBadge(f.confidence);
                  return (
                    <div key={f.id} className="glass-card p-3">
                      <p className="text-xs text-[var(--text-primary)] leading-relaxed mb-2">
                        {f.claim.length > 120 ? f.claim.slice(0, 120) + "..." : f.claim}
                      </p>
                      <div className="flex items-center gap-1.5 flex-wrap">
                        <span className={`px-1.5 py-0.5 rounded text-[9px] font-medium ${badge.bg}`}>
                          {(f.confidence * 100).toFixed(0)}%
                        </span>
                        {f.is_stale === 1 && (
                          <span className="inline-flex items-center gap-1 text-[9px] text-[var(--accent-secondary)]">
                            <span className="w-1.5 h-1.5 rounded-full bg-[var(--accent-secondary)]" />
                            stale
                          </span>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
