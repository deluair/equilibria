"use client";

type Signal = "STABLE" | "WATCH" | "STRESS" | "CRISIS";

interface LayerCardProps {
  name: string;
  code: string;
  score: number | null;
  signal: Signal | null;
  description?: string;
  href?: string;
}

const signalConfig: Record<Signal, { label: string; bg: string; text: string; dot: string }> = {
  STABLE: { label: "Stable", bg: "bg-emerald-50", text: "text-emerald-700", dot: "bg-emerald-500" },
  WATCH: { label: "Watch", bg: "bg-amber-50", text: "text-amber-700", dot: "bg-amber-500" },
  STRESS: { label: "Stress", bg: "bg-orange-50", text: "text-orange-700", dot: "bg-orange-500" },
  CRISIS: { label: "Crisis", bg: "bg-rose-50", text: "text-rose-700", dot: "bg-rose-500" },
};

export default function LayerCard({ name, code, score, signal, description, href }: LayerCardProps) {
  const config = signal ? signalConfig[signal] : null;

  const content = (
    <div className="glass-card p-5 transition-all duration-200">
      <div className="flex items-start justify-between mb-3">
        <div>
          <span className="text-xs font-mono tracking-wider text-[var(--text-muted)] uppercase">
            {code}
          </span>
          <h3 className="text-base font-semibold text-[var(--text-primary)] mt-0.5">{name}</h3>
        </div>
        {config && (
          <span
            className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${config.bg} ${config.text}`}
          >
            <span className={`w-1.5 h-1.5 rounded-full ${config.dot}`} />
            {config.label}
          </span>
        )}
      </div>
      <div className="flex items-end justify-between">
        <div>
          {score !== null ? (
            <span className="text-2xl font-semibold font-mono text-[var(--text-primary)]">
              {score.toFixed(1)}
            </span>
          ) : (
            <span className="text-sm text-[var(--text-muted)]">No data</span>
          )}
          {score !== null && (
            <span className="text-xs text-[var(--text-muted)] ml-1.5">/ 100</span>
          )}
        </div>
        {description && (
          <p className="text-xs text-[var(--text-secondary)] max-w-[60%] text-right leading-relaxed">
            {description}
          </p>
        )}
      </div>
    </div>
  );

  if (href) {
    return <a href={href} className="block no-underline">{content}</a>;
  }
  return content;
}
