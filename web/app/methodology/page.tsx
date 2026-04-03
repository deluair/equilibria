"use client";

interface Module {
  name: string;
  description: string;
  methods: string[];
}

interface Layer {
  code: string;
  name: string;
  modules: Module[];
}

const layers: Layer[] = [
  {
    code: "L1",
    name: "Trade",
    modules: [
      {
        name: "Gravity Model",
        description: "Estimates bilateral trade flows using the structural gravity equation.",
        methods: [
          "OLS and PPML estimation",
          "Anderson-van Wincoop (2003) multilateral resistance terms",
          "Distance, GDP, common language, colonial ties, FTA controls",
        ],
      },
      {
        name: "Revealed Comparative Advantage",
        description: "Balassa (1965) index measuring export specialization.",
        methods: [
          "RCA = (X_ij / X_j) / (X_iw / X_w)",
          "Normalized symmetric RCA (RSCA)",
          "HS6-level product disaggregation",
        ],
      },
      {
        name: "Trade Openness",
        description: "Composite trade integration indicators.",
        methods: [
          "(Exports + Imports) / GDP ratio",
          "Trade complementarity index",
          "Intra-industry trade (Grubel-Lloyd)",
        ],
      },
    ],
  },
  {
    code: "L2",
    name: "Macro",
    modules: [
      {
        name: "GDP Decomposition",
        description: "Expenditure-side breakdown of national accounts.",
        methods: [
          "C + I + G + (X - M) decomposition",
          "Contribution to growth analysis",
          "Seasonal adjustment via X-13ARIMA-SEATS",
        ],
      },
      {
        name: "Phillips Curve",
        description: "Inflation-unemployment tradeoff estimation.",
        methods: [
          "Expectations-augmented Phillips curve",
          "NAIRU estimation via Kalman filter",
          "Sacrifice ratio calculation",
        ],
      },
      {
        name: "Business Cycles",
        description: "Cycle identification and leading indicator construction.",
        methods: [
          "HP filter (lambda=1600 for quarterly)",
          "Baxter-King bandpass filter",
          "Composite leading indicator (CLI)",
        ],
      },
    ],
  },
  {
    code: "L3",
    name: "Labor",
    modules: [
      {
        name: "Wage Analysis",
        description: "Nominal and real wage dynamics across sectors.",
        methods: [
          "Mincer wage equation estimation",
          "Wage-productivity gap decomposition",
          "Sectoral wage premium analysis",
        ],
      },
      {
        name: "Employment Trends",
        description: "Labor market flow analysis and structural shifts.",
        methods: [
          "Beveridge curve (vacancy-unemployment)",
          "Sectoral shift decomposition",
          "Labor force participation modeling",
        ],
      },
    ],
  },
  {
    code: "L4",
    name: "Development",
    modules: [
      {
        name: "Convergence Analysis",
        description: "Income convergence testing across economies.",
        methods: [
          "Beta-convergence (Barro-Sala-i-Martin, 1992)",
          "Sigma-convergence (cross-sectional dispersion)",
          "Club convergence (Phillips-Sul, 2007)",
        ],
      },
      {
        name: "Poverty Dynamics",
        description: "Poverty measurement and decomposition.",
        methods: [
          "Foster-Greer-Thorbecke (FGT) indices",
          "Growth-redistribution decomposition (Datt-Ravallion)",
          "Multidimensional Poverty Index (Alkire-Foster)",
        ],
      },
      {
        name: "Institutional Quality",
        description: "Governance and institutional indicators.",
        methods: [
          "Worldwide Governance Indicators (WGI)",
          "Doing Business / Business Enabling Environment",
          "Economic Freedom indices",
        ],
      },
    ],
  },
  {
    code: "L5",
    name: "Agricultural",
    modules: [
      {
        name: "Food Security",
        description: "Multi-dimensional food security assessment.",
        methods: [
          "Global Food Security Index (GFSI) components",
          "Caloric availability per capita",
          "Import dependency ratio for staples",
        ],
      },
      {
        name: "Price Transmission",
        description: "World-to-domestic price pass-through estimation.",
        methods: [
          "VECM-based transmission elasticities",
          "Asymmetric price transmission (Houck, 1977)",
          "Threshold autoregressive models (TAR)",
        ],
      },
    ],
  },
];

export default function MethodologyPage() {
  return (
    <div>
      <div className="mb-8">
        <h1 className="text-2xl font-semibold tracking-tight text-[var(--text-primary)]">
          Methodology
        </h1>
        <p className="text-sm text-[var(--text-secondary)] mt-1">
          Documentation of analytical modules organized by layer.
          Each module implements peer-reviewed econometric methods.
        </p>
      </div>

      {/* CEAS Overview */}
      <div className="glass-card p-5 mb-8">
        <h2 className="text-sm font-semibold text-[var(--text-primary)] mb-3">
          Composite Equilibrium Assessment Score (CEAS)
        </h2>
        <p className="text-sm text-[var(--text-secondary)] leading-relaxed mb-3">
          The CEAS aggregates five layer-specific scores into a single composite index (0-100).
          Each layer score is computed from its constituent modules using standardized z-scores
          normalized to a 0-100 scale. The composite uses configurable weights that sum to 1.0.
        </p>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-[var(--border)]">
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Layer</th>
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Code</th>
                <th className="text-left py-2 px-3 font-medium text-[var(--text-secondary)]">Modules</th>
                <th className="text-right py-2 px-3 font-medium text-[var(--text-secondary)]">Default Weight</th>
              </tr>
            </thead>
            <tbody>
              {layers.map((l) => (
                <tr key={l.code} className="border-b border-[var(--border)]/50">
                  <td className="py-2 px-3 text-[var(--text-primary)]">{l.name}</td>
                  <td className="py-2 px-3 font-mono text-xs text-[var(--text-muted)]">{l.code}</td>
                  <td className="py-2 px-3 text-xs text-[var(--text-secondary)]">{l.modules.length}</td>
                  <td className="text-right py-2 px-3 font-mono text-xs">0.20</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Layer Modules */}
      <div className="space-y-6">
        {layers.map((layer) => (
          <div key={layer.code}>
            <div className="flex items-center gap-3 mb-4">
              <span className="w-8 h-8 rounded-lg bg-[var(--accent-primary)] text-white flex items-center justify-center text-xs font-mono font-semibold">
                {layer.code.replace("L", "")}
              </span>
              <h2 className="text-lg font-semibold text-[var(--text-primary)]">
                {layer.name}
              </h2>
            </div>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-2">
              {layer.modules.map((mod) => (
                <div key={mod.name} className="glass-card p-5">
                  <h3 className="text-sm font-semibold text-[var(--text-primary)] mb-1">
                    {mod.name}
                  </h3>
                  <p className="text-xs text-[var(--text-secondary)] mb-3 leading-relaxed">
                    {mod.description}
                  </p>
                  <ul className="space-y-1.5">
                    {mod.methods.map((m, i) => (
                      <li key={i} className="flex items-start gap-2 text-xs text-[var(--text-secondary)]">
                        <span className="w-1 h-1 rounded-full bg-[var(--accent-primary)] mt-1.5 flex-shrink-0" />
                        {m}
                      </li>
                    ))}
                  </ul>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
