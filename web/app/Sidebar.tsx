"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const navSections = [
  {
    label: "Overview",
    items: [
      { name: "Dashboard", href: "/", icon: "H" },
    ],
  },
  {
    label: "Layers",
    items: [
      { name: "L1 Trade", href: "/trade", icon: "1" },
      { name: "L2 Macro", href: "/macro", icon: "2" },
      { name: "L3 Labor", href: "/labor", icon: "3" },
      { name: "L4 Development", href: "/development", icon: "4" },
      { name: "L5 Agricultural", href: "/agricultural", icon: "5" },
    ],
  },
  {
    label: "Tools",
    items: [
      { name: "Briefings", href: "/briefings", icon: "B" },
      { name: "AI Chat", href: "/chat", icon: "C" },
    ],
  },
  {
    label: "Reference",
    items: [
      { name: "Methodology", href: "/methodology", icon: "M" },
      { name: "Data Sources", href: "/data", icon: "D" },
    ],
  },
];

export default function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="fixed left-0 top-0 bottom-0 w-64 bg-[var(--bg-card)] border-r border-[var(--border)] flex flex-col z-10">
      <div className="px-5 py-5 border-b border-[var(--border)]">
        <Link href="/" className="no-underline">
          <h1 className="text-lg font-semibold text-[var(--text-primary)] tracking-tight">
            Equilibria
          </h1>
          <p className="text-xs text-[var(--text-muted)] mt-0.5">
            Applied Economics Analysis
          </p>
        </Link>
      </div>

      <nav className="flex-1 overflow-y-auto px-3 py-4">
        {navSections.map((section) => (
          <div key={section.label} className="mb-5">
            <h2 className="text-[10px] font-semibold tracking-widest uppercase text-[var(--text-muted)] px-2 mb-2">
              {section.label}
            </h2>
            <ul className="space-y-0.5">
              {section.items.map((item) => {
                const isActive =
                  item.href === "/"
                    ? pathname === "/"
                    : pathname.startsWith(item.href);

                return (
                  <li key={item.href}>
                    <Link
                      href={item.href}
                      className={`flex items-center gap-3 px-2.5 py-2 rounded-lg text-sm no-underline transition-colors ${
                        isActive
                          ? "bg-[var(--accent-primary)]/10 text-[var(--accent-primary)] font-medium"
                          : "text-[var(--text-secondary)] hover:bg-[var(--bg-primary)] hover:text-[var(--text-primary)]"
                      }`}
                    >
                      <span
                        className={`w-6 h-6 rounded-md flex items-center justify-center text-xs font-mono font-semibold ${
                          isActive
                            ? "bg-[var(--accent-primary)] text-white"
                            : "bg-[var(--bg-primary)] text-[var(--text-muted)]"
                        }`}
                      >
                        {item.icon}
                      </span>
                      {item.name}
                    </Link>
                  </li>
                );
              })}
            </ul>
          </div>
        ))}
      </nav>

      <div className="px-5 py-4 border-t border-[var(--border)]">
        <p className="text-[10px] text-[var(--text-muted)]">
          CEAS v1.0
        </p>
      </div>
    </aside>
  );
}
