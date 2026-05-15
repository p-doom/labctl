/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,svelte}"],
  // Theme is applied as a class on <html>: `dark` (default) or `light`.
  // Tokens resolve to CSS variables so swapping is a class change, no
  // recompile, no FOUC if we set the class before paint.
  darkMode: "class",
  theme: {
    colors: {
      transparent: "transparent",
      current: "currentColor",
      bg: {
        0: "var(--bg-0)",
        1: "var(--bg-1)",
        2: "var(--bg-2)",
        3: "var(--bg-3)",
      },
      fg: {
        0: "var(--fg-0)",
        1: "var(--fg-1)",
        2: "var(--fg-2)",
        3: "var(--fg-3)",
      },
      line: {
        0: "var(--line-0)",
        1: "var(--line-1)",
        2: "var(--line-2)",
      },
      accent: {
        DEFAULT: "var(--accent)",
        dim: "var(--accent-dim)",
        soft: "var(--accent-soft)",
      },
      status: {
        running: {
          DEFAULT: "var(--status-running)",
          soft: "var(--status-running-soft)",
          fg: "var(--status-running-fg)",
        },
        succeeded: {
          DEFAULT: "var(--status-succeeded)",
          soft: "var(--status-succeeded-soft)",
          fg: "var(--status-succeeded-fg)",
        },
        failed: {
          DEFAULT: "var(--status-failed)",
          soft: "var(--status-failed-soft)",
          fg: "var(--status-failed-fg)",
        },
        pending: {
          DEFAULT: "var(--status-pending)",
          soft: "var(--status-pending-soft)",
          fg: "var(--status-pending-fg)",
        },
        neutral: {
          DEFAULT: "var(--status-neutral)",
          soft: "var(--status-neutral-soft)",
          fg: "var(--status-neutral-fg)",
        },
      },
    },
    spacing: {
      0: "0",
      0.5: "2px",
      1: "4px",
      1.5: "6px",
      2: "8px",
      3: "12px",
      4: "16px",
      5: "20px",
      6: "24px",
      8: "32px",
      10: "40px",
      12: "48px",
      16: "64px",
      px: "1px",
      full: "100%",
    },
    fontFamily: {
      sans: ['"Inter Variable"', "Inter", "ui-sans-serif", "system-ui", "sans-serif"],
      mono: ['"JetBrains Mono Variable"', '"JetBrains Mono"', "ui-monospace", "SFMono-Regular", "monospace"],
    },
    fontSize: {
      xs: ["12px", { lineHeight: "16px", letterSpacing: "0.005em" }],
      sm: ["13px", { lineHeight: "20px" }],
      base: ["14px", { lineHeight: "20px" }],
      lg: ["18px", { lineHeight: "24px", letterSpacing: "-0.01em" }],
    },
    fontWeight: {
      normal: "400",
      medium: "500",
    },
    borderRadius: {
      none: "0",
      sm: "3px",
      DEFAULT: "4px",
      md: "6px",
      lg: "8px",
      full: "9999px",
    },
    extend: {
      transitionTimingFunction: {
        out: "cubic-bezier(0.2, 0, 0, 1)",
        emphasis: "cubic-bezier(0.3, 0, 0, 1.05)",
      },
      transitionDuration: {
        150: "150ms",
        250: "250ms",
      },
      boxShadow: {
        panel:
          "0 16px 48px -16px var(--shadow-panel-1), 0 0 0 1px var(--line-1)",
      },
      keyframes: {
        pulseDot: {
          "0%": { opacity: "1", transform: "scale(1)", boxShadow: "0 0 0 0 var(--dot)" },
          "50%": { opacity: "0.2", transform: "scale(0.5)", boxShadow: "0 0 0 5px transparent" },
          "100%": { opacity: "1", transform: "scale(1)", boxShadow: "0 0 0 0 transparent" },
        },
        slideIn: {
          from: { transform: "translateX(8%)", opacity: "0" },
          to: { transform: "translateX(0)", opacity: "1" },
        },
        fadeIn: {
          from: { opacity: "0" },
          to: { opacity: "1" },
        },
        shimmer: {
          "0%": { backgroundPosition: "-200% 0" },
          "100%": { backgroundPosition: "200% 0" },
        },
      },
      animation: {
        "pulse-dot": "pulseDot 1.1s cubic-bezier(0.4, 0, 0.6, 1) infinite",
        "slide-in": "slideIn 250ms cubic-bezier(0.2, 0, 0, 1)",
        "fade-in": "fadeIn 150ms cubic-bezier(0.2, 0, 0, 1)",
        shimmer: "shimmer 1.4s linear infinite",
      },
    },
  },
  plugins: [],
};
