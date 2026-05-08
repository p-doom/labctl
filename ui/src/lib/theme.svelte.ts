// Theme: dark | light | system. Applied as a class on <html>. Persisted
// to localStorage; falls back to prefers-color-scheme. The class is set
// by an inline script in index.html *before* the bundle loads, so there's
// no flash of wrong theme.

export type Theme = "dark" | "light" | "system";

const KEY = "labctl.theme";

function effective(t: Theme): "dark" | "light" {
  if (t !== "system") return t;
  if (typeof window === "undefined") return "dark";
  return window.matchMedia("(prefers-color-scheme: light)").matches
    ? "light"
    : "dark";
}

function apply(t: Theme) {
  if (typeof document === "undefined") return;
  const el = document.documentElement;
  el.classList.remove("dark", "light");
  el.classList.add(effective(t));
}

let _pref = $state<Theme>(read());

function read(): Theme {
  if (typeof localStorage === "undefined") return "system";
  const v = localStorage.getItem(KEY);
  return v === "dark" || v === "light" || v === "system" ? v : "system";
}

if (typeof window !== "undefined") {
  // Re-apply on system-preference change when in `system` mode.
  const mq = window.matchMedia("(prefers-color-scheme: light)");
  mq.addEventListener?.("change", () => {
    if (_pref === "system") apply("system");
  });
}

export const theme = {
  get pref(): Theme {
    return _pref;
  },
  get effective(): "dark" | "light" {
    return effective(_pref);
  },
  set(next: Theme) {
    _pref = next;
    if (typeof localStorage !== "undefined") {
      localStorage.setItem(KEY, next);
    }
    apply(next);
  },
  /** Cycle dark → light → system → dark. */
  cycle() {
    const next: Theme =
      _pref === "dark" ? "light" : _pref === "light" ? "system" : "dark";
    this.set(next);
  },
};
