// Lazy-loaded shiki, fine-grained bundle. Two themes loaded so we can
// swap when the user toggles light/dark; the inactive theme costs nothing
// at runtime — just a small extra build chunk.
//
// toml / json / bash load with the highlighter (recipes and configs use
// them everywhere). The rest are fetched the first time a file needs
// them, so browsing a run never pays for the python grammar.

import type { HighlighterCore, LanguageRegistration } from "shiki/core";

type Grammar = () => Promise<{ default: LanguageRegistration[] }>;

const EXTRA = {
  python: () => import("@shikijs/langs/python"),
  markdown: () => import("@shikijs/langs/markdown"),
  typescript: () => import("@shikijs/langs/typescript"),
  tsx: () => import("@shikijs/langs/tsx"),
  javascript: () => import("@shikijs/langs/javascript"),
  yaml: () => import("@shikijs/langs/yaml"),
  diff: () => import("@shikijs/langs/diff"),
  go: () => import("@shikijs/langs/go"),
  latex: () => import("@shikijs/langs/latex"),
  ini: () => import("@shikijs/langs/ini"),
} satisfies Record<string, Grammar>;

export type Lang = "toml" | "json" | "bash" | keyof typeof EXTRA;

let highlighterPromise: Promise<HighlighterCore> | null = null;
const extraLoads = new Map<string, Promise<void>>();

async function getHighlighter(): Promise<HighlighterCore> {
  if (!highlighterPromise) {
    highlighterPromise = (async () => {
      const [
        { createHighlighterCore },
        { createOnigurumaEngine },
        wasm,
        toml,
        json,
        bash,
        themeDark,
        themeLight,
      ] = await Promise.all([
        import("shiki/core"),
        import("shiki/engine/oniguruma"),
        import("shiki/wasm"),
        import("@shikijs/langs/toml"),
        import("@shikijs/langs/json"),
        import("@shikijs/langs/bash"),
        import("@shikijs/themes/github-dark-default"),
        import("@shikijs/themes/github-light-default"),
      ]);
      return createHighlighterCore({
        themes: [themeDark.default, themeLight.default],
        langs: [toml.default, json.default, bash.default],
        engine: createOnigurumaEngine(wasm.default),
      });
    })();
  }
  return highlighterPromise;
}

async function ensureLang(h: HighlighterCore, lang: Lang): Promise<void> {
  if (!(lang in EXTRA)) return;
  let load = extraLoads.get(lang);
  if (!load) {
    load = EXTRA[lang as keyof typeof EXTRA]().then((m) => h.loadLanguage(...m.default));
    extraLoads.set(lang, load);
  }
  await load;
}

function activeTheme(): "github-dark-default" | "github-light-default" {
  if (typeof document === "undefined") return "github-dark-default";
  return document.documentElement.classList.contains("light")
    ? "github-light-default"
    : "github-dark-default";
}

export async function highlight(code: string, lang: Lang): Promise<string> {
  const h = await getHighlighter();
  await ensureLang(h, lang);
  return h.codeToHtml(code, { lang, theme: activeTheme() });
}
