// Lazy-loaded shiki, fine-grained bundle. Two themes loaded so we can
// swap when the user toggles light/dark; the inactive theme costs nothing
// at runtime — just a small extra build chunk.

import type { HighlighterCore } from "shiki/core";

let highlighterPromise: Promise<HighlighterCore> | null = null;

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

function activeTheme(): "github-dark-default" | "github-light-default" {
  if (typeof document === "undefined") return "github-dark-default";
  return document.documentElement.classList.contains("light")
    ? "github-light-default"
    : "github-dark-default";
}

export async function highlight(
  code: string,
  lang: "toml" | "json" | "bash",
): Promise<string> {
  const h = await getHighlighter();
  return h.codeToHtml(code, { lang, theme: activeTheme() });
}
