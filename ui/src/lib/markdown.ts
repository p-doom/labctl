// Markdown for prompts, NOTES.md and workspace READMEs. Task packages are
// authored by many people and rendered via {@html}, so every render goes
// through DOMPurify. Links that would resolve against labctl itself
// (relative paths into the package) are neutralised to plain text: the
// file browser is the way to open those.

import { marked } from "marked";
import DOMPurify from "dompurify";

const EXTERNAL = /^(https?:|mailto:)/i;

let hooked = false;
function ensureHooks() {
  if (hooked) return;
  hooked = true;
  DOMPurify.addHook("afterSanitizeAttributes", (node) => {
    if (node.tagName === "A") {
      const href = node.getAttribute("href") ?? "";
      if (EXTERNAL.test(href)) {
        node.setAttribute("target", "_blank");
        node.setAttribute("rel", "noopener noreferrer");
      } else {
        node.removeAttribute("href");
        if (href) node.setAttribute("title", href);
      }
    }
    if (node.tagName === "IMG" && !EXTERNAL.test(node.getAttribute("src") ?? "")) {
      node.removeAttribute("src");
    }
  });
}

export function renderMarkdown(src: string): string {
  ensureHooks();
  const html = marked.parse(src, { async: false, gfm: true }) as string;
  return DOMPurify.sanitize(html, { USE_PROFILES: { html: true } });
}
