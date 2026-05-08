// Side-panel back/forward history — like a browser. Lets you walk
// run → input artifact → producer run → ... and pop back.

import { router, type View } from "./router.svelte";

interface PanelEntry {
  view: View;
  selected: string;
}

let stack = $state<PanelEntry[]>([]);
let index = $state(-1);

function currentRouterEntry(): PanelEntry | null {
  if (!router.selected) return null;
  if (router.view !== "runs" && router.view !== "artifacts" && router.view !== "pipelines")
    return null;
  return { view: router.view, selected: router.selected };
}

// Sync incoming router changes into the stack (avoid duplicates).
$effect.root(() => {
  $effect(() => {
    const entry = currentRouterEntry();
    if (!entry) return;
    const top = stack[index];
    if (top && top.view === entry.view && top.selected === entry.selected) return;
    // Truncate forward history when navigating to a new entry.
    stack = [...stack.slice(0, index + 1), entry];
    index = stack.length - 1;
  });
});

export const panelHistory = {
  get canBack() {
    return index > 0;
  },
  get canForward() {
    return index >= 0 && index < stack.length - 1;
  },
  back() {
    if (index <= 0) return;
    index -= 1;
    const entry = stack[index];
    if (entry) router.go(entry.view, entry.selected);
  },
  forward() {
    if (index >= stack.length - 1) return;
    index += 1;
    const entry = stack[index];
    if (entry) router.go(entry.view, entry.selected);
  },
  clear() {
    stack = [];
    index = -1;
  },
};
