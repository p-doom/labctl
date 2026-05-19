<script lang="ts">
  // Tiny child of <SvelteFlow> whose only job is to imperatively fit
  // the viewport onto a neighborhood subset on first load (and again
  // when the focal artifact changes). Lives as a child component so it
  // has access to SvelteFlow's context via `useSvelteFlow()` — the
  // parent <LineageView> can't call the hook directly.

  import { useSvelteFlow } from "@xyflow/svelte";

  interface Props {
    /** Node ids to frame on initial fit. */
    targetIds: string[];
    /** Re-fit when this changes (different artifact opens). */
    fitKey: string;
  }
  let { targetIds, fitKey }: Props = $props();

  const flow = useSvelteFlow();
  let lastFittedKey = $state<string | null>(null);

  $effect(() => {
    if (!targetIds || targetIds.length === 0) return;
    if (lastFittedKey === fitKey) return;
    const ids = targetIds.map((id) => ({ id }));
    const key = fitKey;
    // Defer one frame so SvelteFlow has registered + measured the nodes
    // before the bounding box is computed.
    requestAnimationFrame(() => {
      flow.fitView({ nodes: ids, padding: 0.25, duration: 0 });
      lastFittedKey = key;
    });
  });
</script>
