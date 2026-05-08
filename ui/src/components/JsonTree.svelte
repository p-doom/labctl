<script lang="ts">
  // Universal fallback: pretty-print + syntax-highlight any JSON. Reuses
  // the existing shiki bundle (`json` grammar already loaded for the
  // recipe rendering, so no extra weight). Collapsible past N lines.
  import CodeBlock from "./CodeBlock.svelte";

  interface Props {
    value: unknown;
    collapsedLines?: number;
  }
  let { value, collapsedLines = 18 }: Props = $props();

  let pretty = $derived(safeStringify(value));

  function safeStringify(v: unknown): string {
    try {
      return JSON.stringify(v, null, 2);
    } catch {
      return String(v);
    }
  }
</script>

<CodeBlock code={pretty} lang="json" {collapsedLines} />
