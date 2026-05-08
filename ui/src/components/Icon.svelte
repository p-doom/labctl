<script lang="ts">
  // Tiny icon set drawn inline. Lucide-styled, ~14×14, 1.5px stroke.
  // No external icon library — the set we need is small and consistency
  // matters more than coverage.
  interface Props {
    name:
      | "runs"
      | "pipelines"
      | "artifacts"
      | "evals"
      | "search"
      | "close"
      | "back"
      | "forward"
      | "external"
      | "copy"
      | "folder"
      | "play"
      | "pause"
      | "filter"
      | "chevron-right"
      | "chevron-down"
      | "expand"
      | "collapse"
      | "sun"
      | "moon"
      | "system"
      | "check";
    size?: number;
  }
  let { name, size = 14 }: Props = $props();
</script>

<svg
  width={size}
  height={size}
  viewBox="0 0 16 16"
  fill="none"
  stroke="currentColor"
  stroke-width="1.5"
  stroke-linecap="round"
  stroke-linejoin="round"
  aria-hidden="true"
>
  {#if name === "runs"}
    <rect x="2.5" y="3" width="11" height="2.2" rx="1" />
    <rect x="2.5" y="6.9" width="11" height="2.2" rx="1" />
    <rect x="2.5" y="10.8" width="11" height="2.2" rx="1" />
  {:else if name === "pipelines"}
    <circle cx="4" cy="4" r="1.6" />
    <circle cx="12" cy="4" r="1.6" />
    <circle cx="8" cy="12" r="1.6" />
    <path d="M5.4 4.5L6.8 11" />
    <path d="M10.6 4.5L9.2 11" />
  {:else if name === "artifacts"}
    <path d="M2.5 5.5l5.5-3 5.5 3" />
    <path d="M2.5 5.5v5l5.5 3 5.5-3v-5" />
    <path d="M8 8.5v5" />
    <path d="M2.5 5.5L8 8.5l5.5-3" />
  {:else if name === "evals"}
    <path d="M3 3h10v10H3z" />
    <path d="M5 8.5l2 2 4-4" />
  {:else if name === "search"}
    <circle cx="7" cy="7" r="4" />
    <path d="M10.2 10.2L13 13" />
  {:else if name === "close"}
    <path d="M4 4l8 8M12 4l-8 8" />
  {:else if name === "back"}
    <path d="M10 3L5 8l5 5" />
  {:else if name === "forward"}
    <path d="M6 3l5 5-5 5" />
  {:else if name === "external"}
    <path d="M9 3h4v4" />
    <path d="M13 3l-6 6" />
    <path d="M11.5 8.5V13H3V4.5h4.5" />
  {:else if name === "copy"}
    <rect x="5" y="5" width="8" height="8" rx="1.5" />
    <path d="M3 11V4a1 1 0 0 1 1-1h7" />
  {:else if name === "folder"}
    <path d="M2 5a1 1 0 0 1 1-1h3l1.5 1.5H13a1 1 0 0 1 1 1V12a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1z" />
  {:else if name === "play"}
    <path d="M5 3.5l7 4.5-7 4.5z" fill="currentColor" stroke="none" />
  {:else if name === "pause"}
    <rect x="4.5" y="3.5" width="2.5" height="9" rx="0.5" fill="currentColor" stroke="none" />
    <rect x="9" y="3.5" width="2.5" height="9" rx="0.5" fill="currentColor" stroke="none" />
  {:else if name === "filter"}
    <path d="M2.5 4h11l-4 5v3l-3 1.5V9z" />
  {:else if name === "chevron-right"}
    <path d="M6 3l5 5-5 5" />
  {:else if name === "chevron-down"}
    <path d="M3 6l5 5 5-5" />
  {:else if name === "expand"}
    <path d="M3 7V3h4M13 9v4H9M3 3l4 4M13 13l-4-4" />
  {:else if name === "collapse"}
    <path d="M7 3v4H3M9 13V9h4M3 7l4-4M13 9l-4 4" />
  {:else if name === "sun"}
    <circle cx="8" cy="8" r="3" />
    <path d="M8 1.5v1.5M8 13v1.5M14.5 8H13M3 8H1.5M12.6 3.4l-1.1 1.1M4.5 11.5l-1.1 1.1M12.6 12.6l-1.1-1.1M4.5 4.5L3.4 3.4" />
  {:else if name === "moon"}
    <path d="M13.2 9.6A5.5 5.5 0 1 1 6.4 2.8 4.5 4.5 0 0 0 13.2 9.6z" />
  {:else if name === "system"}
    <rect x="2" y="3" width="12" height="8.5" rx="1.2" />
    <path d="M5.5 13.5h5M8 11.5v2" />
  {:else if name === "check"}
    <path d="M3 8.5l3 3 7-7" stroke-width="2" />
  {/if}
</svg>
