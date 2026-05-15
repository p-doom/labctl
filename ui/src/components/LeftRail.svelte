<script lang="ts">
  import { router, type View } from "../lib/router.svelte";
  import Icon from "./Icon.svelte";

  interface NavItem {
    view: View;
    label: string;
    icon: "runs" | "pipelines" | "artifacts" | "evals";
    shortcut: string;
  }
  // "evals" icon (checkbox-with-checkmark) doubles as the policies icon —
  // a policy is the rubric used to evaluate. No new icon needed.
  const items: NavItem[] = [
    { view: "runs", label: "Runs", icon: "runs", shortcut: "g r" },
    { view: "pipelines", label: "Pipelines", icon: "pipelines", shortcut: "g p" },
    { view: "artifacts", label: "Artifacts", icon: "artifacts", shortcut: "g a" },
    { view: "policies", label: "Policies", icon: "evals", shortcut: "g e" },
  ];
</script>

<nav class="rail">
  <div class="brand">
    <span class="dot"></span>
    <span class="name">labctl</span>
  </div>
  <ul>
    {#each items as item}
      <li>
        <button
          type="button"
          class="item"
          class:active={router.view === item.view}
          onclick={() => router.go(item.view)}
        >
          <span class="icon"><Icon name={item.icon} size={14} /></span>
          <span class="label">{item.label}</span>
          <span class="kbd">{item.shortcut}</span>
        </button>
      </li>
    {/each}
  </ul>
</nav>

<style>
  .rail {
    display: flex;
    flex-direction: column;
    width: 200px;
    flex-shrink: 0;
    background: theme("colors.bg.0");
    border-right: 1px solid theme("colors.line.0");
    padding: 14px 8px;
  }
  .brand {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 4px 8px 16px 8px;
  }
  .dot {
    width: 8px;
    height: 8px;
    border-radius: 2px;
    background: theme("colors.accent.DEFAULT");
  }
  .name {
    font-family: theme("fontFamily.mono");
    font-size: 13px;
    color: theme("colors.fg.0");
    letter-spacing: 0.02em;
  }
  ul {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 1px;
  }
  .item {
    display: grid;
    grid-template-columns: 18px 1fr auto;
    align-items: center;
    gap: 10px;
    padding: 6px 8px;
    border-radius: 4px;
    border: none;
    background: transparent;
    color: theme("colors.fg.1");
    cursor: pointer;
    text-align: left;
    width: 100%;
  }
  .item:hover {
    background: theme("colors.bg.2");
    color: theme("colors.fg.0");
  }
  .item.active {
    background: theme("colors.bg.2");
    color: theme("colors.fg.0");
    box-shadow: inset 2px 0 0 theme("colors.accent.DEFAULT");
  }
  .item.active .icon {
    color: theme("colors.accent.DEFAULT");
  }
  .icon {
    display: flex;
    align-items: center;
    justify-content: center;
    color: theme("colors.fg.2");
  }
  .label {
    font-size: 13px;
  }
  .kbd {
    font-family: theme("fontFamily.mono");
    font-size: 10px;
    color: theme("colors.fg.3");
    letter-spacing: 0.05em;
  }
  .item:not(.active) .kbd {
    opacity: 0;
    transition: opacity 150ms cubic-bezier(0.2, 0, 0, 1);
  }
  .item:hover .kbd {
    opacity: 1;
  }
</style>
