// Integration tests for MetricChart's empty-state surfacing. We can't
// fully test uPlot rendering in jsdom (no real canvas), but we can mount
// the component, feed it various series shapes, and assert that the
// right diagnostic message appears — proving each failure mode is
// distinguishable instead of silently blank.

import { describe, it, expect, afterEach } from "vitest";
import { mount, unmount } from "svelte";
import MetricChart from "./MetricChart.svelte";
import type { ChartSeries } from "../lib/chart-utils";

let active: ReturnType<typeof mount> | null = null;
let host: HTMLElement | null = null;

afterEach(() => {
  if (active) {
    unmount(active);
    active = null;
  }
  if (host) {
    host.remove();
    host = null;
  }
});

function render(props: { series: ChartSeries[]; height?: number }) {
  host = document.createElement("div");
  // Give the host a real width so the chart's container is non-zero.
  // jsdom doesn't compute layout, but offsetWidth/clientWidth read from
  // explicit pixel styles.
  host.style.width = "800px";
  host.style.height = `${props.height ?? 280}px`;
  document.body.appendChild(host);
  active = mount(MetricChart, {
    target: host,
    props,
  });
  return host;
}

describe("MetricChart empty-state diagnostics", () => {
  it("shows 'no runs to plot' when given an empty series list", () => {
    const el = render({ series: [] });
    expect(el.textContent).toContain("No runs to plot");
  });

  it("shows 'no measured checkpoints' when series have zero points", () => {
    const el = render({
      series: [{ id: "run_a", points: [] }],
    });
    expect(el.textContent).toContain("No measured checkpoints yet");
  });

  it("shows 'no checkpoint steps' when points exist but lack step info", () => {
    const el = render({
      series: [
        {
          id: "run_a",
          points: [
            {
              step: null,
              value: 0.5,
              eval_run_id: null,
              state: "succeeded",
              checkpoint_artifact_id: "x",
            },
          ],
        },
      ],
    });
    expect(el.textContent).toContain("Eval values exist but no checkpoint steps");
  });

  it("does NOT show an empty state when at least one point is plottable", () => {
    const el = render({
      series: [
        {
          id: "run_a",
          points: [
            {
              step: 1000,
              value: 0.5,
              eval_run_id: null,
              state: "succeeded",
              checkpoint_artifact_id: "x",
            },
          ],
        },
      ],
    });
    // The empty-state titles should not appear.
    expect(el.textContent).not.toContain("No runs to plot");
    expect(el.textContent).not.toContain("No measured checkpoints yet");
    expect(el.textContent).not.toContain("Eval values exist but no checkpoint steps");
  });

  it("hides the canvas when an empty state is shown", () => {
    const el = render({ series: [] });
    const canvas = el.querySelector(".canvas");
    expect(canvas).not.toBeNull();
    expect(canvas!.classList.contains("hidden")).toBe(true);
  });

  it("does not hide the canvas when data is plottable", () => {
    const el = render({
      series: [
        {
          id: "run_a",
          points: [
            {
              step: 1000,
              value: 0.5,
              eval_run_id: null,
              state: "succeeded",
              checkpoint_artifact_id: "x",
            },
          ],
        },
      ],
    });
    const canvas = el.querySelector(".canvas");
    expect(canvas).not.toBeNull();
    expect(canvas!.classList.contains("hidden")).toBe(false);
  });
});
