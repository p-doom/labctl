import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mount, tick, unmount } from "svelte";
import { api } from "../lib/api";
import type { RolloutData } from "../lib/types";
import RolloutViewer from "./RolloutViewer.svelte";

let active: ReturnType<typeof mount> | null = null;
let host: HTMLElement | null = null;
const scrollIntoView = Element.prototype.scrollIntoView;

beforeEach(() => {
  Element.prototype.scrollIntoView = vi.fn();
});

afterEach(() => {
  vi.restoreAllMocks();
  if (active) {
    unmount(active);
    active = null;
  }
  host?.remove();
  host = null;
  if (scrollIntoView) Element.prototype.scrollIntoView = scrollIntoView;
  else delete (Element.prototype as { scrollIntoView?: unknown }).scrollIntoView;
});

function rollout(length: number): RolloutData {
  return {
    frame_count: length,
    steps: Array.from({ length }, (_, step_num) => ({
      step_num,
      action: step_num === 0 ? "<reset>" : "click()",
      response: step_num === 0 ? "<reset>" : "click()",
      reward: 0,
      done: step_num === length - 1,
      info: {},
    })),
  };
}

async function render(data: RolloutData): Promise<HTMLElement> {
  vi.spyOn(api, "rollout").mockResolvedValue(data);
  host = document.createElement("div");
  document.body.appendChild(host);
  active = mount(RolloutViewer, {
    target: host,
    props: { artifactId: "artifact_test" },
  });
  await vi.waitFor(() => expect(host!.querySelectorAll("tbody tr")).toHaveLength(201));
  return host;
}

describe("RolloutViewer windowing", () => {
  it("bounds the rendered trajectory and scrubber for a 10000-step rollout", async () => {
    const element = await render(rollout(10_000));

    expect(element.querySelectorAll("tbody tr")).toHaveLength(201);
    expect(element.querySelectorAll('input[aria-label="frame scrubber"]')).toHaveLength(1);

    const scrubber = element.querySelector<HTMLInputElement>('input[aria-label="frame scrubber"]')!;
    scrubber.value = "5000";
    scrubber.dispatchEvent(new Event("input", { bubbles: true }));
    await tick();

    expect(element.querySelector(".badge")?.textContent).toContain("frame 5000 / 9999");
    expect(element.querySelector("tbody tr .c-step")?.textContent).toBe("4900");
    expect(element.querySelectorAll("tbody tr")).toHaveLength(201);

    (element.querySelector(".maximize-btn") as HTMLButtonElement).click();
    await tick();

    expect(element.querySelectorAll("tbody tr")).toHaveLength(402);
    expect(element.querySelectorAll('input[aria-label="frame scrubber"]')).toHaveLength(2);
  }, 20_000);
});
