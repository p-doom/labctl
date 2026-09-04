import { afterEach, describe, expect, it, vi } from "vitest";
import { mount, tick, unmount } from "svelte";
import App from "./App.svelte";

let app: ReturnType<typeof mount> | null = null;

afterEach(() => {
  if (app) unmount(app);
  app = null;
  document.body.replaceChildren();
  vi.unstubAllGlobals();
});

describe("App route ownership", () => {
  it("mounts and loads only the runs route", async () => {
    const requests: string[] = [];

    vi.stubGlobal("fetch", vi.fn(async (input: RequestInfo | URL) => {
      const path = String(input).replace(/^\/api/, "");
      requests.push(path);
      const body = path === "/cluster"
        ? { name: "test", registry_db: "test.db" }
        : { runs: [], pipelines: [], artifacts: [], policies: [] };
      return { ok: true, json: async () => body };
    }));
    vi.stubGlobal("EventSource", class {
      onopen: ((event: Event) => void) | null = null;
      onerror: ((event: Event) => void) | null = null;

      constructor(url: string | URL) {
        requests.push(String(url).replace(/^\/api/, ""));
      }

      addEventListener() {}
    });

    window.location.hash = "#/runs";
    window.dispatchEvent(new HashChangeEvent("hashchange"));
    app = mount(App, { target: document.body });

    await vi.waitFor(() => expect(requests).toContain("/runs"));
    await tick();

    expect(requests.sort()).toEqual(["/cluster", "/runs", "/stream"]);
    expect(document.body.querySelectorAll(".content > .view .page")).toHaveLength(1);
  });
});
