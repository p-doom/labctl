import { afterEach, describe, expect, it, vi } from "vitest";
import {
  connectStream,
  loadArtifactDetail,
  loadArtifacts,
  loadCompareView,
  loadLineage,
  loadPipelineDetail,
  loadPipelines,
  loadPolicies,
  loadPolicyDetail,
  loadRecipeHistory,
  loadRecipeView,
  loadRunDetail,
  loadRunEvents,
  loadRunLog,
  loadRuns,
  store,
} from "./store.svelte";

const jsonResponse = (body: unknown) => ({
  ok: true,
  status: 200,
  statusText: "OK",
  json: async () => body,
  text: async () => JSON.stringify(body),
});

const requestPath = (input: RequestInfo | URL) => {
  const raw = typeof input === "string" ? input : input.toString();
  return raw.replace(/^\/api/, "");
};

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("store cache and stream bounds", () => {
  it("bounds every detail cache with least-recently-used eviction", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const path = requestPath(input);
        if (path.endsWith("/events")) return jsonResponse({ events: [] });
        return jsonResponse({ marker: path });
      }),
    );

    const caches = [
      {
        name: "run",
        load: (key: string) => loadRunDetail(key, true),
        read: (key: string) => store.runDetail(key),
      },
      {
        name: "log",
        load: (key: string) => loadRunLog(key, true),
        read: (key: string) => store.runLog(key),
      },
      {
        name: "events",
        load: (key: string) => loadRunEvents(key, true),
        read: (key: string) => store.runEvents(key),
      },
      {
        name: "pipeline",
        load: (key: string) => loadPipelineDetail(key, true),
        read: (key: string) => store.pipelineDetail(key),
      },
      {
        name: "artifact",
        load: (key: string) => loadArtifactDetail(key, true),
        read: (key: string) => store.artifactDetail(key),
      },
      {
        name: "lineage",
        load: (key: string) => loadLineage(key, true),
        read: (key: string) => store.lineage(key),
      },
      {
        name: "history",
        load: (key: string) => loadRecipeHistory(key),
        read: (key: string) => store.recipeHistory(key),
      },
      {
        name: "recipe",
        load: (key: string) => loadRecipeView(key, true),
        read: (key: string) => store.recipeView(key),
      },
      {
        name: "compare",
        load: (key: string) => loadCompareView([key], true),
        read: (key: string) => store.compareView([key]),
      },
      {
        name: "policy",
        load: (key: string) => loadPolicyDetail(key, true),
        read: (key: string) => store.policyDetail(key),
      },
    ];

    for (const cache of caches) {
      for (let index = 0; index < 32; index += 1) {
        await cache.load(`${cache.name}-${index}`);
      }
      expect(cache.read(`${cache.name}-0`)).not.toBeNull();
      await cache.load(`${cache.name}-32`);
      expect(cache.read(`${cache.name}-0`)).not.toBeNull();
      expect(cache.read(`${cache.name}-1`)).toBeNull();
      expect(cache.read(`${cache.name}-32`)).not.toBeNull();
    }

    await loadRunDetail("__proto__", true);
    expect(store.runDetail("__proto__")).not.toBeNull();
  });

  it("keeps recipe view and history requests independent", async () => {
    const requests: string[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const path = requestPath(input);
        requests.push(path);
        return jsonResponse({ marker: path });
      }),
    );

    const [view, history] = await Promise.all([
      loadRecipeView("concurrent", true),
      loadRecipeHistory("concurrent"),
    ]);

    expect(requests.sort()).toEqual([
      "/recipes/concurrent",
      "/recipes/concurrent/history?limit=16",
    ]);
    expect(view).toEqual({ marker: "/recipes/concurrent" });
    expect(history).toEqual({ marker: "/recipes/concurrent/history?limit=16" });
  });

  it("serializes each stream endpoint and preserves the newest generation", async () => {
    const requests: string[] = [];
    const active = new Map<string, number>();
    const maximumActive = new Map<string, number>();
    const blocked = new Set<string>();
    const releases = new Map<string, () => void>();
    const listeners = new Map<string, EventListenerOrEventListenerObject>();
    let runStatus = "queued";

    class FakeEventSource {
      onopen: (() => void) | null = null;
      onerror: (() => void) | null = null;

      addEventListener(type: string, listener: EventListenerOrEventListenerObject) {
        listeners.set(type, listener);
      }

      close() {}
    }

    const runSummary = (status: string) => ({
      id: "fresh-run",
      recipe_name: "recipe",
      recipe_hash: "0123456789abcdef",
      run_dir: "/run",
      repo: "repo",
      status,
      created_at: 1,
      finished_at: status === "completed" ? 2 : null,
      duration_secs: status === "completed" ? 1 : null,
      is_terminal: status === "completed" || status === "failed" || status === "succeeded",
      job_id: "1",
      pipeline_id: null,
      stage_name: null,
      submitted_by: null,
      recipe: {},
      context: {},
      dependency_on: null,
      source_path: "/recipe.toml",
    });

    vi.stubGlobal("EventSource", FakeEventSource);
    vi.stubGlobal(
      "fetch",
      vi.fn(async (input: RequestInfo | URL) => {
        const path = requestPath(input);
        const status = runStatus;
        requests.push(path);
        const current = (active.get(path) ?? 0) + 1;
        active.set(path, current);
        maximumActive.set(path, Math.max(maximumActive.get(path) ?? 0, current));
        try {
          if (blocked.delete(path)) {
            await new Promise<void>((resolve) => releases.set(path, resolve));
          }
          if (path === "/runs") return jsonResponse({ runs: [runSummary(status)] });
          if (path === "/runs/fresh-run") {
            return jsonResponse({ run: runSummary(status), inputs: [], outputs: [], eval_series: [], tracking: { wandb: null } });
          }
          if (path === "/runs/fresh-run/log?tail=200") {
            return jsonResponse({ run_id: "fresh-run", lines: [status], path: null, truncated: false });
          }
          if (path === "/runs/fresh-run/events") {
            return jsonResponse({ events: [{ event_type: status, payload: {}, created_at: 1 }] });
          }
          if (path === "/runs/quiet-run") {
            return jsonResponse({ run: { ...runSummary(status), id: "quiet-run" }, inputs: [], outputs: [], eval_series: [], tracking: { wandb: null } });
          }
          if (path === "/pipelines") return jsonResponse({ pipelines: [] });
          if (path === "/artifacts") return jsonResponse({ artifacts: [] });
          if (path === "/policies") return jsonResponse({ policies: [] });
          if (path === "/policies/policy") return jsonResponse({ policy_name: "policy", runs: [] });
          throw new Error(`unexpected request: ${path}`);
        } finally {
          active.set(path, (active.get(path) ?? 1) - 1);
        }
      }),
    );

    const endpointPaths = [
      "/runs",
      "/runs/fresh-run",
      "/runs/fresh-run/log?tail=200",
      "/runs/fresh-run/events",
    ];
    const emitRunUpdate = () => {
      const listener = listeners.get("run.updated");
      expect(listener).toBeTypeOf("function");
      (listener as (event: MessageEvent) => void)(
        new MessageEvent("run.updated", { data: JSON.stringify({ id: "fresh-run" }) }),
      );
    };
    const waitForStatus = async (status: string) => {
      await vi.waitFor(() => {
        expect(store.runs.data?.[0]?.status).toBe(status);
        expect(store.runDetail("fresh-run")?.run.status).toBe(status);
        expect(store.runLog("fresh-run")?.lines).toEqual([status]);
        expect(store.runEvents("fresh-run")?.[0]?.event_type).toBe(status);
      });
    };
    const expectRequestCount = async (count: number) => {
      await vi.waitFor(() => {
        for (const path of endpointPaths) {
          expect(requests.filter((request) => request === path)).toHaveLength(count);
        }
      });
    };
    const blockEndpoints = () => {
      for (const path of endpointPaths) blocked.add(path);
    };
    const releaseEndpoints = () => {
      for (const path of endpointPaths) releases.get(path)?.();
      releases.clear();
    };

    connectStream();
    await Promise.all([
      loadRuns(true),
      loadRunDetail("fresh-run", true),
      loadRunLog("fresh-run", true),
      loadRunEvents("fresh-run", true),
      loadRunDetail("quiet-run", true),
      loadPipelines(true),
      loadArtifacts(true),
      loadPolicies(true),
      loadPolicyDetail("policy", true),
    ]);

    requests.length = 0;
    runStatus = "running";
    for (let index = 0; index < 8; index += 1) emitRunUpdate();
    await waitForStatus("running");
    await expectRequestCount(1);
    expect(requests).not.toContain("/runs/quiet-run");
    expect(requests).not.toContain("/pipelines");
    expect(requests).not.toContain("/policies");

    requests.length = 0;
    blockEndpoints();
    emitRunUpdate();
    await expectRequestCount(1);
    runStatus = "completed";
    for (let index = 0; index < 8; index += 1) emitRunUpdate();
    releaseEndpoints();
    await waitForStatus("completed");
    await expectRequestCount(2);

    requests.length = 0;
    blockEndpoints();
    const shared = Promise.all([
      loadRuns(true),
      loadRunDetail("fresh-run", true),
      loadRunLog("fresh-run", true),
      loadRunEvents("fresh-run", true),
    ]);
    await expectRequestCount(1);
    runStatus = "failed";
    emitRunUpdate();
    releaseEndpoints();
    await shared;
    await waitForStatus("failed");
    await expectRequestCount(2);

    requests.length = 0;
    runStatus = "succeeded";
    emitRunUpdate();
    await waitForStatus("succeeded");
    await expectRequestCount(1);

    for (const path of endpointPaths) expect(maximumActive.get(path)).toBe(1);

    requests.length = 0;
    const artifactListener = listeners.get("artifact.created");
    expect(artifactListener).toBeTypeOf("function");
    for (let index = 0; index < 8; index += 1) {
      (artifactListener as (event: MessageEvent) => void)(new MessageEvent("artifact.created"));
    }
    await vi.waitFor(() => expect(requests).toEqual(["/artifacts"]));
  });
});
