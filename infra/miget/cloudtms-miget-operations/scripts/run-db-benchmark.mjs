import { mkdir, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";

const DEFAULT_BASE_URL = "https://codex-cloudtms-miget-gateway.kier-88a.workers.dev";
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function argumentsMap(argv) {
  const values = new Map();
  for (let index = 0; index < argv.length; index += 1) {
    const item = argv[index];
    if (!item.startsWith("--")) throw new Error(`Unexpected argument: ${item}`);
    const key = item.slice(2);
    const value = argv[index + 1];
    if (!value || value.startsWith("--")) throw new Error(`Missing value for --${key}`);
    values.set(key, value);
    index += 1;
  }
  return values;
}

function boundedInteger(value, fallback, minimum, maximum, label) {
  if (value === undefined) return fallback;
  const parsed = Number.parseInt(value, 10);
  if (!Number.isInteger(parsed) || parsed < minimum || parsed > maximum) {
    throw new Error(`${label} must be an integer from ${minimum} to ${maximum}`);
  }
  return parsed;
}

function percentile(values, proportion) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((left, right) => left - right);
  const position = (sorted.length - 1) * proportion;
  const lower = Math.floor(position);
  const upper = Math.ceil(position);
  const result = lower === upper
    ? sorted[lower]
    : sorted[lower] + (sorted[upper] - sorted[lower]) * (position - lower);
  return Number(result.toFixed(3));
}

function statistics(values) {
  if (values.length === 0) {
    return {
      count: 0,
      min: null,
      mean: null,
      median: null,
      p50: null,
      p75: null,
      p90: null,
      p95: null,
      p99: null,
      max: null,
      standard_deviation: null,
    };
  }
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance = values.reduce((sum, value) => sum + ((value - mean) ** 2), 0) / values.length;
  return {
    count: values.length,
    min: Number(Math.min(...values).toFixed(3)),
    mean: Number(mean.toFixed(3)),
    median: percentile(values, 0.5),
    p50: percentile(values, 0.5),
    p75: percentile(values, 0.75),
    p90: percentile(values, 0.9),
    p95: percentile(values, 0.95),
    p99: percentile(values, 0.99),
    max: Number(Math.max(...values).toFixed(3)),
    standard_deviation: Number(Math.sqrt(variance).toFixed(3)),
  };
}

function csvCell(value) {
  if (value === null || value === undefined) return "";
  const text = String(value);
  return /[",\r\n]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function percentageChange(candidate, baseline) {
  if (candidate === null || baseline === null || baseline === 0) return null;
  return Number((((candidate - baseline) / baseline) * 100).toFixed(1));
}

function rotated(values, offset) {
  const start = offset % values.length;
  return values.slice(start).concat(values.slice(0, start));
}

async function fetchJson(url, init, timeoutMs) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);
  const startedAt = performance.now();
  try {
    const response = await fetch(url, { ...init, signal: controller.signal });
    const text = await response.text();
    let payload = null;
    try {
      payload = text ? JSON.parse(text) : null;
    } catch {
      payload = null;
    }
    return {
      status: response.status,
      ok: response.ok,
      payload,
      outer_elapsed_ms: Number((performance.now() - startedAt).toFixed(3)),
      timed_out: false,
    };
  } catch (error) {
    return {
      status: null,
      ok: false,
      payload: null,
      outer_elapsed_ms: Number((performance.now() - startedAt).toFixed(3)),
      timed_out: error instanceof Error && error.name === "AbortError",
    };
  } finally {
    clearTimeout(timeout);
  }
}

const args = argumentsMap(process.argv.slice(2));
const baseUrl = new URL(args.get("base-url") ?? DEFAULT_BASE_URL);
if (baseUrl.protocol !== "https:") throw new Error("--base-url must use HTTPS");
const samples = boundedInteger(args.get("samples"), 100, 20, 1_000, "--samples");
const warmup = boundedInteger(args.get("warmup"), 10, 0, 100, "--warmup");
const cold = boundedInteger(args.get("cold"), 5, 0, 20, "--cold");
const timeoutMs = boundedInteger(args.get("timeout-ms"), 15_000, 1_000, 60_000, "--timeout-ms");
const sourceSha = args.get("source-sha") ?? "unknown";
const routeToken = process.env.MIGET_MCP_ROUTE_TOKEN ?? "";
const serviceJwt = process.env.MIGET_SERVICE_ROLE_JWT ?? "";
if (!routeToken || !serviceJwt) {
  throw new Error("MIGET_MCP_ROUTE_TOKEN and MIGET_SERVICE_ROLE_JWT must be present in the process environment");
}

const commonHeaders = {
  Accept: "application/json",
  Authorization: `Bearer ${routeToken}`,
  "cache-control": "no-cache",
  "x-cloudtms-benchmark-postgrest-token": serviceJwt,
};

let timesheetId = args.get("timesheet-id") ?? "";
if (timesheetId && !UUID_PATTERN.test(timesheetId)) throw new Error("--timesheet-id must be a UUID");
if (!timesheetId) {
  const discoveryUrl = new URL("/rest/v1/timesheets", baseUrl);
  discoveryUrl.searchParams.set("is_current", "eq.true");
  discoveryUrl.searchParams.set("contract_id", "not.is.null");
  discoveryUrl.searchParams.set("select", "timesheet_id");
  discoveryUrl.searchParams.set("order", "updated_at.desc");
  discoveryUrl.searchParams.set("limit", "1");
  const discovery = await fetchJson(discoveryUrl, {
    method: "GET",
    headers: {
      Accept: "application/json",
      Authorization: `Bearer ${serviceJwt}`,
      apikey: serviceJwt,
      "cache-control": "no-cache",
    },
  }, timeoutMs);
  const discovered = Array.isArray(discovery.payload) ? discovery.payload[0]?.timesheet_id : null;
  if (!discovery.ok || typeof discovered !== "string" || !UUID_PATTERN.test(discovered)) {
    throw new Error("Could not discover a bounded current TEST timesheet for multi_read");
  }
  timesheetId = discovered;
}

const scenarios = [
  {
    id: "select_1",
    test: "select_1",
    arms: [
      { id: "postgrest_current", path: "postgrest", variant: "current" },
      { id: "hyperdrive_sequential", path: "hyperdrive", variant: "sequential" },
    ],
  },
  {
    id: "small_read",
    test: "small_read",
    arms: [
      { id: "postgrest_current", path: "postgrest", variant: "current" },
      { id: "hyperdrive_sequential", path: "hyperdrive", variant: "sequential" },
    ],
  },
  {
    id: "multi_read",
    test: "multi_read",
    arms: [
      { id: "postgrest_current", path: "postgrest", variant: "current" },
      { id: "postgrest_optimized", path: "postgrest", variant: "optimized" },
      { id: "hyperdrive_sequential", path: "hyperdrive", variant: "sequential" },
      { id: "hyperdrive_optimized", path: "hyperdrive", variant: "optimized" },
    ],
  },
];

async function metadataSnapshot() {
  const url = new URL("/__debug/db-benchmark/metadata", baseUrl);
  const result = await fetchJson(url, { method: "GET", headers: commonHeaders }, timeoutMs);
  if (!result.ok) throw new Error(`Metadata snapshot failed with HTTP ${result.status ?? "network error"}`);
  return result.payload;
}

async function measure(phase, iteration, scenario, arm) {
  const url = new URL(`/__debug/db-benchmark/${arm.path}`, baseUrl);
  url.searchParams.set("test", scenario.test);
  if (arm.variant !== "current") url.searchParams.set("variant", arm.variant);
  if (scenario.test === "multi_read") url.searchParams.set("timesheet_id", timesheetId);
  const result = await fetchJson(url, { method: "GET", headers: commonHeaders }, timeoutMs);
  const payload = result.payload && typeof result.payload === "object" ? result.payload : {};
  return {
    phase,
    iteration,
    scenario: scenario.id,
    arm: arm.id,
    status: result.status,
    ok: result.ok,
    timed_out: result.timed_out,
    outer_elapsed_ms: result.outer_elapsed_ms,
    handler_elapsed_ms: typeof payload.handler_elapsed_ms === "number" ? payload.handler_elapsed_ms : null,
    db_elapsed_ms: typeof payload.db_elapsed_ms === "number" ? payload.db_elapsed_ms : null,
    connect_elapsed_ms: typeof payload.connect_elapsed_ms === "number" ? payload.connect_elapsed_ms : null,
    query_elapsed_ms: typeof payload.query_elapsed_ms === "number" ? payload.query_elapsed_ms : null,
    subrequest_elapsed_ms: typeof payload.subrequest_elapsed_ms === "number" ? payload.subrequest_elapsed_ms : null,
    query_count: typeof payload.query_count === "number" ? payload.query_count : null,
    row_count: typeof payload.row_count === "number" ? payload.row_count : null,
    result_bytes: typeof payload.result_bytes === "number" ? payload.result_bytes : null,
    shape_hash: typeof payload.shape_hash === "string" ? payload.shape_hash : null,
    error: result.ok ? null : (typeof payload.error === "string" ? payload.error : "request failed"),
  };
}

const records = [];
async function runPhase(name, count, retain) {
  for (let iteration = 0; iteration < count; iteration += 1) {
    for (let scenarioIndex = 0; scenarioIndex < scenarios.length; scenarioIndex += 1) {
      const scenario = scenarios[(scenarioIndex + iteration) % scenarios.length];
      for (const arm of rotated(scenario.arms, iteration)) {
        const record = await measure(name, iteration + 1, scenario, arm);
        if (retain) records.push(record);
      }
    }
    const interval = name === "sample" ? Math.max(1, Math.floor(count / 10)) : Math.max(1, count);
    if ((iteration + 1) % interval === 0 || iteration + 1 === count) {
      process.stdout.write(`${name}: ${iteration + 1}/${count}\n`);
    }
  }
}

const startedAt = new Date();
const metadataBefore = await metadataSnapshot();
await runPhase("cold-ish", cold, true);
await runPhase("warmup", warmup, false);
await runPhase("sample", samples, true);
const metadataAfter = await metadataSnapshot();
const completedAt = new Date();

const summaries = [];
for (const scenario of scenarios) {
  for (const arm of scenario.arms) {
    const selected = records.filter(
      (record) => record.phase === "sample" && record.scenario === scenario.id && record.arm === arm.id,
    );
    const successful = selected.filter((record) => record.ok);
    const coldSelected = records.filter(
      (record) => record.phase === "cold-ish" && record.scenario === scenario.id && record.arm === arm.id,
    );
    summaries.push({
      scenario: scenario.id,
      arm: arm.id,
      samples_requested: samples,
      errors: selected.filter((record) => !record.ok).length,
      timeouts: selected.filter((record) => record.timed_out).length,
      outer_elapsed_ms: statistics(successful.map((record) => record.outer_elapsed_ms)),
      handler_elapsed_ms: statistics(successful.map((record) => record.handler_elapsed_ms).filter(Number.isFinite)),
      db_elapsed_ms: statistics(successful.map((record) => record.db_elapsed_ms).filter(Number.isFinite)),
      connect_elapsed_ms: statistics(successful.map((record) => record.connect_elapsed_ms).filter(Number.isFinite)),
      query_elapsed_ms: statistics(successful.map((record) => record.query_elapsed_ms).filter(Number.isFinite)),
      cold_outer_elapsed_ms: statistics(coldSelected.filter((record) => record.ok).map((record) => record.outer_elapsed_ms)),
      query_count: [...new Set(successful.map((record) => record.query_count).filter(Number.isFinite))],
      row_count: [...new Set(successful.map((record) => record.row_count).filter(Number.isFinite))],
      result_bytes: statistics(successful.map((record) => record.result_bytes).filter(Number.isFinite)),
      shape_hashes: [...new Set(successful.map((record) => record.shape_hash).filter(Boolean))],
    });
  }
}

const comparisons = summaries
  .filter((summary) => summary.arm !== "postgrest_current")
  .map((summary) => {
    const baseline = summaries.find(
      (candidate) => candidate.scenario === summary.scenario && candidate.arm === "postgrest_current",
    );
    return {
      scenario: summary.scenario,
      arm: summary.arm,
      p50_change_percent: percentageChange(summary.outer_elapsed_ms.p50, baseline?.outer_elapsed_ms.p50 ?? null),
      p95_change_percent: percentageChange(summary.outer_elapsed_ms.p95, baseline?.outer_elapsed_ms.p95 ?? null),
      mean_change_percent: percentageChange(summary.outer_elapsed_ms.mean, baseline?.outer_elapsed_ms.mean ?? null),
      row_count_equivalent: JSON.stringify(summary.row_count) === JSON.stringify(baseline?.row_count ?? []),
      shape_hash_equivalent: summary.shape_hashes.length === 1
        && baseline?.shape_hashes.length === 1
        && summary.shape_hashes[0] === baseline.shape_hashes[0],
    };
  });

const runId = completedAt.toISOString().replaceAll(":", "-").replace(".", "-");
const outputDirectory = resolve(args.get("out") ?? join("benchmark-results", runId));
await mkdir(outputDirectory, { recursive: true });

const manifest = {
  environment: "TEST",
  worker: baseUrl.origin,
  source_sha: sourceSha,
  started_at: startedAt.toISOString(),
  completed_at: completedAt.toISOString(),
  duration_seconds: Number(((completedAt.getTime() - startedAt.getTime()) / 1_000).toFixed(3)),
  samples,
  warmup,
  cold_ish_samples: cold,
  serial_execution: true,
  cache_mode: "disabled",
  placement_mode: "targeted",
  timesheet_selection: "one current TEST timesheet with a contract; identifier intentionally omitted",
};
const report = { manifest, metadata_before: metadataBefore, metadata_after: metadataAfter, summaries, comparisons };
await writeFile(join(outputDirectory, "benchmark.json"), `${JSON.stringify(report, null, 2)}\n`, "utf8");
await writeFile(join(outputDirectory, "samples.json"), `${JSON.stringify(records, null, 2)}\n`, "utf8");

const csvColumns = Object.keys(records[0] ?? {});
const csv = [
  csvColumns.join(","),
  ...records.map((record) => csvColumns.map((column) => csvCell(record[column])).join(",")),
].join("\n");
await writeFile(join(outputDirectory, "samples.csv"), `${csv}\n`, "utf8");

const markdown = [
  "# CloudTMS TEST Hyperdrive benchmark",
  "",
  `- Source SHA: \`${sourceSha}\``,
  `- Completed: ${manifest.completed_at}`,
  `- Warm samples per arm: ${samples}`,
  `- Warm-up samples per arm (discarded): ${warmup}`,
  `- Cold-ish samples per arm: ${cold}`,
  "- Execution: serial, read-only, Hyperdrive query cache disabled",
  "",
  "| Scenario | Arm | Success | Errors | p50 ms | p95 ms | Mean ms | Std dev | DB p50 ms | Connect p50 ms | Queries |",
  "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|",
  ...summaries.map((summary) => [
    summary.scenario,
    summary.arm,
    summary.outer_elapsed_ms.count,
    summary.errors,
    summary.outer_elapsed_ms.p50 ?? "n/a",
    summary.outer_elapsed_ms.p95 ?? "n/a",
    summary.outer_elapsed_ms.mean ?? "n/a",
    summary.outer_elapsed_ms.standard_deviation ?? "n/a",
    summary.db_elapsed_ms.p50 ?? "n/a",
    summary.connect_elapsed_ms.p50 ?? "n/a",
    summary.query_count.join("/"),
  ].join(" | ").replace(/^/, "| ").concat(" |")),
  "",
  "| Scenario | Candidate | p50 vs current | p95 vs current | Mean vs current | Rows equal | Shape equal |",
  "|---|---|---:|---:|---:|---|---|",
  ...comparisons.map((comparison) => [
    comparison.scenario,
    comparison.arm,
    comparison.p50_change_percent === null ? "n/a" : `${comparison.p50_change_percent}%`,
    comparison.p95_change_percent === null ? "n/a" : `${comparison.p95_change_percent}%`,
    comparison.mean_change_percent === null ? "n/a" : `${comparison.mean_change_percent}%`,
    comparison.row_count_equivalent ? "yes" : "no",
    comparison.shape_hash_equivalent ? "yes" : "no",
  ].join(" | ").replace(/^/, "| ").concat(" |")),
  "",
  "Cold-ish measurements are labelled conservatively: the Worker, database buffers, and Hyperdrive pool cannot be proven globally cold without disrupting shared TEST traffic.",
  "",
];
await writeFile(join(outputDirectory, "summary.md"), `${markdown.join("\n")}\n`, "utf8");

process.stdout.write(`completed: ${outputDirectory}\n`);
