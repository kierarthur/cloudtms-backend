/// <reference path="../worker-configuration.d.ts" />

const REST_PREFIX = "/rest/v1";
const VERSION = "mytms-v1";
const ALLOWED_SCHEMAS = new Set(["identity", "control", "google_control"]);

function json(payload: Record<string, unknown>, status = 200): Response {
  return Response.json(payload, {
    status,
    headers: {
      "cache-control": "no-store",
      "x-content-type-options": "nosniff",
    },
  });
}

function upstreamUrl(request: Request, originValue: string): URL {
  const inbound = new URL(request.url);
  if (inbound.pathname !== REST_PREFIX && !inbound.pathname.startsWith(`${REST_PREFIX}/`)) {
    throw new Error("Unsupported path");
  }

  const origin = new URL(originValue);
  if (origin.protocol !== "https:") throw new Error("HTTPS upstream required");
  const suffix = inbound.pathname.slice(REST_PREFIX.length) || "/";
  origin.pathname = `${origin.pathname.replace(/\/$/, "")}${suffix.startsWith("/") ? suffix : `/${suffix}`}`;
  origin.search = inbound.search;
  origin.hash = "";
  return origin;
}

function upstreamHeaders(request: Request): Headers {
  const headers = new Headers(request.headers);
  for (const name of [
    "host",
    "cf-connecting-ip",
    "cf-ipcountry",
    "cf-ray",
    "cf-visitor",
    "x-forwarded-for",
    "x-forwarded-host",
    "x-forwarded-proto",
  ]) {
    headers.delete(name);
  }
  headers.set("user-agent", "CloudTMS-Miget-Gateway/1.0");
  return headers;
}

async function proxy(request: Request, env: Env): Promise<Response> {
  const inbound = new URL(request.url);
  if (inbound.pathname.startsWith(`${REST_PREFIX}/rpc/`)) {
    const contentProfile = request.headers.get("content-profile");
    const acceptProfile = request.headers.get("accept-profile");
    if (
      !contentProfile
      || !acceptProfile
      || contentProfile !== acceptProfile
      || !ALLOWED_SCHEMAS.has(contentProfile)
    ) {
      return json({ code: "MYTMS_SCHEMA_NOT_ALLOWED" }, 403);
    }
  }

  const upstream = upstreamUrl(request, env.MIGET_POSTGREST_ORIGIN);
  const init: RequestInit = {
    method: request.method,
    headers: upstreamHeaders(request),
    redirect: "manual",
  };
  if (request.method !== "GET" && request.method !== "HEAD") init.body = request.body;

  const response = await fetch(upstream, init);
  const headers = new Headers(response.headers);
  headers.set("x-cloudtms-miget-gateway", VERSION);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    if (url.pathname === "/health") {
      let configured = false;
      try {
        configured = new URL(env.MIGET_POSTGREST_ORIGIN).protocol === "https:";
      } catch {
        configured = false;
      }
      return json({
        ok: configured,
        service: "cloudtms-mytms-miget-gateway",
        version: VERSION,
        upstream_configured: configured,
      }, configured ? 200 : 503);
    }

    if (url.pathname !== REST_PREFIX && !url.pathname.startsWith(`${REST_PREFIX}/`)) {
      return json({ error: "Not found" }, 404);
    }

    try {
      return await proxy(request, env);
    } catch (error) {
      console.error(JSON.stringify({
        message: "MyTMS PostgREST gateway failed",
        error: error instanceof Error ? error.message : "Unknown error",
      }));
      return json({ code: "MYTMS_GATEWAY_UNAVAILABLE" }, 502);
    }
  },
} satisfies ExportedHandler<Env>;
