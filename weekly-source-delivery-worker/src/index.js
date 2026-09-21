import { signWeeklySourceDeliveryRequest } from '../../broker/src/weekly-source/delivery-auth.mjs';

function text(value) {
  return String(value == null ? '' : value).trim();
}

async function invokeRuntime(env, message) {
  if (!env.CLOUDTMS_WEEKLY_SOURCE_RUNTIME?.fetch) {
    throw new Error('WEEKLY_SOURCE_RUNTIME_BINDING_UNAVAILABLE');
  }
  const body = message && typeof message === 'object' && !Array.isArray(message)
    ? message : {};
  const unsigned = new Request(
    'https://cloudtms-weekly-source-runtime.internal/internal/weekly-source-delivery/v1/run',
    {
      method: 'POST',
      headers: { 'content-type': 'application/json; charset=utf-8' },
      body: JSON.stringify({
        operation: 'RUN',
        limit: Number.isSafeInteger(Number(body.limit)) ? Number(body.limit) : 50,
        worker_id: text(body.worker_id) || `queue:${crypto.randomUUID()}`,
        scheduled_started_at_utc: text(body.scheduled_at_utc) || new Date().toISOString(),
      }),
    },
  );
  const response = await env.CLOUDTMS_WEEKLY_SOURCE_RUNTIME.fetch(
    await signWeeklySourceDeliveryRequest(unsigned, env),
  );
  if (!response.ok) {
    const safeStatus = Number(response.status || 0);
    throw new Error(safeStatus >= 500
      ? 'WEEKLY_SOURCE_RUNTIME_TEMPORARY' : 'WEEKLY_SOURCE_RUNTIME_REJECTED');
  }
  return response;
}

export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(env.WEEKLY_SOURCE_DELIVERY_QUEUE.send({
      operation: 'RUN',
      limit: 50,
      scheduled_at_utc: new Date(event.scheduledTime || Date.now()).toISOString(),
    }, { contentType: 'json' }));
  },

  async queue(batch, env) {
    for (const message of batch.messages) {
      try {
        await invokeRuntime(env, message.body);
        message.ack();
      } catch {
        message.retry({ delaySeconds: 30 });
      }
    }
  },

  async fetch() {
    return new Response(JSON.stringify({ ok: false, error_code: 'NOT_FOUND' }), {
      status: 404,
      headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
    });
  },
};

export const weeklySourceDeliveryWorkerInternals = Object.freeze({ invokeRuntime });
