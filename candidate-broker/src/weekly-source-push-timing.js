import { signWeeklySourceDeliveryRequest } from '../../broker/src/weekly-source/delivery-auth.mjs';
import { PUSH_TIMING_INTERNAL_PATH, decodePushTimingServiceRequest, decodePushTimingServiceReply } from '../../broker/src/weekly-source/push-timing-internal.mjs';
import { readPushTimingRequest } from '../../broker/src/weekly-source/push-timing-office.mjs';

// This existing configured service is the actual agency Worker. No caller URL,
// browser agency, control-plane QT RPC or cached allow flag can select it.
async function requestTiming(env, body) {
  decodePushTimingServiceRequest(body);
  const binding = env.MYTMS_IDENTITY_DELIVERY;
  if (!binding || typeof binding.fetch !== 'function') throw new Error('PUSH_TIMING_DEPENDENCY_UNAVAILABLE');
  const request = new Request(`https://cloudtms-identity-delivery.internal${PUSH_TIMING_INTERNAL_PATH}`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body), signal: AbortSignal.timeout(10_000)
  });
  const response = await binding.fetch(await signWeeklySourceDeliveryRequest(request, env));
  const value = await readPushTimingRequest(response);
  if (!response.ok) throw new Error('PUSH_TIMING_DEPENDENCY_UNAVAILABLE');
  return decodePushTimingServiceReply(value, body);
}
export function readWeeklyPushTiming(env, request) { return requestTiming(env, { operation: 'READ_TIMING', request }); }
export function consumeWeeklyPushAdmission(env, request) { return requestTiming(env, { operation: 'CONSUME_START', request }); }
