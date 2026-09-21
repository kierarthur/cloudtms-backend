import {
  signWeeklySourceDeliveryRequest,
  verifyWeeklySourceDeliveryRequest,
} from './delivery-auth.mjs';
import { renderWeeklyManagerQueryEmail } from './manager-email.js';
import { runPendingEntitlementRelease } from './pending-entitlement-release-worker.mjs';
import { runWeeklySourceCompletedPackCopies } from './completed-pack-copy.mjs';

const encoder = new TextEncoder();
const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function text(value) {
  return String(value == null ? '' : value).trim();
}

function upper(value) {
  return text(value).toUpperCase();
}

function canonicalServerUtc(value) {
  const source = text(value);
  if (!source || !source.endsWith('Z')) {
    throw new Error('WEEKLY_SOURCE_SCHEDULED_TIME_INVALID');
  }
  const parsed = new Date(source);
  if (!Number.isFinite(parsed.getTime())) {
    throw new Error('WEEKLY_SOURCE_SCHEDULED_TIME_INVALID');
  }
  return parsed.toISOString();
}

function canonicalJson(value) {
  if (value === null || typeof value !== 'object') return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  return `{${Object.keys(value).sort().map(
    (key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`,
  ).join(',')}}`;
}

function base64Url(bytes) {
  let binary = '';
  for (const byte of bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes)) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/g, '');
}

function hex(bytes) {
  return Array.from(bytes instanceof Uint8Array ? bytes : new Uint8Array(bytes),
    (byte) => byte.toString(16).padStart(2, '0')).join('');
}

async function sha256(value) {
  const bytes = value instanceof Uint8Array ? value : encoder.encode(String(value ?? ''));
  return hex(await crypto.subtle.digest('SHA-256', bytes));
}

async function hmacBytes(secret, namespace, value) {
  if (text(secret).length < 32) throw new Error('WEEKLY_SOURCE_MANAGER_ROUTE_NOT_READY');
  const key = await crypto.subtle.importKey(
    'raw', encoder.encode(secret), { name: 'HMAC', hash: 'SHA-256' }, false, ['sign'],
  );
  return new Uint8Array(await crypto.subtle.sign(
    'HMAC', key, encoder.encode(`${namespace}\u001f${canonicalJson(value)}`),
  ));
}

async function hmacHex(secret, namespace, value) {
  return hex(await hmacBytes(secret, namespace, value));
}

function unwrap(value, functionName) {
  let result = value;
  if (Array.isArray(result) && result.length === 1) [result] = result;
  if (result && typeof result === 'object' && !Array.isArray(result)
      && Object.prototype.hasOwnProperty.call(result, functionName)) {
    result = result[functionName];
  }
  if (Array.isArray(result) && result.length === 1) [result] = result;
  return result;
}

async function rpc(dependencies, functionName, request, timeoutMs = 30_000) {
  const result = await dependencies.rpc(
    functionName, { p_request: request }, { timeoutMs },
  );
  return unwrap(result, functionName);
}

// WP-44 F2.  The only three suppressions the transport owner accepts, and the
// only three the CONTROL PLANE can legitimately answer with.  A suppression is
// a definite answer from the push authority about this Candidate; it finalises
// the command for ever, so it may only ever be recorded when the authority
// actually gave it, together with the authority's own snapshot identity.
const PERMANENT_PUSH_SUPPRESSIONS = Object.freeze([
  'PERSONAL_PREFERENCE', 'NO_ACTIVE_DEVICE', 'PUSH_DELIVERY_UNAVAILABLE',
]);

function safeErrorCode(error, fallback) {
  const candidate = upper(error?.message);
  return /^[A-Z][A-Z0-9_]{2,119}$/.test(candidate) ? candidate : fallback;
}

function safeUuid(value) {
  const candidate = text(value);
  return UUID_RE.test(candidate) ? candidate : null;
}

function json(status, body) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
    },
  });
}

async function boundedJson(response) {
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (bytes.byteLength > 256 * 1024) throw new Error('WEEKLY_SOURCE_DELIVERY_RESPONSE_INVALID');
  try {
    return bytes.byteLength ? JSON.parse(new TextDecoder().decode(bytes)) : null;
  } catch {
    throw new Error('WEEKLY_SOURCE_DELIVERY_RESPONSE_INVALID');
  }
}

function managerEmailModel(rows, reviewUrl) {
  const clients = new Map();
  for (const row of Array.isArray(rows) ? rows : []) {
    const clientId = text(row.clientId);
    const candidateId = text(row.candidateId);
    if (!clientId || !candidateId) throw new Error('WEEKLY_SOURCE_MANAGER_RENDER_INVALID');
    let client = clients.get(clientId);
    if (!client) {
      client = { clientId, clientName: text(row.clientName), candidates: new Map() };
      clients.set(clientId, client);
    }
    let candidate = client.candidates.get(candidateId);
    if (!candidate) {
      candidate = { candidateId, displayName: text(row.displayName), shifts: [] };
      client.candidates.set(candidateId, candidate);
    }
    candidate.shifts.push({
      issueId: text(row.issueId),
      workDate: text(row.workDate),
      sourceStartInstant: text(row.sourceStartInstant),
      start: text(row.start),
      end: text(row.end),
      breakMinutes: Number(row.breakMinutes),
      systemStart: row.systemStart == null ? null : text(row.systemStart),
      systemEnd: row.systemEnd == null ? null : text(row.systemEnd),
      systemBreakMinutes: row.systemBreakMinutes == null ? null : Number(row.systemBreakMinutes),
      systemAbsent: row.systemAbsent === true,
      issueFamily: upper(row.issueFamily),
      candidateRequested: row.candidateRequested === true,
    });
  }
  return {
    reviewUrl,
    clients: [...clients.values()].map((client) => ({
      clientId: client.clientId,
      clientName: client.clientName,
      candidates: [...client.candidates.values()],
    })),
  };
}

async function candidatePushRequest(env, dependencies, body) {
  if (typeof dependencies.candidatePushFetch !== 'function') {
    throw new Error('WEEKLY_PUSH_BINDING_UNAVAILABLE');
  }
  const unsigned = new Request('https://weekly-source-push.internal/internal/weekly-source-push/v1', {
    method: 'POST',
    headers: { 'content-type': 'application/json; charset=utf-8' },
    body: JSON.stringify(body),
  });
  let response;
  try {
    response = await dependencies.candidatePushFetch(
      await signWeeklySourceDeliveryRequest(unsigned, env),
    );
  } catch {
    throw new Error('WEEKLY_PUSH_OUTCOME_UNKNOWN');
  }
  const payload = await boundedJson(response);
  if (!response.ok || payload?.ok === false) {
    throw new Error(text(payload?.error_code) || 'WEEKLY_PUSH_DEPENDENCY_UNAVAILABLE');
  }
  return payload;
}

async function renderCandidateIntent(env, dependencies, due, input) {
  const submitting = upper(input.request_kind) === 'SUBMIT_TIMESHEET';
  const staged = await rpc(dependencies, 'weekly_source_message_render_stage_atomic_v1', {
    message_intent_id: due.message_intent_id,
    projection_publication_id: due.projection_publication_id,
    membership_hash: input.membership_hash,
    policy_version: 'weekly-source-candidate-push-v1',
    renderer_version: '1.0.0',
    structure_version: null,
    subject_text: null,
    html_body: null,
    plain_body: submitting
      ? 'Open MyTMS to submit your Timesheet hours.'
      : 'Open MyTMS to check your Timesheet hours.',
  });
  return staged;
}

async function deterministicManagerToken(env, preparation) {
  const secret = text(env.WEEKLY_SOURCE_MANAGER_TOKEN_SECRET);
  if (secret.length < 32) throw new Error('WEEKLY_SOURCE_MANAGER_ROUTE_NOT_READY');
  return base64Url(await hmacBytes(
    secret,
    'weekly-source-manager-token-v1',
    {
      environment: upper(env.CANDIDATE_APP_ENVIRONMENT || env.WEEKLY_SOURCE_ENVIRONMENT),
      preparation_id: preparation.manager_route_preparation_id,
      credential_generation: Number(preparation.credential_generation),
    },
  ));
}

async function renderManagerIntent(env, dependencies, due, input) {
  if (typeof dependencies.managerControlRpc !== 'function') {
    throw new Error('WEEKLY_SOURCE_MANAGER_ROUTE_NOT_READY');
  }
  const preparation = await rpc(
    dependencies,
    'weekly_source_manager_route_prepare_atomic_v1',
    {
      message_intent_id: due.message_intent_id,
      projection_publication_id: due.projection_publication_id,
    },
  );
  const token = await deterministicManagerToken(env, preparation);
  const routeSecret = text(env.MYTMS_MANAGER_ROUTE_HMAC_SECRET);
  const issuedAt = text(preparation.issued_at_utc);
  const expiresAt = text(preparation.expires_at_utc);
  const registrationFacts = {
    contract_version: 'WEEKLY_QUERY_MANAGER_ROUTE_REGISTRATION_V1',
    environment_label: upper(preparation.environment),
    agency_id: preparation.agency_id,
    authority_kind: 'WEEKLY_QUERY_MANAGER_EMAIL',
    credential_hmac_hex: await hmacHex(
      routeSecret, 'weekly-query-manager-credential-v1', token,
    ),
    credential_key_version: 1,
    review_batch_route_hmac_hex: await hmacHex(
      routeSecret, 'weekly-query-manager-review-batch-v1', preparation.review_batch_id,
    ),
    recipient_generation_route_hmac_hex: await hmacHex(
      routeSecret,
      'weekly-query-manager-recipient-generation-v1',
      preparation.recipient_generation_id,
    ),
    original_membership_hash_hex: preparation.membership_hash,
    credential_generation: Number(preparation.credential_generation),
    issued_at_utc: issuedAt,
    expires_at_utc: expiresAt,
  };
  const semantic = await sha256(canonicalJson(registrationFacts));
  const idempotency = await hmacHex(
    routeSecret,
    'weekly-query-manager-route-idempotency-v1',
    {
      preparation_id: preparation.manager_route_preparation_id,
      review_batch_id: preparation.review_batch_id,
      credential_generation: Number(preparation.credential_generation),
    },
  );
  const registration = await dependencies.managerControlRpc(
    'control',
    'weekly_query_manager_route_register_v1',
    {
      p_registration: {
        ...registrationFacts,
        semantic_sha256_hex: semantic,
        idempotency_key_hmac_hex: idempotency,
      },
      p_now_utc: new Date().toISOString(),
    },
  );
  const resolved = await dependencies.managerControlRpc(
    'control',
    'weekly_query_manager_route_resolve_v1',
    {
      p_resolution: {
        environment_label: upper(preparation.environment),
        credential_hmac_hex: registrationFacts.credential_hmac_hex,
        credential_key_version: 1,
        operation_id: 'getManagerWeeklyQueryBatch',
      },
      p_now_utc: new Date().toISOString(),
    },
  );
  const originResult = await dependencies.managerControlRpc(
    'control',
    'manager_review_origin_resolve_v1',
    {
      p_agency_id: preparation.agency_id,
      p_environment_label: upper(preparation.environment),
    },
  );
  const origin = text(originResult?.manager_review_public_origin).replace(/\/$/, '');
  if (!/^https:\/\/[A-Za-z0-9.-]+(?::\d{1,5})?$/.test(origin)) {
    throw new Error('WEEKLY_SOURCE_MANAGER_ORIGIN_UNAVAILABLE');
  }
  const reviewUrl = `${origin}/manager/weekly-query/${encodeURIComponent(
    preparation.review_batch_id,
  )}#token=${encodeURIComponent(token)}`;
  const rendered = await renderWeeklyManagerQueryEmail(
    managerEmailModel(input.rows, reviewUrl),
    { configuredOrigin: origin },
  );
  return rpc(dependencies, 'weekly_source_message_render_stage_atomic_v1', {
    message_intent_id: due.message_intent_id,
    projection_publication_id: due.projection_publication_id,
    membership_hash: input.membership_hash,
    policy_version: rendered.policyVersion,
    renderer_version: rendered.rendererVersion,
    structure_version: rendered.structureVersion,
    subject_text: rendered.subject,
    html_body: rendered.html,
    plain_body: rendered.text,
    credential_hash: await sha256(token),
    control_plane_ticket_id: registration.manager_route_ticket_id,
    agency_receipt_id: preparation.review_batch_id,
    data_plane_identity: resolved.registry_binding_key,
    route_version: String(resolved.route_version),
    credential_version: '1',
    manager_route_preparation_id: preparation.manager_route_preparation_id,
  });
}

async function renderDueIntents(env, dependencies, limit) {
  const due = await rpc(dependencies, 'weekly_source_message_render_due_list_v1', { limit });
  let rendered = 0;
  let failed = 0;
  for (const item of Array.isArray(due?.intents) ? due.intents : []) {
    try {
      const input = await rpc(dependencies, 'weekly_source_message_render_input_v1', {
        message_intent_id: item.message_intent_id,
        projection_publication_id: item.projection_publication_id,
      });
      if (upper(input?.audience_kind) === 'CANDIDATE') {
        await renderCandidateIntent(env, dependencies, item, input);
      } else if (upper(input?.audience_kind) === 'MANAGER') {
        await renderManagerIntent(env, dependencies, item, input);
      } else {
        throw new Error('WEEKLY_SOURCE_MESSAGE_AUDIENCE_UNSUPPORTED');
      }
      rendered += 1;
    } catch (error) {
      failed += 1;
      console.warn('[weekly-source-delivery] render failed', {
        message_intent_id: UUID_RE.test(text(item?.message_intent_id))
          ? text(item.message_intent_id) : null,
        error_code: /^[A-Z][A-Z0-9_]{2,119}$/.test(text(error?.message))
          ? text(error.message) : 'WEEKLY_SOURCE_RENDER_FAILED',
      });
    }
  }
  return { due: Number(due?.due_count || 0), rendered, failed };
}

async function prepareDispatchTargets(env, dependencies, workerId, limit) {
  const claim = await rpc(dependencies, 'weekly_source_message_dispatch_claim_v1', {
    worker_id: workerId,
    limit,
    lease_seconds: 120,
  });
  let prepared = 0;
  let suppressed = 0;
  let deferred = 0;
  let failed = 0;
  for (const command of Array.isArray(claim?.commands) ? claim.commands : []) {
    try {
      let request;
      if (upper(command.audience_kind) === 'CANDIDATE') {
        // WP-44 F2.  A snapshot that fails is a TRANSIENT failure of this tick,
        // never a permanent suppression of this Candidate's notification, and
        // this runtime never invents a control-plane snapshot identity it was
        // not given.  Only an answer the push authority actually returned, with
        // the authority's own snapshot id, may finalise the command.
        let snapshot = null;
        let snapshotFailure = null;
        try {
          snapshot = await candidatePushRequest(env, dependencies, {
            operation: 'SNAPSHOT',
            request: {
              environment: upper(command.environment),
              agency_id: command.agency_id,
              local_candidate_id: command.candidate_id,
              candidate_generation_id: command.candidate_generation_id,
              dispatch_command_id: command.dispatch_command_id,
              category: 'timesheet_expense_attention',
              semantic_hash: command.rendered_content_hash,
              idempotency_key: command.provider_idempotency_key,
            },
          });
        } catch (error) {
          // Binding absent, control plane down, ECONNRESET, malformed reply:
          // none of these is an answer about this Candidate.
          snapshotFailure = safeErrorCode(error, 'PUSH_SNAPSHOT_UNAVAILABLE');
        }
        if (snapshotFailure === null) {
          const state = upper(snapshot?.state);
          const suppression = upper(snapshot?.suppression_reason);
          const snapshotId = safeUuid(snapshot?.snapshot_id);
          const targets = Array.isArray(snapshot?.targets) ? snapshot.targets : null;
          if (snapshotId === null) {
            snapshotFailure = 'PUSH_SNAPSHOT_IDENTITY_MISSING';
          } else if (state === 'ELIGIBLE') {
            if (targets === null || targets.length < 1) {
              snapshotFailure = 'PUSH_SNAPSHOT_TARGETS_MISSING';
            } else {
              request = {
                dispatch_command_id: command.dispatch_command_id,
                lease_token: command.lease_token,
                worker_id: workerId,
                control_plane_snapshot_id: snapshotId,
                suppression_reason: null,
                targets,
              };
            }
          } else if (state === 'SUPPRESSED'
              && PERMANENT_PUSH_SUPPRESSIONS.includes(suppression)) {
            request = {
              dispatch_command_id: command.dispatch_command_id,
              lease_token: command.lease_token,
              worker_id: workerId,
              control_plane_snapshot_id: snapshotId,
              suppression_reason: suppression,
              targets: [],
            };
          } else {
            // The authority answered with something this runtime does not
            // understand.  Guessing a permanent suppression from it is what
            // silenced the notification; defer instead.
            snapshotFailure = 'PUSH_SNAPSHOT_STATE_UNRECOGNISED';
          }
        }
        if (snapshotFailure !== null) {
          // Recorded, Office-visible and RE-CLAIMABLE.  No target set is
          // registered, so no fabricated snapshot identity is persisted and the
          // command is not finalised.
          await rpc(
            dependencies,
            'weekly_source_message_dispatch_snapshot_failure_atomic_v1',
            {
              dispatch_command_id: command.dispatch_command_id,
              lease_token: command.lease_token,
              worker_id: workerId,
              failure_code: snapshotFailure,
            },
          );
          deferred += 1;
          console.warn('[weekly-source-delivery] push snapshot deferred', {
            dispatch_command_id: UUID_RE.test(text(command?.dispatch_command_id))
              ? text(command.dispatch_command_id) : null,
            error_code: snapshotFailure,
          });
          continue;
        }
      } else if (upper(command.audience_kind) === 'MANAGER') {
        request = {
          dispatch_command_id: command.dispatch_command_id,
          lease_token: command.lease_token,
          worker_id: workerId,
          control_plane_snapshot_id: null,
          suppression_reason: null,
          targets: [{
            external_target_id: command.manager_recipient_route_id,
            target_fingerprint: command.manager_recipient_fingerprint,
            target_snapshot_hash: command.rendered_content_hash,
            target_version: 1,
            provider: 'POWER_AUTOMATE',
            safe_target_snapshot: {
              recipient_route_id: command.manager_recipient_route_id,
            },
          }],
        };
      } else {
        throw new Error('WEEKLY_SOURCE_TARGET_AUDIENCE_INVALID');
      }
      const registered = await rpc(
        dependencies, 'weekly_source_message_targets_register_atomic_v1', request,
      );
      if (registered?.suppressed === true) suppressed += 1;
      else prepared += 1;
    } catch (error) {
      failed += 1;
      console.warn('[weekly-source-delivery] target preparation failed', {
        dispatch_command_id: UUID_RE.test(text(command?.dispatch_command_id))
          ? text(command.dispatch_command_id) : null,
        error_code: /^[A-Z][A-Z0-9_]{2,119}$/.test(text(error?.message))
          ? text(error.message) : 'WEEKLY_SOURCE_TARGET_PREPARATION_FAILED',
      });
    }
  }
  return { claimed: Number(claim?.claimed_count || 0), prepared, suppressed, deferred, failed };
}

async function deliverManagerTarget(dependencies, target) {
  let result;
  try {
    result = await dependencies.sendManagerEmail({
      to: target.manager_recipient,
      subject: target.subject_text,
      htmlBody: target.html_body,
      textBody: target.plain_body,
      attachmentsV2: [],
      meta: {
        kind: 'WEEKLY_SOURCE_MANAGER_QUERY',
        dispatch_command_id: target.dispatch_command_id,
      },
    });
  } catch {
    return {
      outcome: 'AMBIGUOUS',
      bounded_provider_receipt: {},
      bounded_error: { error_code: 'PROVIDER_OUTCOME_UNKNOWN' },
    };
  }
  if (result?.ok === true) {
    return {
      outcome: 'ACCEPTED',
      provider_message_id: text(result.provider_message_id) || undefined,
      bounded_provider_receipt: {
        provider_status: Number(result.status || 202),
        ...(text(result.provider_message_id)
          ? { provider_request_id: text(result.provider_message_id) } : {}),
      },
      bounded_error: {},
    };
  }
  const status = Number(result?.status || 0);
  if (status === 429 || status >= 500) {
    return {
      outcome: 'TRANSIENT_FAILURE',
      bounded_provider_receipt: status ? { provider_status: status } : {},
      bounded_error: {
        error_code: 'PROVIDER_TEMPORARY',
        ...(status ? { provider_status: status } : {}),
      },
    };
  }
  return {
    outcome: 'DEFINITELY_REJECTED',
    bounded_provider_receipt: status ? { provider_status: status } : {},
    bounded_error: {
      error_code: status ? 'PROVIDER_REJECTED' : 'PROVIDER_NOT_READY',
      ...(status ? { provider_status: status } : {}),
    },
  };
}

async function deliverCandidateTarget(env, dependencies, target, providerAttemptId) {
  try {
    return await candidatePushRequest(env, dependencies, {
      operation: 'DELIVER',
      provider_attempt_id: providerAttemptId,
      target: {
        dispatch_target_id: target.dispatch_target_id,
        dispatch_command_id: target.dispatch_command_id,
        provider: target.provider,
        tranche_kind: target.tranche_kind,
        deep_link: target.deep_link,
        safe_target_snapshot: target.safe_target_snapshot,
        target_snapshot_hash: target.target_snapshot_hash,
        provider_idempotency_key: target.provider_idempotency_key,
      },
    });
  } catch (error) {
    return {
      outcome: text(error?.message) === 'WEEKLY_PUSH_OUTCOME_UNKNOWN'
        ? 'AMBIGUOUS' : 'TRANSIENT_FAILURE',
      bounded_provider_receipt: {},
      bounded_error: {
        error_code: text(error?.message) === 'WEEKLY_PUSH_OUTCOME_UNKNOWN'
          ? 'PROVIDER_OUTCOME_UNKNOWN' : 'PUSH_DEPENDENCY_TEMPORARY',
      },
    };
  }
}

async function deliverTargets(env, dependencies, workerId, limit) {
  const claim = await rpc(dependencies, 'weekly_source_message_dispatch_target_claim_v1', {
    worker_id: workerId,
    limit,
    lease_seconds: 120,
  });
  let accepted = 0;
  let transient = 0;
  let rejected = 0;
  let ambiguous = 0;
  let retired = 0;
  let unstarted = 0;
  let unrecorded = 0;
  for (const target of Array.isArray(claim?.targets) ? claim.targets : []) {
    const safeTargetId = safeUuid(target?.dispatch_target_id);
    let start;
    try {
      start = await rpc(dependencies, 'weekly_source_message_dispatch_target_start_atomic_v1', {
        dispatch_target_id: target.dispatch_target_id,
        lease_token: target.lease_token,
        worker_id: workerId,
      });
    } catch (error) {
      // WP-44 F1.  A start that RAISES began no submission.  Nothing is sent,
      // nothing is recorded at the provider, and the remaining targets of this
      // page still run.  The refusal is counted and logged so it is not silent.
      unstarted += 1;
      console.warn('[weekly-source-delivery] target start refused', {
        dispatch_target_id: safeTargetId,
        error_code: safeErrorCode(error, 'WEEKLY_SOURCE_TARGET_START_FAILED'),
      });
      continue;
    }
    // WP-44 F1.  The SUBMISSION_STARTED fence RETURNS `{ok:false, reason}` on
    // each of its retirement branches (04A section 9, 02 section 7.15): it has
    // retired the target, revoked the review batch and receipt and written no
    // attempt row.  A returned refusal is therefore exactly as binding as a
    // raised one - the message it was holding is stale and its link is revoked -
    // so nothing may be sent and the run continues to its healthy siblings.
    // The outcome itself is already durable: the fence recorded the retirement
    // and re-aggregated the command inside its own transaction.
    if (start?.ok !== true || safeUuid(start?.provider_attempt_id) === null) {
      retired += 1;
      console.warn('[weekly-source-delivery] target start did not begin a submission', {
        dispatch_target_id: safeTargetId,
        reason: /^[A-Z][A-Z0-9_]{2,119}$/.test(upper(start?.reason))
          ? upper(start.reason) : 'WEEKLY_SOURCE_TARGET_NOT_STARTED',
      });
      continue;
    }
    const outcome = upper(target.channel) === 'PUSH'
      ? await deliverCandidateTarget(env, dependencies, target, start.provider_attempt_id)
      : await deliverManagerTarget(dependencies, target);
    try {
      await rpc(dependencies, 'weekly_source_message_dispatch_target_result_atomic_v1', {
        provider_attempt_id: start.provider_attempt_id,
        outcome: outcome.outcome,
        provider_message_id: outcome.provider_message_id ?? null,
        bounded_provider_receipt: outcome.bounded_provider_receipt || {},
        bounded_error: outcome.bounded_error || {},
      });
    } catch (error) {
      // WP-44 F1.  A provider result that cannot be recorded must not starve the
      // remaining targets either.  The attempt stays open and the claim owner
      // turns it into AMBIGUOUS with a delivery-failure row when its lease
      // expires, which is the durable record; this line makes the gap visible
      // in the meantime instead of aborting the tick.
      unrecorded += 1;
      console.warn('[weekly-source-delivery] target result not recorded', {
        dispatch_target_id: safeTargetId,
        error_code: safeErrorCode(error, 'WEEKLY_SOURCE_TARGET_RESULT_FAILED'),
      });
      continue;
    }
    if (outcome.outcome === 'ACCEPTED') accepted += 1;
    else if (outcome.outcome === 'TRANSIENT_FAILURE') transient += 1;
    else if (outcome.outcome === 'AMBIGUOUS') ambiguous += 1;
    else rejected += 1;
  }
  return {
    claimed: Number(claim?.claimed_count || 0),
    accepted, transient, rejected, ambiguous, retired, unstarted, unrecorded,
  };
}

export async function runWeeklySourceDelivery(env, dependencies = {}, options = {}) {
  const limit = Math.max(1, Math.min(100, Number(options.limit || 50)));
  const workerId = text(options.workerId) || `weekly-source:${crypto.randomUUID()}`;
  const scheduledStartedAtUtc = canonicalServerUtc(
    options.scheduledStartedAtUtc
      ?? (typeof dependencies.nowUtc === 'function' ? dependencies.nowUtc() : new Date().toISOString()),
  );
  const scheduler = await rpc(dependencies, 'weekly_source_query_scheduler_tick_v1', {
    now_utc: scheduledStartedAtUtc,
    limit: Math.min(1000, limit * 4),
  });
  const renders = await renderDueIntents(env, dependencies, limit);
  const preparation = await prepareDispatchTargets(env, dependencies, workerId, limit);
  const delivery = await deliverTargets(env, dependencies, workerId, limit * 2);
  // The completed-Timesheet informational copy has its own immutable document
  // identity and durable mail outbox.  It is isolated here so rendering or mail
  // preparation can never interrupt Candidate/manager query delivery or any
  // later entitlement-release work.
  let completedPackCopy;
  try {
    completedPackCopy = await runWeeklySourceCompletedPackCopies(
      env, dependencies, { limit }
    );
  } catch (error) {
    completedPackCopy = {
      ok: false,
      error_code: safeErrorCode(error, 'WEEKLY_COMPLETED_PACK_COPY_UNAVAILABLE'),
      status_updates: 0,
      due: 0,
      committed: 0,
      replayed: 0,
      failed: 0,
      failures: []
    };
  }
  // proof/32 section 2 "Invocation point": after the query scheduler and after
  // every candidate/manager delivery step, inside its OWN try/catch, so the
  // release step can never prevent or undo the delivery work that already ran.
  let pendingEntitlementRelease;
  try {
    pendingEntitlementRelease = await runPendingEntitlementRelease(dependencies, { workerId });
  } catch (error) {
    const candidate = upper(error?.message);
    pendingEntitlementRelease = {
      ok: false,
      error_code: /^[A-Z][A-Z0-9_]{2,119}$/.test(candidate)
        ? candidate : 'WEEKLY_SOURCE_PENDING_RELEASE_UNAVAILABLE',
      claimed: 0,
      bundles: [],
    };
  }
  return {
    ok: true,
    scheduler,
    renders,
    preparation,
    delivery,
    completed_pack_copy: completedPackCopy,
    pending_entitlement_release: pendingEntitlementRelease,
  };
}

export async function handleWeeklySourceDeliveryRuntime(request, env, dependencies = {}) {
  if (new URL(request.url).pathname !== '/internal/weekly-source-delivery/v1/run') return null;
  try {
    if (request.method !== 'POST'
        || !(await verifyWeeklySourceDeliveryRequest(request, env))) {
      return json(401, { ok: false, error_code: 'WEEKLY_SOURCE_DELIVERY_AUTH_REQUIRED' });
    }
    const body = await boundedJson(request.clone());
    if (!body || typeof body !== 'object' || Array.isArray(body)
        || Object.keys(body).some((key) => ![
          'operation', 'limit', 'worker_id', 'scheduled_started_at_utc',
        ].includes(key))
        || upper(body.operation) !== 'RUN') {
      return json(400, { ok: false, error_code: 'WEEKLY_SOURCE_DELIVERY_REQUEST_INVALID' });
    }
    return json(200, await runWeeklySourceDelivery(env, dependencies, {
      limit: body.limit,
      workerId: body.worker_id,
      scheduledStartedAtUtc: body.scheduled_started_at_utc,
    }));
  } catch (error) {
    const candidate = upper(error?.message);
    return json(503, {
      ok: false,
      error_code: /^[A-Z][A-Z0-9_]{2,119}$/.test(candidate)
        ? candidate : 'WEEKLY_SOURCE_DELIVERY_UNAVAILABLE',
    });
  }
}

export const weeklySourceDeliveryRuntimeInternals = Object.freeze({
  canonicalServerUtc,
  canonicalJson,
  deliverCandidateTarget,
  deliverManagerTarget,
  // WP-44 F1: exported so a retired start can be driven end to end in a test.
  deliverTargets,
  managerEmailModel,
  prepareDispatchTargets,
  renderDueIntents,
});
