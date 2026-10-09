import { calculateWeeklySourceContractRowForServer } from './upload-publication-owner.mjs';
import { buildWeeklySourceCanonicalEconomicSnapshot } from './economic-snapshot.js';

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const HASH = /^[0-9a-f]{64}$/;
const BUCKETS = ['day', 'night', 'sat', 'sun', 'bh'];
function fail(code) { throw Object.assign(new Error(code), { code }); }
function actorId(actor) {
  if (!UUID.test(String(actor?.id ?? ''))) fail('WEEKLY_SOURCE_OFFICE_ACTOR_REQUIRED');
  return actor.id;
}

// Real existing row/Contract calculator, including clocks, breaks, overnight,
// five buckets, independent charge comparison and its exact rounding order.
// Call only with the DB context returned by the service-only Office loader.
export function calculateDestinationRepriceContext(context) {
  if (context?.schema_version !== 'WEEKLY_SOURCE_DESTINATION_RATE_CONTEXT_V1'
      || !Array.isArray(context.components) || !context.components.length) {
    fail('WEEKLY_SOURCE_DESTINATION_REPRICE_CONTEXT_INVALID');
  }
  return context.components.map(item => {
    if (item.component?.component_kind !== 'WORKED_TIME') {
      fail('WEEKLY_SOURCE_DESTINATION_REPRICE_COMPONENT_POLICY_REQUIRED');
    }
    const result = calculateWeeklySourceContractRowForServer(item.server_row, item.destination_contract);
    if (result.valid !== true) fail(result.code ?? 'WEEKLY_SOURCE_DESTINATION_REPRICE_CALCULATION_FAILED');
    const economic = buildWeeklySourceCanonicalEconomicSnapshot({
      resolvedFacts: {
        sourceMode: result.sourceMode,
        rateMethod: result.mode,
        payRates: result.payRates,
        chargeRates: result.chargeRates,
      },
      priceComparison: result.comparison,
    });
    const original = item.original_economic;
    for (const key of ['rate_method', 'sign', 'paid_minutes', 'break_minutes']) {
      if (economic[key] !== original?.[key]) fail('WEEKLY_SOURCE_DESTINATION_REPRICE_WORK_FACTS_CHANGED');
    }
    for (const key of BUCKETS) {
      if (economic.bucket_minutes[key] !== original?.bucket_minutes?.[key]
          || economic.hours[key] !== original?.hours?.[key]) {
        fail('WEEKLY_SOURCE_DESTINATION_REPRICE_BUCKET_POLICY_REVIEW_REQUIRED');
      }
    }
    const price = result.comparison;
    return {
      component_id: item.component.component_id,
      economic_snapshot: economic,
      price_comparison: {
        source_mode: result.sourceMode, result: price.result,
        requires_office_acceptance: price.requiresOfficeAcceptance === true,
        acceptance_kind: price.acceptanceKind ?? null,
        calculated_comparison_charge_pence: price.calculatedComparisonChargePence,
        source_validation_charge_pence: price.sourceValidationChargePence,
        invoice_presentation_charge_pence: price.invoicePresentationChargePence,
        amount_authority: price.amountAuthority,
        canonical_pay_pence: price.canonicalPayPence,
        signed_difference_pence: price.signedDifferencePence ?? null,
      },
    };
  });
}

// Existing Office caller must use its authenticated actor and RPC adapter.
// Browser receives no rate-authority capability. Accept rederives through SQL
// and binds the entire shown vector, not this JavaScript hash or caller money.
export function createWeeklySourceDestinationRepriceOwner({ rpc }) {
  if (typeof rpc !== 'function') fail('WEEKLY_SOURCE_DESTINATION_REPRICE_RPC_REQUIRED');
  return Object.freeze({
    async preview({ actor, baseRequest }) {
      const actorUserId = actorId(actor);
      const loaded = await rpc('weekly_source_destination_reprice_preview_v1', {
        p_actor: actorUserId, p_base_request: baseRequest, p_calculations: null,
      });
      if (loaded?.ok !== true) fail('WEEKLY_SOURCE_DESTINATION_REPRICE_CONTEXT_UNAVAILABLE');
      const calculations = calculateDestinationRepriceContext(loaded.context);
      const qualified = await rpc('weekly_source_destination_reprice_preview_v1', {
        p_actor: actorUserId, p_base_request: baseRequest, p_calculations: calculations,
      });
      if (qualified?.ok !== true || !HASH.test(String(qualified.quote?.review_sha256 ?? ''))) {
        fail('WEEKLY_SOURCE_DESTINATION_REPRICE_QUOTE_UNAVAILABLE');
      }
      return Object.freeze({
        schema_version: 'WEEKLY_SOURCE_DESTINATION_REPRICE_REVIEW_V1',
        base_request: baseRequest,
        calculations,
        review_sha256: qualified.quote.review_sha256,
        source_approval: qualified.quote.review,
        financial_comparison_resolved: false,
      });
    },
    async accept({ actor, reviewed, idempotencyKey, reason }) {
      const actorUserId = actorId(actor);
      if (reviewed?.schema_version !== 'WEEKLY_SOURCE_DESTINATION_REPRICE_REVIEW_V1'
          || !HASH.test(String(reviewed.review_sha256 ?? ''))
          || typeof reason !== 'string' || !reason.trim()
          || typeof idempotencyKey !== 'string' || idempotencyKey.length < 16) {
        fail('WEEKLY_SOURCE_DESTINATION_REPRICE_ACCEPT_INVALID');
      }
      return rpc('weekly_source_destination_reprice_accept_v1', {
        p_actor: actorUserId,
        p_base_request: reviewed.base_request,
        p_calculations: reviewed.calculations,
        p_review_sha256: reviewed.review_sha256,
        p_idempotency_key: idempotencyKey,
        p_reason: reason,
      });
    },
  });
}
