// Closed metadata boundary for retained Banking Pay mail. It is NOT an owner
// lookup or a send permission: callers must obtain fresh owner status, and hold
// financial/unidentifiable rows unless that lookup positively returns LEGACY.
// Keep payment_scope_json out of this test: factual Source/Candidate/Invoice
// mails legitimately carry that column and must continue to be delivered.
export const BPAY_NEXT_LEGACY_EMAIL_CLASSIFICATION = Object.freeze({
  financial: 'LEGACY_FINANCIAL',
  factual: 'NON_FINANCIAL',
  invalid: 'INVALID_METADATA'
});

const financialTypes = new Set(['REMITTANCE', 'PAYMENT_CANCELLATION']);
// Exact non-financial members of the saved mail_outbox_type_check, rather than
// accepting an unknown future/garbled type as evidence that a row is factual.
const factualTypes = new Set([
  'INVOICE', 'TSO_FAILURE', 'BROADCAST', 'TIMESHEET_QR',
  'TIMESHEET_REFUSAL', 'TIMESHEET_GENERAL', 'TIMESHEET_QUERY', 'MAILSHOT_EMAIL'
]);
const financialContexts = new Set([
  'PAY_BATCH', 'PAY_BATCHES', 'PAY_BATCH_REMITTANCE',
  'PAY_PAYMENT_CORRECTION_REQUESTS'
]);
const financialReferencePrefixes = Object.freeze([
  // The retained renderer itself uses /^remittance/i, not just remittance:.
  'REMITTANCE', 'PAYOUT_NOTICE:', 'PAY_AUTH_INVITE:',
  'PAY_BATCH_PROCESSING:', 'PAY_BATCH_SCHEDULED:', 'PAY_BATCH_COMPLETED:',
  'PAYMENT_CANCELLATION_NOTICE_V1:'
]);

function ownText(row, name) {
  const field = Object.getOwnPropertyDescriptor(row, name);
  if (!field) return { text: '', invalid: false };
  if (!Object.hasOwn(field, 'value')) return { text: '', invalid: true };
  if (field.value === null || field.value === undefined) return { text: '', invalid: false };
  if (typeof field.value !== 'string') return { text: '', invalid: true };
  // Match the immutable SQL predicate exactly, independent of database locale.
  return { text: field.value.trim().replace(/[a-z]/g, (letter) =>
    String.fromCharCode(letter.charCodeAt(0) - 32)), invalid: false };
}

export function classifyBpayNextLegacyEmail(row) {
  try {
    if (row === null || typeof row !== 'object' || Array.isArray(row)) {
      return BPAY_NEXT_LEGACY_EMAIL_CLASSIFICATION.invalid;
    }
    const prototype = Object.getPrototypeOf(row);
    if (prototype !== Object.prototype && prototype !== null) {
      return BPAY_NEXT_LEGACY_EMAIL_CLASSIFICATION.invalid;
    }
    const type = ownText(row, 'type');
    const context = ownText(row, 'context_kind');
    const reference = ownText(row, 'reference');
    // One genuine financial marker is sufficient even when another field is
    // absent or malformed; do not allow missing context_id to hide a remittance.
    if (financialTypes.has(type.text) || financialContexts.has(context.text)
        || financialReferencePrefixes.some((prefix) => reference.text.startsWith(prefix))) {
      return BPAY_NEXT_LEGACY_EMAIL_CLASSIFICATION.financial;
    }
    if (type.invalid || context.invalid || reference.invalid || !factualTypes.has(type.text)) {
      return BPAY_NEXT_LEGACY_EMAIL_CLASSIFICATION.invalid;
    }
    return BPAY_NEXT_LEGACY_EMAIL_CLASSIFICATION.factual;
  } catch {
    return BPAY_NEXT_LEGACY_EMAIL_CLASSIFICATION.invalid;
  }
}

// True means HOLD under NEXT/DISABLED/unavailable ownership, not financial
// authority. Never turn a held row into FAILED/SENT or acknowledge delivery.
export function isBpayNextLegacyFinancialEmail(row) {
  return classifyBpayNextLegacyEmail(row) !== BPAY_NEXT_LEGACY_EMAIL_CLASSIFICATION.factual;
}
