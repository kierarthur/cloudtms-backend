// WP-PP / R-195, R-201, R-216. Permission only; this module decides no money,
// source scope, approval state, Draft freshness or provider action.
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const USER_FIELDS = 'id,is_active,role,payment_authoriser,payment_golden_key';

export function isActivePaymentActor(row) {
  return row?.is_active === true && (
    String(row.role ?? '').trim().toLowerCase() === 'admin'
    || row.payment_authoriser === true
    || row.payment_golden_key === true
  );
}

function denied(status, code, onDenied) {
  const refusal = Object.freeze({ status, code });
  const response = typeof onDenied === 'function' ? onDenied(refusal)
    : new Response(JSON.stringify({ error: code }), {
      status,
      headers: { 'content-type': 'application/json; charset=utf-8', 'cache-control': 'no-store' },
    });
  return { ok: false, response };
}

/**
 * Re-read the current actor through the existing service transport. readUser is
 * an optional server-owned transport adapter, never a browser-provided row.
 * onDenied preserves a caller's established HTTP envelope (including Banking
 * cancellation). No sensitive response body is included in a dependency error.
 */
export async function requireActivePaymentActor(env, actorUserId, { onDenied, readUser } = {}) {
  if (typeof actorUserId !== 'string' || !UUID.test(actorUserId)) {
    return denied(401, 'Unauthorized', onDenied);
  }
  let row;
  try {
    if (typeof readUser === 'function') {
      row = await readUser(actorUserId, USER_FIELDS);
    } else {
      const base = String(env?.SUPABASE_URL ?? '').replace(/\/$/, '');
      const key = env?.SUPABASE_SERVICE_ROLE_KEY;
      if (!base || !key) throw new Error('PAYMENT_PERMISSION_UNAVAILABLE');
      const response = await fetch(`${base}/rest/v1/tms_users?id=eq.${encodeURIComponent(actorUserId)}&select=${USER_FIELDS}&limit=1`, {
        headers: { apikey: key, authorization: `Bearer ${key}` },
        signal: AbortSignal.timeout(12_000),
      });
      if (!response.ok) throw new Error('PAYMENT_PERMISSION_UNAVAILABLE');
      const rows = await response.json();
      if (!Array.isArray(rows) || rows.length > 1) throw new Error('PAYMENT_PERMISSION_UNAVAILABLE');
      row = rows[0] ?? null;
    }
  } catch {
    return denied(500, 'Failed to validate payment permission', onDenied);
  }
  if (!row || row.is_active !== true
      || String(row.id ?? '').toLowerCase() !== actorUserId.toLowerCase()) {
    return denied(401, 'Unauthorized', onDenied);
  }
  if (!isActivePaymentActor(row)) return denied(403, 'PAYMENT_PERMISSION_REQUIRED', onDenied);
  return { ok: true, user: row };
}
