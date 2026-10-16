const REMINDER_RPC = 'weekly_source_candidate_reminder_atomic_v1';
const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/;
const KEYS = ['version', 'candidate_id', 'client_id', 'audience',
  'next_allowed_utc', 'next_allowed_local'];

// Match only this owner's closed refusal. A transport failure cannot establish
// a pause, and arbitrary SQL detail must never become Office product wording.
export function contactReminderRefusal(error) {
  if (error?.fn !== REMINDER_RPC || !Number.isInteger(error.status)
      || error.status < 400 || error.status > 599
      || error.json?.code !== '55000' || error.json?.message !== 'CONTACT_PAUSED'
      || typeof error.json?.details !== 'string' || error.json.details.length > 1000) return null;
  let value;
  try { value = JSON.parse(error.json.details); } catch { return null; }
  if (!value || typeof value !== 'object' || Array.isArray(value)
      || Object.keys(value).length !== KEYS.length || !KEYS.every(key => Object.hasOwn(value, key))
      || value.version !== 'CONTACT_PAUSE_REFUSAL_V1' || value.audience !== 'WORKER'
      || typeof value.candidate_id !== 'string' || !UUID.test(value.candidate_id)
      || typeof value.client_id !== 'string' || !UUID.test(value.client_id)
      || typeof value.next_allowed_utc !== 'string' || !UTC.test(value.next_allowed_utc)
      || typeof value.next_allowed_local !== 'string') return null;
  const instant = new Date(value.next_allowed_utc);
  if (!Number.isFinite(instant.getTime())
      || instant.toISOString() !== `${value.next_allowed_utc.slice(0, 23)}Z`) return null;
  const parts = Object.fromEntries(new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/London', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).formatToParts(instant).map(part => [part.type, part.value]));
  if (value.next_allowed_local !== `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute} Europe/London`) return null;
  return {
    ok: false,
    error_code: 'CONTACT_PAUSED',
    message: `Contact paused until ${parts.day}/${parts.month}/${parts.year} ${parts.hour}:${parts.minute} (UK)`,
  };
}
