export function normalisePostgresTimestampIso(value) {
  let text = String(value ?? '').trim();
  if (!text) return null;

  if (!text.includes('T') && /^\d{4}-\d{2}-\d{2}\s/.test(text)) {
    text = text.replace(/\s+/, 'T');
  }
  if (/[+\-]\d{2}$/.test(text)) text += ':00';
  if (/[+\-]\d{4}$/.test(text)) {
    text = text.replace(/([+\-]\d{2})(\d{2})$/, '$1:$2');
  }

  const instant = new Date(text);
  return Number.isFinite(instant.getTime()) ? instant.toISOString() : null;
}
