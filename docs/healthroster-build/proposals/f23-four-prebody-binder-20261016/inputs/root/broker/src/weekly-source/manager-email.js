export const WEEKLY_MANAGER_EMAIL_POLICY = Object.freeze({
  policyId: 'cloudtms-weekly-manager-query-email',
  policyVersion: '1.8.0',
  rendererVersion: '1.4.0',
  structureVersion: '1.0.0',
  locale: 'en-GB',
  timezone: 'Europe/London',
});

const ISSUE_FAMILIES = new Set(['HOURS_DIFFER', 'NHSP_ABSENT', 'SOURCE_ABSENT', 'HEALTHROSTER_NOT_FINALISED']);
const OPAQUE_ID = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/;
const FORBIDDEN_TEXT = /[\u0000-\u001f\u007f-\u009f\u061c\u200e\u200f\u202a-\u202e\u2066-\u2069]/u;
const WEEKDAYS = Object.freeze(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
const MONTHS = Object.freeze(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']);
const STYLE = 'html,body{margin:0;padding:0;background:#eef2f7;color:#172033;font-family:Segoe UI, Arial, sans-serif}body{padding:34px 18px}.preheader{display:none!important;visibility:hidden;opacity:0;height:0;width:0;overflow:hidden}.layout{width:100%;border-collapse:collapse}.outer{width:100%;max-width:900px;margin:0 auto}.card{background:#fff;border:1px solid #d9e0ea;border-radius:12px;overflow:hidden}.brand{padding:16px 24px;background:#07111f;color:#fff;font-weight:700;font-size:18px}.content{padding:26px 28px 30px}h1{margin:0 0 8px;font-size:25px;line-height:1.25}.intro{margin:0 0 22px;color:#536178;line-height:1.5}.client-block{margin:0 0 28px}.client-block>h2{margin:0 0 16px;padding:12px 14px;background:#eef2ff;border-left:4px solid #4f46e5;border-radius:6px;font-size:17px}.candidate{margin:0 0 22px}.candidate h3{margin:0 0 8px;font-size:16px}.table-scroll{max-width:100%;overflow-x:auto;border-radius:8px}.table-scroll:focus{outline:3px solid #4f46e5;outline-offset:2px}.data-table{width:100%;min-width:680px;border-collapse:separate;border-spacing:0;border:1px solid #d9e0ea;border-radius:8px;overflow:hidden}.data-table caption{position:absolute;width:1px;height:1px;padding:0;margin:-1px;overflow:hidden;clip:rect(0,0,0,0);white-space:nowrap;border:0}.data-table th,.data-table td{text-align:left;padding:11px 12px;border-bottom:1px solid #e5eaf1;vertical-align:top;font-size:13px;line-height:1.35}.data-table th{background:#f6f8fb;font-size:11px;color:#536178;text-transform:uppercase;letter-spacing:.035em}.data-table tr:last-child td{border-bottom:0}.data-table th:nth-child(1),.data-table td:nth-child(1){width:18%}.data-table th:nth-child(2),.data-table td:nth-child(2),.data-table th:nth-child(3),.data-table td:nth-child(3){width:27%}.data-table td:nth-child(4){font-weight:600}.cta-wrap{text-align:center;margin:28px 0 18px}.cta{display:inline-flex;align-items:center;justify-content:center;min-height:44px;background:#4f46e5;color:#fff!important;text-decoration:none;padding:0 24px;border-radius:8px;font-weight:700}.security,.ignore{margin:8px auto 0;max-width:650px;text-align:center;color:#536178;font-size:12px;line-height:1.45}.ignore{margin-top:4px}@media(max-width:680px){body{padding:0}.card{border-radius:0}.content{padding:20px 14px}}';

function fail(code, message) {
  const error = new Error(message);
  error.code = code;
  throw error;
}

function exactKeys(value, allowed, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) fail('MANAGER_EMAIL_INPUT_INVALID', `${label} is invalid.`);
  const extras = Object.keys(value).filter((key) => !allowed.includes(key));
  if (extras.length) fail('MANAGER_EMAIL_FIELD_FORBIDDEN', `${label} contains a forbidden field.`);
}

function displayText(value, label) {
  const text = String(value ?? '');
  if (!text || FORBIDDEN_TEXT.test(text) || Array.from(text).some((char) => {
    const point = char.codePointAt(0);
    return point >= 0xd800 && point <= 0xdfff;
  })) fail('MANAGER_EMAIL_TEXT_INVALID', `${label} is invalid.`);
  return text;
}

function opaqueId(value, label) {
  const text = String(value ?? '');
  if (!OPAQUE_ID.test(text)) fail('MANAGER_EMAIL_ID_INVALID', `${label} is invalid.`);
  return text;
}

function hhmm(value, label) {
  const text = String(value ?? '');
  const match = /^(\d{2}):(\d{2})$/.exec(text);
  if (!match || Number(match[1]) > 23 || Number(match[2]) > 59) fail('MANAGER_EMAIL_TIME_INVALID', `${label} is invalid.`);
  return text;
}

function minutes(value, label) {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 0 || number > 10080) fail('MANAGER_EMAIL_BREAK_INVALID', `${label} is invalid.`);
  return number;
}

function workDate(value) {
  const text = String(value ?? '');
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(text);
  if (!match) fail('MANAGER_EMAIL_DATE_INVALID', 'Work date is invalid.');
  const date = new Date(`${text}T00:00:00.000Z`);
  if (!Number.isFinite(date.getTime()) || date.toISOString().slice(0, 10) !== text) fail('MANAGER_EMAIL_DATE_INVALID', 'Work date is invalid.');
  return { text, date };
}

function sourceInstant(value) {
  const text = String(value ?? '');
  if (!text || !Number.isFinite(new Date(text).getTime())) fail('MANAGER_EMAIL_INSTANT_INVALID', 'Source start time is invalid.');
  return text;
}

function asciiFold(value) {
  return Array.from(value, (char) => {
    const point = char.codePointAt(0);
    return point >= 0x41 && point <= 0x5a ? String.fromCodePoint(point + 0x20) : char;
  }).join('');
}

function compareCodePoints(a, b) {
  const left = Array.from(a, (char) => char.codePointAt(0));
  const right = Array.from(b, (char) => char.codePointAt(0));
  const count = Math.min(left.length, right.length);
  for (let index = 0; index < count; index += 1) {
    if (left[index] !== right[index]) return left[index] < right[index] ? -1 : 1;
  }
  return left.length - right.length;
}

function compareMany(left, right, selectors) {
  for (const selector of selectors) {
    const result = compareCodePoints(selector(left), selector(right));
    if (result) return result;
  }
  return 0;
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"]/g, (char) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[char]);
}

function formatHours(start, end, breakMinutes) {
  return `${start}-${end} (${breakMinutes === 0 ? 'no break' : `${breakMinutes} min break`})`;
}

function normaliseShift(raw) {
  exactKeys(raw, ['issueId', 'workDate', 'sourceStartInstant', 'start', 'end', 'breakMinutes', 'systemStart', 'systemEnd', 'systemBreakMinutes', 'systemAbsent', 'issueFamily', 'candidateRequested'], 'Shift');
  const family = String(raw.issueFamily ?? '').toUpperCase();
  if (!ISSUE_FAMILIES.has(family)) fail('MANAGER_EMAIL_ISSUE_FAMILY_INVALID', 'Issue type is invalid.');
  const date = workDate(raw.workDate);
  const candidateStart = hhmm(raw.start, 'Candidate start');
  const candidateEnd = hhmm(raw.end, 'Candidate finish');
  const candidateBreak = minutes(raw.breakMinutes, 'Candidate break');
  let systemHours;
  if (family === 'NHSP_ABSENT') systemHours = 'Missing or not yet authorised';
  else if (family === 'SOURCE_ABSENT') systemHours = 'Not shown in system';
  else if (family === 'HEALTHROSTER_NOT_FINALISED') systemHours = 'Not finalised · no actual hours';
  else {
    if (raw.systemAbsent === true) fail('MANAGER_EMAIL_SYSTEM_HOURS_INVALID', 'System hours are required for this issue.');
    systemHours = formatHours(hhmm(raw.systemStart, 'System start'), hhmm(raw.systemEnd, 'System finish'), minutes(raw.systemBreakMinutes, 'System break'));
  }
  return {
    issueId: opaqueId(raw.issueId, 'Issue ID'),
    workDate: date.text,
    date: date.date,
    sourceStartInstant: sourceInstant(raw.sourceStartInstant),
    candidateHours: formatHours(candidateStart, candidateEnd, candidateBreak),
    systemHours,
    candidateRequested: raw.candidateRequested === true,
  };
}

function normaliseInput(input, options) {
  exactKeys(input, ['reviewUrl', 'clients'], 'Email request');
  const configuredOrigin = String(options?.configuredOrigin ?? '').trim();
  if (!configuredOrigin) fail('MANAGER_EMAIL_ORIGIN_REQUIRED', 'The manager review origin is unavailable.');
  let configured;
  let review;
  try {
    configured = new URL(configuredOrigin);
    review = new URL(String(input.reviewUrl ?? ''));
  } catch {
    fail('MANAGER_EMAIL_URL_INVALID', 'The manager review link is invalid.');
  }
  if (configured.protocol !== 'https:' || review.protocol !== 'https:' || configured.origin !== review.origin) {
    fail('MANAGER_EMAIL_URL_INVALID', 'The manager review link is outside the configured secure site.');
  }
  if (!Array.isArray(input.clients) || input.clients.length < 1 || input.clients.length > 100) {
    fail('MANAGER_EMAIL_CLIENT_COUNT_INVALID', 'The manager email client count is invalid.');
  }
  let candidateCount = 0;
  let shiftCount = 0;
  const clients = input.clients.map((rawClient) => {
    exactKeys(rawClient, ['clientId', 'clientName', 'candidates'], 'Client');
    if (!Array.isArray(rawClient.candidates) || rawClient.candidates.length < 1) fail('MANAGER_EMAIL_CANDIDATES_REQUIRED', 'A client has no candidates.');
    const client = {
      clientId: opaqueId(rawClient.clientId, 'Client ID'),
      clientName: displayText(rawClient.clientName, 'Client name'),
      candidates: rawClient.candidates.map((rawCandidate) => {
        candidateCount += 1;
        exactKeys(rawCandidate, ['candidateId', 'displayName', 'shifts'], 'Candidate');
        if (!Array.isArray(rawCandidate.shifts) || rawCandidate.shifts.length < 1) fail('MANAGER_EMAIL_SHIFTS_REQUIRED', 'A candidate has no shifts.');
        const candidate = {
          candidateId: opaqueId(rawCandidate.candidateId, 'Candidate ID'),
          displayName: displayText(rawCandidate.displayName, 'Candidate name'),
          shifts: rawCandidate.shifts.map((rawShift) => {
            shiftCount += 1;
            return normaliseShift(rawShift);
          }),
        };
        candidate.shifts.sort((a, b) => compareMany(a, b, [
          (item) => item.workDate,
          (item) => item.sourceStartInstant,
          (item) => item.issueId,
        ]));
        return candidate;
      }),
    };
    client.candidates.sort((a, b) => compareMany(a, b, [
      (item) => asciiFold(item.displayName),
      (item) => item.candidateId,
    ]));
    return client;
  });
  if (candidateCount > 100) fail('MANAGER_EMAIL_CANDIDATE_CAPACITY', 'The manager email contains more than 100 candidates.');
  if (shiftCount > 500) fail('MANAGER_EMAIL_SHIFT_CAPACITY', 'This manager has more than 500 shifts waiting for one email.');
  clients.sort((a, b) => compareMany(a, b, [
    (item) => asciiFold(item.clientName),
    (item) => item.clientId,
  ]));
  return { reviewUrl: review.toString(), clients, candidateCount, shiftCount };
}

function dayDate(shift) {
  return `${WEEKDAYS[shift.date.getUTCDay()]} ${shift.date.getUTCDate()} ${MONTHS[shift.date.getUTCMonth()]} ${shift.date.getUTCFullYear()}`;
}

function renderHtml(model, subject) {
  const clientCount = model.clients.length;
  const shiftWord = model.shiftCount === 1 ? 'shift' : 'shifts';
  const preheader = clientCount === 1
    ? `Review ${model.shiftCount} ${shiftWord} for ${model.clients[0].clientName}.`
    : `Review ${model.shiftCount} ${shiftWord} for ${clientCount} clients.`;
  const blocks = model.clients.map((client) => `<section class="client-block"><h2>${escapeHtml(client.clientName)}</h2>${client.candidates.map((candidate) => {
    const caption = `${candidate.displayName} — ${client.clientName} timesheet queries`;
    const rows = candidate.shifts.map((shift) => `<tr>\n<td>${escapeHtml(dayDate(shift))}</td>\n<td>${escapeHtml(shift.candidateHours)}</td>\n<td>${escapeHtml(shift.systemHours)}</td>\n<td>${shift.candidateRequested ? 'Candidate requested your review' : ''}</td>\n</tr>`).join('');
    return `<section class="candidate"><h3>${escapeHtml(candidate.displayName)}</h3><div class="table-scroll" role="region" aria-label="${escapeHtml(caption)}" tabindex="0"><table class="data-table"><caption>${escapeHtml(caption)}</caption><thead><tr><th scope="col">Day/date</th><th scope="col">Candidate says they worked these hours</th><th scope="col">System hours</th><th scope="col">Candidate response</th></tr></thead><tbody>${rows}</tbody></table></div></section>`;
  }).join('')}</section>`).join('');
  return `<!doctype html>\n<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>${escapeHtml(subject)}</title><style>${STYLE}</style></head>\n<body><span class="preheader">${escapeHtml(preheader)}</span><table role="presentation" class="layout"><tbody><tr><td><div class="outer"><article class="card"><div class="brand">CloudTMS</div><div class="content"><h1>Timesheet queries requiring your review</h1><p class="intro">Please review the shifts below. You can respond to some now and return to the remaining queries later.</p>${blocks}<div class="cta-wrap"><a class="cta" href="${escapeHtml(model.reviewUrl)}">Review all queries</a></div><p class="security">This secure link lets you save some responses and return later. It expires after seven days.</p><p class="ignore">If you were not expecting this email, please contact the Office team.</p></div></article></div></td></tr></tbody></table></body></html>`;
}

function renderText(model, subject) {
  const divider = '----------------------------------------';
  const clients = model.clients.map((client) => `${divider}\n${client.clientName}\n${divider}\n\n${client.candidates.map((candidate) => `${candidate.displayName}\n${candidate.shifts.map((shift) => `${dayDate(shift)} | Candidate says they worked these hours: ${shift.candidateHours} | System hours: ${shift.systemHours}${shift.candidateRequested ? ' | Candidate response: Candidate requested your review' : ''}`).join('\n')}`).join('\n\n')}`).join('\n\n');
  return `${subject}\n\nTimesheet queries requiring your review\nPlease review the shifts below. You can respond to some now and return to the remaining queries later.\n\n${clients}\n\nReview all queries\n${model.reviewUrl}\n\nThis secure link lets you save some responses and return later. It expires after seven days.\nIf you were not expecting this email, please contact the Office team.\n`;
}

async function sha256(value) {
  const bytes = new TextEncoder().encode(value);
  const digest = new Uint8Array(await crypto.subtle.digest('SHA-256', bytes));
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, '0')).join('');
}

export async function renderWeeklyManagerQueryEmail(input, options = {}) {
  const model = normaliseInput(input, options);
  const shiftWord = model.shiftCount === 1 ? 'shift' : 'shifts';
  const subject = `Timesheet queries requiring your review - ${model.shiftCount} ${shiftWord}`;
  const html = renderHtml(model, subject);
  const text = renderText(model, subject);
  return Object.freeze({
    subject,
    html,
    text,
    shiftCount: model.shiftCount,
    clientCount: model.clients.length,
    policyId: WEEKLY_MANAGER_EMAIL_POLICY.policyId,
    policyVersion: WEEKLY_MANAGER_EMAIL_POLICY.policyVersion,
    rendererVersion: WEEKLY_MANAGER_EMAIL_POLICY.rendererVersion,
    structureVersion: WEEKLY_MANAGER_EMAIL_POLICY.structureVersion,
    subjectSha256: await sha256(subject),
    htmlSha256: await sha256(html),
    textSha256: await sha256(text),
  });
}
