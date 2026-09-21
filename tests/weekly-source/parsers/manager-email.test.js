import assert from 'node:assert/strict';
import test from 'node:test';

import {
  WEEKLY_MANAGER_EMAIL_POLICY,
  renderWeeklyManagerQueryEmail,
} from '../../../broker/src/weekly-source/manager-email.js';

const janeOne = { issueId: 'q-001', workDate: '2026-09-01', sourceStartInstant: '2026-09-01T08:00:00Z', start: '09:00', end: '18:00', breakMinutes: 30, systemStart: '09:00', systemEnd: '17:00', systemBreakMinutes: 30, issueFamily: 'HOURS_DIFFER', candidateRequested: true };
const janeMissing = { issueId: 'q-002', workDate: '2026-09-02', sourceStartInstant: '2026-09-02T07:00:00Z', start: '08:00', end: '16:00', breakMinutes: 30, systemAbsent: true, issueFamily: 'NHSP_ABSENT', candidateRequested: true };
const amara = { issueId: 'q-003', workDate: '2026-09-03', sourceStartInstant: '2026-09-03T19:00:00Z', start: '20:00', end: '08:00', breakMinutes: 60, systemStart: '20:00', systemEnd: '07:30', systemBreakMinutes: 60, issueFamily: 'HOURS_DIFFER', candidateRequested: false };
const lewis = { issueId: 'q-006', workDate: '2026-09-04', sourceStartInstant: '2026-09-04T06:30:00Z', start: '07:30', end: '15:30', breakMinutes: 30, systemStart: '08:00', systemEnd: '16:00', systemBreakMinutes: 30, issueFamily: 'HOURS_DIFFER', candidateRequested: false };

test('policy fixture renders byte-identical deterministic HTML and text', async () => {
  const result = await renderWeeklyManagerQueryEmail({
    reviewUrl: 'https://example.invalid/weekly-review/fixture-initial',
    clients: [
      { clientId: 'client-st-marys', clientName: "St Mary's NHS Trust", candidates: [
        { candidateId: 'can-jane-smith', displayName: 'Jane Smith', shifts: [janeOne, janeMissing] },
        { candidateId: 'can-amara-patel', displayName: 'Amara Patel', shifts: [amara] },
      ] },
      { clientId: 'client-riverside', clientName: 'Riverside Community Trust', candidates: [
        { candidateId: 'can-lewis-king', displayName: 'Lewis King', shifts: [lewis] },
      ] },
    ],
  }, { configuredOrigin: 'https://example.invalid' });
  assert.equal(result.htmlSha256, '9dd7b3734d9eddd743302da2530960ff0adcd6246904184a238c606ffdb433f0');
  assert.equal(result.textSha256, 'f8318b570eeec133a34b65ef0af58ad0127c9cc5bac83c7848413550595a3a7c');
  assert.equal(result.shiftCount, 4);
  assert.equal(result.policyVersion, '1.8.0');
});

test('new-incident fixture remains byte-identical and contains all current rows', async () => {
  const result = await renderWeeklyManagerQueryEmail({
    reviewUrl: 'https://example.invalid/weekly-review/fixture-new-incident',
    clients: [{ clientId: 'client-st-marys', clientName: "St Mary's NHS Trust", candidates: [
      { candidateId: 'can-jane-smith', displayName: 'Jane Smith', shifts: [janeOne, { ...janeMissing, issueId: 'q-005', workDate: '2026-09-04', sourceStartInstant: '2026-09-04T18:30:00Z', start: '19:30', end: '07:30', breakMinutes: 60 }] },
      { candidateId: 'can-amara-patel', displayName: 'Amara Patel', shifts: [amara] },
    ] }],
  }, { configuredOrigin: 'https://example.invalid' });
  assert.equal(result.htmlSha256, 'a66ad942e14bfb8cc521f034f4c0c6167e065e065c7139de6ce53e2c6a625604');
  assert.equal(result.textSha256, '9417e3e769d5ff9811c694876f16d455d55f6856f7e7e9f222bb95aecb919895');
  assert.equal(result.shiftCount, 3);
});

test('renderer refuses money, insecure or cross-origin links and oversize cohorts', async () => {
  const base = { reviewUrl: 'https://example.invalid/weekly-review/one', clients: [{ clientId: 'client-one', clientName: 'Client', candidates: [{ candidateId: 'candidate-one', displayName: 'Candidate', shifts: [janeOne] }] }] };
  await assert.rejects(() => renderWeeklyManagerQueryEmail({ ...base, pay: 100 }, { configuredOrigin: 'https://example.invalid' }), { code: 'MANAGER_EMAIL_FIELD_FORBIDDEN' });
  await assert.rejects(() => renderWeeklyManagerQueryEmail({ ...base, reviewUrl: 'http://example.invalid/weekly-review/one' }, { configuredOrigin: 'https://example.invalid' }), { code: 'MANAGER_EMAIL_URL_INVALID' });
  await assert.rejects(() => renderWeeklyManagerQueryEmail({ ...base, reviewUrl: 'https://other.invalid/weekly-review/one' }, { configuredOrigin: 'https://example.invalid' }), { code: 'MANAGER_EMAIL_URL_INVALID' });
  const shifts = Array.from({ length: 501 }, (_, index) => ({ ...janeOne, issueId: `q-${index}` }));
  await assert.rejects(() => renderWeeklyManagerQueryEmail({ ...base, clients: [{ ...base.clients[0], candidates: [{ ...base.clients[0].candidates[0], shifts }] }] }, { configuredOrigin: 'https://example.invalid' }), { code: 'MANAGER_EMAIL_SHIFT_CAPACITY' });
});

test('NHSP wording is scoped and candidate-request text appears only after a positive candidate assertion', async () => {
  const result = await renderWeeklyManagerQueryEmail({
    reviewUrl: 'https://example.invalid/weekly-review/one',
    clients: [{ clientId: 'client-one', clientName: 'Client', candidates: [{ candidateId: 'candidate-one', displayName: 'Candidate', shifts: [
      { ...janeMissing, candidateRequested: false },
      { ...janeMissing, issueId: 'q-source', issueFamily: 'SOURCE_ABSENT', candidateRequested: false },
    ] }] }],
  }, { configuredOrigin: 'https://example.invalid' });
  assert.match(result.html, /Missing or not yet authorised/);
  assert.match(result.html, /Not shown in system/);
  assert.doesNotMatch(result.html, /Candidate requested your review/);
  assert.equal(WEEKLY_MANAGER_EMAIL_POLICY.rendererVersion, '1.4.0');
});
