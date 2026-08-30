import assert from 'node:assert/strict';
import fs from 'node:fs';
import test from 'node:test';
import jpeg from 'jpeg-js';
import QRCode from 'qrcode';
import { decodeCandidatePaperQrTextsFromJpeg } from '../broker/src/candidate-paper-page-image.js';
import {
  buildTsq2PagePayload,
  buildTsq2String,
  verifyTsq2String
} from '../broker/src/timesheet-qr-payload.js';

const read = path => fs.readFileSync(new URL(`../${path}`, import.meta.url), 'utf8');

function qrPixels(text, scale = 10, margin = 4) {
  const qr = QRCode.create(text, { errorCorrectionLevel: 'M' });
  const size = (qr.modules.size + margin * 2) * scale;
  const data = Buffer.alloc(size * size * 4, 255);
  for (let row = 0; row < qr.modules.size; row += 1) {
    for (let column = 0; column < qr.modules.size; column += 1) {
      if (!qr.modules.get(row, column)) continue;
      const top = (row + margin) * scale;
      const left = (column + margin) * scale;
      for (let y = top; y < top + scale; y += 1) {
        for (let x = left; x < left + scale; x += 1) {
          const offset = (y * size + x) * 4;
          data[offset] = 0;
          data[offset + 1] = 0;
          data[offset + 2] = 0;
        }
      }
    }
  }
  return { data, width: size, height: size };
}

function jpegWithCodes(codes) {
  const rendered = codes.map(value => qrPixels(value));
  const gap = 40;
  const width = rendered.reduce((sum, item) => sum + item.width, 0) + gap * (rendered.length + 1);
  const height = Math.max(...rendered.map(item => item.height)) + gap * 2;
  const data = Buffer.alloc(width * height * 4, 255);
  let xOffset = gap;
  for (const item of rendered) {
    for (let y = 0; y < item.height; y += 1) {
      item.data.copy(data, ((y + gap) * width + xOffset) * 4, y * item.width * 4, (y + 1) * item.width * 4);
    }
    xOffset += item.width + gap;
  }
  return jpeg.encode({ data, width, height }, 92).data;
}

function base64Url(bytes) {
  return Buffer.from(bytes).toString('base64url');
}

async function legacyTsq2String(payload, env) {
  const encoded = base64Url(new TextEncoder().encode(JSON.stringify(payload)));
  const key = await crypto.subtle.importKey(
    'raw', new TextEncoder().encode(env.QR_SIGNING_SECRET),
    { name: 'HMAC', hash: 'SHA-256' }, false, ['sign']
  );
  const signature = await crypto.subtle.sign(
    'HMAC', key, new TextEncoder().encode(`TSQ2.${encoded}`)
  );
  return `TSQ2.${encoded}.${base64Url(new Uint8Array(signature))}`;
}

test('server decodes the exact signed TSQ2 identity from actual JPEG bytes', async () => {
  const env = { QR_SIGNING_SECRET: 'test-only-qr-signing-secret' };
  const payload = await buildTsq2PagePayload({
    workflow_id: '00000000-0000-4000-8000-000000000001',
    timesheet_id: '00000000-0000-4000-8000-000000000002',
    workflow_generation: 3,
    paper_return_manifest_sha256: 'a'.repeat(64),
    ordinal: 2,
    page_key: 'EXPENSE_SUMMARY:OTHER:1',
    page_kind: 'S',
    category_code: 'O',
    category_occurrence: 1
  });
  const text = await buildTsq2String(payload, env);
  const result = decodeCandidatePaperQrTextsFromJpeg(jpegWithCodes([text]));
  assert.deepEqual(result.qr_texts, [text]);
  assert.deepEqual(await verifyTsq2String(result.qr_texts[0], env), payload);
  assert.ok(result.width > 0 && result.height > 0);
});

test('new TSQ2 pages retain the full signed identity with materially simpler camera codes', async () => {
  const env = { QR_SIGNING_SECRET: 'test-only-qr-signing-secret' };
  const payload = await buildTsq2PagePayload({
    workflow_id: '00000000-0000-4000-8000-000000000001',
    timesheet_id: '00000000-0000-5000-8000-000000000002',
    workflow_generation: 9,
    paper_return_manifest_sha256: 'f'.repeat(64),
    ordinal: 3,
    page_key: 'EXPENSE_EVIDENCE:ACCOMMODATION:1',
    page_kind: 'E',
    category_code: 'A',
    category_occurrence: 1
  });
  const compact = await buildTsq2String(payload, env);
  const legacy = await legacyTsq2String(payload, env);

  assert.deepEqual(await verifyTsq2String(compact, env), payload);
  assert.deepEqual(await verifyTsq2String(legacy, env), payload);
  assert.ok(compact.length < legacy.length * 0.7, `${compact.length} should be much shorter than ${legacy.length}`);
  assert.ok(
    QRCode.create(compact, { errorCorrectionLevel: 'L' }).modules.size
      < QRCode.create(legacy, { errorCorrectionLevel: 'L' }).modules.size
  );
});

test('server never treats a photographed page containing two QRs as one valid page', async () => {
  const env = { QR_SIGNING_SECRET: 'test-only-qr-signing-secret' };
  const base = {
    workflow_id: '00000000-0000-4000-8000-000000000001',
    timesheet_id: '00000000-0000-4000-8000-000000000002',
    workflow_generation: 3,
    paper_return_manifest_sha256: 'a'.repeat(64),
    page_kind: 'E',
    category_code: 'A'
  };
  const first = await buildTsq2String(await buildTsq2PagePayload({
    ...base, ordinal: 3, page_key: 'EXPENSE_EVIDENCE:ACCOMMODATION:1', category_occurrence: 1
  }), env);
  const second = await buildTsq2String(await buildTsq2PagePayload({
    ...base, ordinal: 4, page_key: 'EXPENSE_EVIDENCE:ACCOMMODATION:2', category_occurrence: 2
  }), env);
  const result = decodeCandidatePaperQrTextsFromJpeg(jpegWithCodes([first, second]));
  assert.notEqual(result.qr_texts.length, 1);
});

test('whole-pack database adapter accepts only the exact complete manifest in one transaction', () => {
  const sql = read('supabase/repeatable/30082026_1414_candidate_paper_return_pack_complete_v2.sql');
  assert.match(sql, /create or replace function public\.candidate_paper_return_pack_complete_v2/i);
  assert.match(sql, /jsonb_array_length\(p_verified_pages\) not between 1 and 100/i);
  assert.match(sql, /v_page_count<>v_expected_count/i);
  assert.match(sql, /count\(distinct supplied\.page->>'page_key'\)/i);
  assert.match(sql, /count\(distinct supplied\.page->>'component_id'\)/i);
  assert.match(sql, /candidate_paper_return_component_complete_v2\(/i);
  assert.match(sql, /candidate_workflow_transition_atomic_v1\([\s\S]*'PAPER_RETURN'/i);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('private adapter stages verified JPEG pages and submits sealed receipts only as a pack', () => {
  const backend = read('broker/src/candidate-app-backend.js');
  assert.match(backend, /state:\s*'STAGED_FOR_PACK_CONFIRMATION'/);
  assert.match(backend, /paper_return_staged:\s*true/);
  assert.match(backend, /candidatePaperReturnPackReceipts\([\s\S]*candidate_paper_return_pack_complete_v2/);
  assert.match(backend, /await env\.R2\?\.head\(text\(receipt\.storage_key\)\)/);
  assert.match(backend, /const missingPageKeys = expectedPages[\s\S]*candidatePaperPagesError\([\s\S]*CANDIDATE_PAPER_RETURN_PACK_INCOMPLETE/);
  assert.match(backend, /candidatePaperPageError\(409, 'CANDIDATE_PAPER_RETURN_PACK_STALE', pageKey\)/);
  assert.match(backend, /replacement_page_keys/);
});

test('QR mileage has one pack signing footer while standalone mileage retains its own footer', () => {
  const backend = read('broker/src/candidate-app-backend.js');
  const mileage = backend.slice(
    backend.indexOf('async function mileageClaimFormBytes('),
    backend.indexOf('async function paperExpensePageBytes(')
  );
  assert.match(mileage, /if \(paperReturnQrText\) \{[\s\S]*Manager signature[\s\S]*Date[\s\S]*\} else \{[\s\S]*Manager signature[\s\S]*Date/);
  assert.match(mileage, /paperReturnQrText\) \{[\s\S]*height: 68[\s\S]*y: 58[\s\S]*y: 46/);
  assert.match(mileage, /x: 465, y: 682, size: 88/);
  assert.equal((mileage.match(/page\.drawText\('Manager signature'/g) || []).length, 2);
  assert.doesNotMatch(mileage, /immutable CloudTMS paper-return manifest|Workflow \$\{workflow\.id\}/);
});

test('landscape Timesheet pages are re-rendered with a dedicated large QR panel', () => {
  const backend = read('broker/src/candidate-app-backend.js');
  const timesheetPage = backend.slice(
    backend.indexOf('async function candidatePaperTimesheetPageBytes('),
    backend.indexOf('function mileageJourneyRows(')
  );
  assert.match(timesheetPage, /buildOfficialCandidateModel\([\s\S]*paper_return_qr_text:\s*qrText/);
  assert.match(timesheetPage, /renderOfficialTimesheetPdfBytes\(model, assets\)/);
  assert.doesNotMatch(timesheetPage, /drawCandidatePaperPageQr/);
  const renderer = read('broker/src/timesheet-official-pdf.js');
  assert.match(renderer, /paper_return_qr_panel === true/);
  assert.match(renderer, /paperReturnQrPanel \? 48 : 28/);
  assert.match(renderer, /Math\.max\(layout\.detailsHeight, 48\)/);
  assert.match(renderer, /const quiet = 4/);
  assert.doesNotMatch(renderer, /drawBox\(page, qrBox\.x, qrBox\.top, qrBox\.width, qrBox\.height\)/);
});

test('expense QR pages reserve a clear area without shrinking the physical code', () => {
  const backend = read('broker/src/candidate-app-backend.js');
  const expensePage = backend.slice(
    backend.indexOf('async function renderExpensePage('),
    backend.indexOf('function candidateRpcArgs(')
  );
  assert.match(expensePage, /isPaperReturn \? 650 : 760/);
  assert.match(expensePage, /isPaperReturn \? 510 : 570/);
  assert.match(expensePage, /x: page\.getWidth\(\) - 124,[\s\S]*y: 688,[\s\S]*size: 88/);
  assert.match(expensePage, /isPaperReturn \? 625 : 670/);
  assert.match(expensePage, /component_kind\) === 'EXPENSE_SUMMARY'[\s\S]*height: 96[\s\S]*y: 100[\s\S]*Manager name[\s\S]*y: 58[\s\S]*Manager signature/);
  assert.match(expensePage, /else if \(isPaperReturn\) \{[\s\S]*height: 68[\s\S]*y: 58[\s\S]*Manager signature[\s\S]*Date/);
  assert.doesNotMatch(expensePage, /CloudTMS workflow:|Page identity:/);
});
