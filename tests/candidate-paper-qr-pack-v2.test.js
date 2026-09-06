import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
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
import {
  candidateAppBackendInternals,
  handleCandidateAppRequest
} from '../broker/src/candidate-app-backend.js';

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

test('Candidate mileage upload accepts ordinary JPEG evidence and forbids the retired source-form QR proof', async () => {
  const session = {
    id: '00000000-0000-4000-8000-000000000201',
    session_id: '00000000-0000-4000-8000-000000000201',
    account_id: '00000000-0000-4000-8000-000000000202',
    selected_candidate_id: '00000000-0000-4000-8000-000000000203',
    environment: 'TEST', status: 'ACTIVE', rotation: 1,
    expires_at_utc: '2099-01-01T00:00:00.000Z',
    absolute_expires_at_utc: '2099-01-02T00:00:00.000Z'
  };
  const workflow = {
    id: '00000000-0000-4000-8000-000000000204',
    account_id: session.account_id, candidate_id: session.selected_candidate_id,
    environment: 'TEST', generation: 2, state: 'WORKER_DRAFT',
    week_ending_date: '2026-08-30',
    target_timesheet_id: '00000000-0000-4000-8000-000000000205',
    anchor_timesheet_id: '00000000-0000-4000-8000-000000000205'
  };
  const componentId = '00000000-0000-4000-8000-000000000206';
  const env = {
    CANDIDATE_APP_ENVIRONMENT: 'TEST',
    CANDIDATE_PRIVATE_SESSION_TOKEN_SECRET: 'test-only-session-secret-material',
    CANDIDATE_PRIVATE_UPLOAD_TOKEN_SECRET: 'test-only-upload-secret-material',
    SUPABASE_URL: 'https://test.supabase.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'test-placeholder'
  };
  const jpegBytes = jpeg.encode({ data: Buffer.alloc(64 * 64 * 4, 255), width: 64, height: 64 }, 92).data;
  const sourceSha256 = createHash('sha256').update(jpegBytes).digest('hex');
  let storedOptions;
  env.R2 = {
    async put(_key, _bytes, options) { storedOptions = options; return { etag: 'created' }; },
    async head() { return null; }
  };
  const token = await candidateAppBackendInternals.createAccessToken(env, session);
  const originalFetch = globalThis.fetch;
  globalThis.fetch = async url => {
    const target = new URL(String(url));
    if (target.pathname.endsWith('/candidate_app_sessions')) return Response.json([session]);
    if (target.pathname.endsWith('/candidate_submission_workflows')) return Response.json([workflow]);
    if (target.pathname.endsWith('/candidate_submission_components')) return Response.json([]);
    throw new Error(`Unexpected TEST request GET ${target.pathname}`);
  };
  const deps = {
    async rpc(name) {
      if (name === 'candidate_component_prepare_atomic_v1') {
        return {
          ok: true, component_id: componentId, workflow_generation: 2,
          storage_key: `candidate-app/test/${workflow.id}/2/mileage.jpg`,
          media_type: 'image/jpeg', byte_size: jpegBytes.byteLength,
          component_kind: 'MILEAGE_FORM', document_role: 'MILEAGE_CLAIM_FORM',
          expense_category: 'MILEAGE', paper_return_page_key: null, state: 'PENDING'
        };
      }
      if (name === 'candidate_workflow_transition_atomic_v1') {
        return { ok: true, state: 'IMMUTABLE' };
      }
      throw new Error(`Unexpected TEST RPC ${name}`);
    }
  };
  try {
    const prepared = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflow.id}/components/prepare`, {
        method: 'POST',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify({
          generation: 2,
          component_kind: 'MILEAGE_FORM',
          document_role: 'MILEAGE_CLAIM_FORM',
          expense_category: 'MILEAGE',
          media_type: 'image/jpeg',
          byte_size: jpegBytes.byteLength,
          source_content_sha256: sourceSha256,
          idempotency_key: '00000000-0000-4000-8000-000000000207'
        })
      }
    ), env, {}, { routeAudience: 'PRIVATE', ...deps });
    const preparedBody = await prepared.json();
    assert.equal(prepared.status, 201, JSON.stringify(preparedBody));
    const uploaded = await handleCandidateAppRequest(new Request(
      `https://private.test${preparedBody.upload.url}`, {
        method: 'PUT',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'image/jpeg' },
        body: jpegBytes
      }
    ), env, {}, { routeAudience: 'PRIVATE', ...deps });
    const uploadedBody = await uploaded.json();
    assert.equal(uploaded.status, 200, JSON.stringify(uploadedBody));
    assert.equal(uploadedBody.state, 'IMMUTABLE');
    assert.equal(storedOptions.customMetadata.sha256, sourceSha256);
    for (const key of Object.keys(storedOptions.customMetadata)) {
      assert.doesNotMatch(key, /mileage_form_qr|mileage_form_semantic|mileage_form_mileage/);
    }

    const retiredProof = await handleCandidateAppRequest(new Request(
      `https://private.test/candidate-app/v1/workflows/${workflow.id}/components/prepare`, {
        method: 'POST',
        headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
        body: JSON.stringify({
          generation: 2,
          component_kind: 'MILEAGE_FORM',
          document_role: 'MILEAGE_CLAIM_FORM',
          expense_category: 'MILEAGE',
          media_type: 'image/jpeg',
          byte_size: jpegBytes.byteLength,
          idempotency_key: '00000000-0000-4000-8000-000000000208',
          mileage_form_proof: { qr_text: 'retired' }
        })
      }
    ), env, {}, { routeAudience: 'PRIVATE', ...deps });
    const retiredProofBody = await retiredProof.json();
    assert.equal(retiredProofBody.error_code, 'CANDIDATE_MILEAGE_FORM_QR_PROOF_FORBIDDEN');
    assert.equal(retiredProof.status, 403);
  } finally {
    globalThis.fetch = originalFetch;
  }
});

test('printed Mileage carries each accepted photograph as a normal evidence page with a unique pack QR', () => {
  const backend = read('broker/src/candidate-app-backend.js');
  const render = backend.slice(
    backend.indexOf('async function renderExpensePage('),
    backend.indexOf('function candidateRpcArgs(')
  );
  assert.match(render, /const isMileageEvidence = upper\(component\.component_kind\) === 'MILEAGE_FORM'/);
  assert.match(render, /Total mileage for this claim:/);
  assert.match(render, /await drawCandidatePaperPageQr\(page, paperReturnQrText/);
  assert.match(render, /page\.drawText\('Manager signature'/);
  assert.match(render, /page\.drawText\('Date'/);
  const assembly = backend.slice(
    backend.indexOf('async function assembleCandidatePaperPack('),
    backend.indexOf('function candidatePaperPackComponentForPage(')
  );
  assert.match(assembly, /const pageQrText = expected\.manifest_version === 2/);
  assert.doesNotMatch(assembly, /kind !== 'MILEAGE_FORM'/);
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

test('Mileage return proof accepts only the exact current pack-page identity', () => {
  const sql = read('supabase/repeatable/30082026_1339_candidate_paper_manifest_page_qr_v2.sql');
  assert.match(sql, /lower\(p_qr_payload->>'m'\)=v_manifest_hex/i);
  assert.match(sql, /p_qr_payload->>'k'=v_page->>'page_kind_code'/i);
  assert.match(sql, /'qr_identity_kind','PACK_PAGE'/i);
  assert.doesNotMatch(sql, /SOURCE_MILEAGE_FORM|v_source_mileage_qr/i);
  assert.doesNotMatch(sql, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
});

test('private adapter stages verified JPEG pages and submits sealed receipts only as a pack', () => {
  const backend = read('broker/src/candidate-app-backend.js');
  assert.match(backend, /state:\s*'STAGED_FOR_PACK_CONFIRMATION'/);
  assert.match(backend, /paper_return_staged:\s*true/);
  assert.match(backend, /candidatePaperReturnPackReceipts\([\s\S]*candidate_paper_return_pack_complete_v2/);
  assert.match(backend, /!expected \|\| seenPageKeys\.has\(pageKey\)/);
  assert.match(backend, /await env\.R2\?\.head\(text\(receipt\.storage_key\)\)/);
  assert.match(backend, /const missingPageKeys = expectedPages[\s\S]*candidatePaperPagesError\([\s\S]*CANDIDATE_PAPER_RETURN_PACK_INCOMPLETE/);
  assert.match(backend, /candidatePaperPageError\(409, 'CANDIDATE_PAPER_RETURN_PACK_STALE', pageKey\)/);
  assert.match(backend, /replacement_page_keys/);
});

test('Mileage is worker-completed unsigned, then carried into the pack with one signing area and one pack QR', () => {
  const backend = read('broker/src/candidate-app-backend.js');
  const mileageForm = backend.slice(
    backend.indexOf('async function mileageClaimFormBytes('),
    backend.indexOf('async function paperExpensePageBytes(')
  );
  assert.doesNotMatch(mileageForm, /Manager signature|page\.drawText\('Date'/);
  assert.doesNotMatch(mileageForm, /drawCandidatePaperPageQr|candidatePaperPageQrText/);
  const assembly = backend.slice(
    backend.indexOf('async function assembleCandidatePaperPack('),
    backend.indexOf('function candidatePaperPackComponentForPage(')
  );
  assert.match(assembly, /expected\.manifest_version === 2\s*\? await candidatePaperPageQrText/);
  assert.doesNotMatch(assembly, /kind !== 'MILEAGE_FORM'/);
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
  assert.match(expensePage, /const displayOrdinal = isPaperReturn[\s\S]*\? contract\.review_ordinal[\s\S]*: component\.review_ordinal \|\| contract\.review_ordinal/);
  assert.doesNotMatch(expensePage, /CloudTMS workflow:|Page identity:/);
});
