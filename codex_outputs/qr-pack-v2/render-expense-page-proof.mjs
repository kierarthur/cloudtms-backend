import { createHash } from 'node:crypto';
import { mkdir, writeFile } from 'node:fs/promises';
import jpeg from 'jpeg-js';
import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';
import { candidateAppBackendInternals } from '../../broker/src/candidate-app-backend.js';
import { buildTsq2PagePayload, buildTsq2String } from '../../broker/src/timesheet-qr-payload.js';

const { renderExpensePage } = candidateAppBackendInternals;
const outputDirectory = new URL('./pdf/', import.meta.url);
await mkdir(outputDirectory, { recursive: true });

const brandingBase = {
  contract_version: 'CANDIDATE_DOCUMENT_BRANDING_V1',
  agency_name: 'Arthur Rai Medical Services Limited',
  logo_key: null,
  logo_sha256: null,
  logo_media_type: null
};
const branding = {
  ...brandingBase,
  branding_contract_sha256: createHash('sha256')
    .update(JSON.stringify(brandingBase)).digest('hex')
};
const workflow = {
  id: '00000000-0000-4000-8000-000000000981',
  generation: 1,
  week_ending_date: '2026-08-30',
  immutable_submission_json: {
    official_presentation: {
      branding,
      worker: { first_name: 'Kier', surname: 'Arthur' },
      client: { name: 'Arthur Rai Medical Services' }
    },
    expense_submission: {
      canonical_tsfin_snapshot: {
        accommodation_pay_ex_vat: 20,
        travel_pay_ex_vat: 8.5,
        other_pay_ex_vat: 4,
        expenses_pay_ex_vat: 32.5
      }
    }
  }
};
const timesheetId = '00000000-0000-4000-8000-000000000980';
const manifestSha256 = '6'.repeat(64);
async function pageQrText({ ordinal, pageKey, pageKind, categoryCode = '', categoryOccurrence = 1 }) {
  return buildTsq2String(await buildTsq2PagePayload({
    workflow_id: workflow.id,
    timesheet_id: timesheetId,
    workflow_generation: workflow.generation,
    paper_return_manifest_sha256: manifestSha256,
    ordinal,
    page_key: pageKey,
    page_kind: pageKind,
    category_code: categoryCode,
    category_occurrence: categoryOccurrence
  }), { QR_SIGNING_SECRET: 'visual-proof-only-qr-signing-secret' });
}
const commonContract = {
  review_ordinal: 2
};

const summaryComponent = {
  id: '00000000-0000-4000-8000-000000000982',
  component_kind: 'EXPENSE_SUMMARY',
  document_role: 'EXPENSE_APPROVAL_SUMMARY',
  expense_category: 'OTHER',
  review_ordinal: 2
};
const summary = await renderExpensePage(
  {}, {
    ...commonContract,
    paper_return_qr_text: await pageQrText({
      ordinal: 2, pageKey: 'EXPENSE_SUMMARY::1', pageKind: 'S', categoryCode: 'O'
    }),
    paper_return_display_name: 'Expense summary'
  }, { workflow, component: summaryComponent }, 'REVIEW'
);
await writeFile(new URL('expense-summary-qr.pdf', outputDirectory), summary.pdf_bytes);

const receiptWidth = 1400;
const receiptHeight = 900;
const pixels = Buffer.alloc(receiptWidth * receiptHeight * 4, 255);
for (let y = 0; y < receiptHeight; y += 1) {
  for (let x = 0; x < receiptWidth; x += 1) {
    const offset = (y * receiptWidth + x) * 4;
    if (y < 90) {
      pixels[offset] = 12; pixels[offset + 1] = 45; pixels[offset + 2] = 92;
    } else if (x > 90 && x < 1310 && y > 170 && y < 730 && (y % 95) < 4) {
      pixels[offset] = 80; pixels[offset + 1] = 90; pixels[offset + 2] = 105;
    }
  }
}
const receiptBytes = jpeg.encode({ data: pixels, width: receiptWidth, height: receiptHeight }, 88).data;
const receiptSha = createHash('sha256').update(receiptBytes).digest('hex');
const sourceId = '00000000-0000-4000-8000-000000000983';
const pdfSourceId = '00000000-0000-4000-8000-000000000984';
const sourcePdf = await PDFDocument.create({ updateMetadata: false });
const sourcePdfPage = sourcePdf.addPage([595.28, 841.89]);
const sourcePdfBold = await sourcePdf.embedFont(StandardFonts.HelveticaBold);
const sourcePdfRegular = await sourcePdf.embedFont(StandardFonts.Helvetica);
sourcePdfPage.drawRectangle({ x: 34, y: 770, width: 527, height: 44, color: rgb(0.88, 0.93, 0.98) });
sourcePdfPage.drawText('Travel receipt supplied by Candidate', { x: 52, y: 786, size: 15, font: sourcePdfBold });
sourcePdfPage.drawText('Rail travel to the placement', { x: 52, y: 730, size: 13, font: sourcePdfRegular });
sourcePdfPage.drawText('Amount paid: GBP 8.50', { x: 52, y: 698, size: 13, font: sourcePdfBold });
sourcePdfPage.drawText('This is a one-page PDF source proof.', { x: 52, y: 655, size: 11, font: sourcePdfRegular });
const sourcePdfBytes = new Uint8Array(await sourcePdf.save());
const sourcePdfSha = createHash('sha256').update(sourcePdfBytes).digest('hex');
const sourceRecords = {
  [sourceId]: {
    id: sourceId,
    storage_key: 'visual-proof/accommodation-receipt.jpg',
    source_content_sha256: receiptSha
  },
  [pdfSourceId]: {
    id: pdfSourceId,
    storage_key: 'visual-proof/travel-receipt.pdf',
    source_content_sha256: sourcePdfSha
  }
};
const originalFetch = globalThis.fetch;
globalThis.fetch = async (input) => {
  if (String(input).includes('/candidate_submission_components')) {
    const found = Object.values(sourceRecords).find((record) => String(input).includes(record.id));
    return new Response(JSON.stringify(found ? [found] : []), {
      status: 200, headers: { 'content-type': 'application/json' }
    });
  }
  throw new Error(`Unexpected visual-proof request: ${input}`);
};
try {
  const supplementaryComponent = {
    id: sourceId,
    component_kind: 'EXPENSE_EVIDENCE',
    document_role: 'EXPENSE_EVIDENCE',
    expense_category: 'ACCOMMODATION',
    review_ordinal: 3
  };
  const supplementary = await renderExpensePage({
    SUPABASE_URL: 'https://visual-proof.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'visual-proof-not-a-secret',
    R2: {
      get: async (key) => {
        if (key === 'visual-proof/accommodation-receipt.jpg') return {
          arrayBuffer: async () => receiptBytes.buffer.slice(
            receiptBytes.byteOffset, receiptBytes.byteOffset + receiptBytes.byteLength
          ),
          httpMetadata: { contentType: 'image/jpeg' }
        };
        if (key === 'visual-proof/travel-receipt.pdf') return {
          arrayBuffer: async () => sourcePdfBytes.buffer.slice(
            sourcePdfBytes.byteOffset, sourcePdfBytes.byteOffset + sourcePdfBytes.byteLength
          ),
          httpMetadata: { contentType: 'application/pdf' }
        };
        return null;
      }
    }
  }, {
    ...commonContract,
    review_ordinal: 3,
    paper_return_qr_text: await pageQrText({
      ordinal: 3, pageKey: 'EXPENSE_EVIDENCE:ACCOMMODATION:1', pageKind: 'E',
      categoryCode: 'A', categoryOccurrence: 1
    }),
    paper_return_display_name: 'Accommodation 1',
    render_input: { source_component_id: sourceId }
  }, { workflow, component: supplementaryComponent }, 'REVIEW');
  await writeFile(new URL('accommodation-1-qr.pdf', outputDirectory), supplementary.pdf_bytes);

  const pdfSupplementaryComponent = {
    id: pdfSourceId,
    component_kind: 'EXPENSE_EVIDENCE',
    document_role: 'EXPENSE_EVIDENCE',
    expense_category: 'TRAVEL',
    review_ordinal: 4
  };
  const pdfSupplementary = await renderExpensePage({
    SUPABASE_URL: 'https://visual-proof.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'visual-proof-not-a-secret',
    R2: {
      get: async (key) => key === 'visual-proof/travel-receipt.pdf' ? {
        arrayBuffer: async () => sourcePdfBytes.buffer.slice(
          sourcePdfBytes.byteOffset, sourcePdfBytes.byteOffset + sourcePdfBytes.byteLength
        ),
        httpMetadata: { contentType: 'application/pdf' }
      } : null
    }
  }, {
    ...commonContract,
    review_ordinal: 4,
    paper_return_qr_text: await pageQrText({
      ordinal: 4, pageKey: 'EXPENSE_EVIDENCE:TRAVEL:1', pageKind: 'E',
      categoryCode: 'T', categoryOccurrence: 1
    }),
    paper_return_display_name: 'Travel 1',
    render_input: { source_component_id: pdfSourceId }
  }, { workflow, component: pdfSupplementaryComponent }, 'REVIEW');
  await writeFile(new URL('travel-1-pdf-source-qr.pdf', outputDirectory), pdfSupplementary.pdf_bytes);
} finally {
  globalThis.fetch = originalFetch;
}

console.log('QR_EXPENSE_VISUAL_PROOFS_RENDERED');
