import assert from 'node:assert/strict';
import test from 'node:test';
import { PDFDocument } from 'pdf-lib';

import { weeklySourceInvoiceRenderInternals } from '../../broker/src/index.js';

test('exact invoice renderer stores a verified PDF and renders Weekly Source backing-report identity', async () => {
  const invoiceId = '00000000-0000-4000-8000-000000000501';
  const stored = new Map();
  const renderedHtml = [];
  const renderedPdfOptions = [];
  const patchBodies = [];

  const seed = await PDFDocument.create();
  seed.addPage([595, 842]);
  const browserPdf = new Uint8Array(await seed.save());

  const bucket = {
    async put(key, bytes) {
      stored.set(String(key), new Uint8Array(bytes));
      return { key };
    },
    async head(key) {
      const bytes = stored.get(String(key));
      return bytes ? { size: bytes.byteLength } : null;
    },
    async get() { return null; },
  };

  const env = {
    R2: bucket,
    SUPABASE_URL: 'https://database.invalid',
    SUPABASE_SERVICE_ROLE_KEY: 'fixture-only',
    UPLOAD_TOKEN_SECRET: 'fixture-only-upload-token-secret',
    PUBLIC_DOWNLOAD_BASE_URL: 'https://worker.invalid/api/files/download',
  };
  const request = new Request('https://worker.invalid/api/invoices/render');

  const previousFetch = globalThis.fetch;
  globalThis.fetch = async (url, init = {}) => {
    assert.equal(init.method, 'PATCH');
    assert.match(String(url), new RegExp(`/rest/v1/invoices\\?id=eq\\.${invoiceId}$`));
    patchBodies.push(JSON.parse(String(init.body)));
    return new Response(null, { status: 204 });
  };

  try {
    const result = await weeklySourceInvoiceRenderInternals.renderInvoiceBundleAndStoreForTest(
      env,
      request,
      invoiceId,
      {
        async sbRpc(_env, name, args) {
          assert.equal(name, 'invoice_render_manifest');
          assert.deepEqual(args, { p_invoice_id: invoiceId });
          return [{
            invoice: {
              id: invoiceId,
              invoice_no: 'SB-2026-009',
              status: 'ISSUED',
              issued_at_utc: '2026-09-09T15:00:00Z',
              due_at_utc: '2026-10-09T15:00:00Z',
              subtotal_ex_vat: 20,
              vat_amount: 4,
              total_inc_vat: 24,
              header_snapshot_json: {},
            },
            header_snapshot_json: {
              client_name: 'St Mary\'s NHS Trust',
              meta: { backing_report_numbers: ['BR 3'] },
              hide_bank_footer: true,
              attach_policy: { ts_attach_to_invoice: false },
            },
            attach_policy: { ts_attach_to_invoice: false },
            lines: [{
              description: 'Final source movement',
              timesheet_id: null,
              total_charge_ex_vat: 20,
              vat_rate_pct: 20,
              vat_amount: 4,
              total_inc_vat: 24,
              meta_json: { source_movement_id: '00000000-0000-4000-8000-000000000601' },
            }],
            evidence: [],
            timesheet_evidence: [],
            evidence_other: [],
            reference_rows: [],
            tsfin_external_source_rows: [],
            hr_source_rows_cache: [{ header_columns: ['unused'], rows_json: [] }],
            timesheet_doc_flags_by_id: {},
          }];
        },
        async withBrowser(_env, render) {
          const browser = {
            async newPage() {
              return {
                on() {},
                async setContent(html) { renderedHtml.push(String(html)); },
                async emulateMediaType() {},
                async pdf(options) {
                  renderedPdfOptions.push(options);
                  return browserPdf.buffer.slice(browserPdf.byteOffset, browserPdf.byteOffset + browserPdf.byteLength);
                },
                async close() {},
              };
            },
          };
          return render(browser);
        },
      },
    );

    assert.equal(result.ok, true);
    assert.equal(result.cached, false);
    assert.equal(result.pdf_key, `docs-pdf/invoices/invoice_${invoiceId}.pdf`);
    assert.equal(renderedHtml.length, 1);
    assert.match(renderedHtml[0], /Final source movement/);
    assert.match(renderedPdfOptions[0].headerTemplate, /Backing report/);
    assert.match(renderedPdfOptions[0].headerTemplate, /BR 3/);
    assert.equal(patchBodies.length, 1);
    assert.equal(patchBodies[0].invoice_pdf_r2_key, result.pdf_key);
    const storedPdf = stored.get(result.pdf_key);
    assert.ok(storedPdf?.byteLength > 0);
    const verified = await PDFDocument.load(storedPdf);
    assert.equal(verified.getPageCount(), 1);
  } finally {
    globalThis.fetch = previousFetch;
  }
});
