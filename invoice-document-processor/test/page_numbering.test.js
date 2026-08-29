import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { PDFDocument, PDFName } from 'pdf-lib';
import { applyGlobalPageNumbers } from '../container/page-numbering.mjs';

test('global numbering preserves page count and mixed page geometry', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'cloudtms-page-numbers-'));
  try {
    const inputPath = join(directory, 'input.pdf');
    const outputPath = join(directory, 'output.pdf');
    const source = await PDFDocument.create();
    source.addPage([595.28, 841.89]);
    source.addPage([841.89, 595.28]);
    await writeFile(inputPath, await source.save());
    const result = await applyGlobalPageNumbers(inputPath, outputPath, {
      page_numbering_contract: 'FINAL_MERGE_GLOBAL_V1',
      document_entity_type: 'INVOICE',
      document_version_id: '00000000-0000-4000-8000-000000000001'
    });
    assert.equal(result.page_count, 2);
    const output = await PDFDocument.load(await readFile(outputPath));
    assert.equal(output.getPageCount(), 2);
    assert.ok(output.getPage(0).getHeight() > output.getPage(0).getWidth());
    assert.ok(output.getPage(1).getWidth() > output.getPage(1).getHeight());
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('global numbering is invoice-only and requires the exact contract', async () => {
  await assert.rejects(
    () => applyGlobalPageNumbers('missing.pdf', 'missing-out.pdf', {
      page_numbering_contract: 'FINAL_MERGE_GLOBAL_V1',
      document_entity_type: 'TIMESHEET'
    }),
    error => error?.code === 'PAGE_NUMBERING_ENTITY_UNSUPPORTED'
  );
  await assert.rejects(
    () => applyGlobalPageNumbers('missing.pdf', 'missing-out.pdf', {
      page_numbering_contract: 'UNKNOWN',
      document_entity_type: 'INVOICE'
    }),
    error => error?.code === 'PAGE_NUMBERING_CONTRACT_UNSUPPORTED'
  );
});

test('selective numbering keeps the physical total but omits official timesheet pages', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'cloudtms-selective-page-numbers-'));
  try {
    const inputPath = join(directory, 'input.pdf');
    const outputPath = join(directory, 'output.pdf');
    const source = await PDFDocument.create();
    source.addPage([595.28, 841.89]);
    source.addPage([841.89, 595.28]);
    await writeFile(inputPath, await source.save());
    const result = await applyGlobalPageNumbers(inputPath, outputPath, {
      page_numbering_contract: 'FINAL_MERGE_SELECTIVE_V2',
      page_numbering_excluded_pages: [2],
      document_entity_type: 'INVOICE',
      document_version_id: '00000000-0000-4000-8000-000000000001'
    });
    assert.deepEqual(result.page_numbering_excluded_pages, [2]);
    assert.equal(result.numbered_page_count, 1);
    const output = await PDFDocument.load(await readFile(outputPath));
    const [invoicePage, timesheetPage] = output.getPages();
    const invoiceFonts = invoicePage.node.Resources()?.get(PDFName.of('Font'));
    const timesheetFonts = timesheetPage.node.Resources()?.get(PDFName.of('Font'));
    assert.ok(invoiceFonts, 'invoice page must contain the page-number font resource');
    assert.equal(
      timesheetFonts,
      undefined,
      'official timesheet page must remain unnumbered'
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('selective numbering rejects duplicate and out-of-range exclusions', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'cloudtms-selective-page-number-errors-'));
  try {
    const inputPath = join(directory, 'input.pdf');
    const outputPath = join(directory, 'output.pdf');
    const source = await PDFDocument.create();
    source.addPage();
    await writeFile(inputPath, await source.save());
    for (const page_numbering_excluded_pages of [[1, 1], [2], [0]]) {
      await assert.rejects(
        () => applyGlobalPageNumbers(inputPath, outputPath, {
          page_numbering_contract: 'FINAL_MERGE_SELECTIVE_V2',
          page_numbering_excluded_pages,
          document_entity_type: 'INVOICE'
        }),
        error => error?.code === 'PAGE_NUMBERING_EXCLUSIONS_INVALID'
      );
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
