import { readFile, writeFile } from 'node:fs/promises';
import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';

function pageNumberingError(code, category) {
  return Object.assign(new Error(code), { code, category });
}
export async function applyGlobalPageNumbers(inputPath, outputPath, options = {}) {
  const contract = String(options.page_numbering_contract || '');
  if (!['FINAL_MERGE_GLOBAL_V1', 'FINAL_MERGE_SELECTIVE_V2'].includes(contract)) {
    throw pageNumberingError('PAGE_NUMBERING_CONTRACT_UNSUPPORTED', 'POLICY_VIOLATION');
  }
  if (String(options.document_entity_type || '').toUpperCase() !== 'INVOICE') {
    throw pageNumberingError('PAGE_NUMBERING_ENTITY_UNSUPPORTED', 'POLICY_VIOLATION');
  }
  const input = await readFile(inputPath);
  const document = await PDFDocument.load(input, {
    ignoreEncryption: false,
    updateMetadata: false
  });
  const font = await document.embedFont(StandardFonts.Helvetica);
  const pages = document.getPages();
  const total = pages.length;
  if (total < 1) {
    throw pageNumberingError('PAGE_NUMBERING_INPUT_EMPTY', 'PERMANENT_INPUT');
  }
  const rawExcluded = options.page_numbering_excluded_pages ?? [];
  if (!Array.isArray(rawExcluded)) {
    throw pageNumberingError('PAGE_NUMBERING_EXCLUSIONS_INVALID', 'POLICY_VIOLATION');
  }
  const excludedPages = rawExcluded.map(value => Number(value));
  if (
    excludedPages.some(value => !Number.isSafeInteger(value) || value < 1 || value > total)
    || new Set(excludedPages).size !== excludedPages.length
  ) {
    throw pageNumberingError('PAGE_NUMBERING_EXCLUSIONS_INVALID', 'POLICY_VIOLATION');
  }
  excludedPages.sort((left, right) => left - right);
  if (contract === 'FINAL_MERGE_GLOBAL_V1' && excludedPages.length) {
    throw pageNumberingError('PAGE_NUMBERING_EXCLUSIONS_UNSUPPORTED', 'POLICY_VIOLATION');
  }
  const excluded = new Set(excludedPages);
  for (let index = 0; index < total; index += 1) {
    if (excluded.has(index + 1)) continue;
    const page = pages[index];
    const label = `${index + 1} / ${total}`;
    const size = 8;
    const width = font.widthOfTextAtSize(label, size);
    page.drawText(label, {
      x: Math.max(6, (page.getWidth() - width) / 2),
      y: 7,
      size,
      font,
      color: rgb(0.25, 0.29, 0.36)
    });
  }
  await writeFile(outputPath, await document.save({
    useObjectStreams: false,
    addDefaultPage: false,
    updateFieldAppearances: false
  }));
  const verification = await PDFDocument.load(await readFile(outputPath), {
    ignoreEncryption: false,
    updateMetadata: false
  });
  if (verification.getPageCount() !== total) {
    throw pageNumberingError('PAGE_NUMBERING_PAGE_COUNT_CHANGED', 'PROCESSOR_BUG');
  }
  return {
    page_count: total,
    numbered_page_count: total - excludedPages.length,
    page_numbering_contract: contract,
    page_numbering_excluded_pages: excludedPages
  };
}
