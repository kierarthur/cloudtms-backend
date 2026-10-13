import {validateZeroRoundingSuppressions} from './invoice-presentation-contract.js';
export function validateLegacyInvoiceManifestSuppressions(manifest) {
  if (manifest.source_rounding_suppressions==null) return manifest;
  const invoice=manifest.invoice||manifest.invoice_row;
  if (!invoice?.id || !Array.isArray(manifest.lines)) throw new Error('INVOICE_ZERO_ROUNDING_SUPPRESSION_INVALID');
  validateZeroRoundingSuppressions({invoice_id:invoice.id,source_rounding_suppressions:manifest.source_rounding_suppressions,
    lines:manifest.lines.map(line=>({source_invoice_line_id:line.id}))});
  return manifest;
}
