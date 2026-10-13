// Invoked by the complete native frozen-model validator and direct line renderer.
// The queue's existing model/snapshot hashes authenticate the server qualification.
// This is a closed presentation witness, never permission to hide arbitrary rows.
export function validateZeroRoundingSuppressions(model) {
  const fail = detail => { throw Object.assign(new Error('INVOICE_ZERO_ROUNDING_SUPPRESSION_INVALID'), {code:'INVOICE_ZERO_ROUNDING_SUPPRESSION_INVALID',detail}); };
  if (model.source_rounding_suppressions == null) return model;
  if (!Array.isArray(model.source_rounding_suppressions) || model.source_rounding_suppressions.length===0) fail('witness_array');
  const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
  const hash=/^[0-9a-f]{64}$/;
  const ids=new Set();
  const fields=['contract','invoice_id','invoice_line_id','presentation_line_id','presentation_hash','source_hash',
    'rounding_authority_sha256','total_pay_pence','net_pence','vat_pence','gross_pence'];
  for (const witness of model.source_rounding_suppressions) {
    if (!witness || typeof witness!=='object' || Array.isArray(witness)
      || Object.keys(witness).length!==fields.length || fields.some(field=>!Object.hasOwn(witness,field))
      || witness.contract!=='WEEKLY_SOURCE_ZERO_ROUNDING_SUPPRESSION_V1') fail('shape');
    if (['invoice_id','invoice_line_id','presentation_line_id'].some(field=>!uuid.test(witness[field]??''))
      || witness.invoice_id!==model.invoice_id || ids.has(witness.invoice_line_id)) fail('identity');
    if (['presentation_hash','source_hash','rounding_authority_sha256'].some(field=>!hash.test(witness[field]??''))) fail('hash');
    if (['total_pay_pence','net_pence','vat_pence','gross_pence'].some(field=>witness[field]!==0)) fail('zero');
    if (!Array.isArray(model.lines) || model.lines.some(line=>line.source_invoice_line_id===witness.invoice_line_id)) fail('visible_line');
    ids.add(witness.invoice_line_id);
  }
  return model;
}
