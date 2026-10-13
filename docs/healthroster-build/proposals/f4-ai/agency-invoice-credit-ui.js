(function(root){
  'use strict';
  const CONTRACT='TIMESHEET_CORRECTION_CREDIT_CANDIDATES_V1';
  const reasons={CREDIT_SCOPE_UNIT_NOT_AUTHORISED:'Both correction records need authorisation.',
    CREDIT_SCOPE_SUPPORT_UNPROVED:'The original HealthRoster attachment needs checking.',
    CREDIT_SCOPE_ORIGINAL_DOCUMENT_UNPROVED:'The original invoice document needs checking.',
    CREDIT_SCOPE_ORIGINAL_SIGNED_ASSET_UNPROVED:'The original signed Timesheet needs checking.',
    CREDIT_SCOPE_ORIGINAL_SIGNED_WITNESS_UNPROVED:'The original signatures need checking.',
    CREDIT_SCOPE_ORIGINAL_REFERENCES_UNPROVED:'The original invoice references need checking.',
    CREDIT_SCOPE_ORIGINAL_DOCUMENT_PENCE_MISMATCH:'The original invoice amounts need checking.',
    CREDIT_ALREADY_EXISTS:'A credit note already covers the original invoice.'};
  const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
  function rows(envelope){
    if(envelope==null)return [];
    if(envelope.contract_version!==CONTRACT||!Array.isArray(envelope.rows)||envelope.rows.length>25000
      ||envelope.rows.some(row=>row.selectable!==false||row.automatic!==true||typeof row.selection_key!=='string'
        ||row.selection_key!==`healthroster-credit:${row.original_invoice_id}:${row.reversal_timesheet_id}`
        ||!uuid.test(row.original_invoice_id)||!uuid.test(row.reversal_timesheet_id)))throw new Error('Credit note details are not available.');
    return envelope.rows;
  }
  function render(envelope,escapeHtml){
    const items=rows(envelope);if(!items.length)return '';
    return `<section class="invbatch-group"><h3>Automatic credit notes</h3><table class="invbatch-table"><thead><tr><th>Invoice</th><th>Worker / week</th><th>Credit note</th></tr></thead><tbody>${items.map(row=>`<tr><td>${escapeHtml(row.invoice_number)}</td><td>${escapeHtml(row.candidate_name)} · w/e ${escapeHtml(row.week_ending_date)}</td><td>${row.blocker_code?`Could not be raised · ${escapeHtml(reasons[row.blocker_code]||'The original invoice or correction needs checking.')}`:'Credit note will be raised automatically.'}</td></tr>`).join('')}</tbody></table></section>`;
  }
  function renderResults(results,escapeHtml){
    if(!Array.isArray(results)||!results.length)return '';
    return `<section class="invbatch-group"><h3>Credit note results</h3><ul>${results.map(row=>`<li>${row.already_credited?'Already credited':row.blocked?'Credit note could not be raised':row.accepted?'Credit note preparation started':'Credit note could not be started'}${row.code&&!row.already_credited?` · ${escapeHtml(reasons[row.code]||'Check the invoice Outbox for details.')}`:''}</li>`).join('')}</ul><p>Progress and recovery are available in the invoice Outbox.</p></section>`;
  }
  function register(results,registerNative,options){
    const receipts=(Array.isArray(results)?results:[]).map(row=>row.operation_receipt).filter(Boolean);
    if(receipts.length&&typeof registerNative==='function')return registerNative({operation_ids:receipts.map(row=>row.operation_id),per_command_results:receipts},
      {...options,response_family:'GENERATE_CREDIT_NOTE',purpose:'GENERATE_CREDIT_NOTE',explicit_operation_ids:true});
    return [];
  }
  root.CloudTmsAutomaticCreditNotes=Object.freeze({rows,render,renderResults,register});
})(typeof window==='undefined'?globalThis:window);
