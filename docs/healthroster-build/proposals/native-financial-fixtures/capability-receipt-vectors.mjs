// Independent literal oracle for root's census regression. This is not an
// application producer, an activation certificate or a substitute for SQL.
const validUuid=x=>/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(String(x));
export function financialReceiptContractOracle({receipt,financial,movements}){
 if(!Array.isArray(receipt.source_unit_outcomes_json)||!receipt.source_unit_outcomes_json.length)throw Error('FINANCIAL_IDENTITY_INVALID');
 if(receipt.published_timesheet_financial_id!==financial.id||receipt.root_timesheet_id!==financial.timesheet_id||receipt.client_id!==financial.client_id)throw Error('FINANCIAL_IDENTITY_INVALID');
 const mapped=receipt.source_unit_outcomes_json.map(unit=>{
  if(!unit||typeof unit!=='object'||Array.isArray(unit)||!validUuid(unit.movement_id)||! /^[a-f0-9]{64}$/.test(String(unit.movement_economic_hash)))throw Error('FINANCIAL_IDENTITY_INVALID');
  const matches=movements.filter(m=>m.id===unit.movement_id&&m.movement_economic_hash===unit.movement_economic_hash&&m.final_revision_id===receipt.final_revision_id&&m.invoice_timesheet_id===receipt.root_timesheet_id&&m.actual_client_id===receipt.client_id&&m.candidate_id===financial.candidate_id&&m.source_profile_kind!=='SELF_BILL_REPORT'&&!['SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION'].includes(m.source_line_kind));
  if(matches.length!==1||!validUuid(matches[0].contract_id))throw Error('FINANCIAL_IDENTITY_INVALID');return matches[0];
 });
 const contracts=[...new Set(mapped.map(m=>m.contract_id))];if(contracts.length!==1)throw Error('FINANCIAL_IDENTITY_INVALID');return contracts[0];
}
