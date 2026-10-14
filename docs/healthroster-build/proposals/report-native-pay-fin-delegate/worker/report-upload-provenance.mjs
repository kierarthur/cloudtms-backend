// Source-only adjacent Report projection. No pricing, pay duration, economic
// identity, row hash or manifest hash is derived here. SQL owns those hashes.
export const REPORT_PROFILE = 'ACACIUM_SELF_BILL_REPORT_V1';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const SHA = /^[0-9a-f]{64}$/;
const MONEY = /^-?[0-9]{1,12}$/;
const BODY_KEYS = ['source_group_id','source_cycle_id','report_scope_id','client_id','profile_id',
  'parser_options','filename','original_filename','file_key','import_use','purpose'];
const ROW_KEYS = ['worksheetIndex','physicalRow','sourceRowId','rowKind','sourcePosition','requestId',
  'workerName','date','reportNumber','invoiceDate','requestedOn','reportTimes','pricingEvidence'];
const COMPONENT_KEYS = ['coordinate','sourceKind','cellKind','formulaPresent','originalToken',
  'decodedToken','tokenTruncated','parseState','parsedPence','normalisation','rounding'];

function fail(code='WEEKLY_SOURCE_REPORT_PARSER_FACTS_INVALID',message='The report evidence could not be verified.') {
  const error = new Error(message); error.code=code; error.status=409; throw error;
}
const object = value => value !== null && typeof value==='object' && !Array.isArray(value);
function closed(value,keys,code) {
  if (!object(value) || Object.keys(value).some(key=>!keys.includes(key))) fail(code);
}
function exact(value,keys,code) { closed(value,keys,code); if(keys.some(key=>!Object.hasOwn(value,key))) fail(code); }
const validUuid = value => typeof value==='string' && UUID.test(value);
const bounded = (value,max) => typeof value==='string' && value.length>0 && value.length<=max;
function date(value) {
  if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(value)) fail();
  const d=new Date(`${value}T12:00:00Z`); if(!Number.isFinite(d.getTime())||d.toISOString().slice(0,10)!==value) fail();
  return value;
}
function naive(value) {
  if(typeof value!=='string'||!/^\d{4}-\d{2}-\d{2}T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$/.test(value)) fail();
  date(value.slice(0,10)); return value;
}
function pence(value) { if(typeof value!=='string'||!MONEY.test(value)) fail(); return value; }
function clock(value) { if(typeof value!=='string'||!/^([01][0-9]|2[0-3]):[0-5][0-9]$/.test(value)) fail(); return value; }
function nextDay(value) {const d=new Date(`${date(value)}T12:00:00Z`);d.setUTCDate(d.getUTCDate()+1);return d.toISOString().slice(0,10);}

export function assertReportUploadRequest(body) {
  closed(body,BODY_KEYS,'WEEKLY_SOURCE_REPORT_REQUEST_UNKNOWN_FIELD');
  if(body.purpose!=null&&body.purpose!=='ORDINARY') fail('WEEKLY_SOURCE_SELF_BILL_REPORT_CORRECT_FINAL_UNSUPPORTED');
  if(body.import_use!=null&&body.import_use!=='PREPARE_FINALISATION') fail('WEEKLY_SOURCE_REPORT_IMPORT_USE_INVALID');
  if(body.parser_options!=null) closed(body.parser_options,['profileId'],'WEEKLY_SOURCE_REPORT_PARSER_OPTIONS_FORBIDDEN');
  if((body.profile_id??body.parser_options?.profileId)!==REPORT_PROFILE
    ||(body.profile_id!=null&&body.parser_options?.profileId!=null&&body.profile_id!==body.parser_options.profileId)) fail('WEEKLY_SOURCE_REPORT_PROFILE_INVALID');
  for(const key of ['source_group_id','source_cycle_id','report_scope_id','client_id']) {
    if(body[key]!=null&&body[key]!==''&&!validUuid(body[key])) fail('WEEKLY_SOURCE_REPORT_SCOPE_INVALID');
  }
}

export function assertReportRuleContext(context) {
  const rule=context?.effective_rule_set,values=rule?.values;
  if(context?.ok!==true||context.source_family!=='ROSTER'||!validUuid(context.agency_id)
    ||!validUuid(context.source_group_id)||!validUuid(context.source_cycle_id)||!validUuid(context.client_id)
    ||!bounded(context.client_name,200)||rule?.contract!=='WEEKLY_SOURCE_EFFECTIVE_RULE_SET_V1'
    ||rule.source_group_id!==context.source_group_id||!validUuid(rule.rule_set_version_id)
    ||!validUuid(rule.assignment_id)||!Number.isSafeInteger(Number(rule.assignment_version))||Number(rule.assignment_version)<1
    ||!SHA.test(rule.values_sha256??'')||!object(values)
    ||values.invoice_sender!=='CLIENT_SELF_BILL'||values.self_bill_source!=='SEPARATE_FINAL_REPORT'
    ||values.final_report_profile_kind!==REPORT_PROFILE||values.invoice_membership!=='FOLLOW_FINAL_REPORT_LINES'
    ||values.invoice_amount_basis!=='SOURCE_AMOUNT'||values.source_amount_basis!=='REPORT_LINE_NET'
    ||values.line_vat_method!=='LINE_VAT_FROM_REPORT'||values.pay_membership!=='ON_ACCEPTED_REPORT_OR_PROTECTED') {
    fail('WEEKLY_SOURCE_SELF_BILL_REPORT_RULES_NOT_ENABLED','The actual source settings do not enable this report.');
  }
  return values;
}

function component(value,expected,ordinal,column) {
  closed(value,COMPONENT_KEYS);
  if(!bounded(value.coordinate,10)||value.coordinate!==`${column}${ordinal}`
    ||value.sourceKind!=='XLSX'||!['NUMBER','STRING'].includes(value.cellKind)
    ||value.formulaPresent!==false||typeof value.tokenTruncated!=='boolean'
    ||typeof value.originalToken!=='string'||value.originalToken.length>128
    ||typeof value.decodedToken!=='string'||value.decodedToken.length>128
    ||value.parseState!=='PARSED'||pence(value.parsedPence)!==expected) fail();
  for(const key of ['normalisation','rounding']) if(Object.hasOwn(value,key)&&!bounded(value[key],200)) fail();
  return {...value,sourceKind:value.cellKind==='NUMBER'?'XLSX_NUMERIC_TOKEN':'XLSX_STRING_TOKEN'};
}

export function selfBillReportRows(parsed,context) {
  const values=assertReportRuleContext(context);
  if(parsed?.ok!==true||parsed.profileId!==REPORT_PROFILE||parsed.sourceKind!=='XLSX'
    ||!SHA.test(parsed.sourceFileSha256??'')||parsed.headerRow!==1||parsed.selectedWorksheet?.name!=='Allocate'
    ||!Array.isArray(parsed.rows)||parsed.rows.length<1||parsed.rows.length>100000) fail();
  const report=parsed.report;
  exact(report,['reportNumber','invoiceDate','layout','lineCount','netPence','vatPence','grossPence']);
  if(!['A','B'].includes(report.layout)||!/^XYOXLR[0-9]{6}$/.test(report.reportNumber??'')
    ||report.reportNumber.slice(6)!==date(report.invoiceDate).slice(8,10)+report.invoiceDate.slice(5,7)+report.invoiceDate.slice(2,4)
    ||report.lineCount!==parsed.rows.length
    ||(report.layout==='A'&&values.invoice_vat_method==='ONCE_ON_TOTAL_TRUNCATE')
    ||(report.layout==='B'&&values.invoice_vat_method==='ONCE_ON_TOTAL_HALF_UP')) fail('WEEKLY_SOURCE_REPORT_LAYOUT_VAT_METHOD_MISMATCH');
  const seen=new Set(),ordinals=new Set();let net=0n,vat=0n,gross=0n;
  const rows=parsed.rows.map(row=>{
    exact(row,ROW_KEYS);exact(row.reportTimes,['start','end','overnight']);
    exact(row.pricingEvidence,['state','authority','netPence','vatPence','grossPence','components']);
    exact(row.pricingEvidence.components,['net','vat','gross']);
    if(row.rowKind!=='SOURCE_REPORT_LINE'||row.sourcePosition!=='INVOICE_REPORT_ONLY'
      ||row.pricingEvidence.state!=='PARSED'||row.pricingEvidence.authority!=='SELF_BILL_REPORT_INVOICE_ONLY'
      ||!Number.isSafeInteger(row.physicalRow)||row.physicalRow<2||row.worksheetIndex!==parsed.selectedWorksheet.index
      ||row.sourceRowId!==`worksheet:${row.worksheetIndex}:row:${row.physicalRow}`
      ||!bounded(row.requestId,160)||row.requestId.trim()!==row.requestId||!bounded(row.workerName,200)
      ||row.reportNumber!==report.reportNumber||row.invoiceDate!==report.invoiceDate
      ||typeof row.reportTimes.overnight!=='boolean'||seen.has(row.requestId)||ordinals.has(row.physicalRow)) fail();
    seen.add(row.requestId);ordinals.add(row.physicalRow);
    const work=date(row.date),start=clock(row.reportTimes.start),end=clock(row.reportTimes.end),overnight=end<=start;
    if(overnight!==row.reportTimes.overnight) fail('WEEKLY_SOURCE_REPORT_TIMES_INVALID');
    const n=pence(row.pricingEvidence.netPence),v=pence(row.pricingEvidence.vatPence),g=pence(row.pricingEvidence.grossPence);
    net+=BigInt(n);vat+=BigInt(v);gross+=BigInt(g);
    const components=Object.fromEntries([['net',n],['vat',v],['gross',g]].map(([key,money])=>{
      const map=parsed.resolvedColumnMap?.[key];if(!map||!Number.isSafeInteger(map.index)||map.index<1||!/^[A-Z]{1,3}$/.test(map.column??'')) fail();
      return [key,component(row.pricingEvidence.components[key],money,row.physicalRow,map.column)];
    }));
    return {source_row_ordinal:row.physicalRow,external_source_key:row.requestId,
      source_candidate_identity:row.workerName,source_client_identity:context.client_name,
      work_date:work,start_at_local:`${work}T${start}:00`,end_at_local:`${overnight?nextDay(work):work}T${end}:00`,
      source_shift_charge_pence:n,source_vat_pence:v,source_gross_pence:g,
      bounded_raw_columns_json:{reportTimes:{...row.reportTimes},requestedOn:naive(row.requestedOn),components}};
  });
  if(pence(report.netPence)!==net.toString()||pence(report.vatPence)!==vat.toString()||pence(report.grossPence)!==gross.toString()) fail('WEEKLY_SOURCE_REPORT_MANIFEST_MISMATCH');
  return rows;
}

export function reportScopeRequest(parsed,context,actorUserId) {
  if(!validUuid(actorUserId)) fail('WEEKLY_SOURCE_REPORT_ACTOR_INVALID');
  const rows=selfBillReportRows(parsed,context),report=parsed.report;
  return {actor_user_id:actorUserId,source_group_id:context.source_group_id,source_cycle_id:context.source_cycle_id,
    client_id:context.client_id,report_number:report.reportNumber,invoice_date:report.invoiceDate,layout:report.layout,
    content_sha256:parsed.sourceFileSha256,line_count:report.lineCount,net_pence:report.netPence,
    vat_pence:report.vatPence,gross_pence:report.grossPence,rows};
}

export function assertReportScopeReceipt(receipt,context) {
  exact(receipt,['ok','contract','report_scope_id','self_bill_report_id','source_group_id','source_cycle_id','client_id',
    'client_name','authority_scope_version','cutoff_at_utc','row_manifest_hash','idempotent'],'WEEKLY_SOURCE_REPORT_SCOPE_RECEIPT_INVALID');
  if(receipt.ok!==true||receipt.contract!=='WEEKLY_SOURCE_SELF_BILL_REPORT_SCOPE_V1'||!validUuid(receipt.report_scope_id)
    ||!validUuid(receipt.self_bill_report_id)||receipt.source_group_id!==context.source_group_id
    ||receipt.source_cycle_id!==context.source_cycle_id||receipt.client_id!==context.client_id||receipt.client_name!==context.client_name
    ||(context.report_scope_id!=null&&context.report_scope_id!==receipt.report_scope_id)
    ||!SHA.test(receipt.row_manifest_hash??'')||!Number.isSafeInteger(Number(receipt.authority_scope_version))
    ||Number(receipt.authority_scope_version)<1||typeof receipt.idempotent!=='boolean') fail('WEEKLY_SOURCE_REPORT_SCOPE_RECEIPT_INVALID');
  return receipt;
}

export function assertReportPublicationContext(context,expected) {
  assertReportRuleContext(context);
  if(context.authority_scope_kind!=='SELF_BILL_REPORT_SCOPE'||context.invoice_only!==true||context.profile_id!==REPORT_PROFILE
    ||!validUuid(context.self_bill_report_id)||context.report_scope_id!==expected.report_scope_id
    ||context.source_group_id!==expected.source_group_id||context.source_cycle_id!==expected.source_cycle_id
    ||context.client_id!==expected.client_id||context.upload_id!==expected.upload_id
    ||!Array.isArray(context.rows)||context.rows.length!==0
    ||!Number.isSafeInteger(Number(context.authority_scope_version))||Number(context.authority_scope_version)<1
    ||!SHA.test(context.row_manifest_hash??'')) fail('WEEKLY_SOURCE_REPORT_PUBLICATION_CONTEXT_INVALID');
  return context;
}

export function assertReportStageRowsReceipt(receipt,uploadId) {
  exact(receipt,['ok','status','upload_id','inserted','stored'],'WEEKLY_SOURCE_REPORT_STAGE_ROWS_FAILED');
  if(receipt.ok!==true||receipt.status!=='STAGING'||receipt.upload_id!==uploadId) fail('WEEKLY_SOURCE_REPORT_STAGE_ROWS_FAILED');
  for(const counts of [receipt.inserted,receipt.stored]) {
    exact(counts,['physical_rows','normalised_rows','money_evidence','expense_evidence'],'WEEKLY_SOURCE_REPORT_STAGE_ROWS_FAILED');
    if(Object.values(counts).some(n=>!Number.isSafeInteger(n)||n<0)||counts.expense_evidence!==0) fail('WEEKLY_SOURCE_REPORT_STAGE_ROWS_FAILED');
  }
}

export function assertReportProjectionRowsReceipt(receipt,publicationId) {
  exact(receipt,['ok','status','publication_id','rows_applied','expected_rows','invoice_only'],'WEEKLY_SOURCE_REPORT_PROJECTION_ROWS_FAILED');
  if(receipt.ok!==true||receipt.status!=='APPLIED'||receipt.publication_id!==publicationId
    ||receipt.rows_applied!==0||receipt.expected_rows!==0||receipt.invoice_only!==true) fail('WEEKLY_SOURCE_REPORT_PROJECTION_ROWS_FAILED');
}
