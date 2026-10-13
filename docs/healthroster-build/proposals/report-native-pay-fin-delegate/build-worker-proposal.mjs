import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
const dest=path.dirname(fileURLToPath(import.meta.url));
const lane=path.resolve(dest,'../../../..'),root=path.resolve(lane,'../healthroster-v2-backend-13102026');
const worker=path.join(dest,'worker');fs.mkdirSync(worker,{recursive:true});
const pins=[],hunks=[];const hash=x=>crypto.createHash('sha256').update(x).digest('hex');
function read(p){const b=fs.readFileSync(path.join(root,p));pins.push({path:p,bytes:b.length,sha256:hash(b)});return b.toString('utf8');}
function change(body,before,after,file){
  if(body.split(before).length!==2)throw new Error('HUNK_COUNT '+file+' '+before.slice(0,70));
  hunks.push({file,before,after});return body.replace(before,()=>after);
}
function lines(body,value){return value.replace(/\n/g,body.includes('\r\n')?'\r\n':'\n');}
const adapterInput='broker/src/weekly-source/upload-staging-adapter.mjs';let adapter=read(adapterInput);
const reportImport="import { REPORT_PROFILE, assertReportUploadRequest, selfBillReportRows } from './report-upload-provenance.mjs';\n";
adapter=change(adapter,"import { XLSX_BINARY64_SAME_VALUE_PENCE_V1 } from './money.js';",lines(adapter,reportImport)+"import { XLSX_BINARY64_SAME_VALUE_PENCE_V1 } from './money.js';",'worker/upload-staging-adapter.mjs');
const reportRow=`  if (parsed.profileId === REPORT_PROFILE) {
    const dto = selfBillReportRows({ ...parsed, rows: [row], report: { ...parsed.report,
      lineCount: 1, netPence: row.pricingEvidence.netPence, vatPence: row.pricingEvidence.vatPence,
      grossPence: row.pricingEvidence.grossPence } }, context)[0];
    return { ...dto, break_minutes: null, actual_net_minutes: null, row_finalisation_state: 'SOURCE_REPORT_LINE',
      finalised_by: null, role_band_source: null, source_commission_pence: null, source_total_cost_pence: null,
      source_money_parse_state: 'VALID', source_qualification_profile_version: 'SOURCE_INVOICE_MONEY_PENCE_V1',
      source_expense_pence: null, source_expense_parse_state: 'NOT_APPLICABLE' };
  }
`;
adapter=change(adapter,'function normalisedRow(parsed, row, context) {','function normalisedRow(parsed, row, context) {'+lines(adapter,'\n'+reportRow),'worker/upload-staging-adapter.mjs');
adapter=change(adapter,'function coverage(parsed, body) {','function coverage(parsed, body) {'+lines(adapter,`
  if (parsed.profileId === REPORT_PROFILE) return {
    suggested_coverage_start_local_date: null, suggested_coverage_end_local_date: null,
    confirmed_coverage_start_local_date: null, confirmed_coverage_end_local_date: null,
    coverage_timezone: null, coverage_confirmation_version: null, coverage_confirmed_at_utc: null,
    coverage_shrink_acknowledged: null, coverage_state: 'COMPLETE', coverage_proof_kind: 'SELF_BILL_REPORT_SCOPE',
  };
`),'worker/upload-staging-adapter.mjs');
adapter=change(adapter,'  assertWeeklySourceCoverageFloor(parsed, body, context);',lines(adapter,`
  if (parsed.profileId === REPORT_PROFILE) {
    // actor_user_id was injected by the authenticated owner before this pure adapter.
    assertReportUploadRequest(Object.fromEntries(Object.entries(body).filter(([key]) => key !== 'actor_user_id')));
    selfBillReportRows(parsed, context);
  }
`)+ '  assertWeeklySourceCoverageFloor(parsed, body, context);','worker/upload-staging-adapter.mjs');
adapter=change(adapter,"  const importUse = body.import_use ?? (parsed.profileId === 'NHSP_FINAL_BACKING_V1'", "  const importUse = body.import_use ?? (parsed.profileId === REPORT_PROFILE || parsed.profileId === 'NHSP_FINAL_BACKING_V1'",'worker/upload-staging-adapter.mjs');
adapter=change(adapter,'  const headerMap = Object.fromEntries(',lines(adapter,`
  if (parsed.profileId === REPORT_PROFILE) Object.assign(metadata, {
    self_bill_report_id: context.self_bill_report_id, report_number: parsed.report.reportNumber,
    invoice_date: parsed.report.invoiceDate, report_layout: parsed.report.layout,
  });
`)+ '  const headerMap = Object.fromEntries(','worker/upload-staging-adapter.mjs');
adapter=change(adapter,'      money_lexical_authority_version: NHSP.has(parsed.profileId) ? XLSX_BINARY64_SAME_VALUE_PENCE_V1 : null,',
  "      money_lexical_authority_version: parsed.profileId === REPORT_PROFILE ? 'SOURCE_INVOICE_MONEY_PENCE_V1'\n        : NHSP.has(parsed.profileId) ? XLSX_BINARY64_SAME_VALUE_PENCE_V1 : null,".replace(/\n/g,adapter.includes('\r\n')?'\r\n':'\n'),'worker/upload-staging-adapter.mjs');
adapter=change(adapter,'    moneyEvidence: NHSP.has(parsed.profileId) ? nhspMoneyEvidence(parsed)',lines(adapter,`
    moneyEvidence: parsed.profileId === REPORT_PROFILE ? (parsed.rows ?? []).flatMap(row => [
      ['SOURCE_NET', 'net', row.pricingEvidence.netPence], ['SOURCE_VAT', 'vat', row.pricingEvidence.vatPence],
      ['SOURCE_GROSS', 'gross', row.pricingEvidence.grossPence],
    ].map(([kind, field, money]) => ({ ...evidenceRecord(parsed, row.physicalRow, field,
      row.pricingEvidence.components[field], money, 'VALID'), money_field_kind: kind })))
      : NHSP.has(parsed.profileId) ? nhspMoneyEvidence(parsed)`),'worker/upload-staging-adapter.mjs');
fs.writeFileSync(path.join(worker,'upload-staging-adapter.mjs'),adapter);

const ownerInput='broker/src/weekly-source/upload-publication-owner.mjs';let owner=read(ownerInput);
owner=change(owner,"import { assertMagnitContractPricing, assertMagnitWholeShiftResult } from './magnit-contract-price-admission.js';",lines(owner,`import { REPORT_PROFILE, assertReportUploadRequest, assertReportRuleContext, reportScopeRequest,
  assertReportScopeReceipt, assertReportPublicationContext, assertReportStageRowsReceipt,
  assertReportProjectionRowsReceipt } from './report-upload-provenance.mjs';
`)+"import { assertMagnitContractPricing, assertMagnitWholeShiftResult } from './magnit-contract-price-admission.js';",'worker/upload-publication-owner.mjs');
owner=change(owner,'function parserOptions(body, context) {','function parserOptions(body, context) {'+lines(owner,`
  if (text(body.profile_id || body.parser_options?.profileId) === REPORT_PROFILE) {
    assertReportUploadRequest(body); const values = assertReportRuleContext(context);
    return { profileId: REPORT_PROFILE, expectedClient: context.client_name,
      effectiveRuleSet: context.effective_rule_set, ruleValues: values };
  }
`),'worker/upload-publication-owner.mjs');
const resolver=`async function prepareReportScope(dependencies, body, parsed, context, actorUserId) {
  if (parsed?.ok !== true || parsed.profileId !== REPORT_PROFILE) return { body, context };
  assertReportUploadRequest(body); assertReportRuleContext(context);
  const resolved = assertReportScopeReceipt(await rpc(dependencies,
    'weekly_source_self_bill_report_scope_resolve_atomic_v1', reportScopeRequest(parsed, context, actorUserId)), context);
  const nextBody = { ...body, report_scope_id: resolved.report_scope_id, import_use: 'PREPARE_FINALISATION' };
  const next = validateContext(await rpc(dependencies, 'weekly_source_upload_context_v1', scopeRequest(nextBody, actorUserId)));
  assertReportRuleContext(next);
  if (next.authority_scope_kind !== 'SELF_BILL_REPORT_SCOPE' || next.self_bill_report_id !== resolved.self_bill_report_id
    || next.report_scope_id !== resolved.report_scope_id || next.source_cycle_id !== context.source_cycle_id
    || next.source_group_id !== context.source_group_id || next.client_id !== context.client_id
    || Number(next.authority_scope_version) !== Number(resolved.authority_scope_version)
    || JSON.stringify(stable(next.effective_rule_set)) !== JSON.stringify(stable(context.effective_rule_set))) {
    fail('WEEKLY_SOURCE_REPORT_SCOPE_CHANGED', 'The report scope changed before staging.', 409);
  }
  return { body: nextBody, context: next };
}

`;
owner=change(owner,'function nhspFinalTrust(parsed) {',lines(owner,resolver)+'function nhspFinalTrust(parsed) {','worker/upload-publication-owner.mjs');
owner=change(owner,'export function buildWeeklySourceProjectionRows(context, body = {}) {','export function buildWeeklySourceProjectionRows(context, body = {}) {'+lines(owner,`
  if (context?.profile_id === REPORT_PROFILE || context?.authority_scope_kind === 'SELF_BILL_REPORT_SCOPE') {
    assertReportPublicationContext(context, { ...context, upload_id: context.upload_id });
    return []; // Native Report owner binds existing export authority; no price/contract calculator.
  }
`),'worker/upload-publication-owner.mjs');
const stageRowsStart=owner.indexOf("      await rpc(dependencies, 'weekly_source_upload_stage_rows_atomic_v1', {");
const stageRowsEnd=owner.indexOf('      });',stageRowsStart)+9;
if(stageRowsStart<0)throw new Error('Stage rows native call');
const stageRowsBefore=owner.slice(stageRowsStart,stageRowsEnd);
const stageRowsAfter=stageRowsBefore.replace('      await rpc(',()=> '      const stageRowsReceipt = await rpc(')
  +lines(owner,`\n      if (parsed.profileId === REPORT_PROFILE) assertReportStageRowsReceipt(stageRowsReceipt, uploadId);`);
owner=change(owner,stageRowsBefore,stageRowsAfter,'worker/upload-publication-owner.mjs');
const projectionStart=owner.indexOf("  await rpc(dependencies, 'weekly_source_projection_rows_apply_atomic_v1', {");
const projectionEnd=owner.indexOf('  }, { request: false });',projectionStart)+'  }, { request: false });'.length;
if(projectionStart<0)throw new Error('Native projection apply call');
const projectionBefore=owner.slice(projectionStart,projectionEnd);
const projectionAfter=projectionBefore.replace('  await rpc(',()=> '  const projectionRowsReceipt = await rpc(')
  +lines(owner,`\n  if (context.profile_id === REPORT_PROFILE) assertReportProjectionRowsReceipt(projectionRowsReceipt, begun.publication_id);`);
owner=change(owner,projectionBefore,projectionAfter,'worker/upload-publication-owner.mjs');
const recheckStart=owner.indexOf("        await rpc(dependencies, 'weekly_source_projection_rows_apply_atomic_v1', {");
const recheckEnd=owner.indexOf('        }, { request: false });',recheckStart)+'        }, { request: false });'.length;
if(recheckStart<0)throw new Error('Native saved recheck apply call');
const recheckBefore=owner.slice(recheckStart,recheckEnd);
const recheckAfter=recheckBefore.replace('        await rpc(',()=> '        const reportRecheckRowsReceipt = await rpc(')
  +lines(owner,`\n        if (context.profile_id === REPORT_PROFILE) assertReportProjectionRowsReceipt(reportRecheckRowsReceipt, begun.publication_id);`);
owner=change(owner,recheckBefore,recheckAfter,'worker/upload-publication-owner.mjs');
owner=change(owner,"  const publishedContext = body.purpose === 'FINAL_SOURCE_CORRECTION'", "  const publishedContext = (body.purpose === 'FINAL_SOURCE_CORRECTION' || parsed.profileId === REPORT_PROFILE)",'worker/upload-publication-owner.mjs');
owner=change(owner,'  return { adapted, started, sealed, published, publishedContext, uploadId, version };',lines(owner,`
  if (parsed.profileId === REPORT_PROFILE) {
    assertReportPublicationContext(publishedContext, { ...context, upload_id: uploadId });
    if (published.status !== 'CURRENT' || publishedContext.projection_state !== 'CURRENT'
      || publishedContext.projection_publication_id !== published.publication_id
      || publishedContext.row_manifest_hash !== (sealed.row_manifest_hash ?? started.row_manifest_hash)) {
      fail('WEEKLY_SOURCE_REPORT_PUBLICATION_STALE', 'The accepted report publication could not be proved.', 409);
    }
    requiredSha256(publishedContext.comparison_manifest_hash, 'Report comparison proof');
    requiredSha256(publishedContext.issue_set_hash, 'Report issue proof');
  }
`)+ '  return { adapted, started, sealed, published, publishedContext, uploadId, version };','worker/upload-publication-owner.mjs');
owner=change(owner,'      const duplicate = parsed?.ok === true ? duplicateSourceKey(parsed) : null;',lines(owner,`
      ({ body, context } = await prepareReportScope(dependencies, body, parsed, context, actorUserId));
`)+ '      const duplicate = parsed?.ok === true ? duplicateSourceKey(parsed) : null;','worker/upload-publication-owner.mjs');
const acceptStart="    async acceptUpload({ body = {}, bytes, actor, parseWeeklySourceFile }) {";
const start=owner.indexOf(acceptStart),end=owner.indexOf('    async stageReplacementSource',start);
if(start<0||end<0)throw new Error('Accept owner boundary');
const acceptBefore=owner.slice(start,end);let accept=acceptBefore;
accept=accept.replace('      const context = validateContext(',()=> '      let context = validateContext(');
accept=accept.replace('      const result = await stageParsed(',()=>lines(owner,`      ({ body, context } = await prepareReportScope(dependencies, body, parsed, context, actorUserId));
`)+ '      const result = await stageParsed(');
accept=accept.replace('        source_profile: parsed.profileId,',()=>lines(owner,`        source_profile: parsed.profileId,
        ...(parsed.profileId === REPORT_PROFILE ? {
          authority_scope_kind: 'SELF_BILL_REPORT_SCOPE', report_scope_id: context.report_scope_id,
          self_bill_report_id: context.self_bill_report_id, invoice_only: true,
          row_manifest_hash: result.publishedContext.row_manifest_hash,
          comparison_manifest_hash: result.publishedContext.comparison_manifest_hash,
          issue_set_hash: result.publishedContext.issue_set_hash,
        } : {}),`));
owner=change(owner,acceptBefore,accept,'worker/upload-publication-owner.mjs');
fs.writeFileSync(path.join(worker,'upload-publication-owner.mjs'),owner);

// A narrow native context repair is necessary: the frozen Report early return
// omitted environment/agency/profile/version required by the actual owner.
const contextFile='supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_upload_context.sqlinc';
let context=read(contextFile);
const earlyBefore=`    return jsonb_build_object('ok',true,'context_version','WEEKLY_SOURCE_UPLOAD_CONTEXT_V1',
      'source_group_id',v_group.id,'source_cycle_id',v_cycle.id,'report_scope_id',v_scope.id,
      'client_id',v_client_id,'client_name',v_client_name,'upload_id',v_upload.id,
      'self_bill_report_id',v_scope.self_bill_report_id,'invoice_only',true,'rows','[]'::jsonb);`;
const earlyAfter=`    select * into strict v_publication from public.weekly_source_projection_publications
      where upload_id=v_upload.id and authority_scope_kind='SELF_BILL_REPORT_SCOPE'
        and report_scope_id=v_scope.id and authority_scope_version=v_scope.version and state in ('BUILDING','CURRENT');
    return jsonb_build_object('ok',true,'context_version','WEEKLY_SOURCE_UPLOAD_CONTEXT_V1',
      'environment',v_group.environment,'agency_id',v_group.agency_id,'source_family',v_group.source_family,
      'source_group_id',v_group.id,'source_cycle_id',v_cycle.id,'report_scope_id',v_scope.id,
      'client_id',v_client_id,'client_name',v_client_name,'upload_id',v_upload.id,
      'authority_scope_kind','SELF_BILL_REPORT_SCOPE','authority_scope_version',v_scope.version,
      'profile_id','ACACIUM_SELF_BILL_REPORT_V1','self_bill_report_id',v_scope.self_bill_report_id,
      'effective_rule_set',private.weekly_source_effective_rule_set_v1(v_client_id,v_cycle.id),
      'row_manifest_hash',encode(v_upload.row_manifest_hash,'hex'),
      'projection_publication_id',v_publication.id,'projection_state',v_publication.state,
      'comparison_manifest_hash',encode(v_publication.comparison_manifest_hash,'hex'),
      'issue_set_hash',encode(v_publication.issue_set_hash,'hex'),'invoice_only',true,'rows','[]'::jsonb);`;
const earlyStart=context.indexOf(earlyBefore.split('\n')[0]);
if(earlyStart<0)throw new Error('Report early context anchor');
const earlyActual=context.slice(earlyStart,context.indexOf(');',earlyStart)+2);
if(earlyActual.replace(/\r\n/g,'\n')!==earlyBefore)throw new Error('Report early context drift');
context=change(context,earlyActual,lines(context,earlyAfter),'08_complete_upload_context.sqlinc');
const discoverBefore="    )||case when v_coverage_context is null then '{}'::jsonb";
context=change(context,discoverBefore,lines(context,`    )||case when v_scope.self_bill_report_id is null then '{}'::jsonb
      else jsonb_build_object('self_bill_report_id',v_scope.self_bill_report_id) end
      ||case when v_coverage_context is null then '{}'::jsonb`),'08_complete_upload_context.sqlinc');
fs.writeFileSync(path.join(dest,'08_complete_upload_context.sqlinc'),context);

// Source-only diagnosis/forward repair. Frozen Report39 and Money1432 contain
// the two intact regexes. Root composition removed literal $' suffixes. Use a
// callback for every replacement, preserving regex dollars and all other code.
function regexRepair(body,file){for(const field of ['total_pay_pence','calculated_charge_pence']) {
  const broken=`coalesce(v_economic->>'${field}','')!~'^[-]?[0-9]+`;
  const linesBody=body.replace(/\r\n/g,'\n');
  if(linesBody.includes(broken+"$'"))continue;
  body=change(body,broken,broken+"$'",file);
}return body;}
const reportProjection='supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_projection_rows.sqlinc';
fs.writeFileSync(path.join(dest,'09_complete_report_projection_rows_forwardfix.sqlinc'),regexRepair(read(reportProjection),'09_complete_report_projection_rows_forwardfix.sqlinc'));
const m1='supabase/repeatable/13102026_1561_weekly_source_magnit_contract_price_v1.sql';const m1source=read(m1);
const fn='create or replace function public.weekly_source_projection_rows_apply_atomic_v1(';
const pos=m1source.indexOf(fn);if(pos<0)throw new Error('M1 controlling projection missing');
const m1body=m1source.slice(pos,m1source.indexOf('$function$;',pos)+11);
fs.writeFileSync(path.join(dest,'10_complete_m1_projection_rows_forwardfix.sqlinc'),regexRepair(m1body,'10_complete_m1_projection_rows_forwardfix.sqlinc'));

// Copy the actual unchanged relative-import graph for executable local tests.
// These are source copies only, never edits to the owning Worker modules.
const visited=new Set();
function copyGraph(p){if(visited.has(p))return;visited.add(p);const content=read(p);
  const relative=p.slice('broker/src/weekly-source/'.length),target=path.join(worker,relative);
  if(![adapterInput,ownerInput].includes(p)){fs.mkdirSync(path.dirname(target),{recursive:true});fs.writeFileSync(target,content);}
  for(const match of content.matchAll(/(?:from\s+|import\s*)['"]([^'"]+)['"]/g)) {
    if(!match[1].startsWith('.'))continue;const dependency=path.posix.normalize(path.posix.join(path.posix.dirname(p),match[1]));
    if(!dependency.startsWith('broker/src/weekly-source/'))throw new Error('Unexpected source import '+dependency);
    copyGraph(dependency);
  }
}
copyGraph(adapterInput);copyGraph(ownerInput);copyGraph('broker/src/weekly-source/index.js');
for(const p of ['supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_row_authority.sqlinc',
  'supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_upload_rows.sqlinc',
  'supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_projection_begin.sqlinc',
  'supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_projection_publish.sqlinc'])read(p);
for(const [input,output] of [
  ['tests/weekly-source/weekly-source-upload-publication-owner.test.mjs','native-upload-publication-regression.test.mjs'],
  ['tests/weekly-source/parsers/acacium-report.test.js','native-report-parser-regression.test.mjs'],
]) {
  const source=read(input);
  const next=source.replace(/from\s+(['"])(?:\.\.\/)+broker\/src\/weekly-source\//g,()=> "from './worker/");
  fs.writeFileSync(path.join(dest,output),next);
}
fs.writeFileSync(path.join(dest,'WORKER_HUNKS.json'),JSON.stringify(hunks,null,2)+'\n');
fs.writeFileSync(path.join(dest,'WORKER_INPUTS.json'),JSON.stringify({schema_version:'REPORT_NATIVE_WORKER_INPUTS_V1',inputs:[...new Map(pins.map(p=>[p.path,p])).values()]},null,2)+'\n');
console.log(JSON.stringify({generated:true,worker_source_inputs:visited.size,recorded_hunks:hunks.length,root_or_frozen_edits:0}));
