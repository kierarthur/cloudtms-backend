// Local-only genuine saved calculator/composer/SQL-owner proof. Never invokes
// a Worker, hosted database, provider, email or payment. The optional exact
// local POSITION proof advances only fixture-created publication commands. All
// fixture facts live in one outer transaction and are unconditionally rolled back.
import assert from 'node:assert/strict';
import { readFile as readSavedFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { spawn } from 'node:child_process';
import path from 'node:path';
import Module from 'node:module';
import { build } from 'esbuild';
import { orchestrateWeeklyProtectedAction } from '../broker/src/weekly-source/protected-action-orchestrator.mjs';
import { weeklySourceAuthoriseRouting } from '../broker/src/weekly-source/authorise-routing.mjs';
import { decodeNextPaidEvidencePage } from '../broker/src/weekly-source/next-paid-evidence.mjs';

const root = path.resolve(import.meta.dirname, '..');
const container = 'codex-bpay-reset-release-pg17-20261003';
const database = 'source_local_joined_20261005';
const bankRoot = path.resolve(root, '../banking-pay-reset-implementation-20261003');
const loadedPins=new Map();
async function readFile(filename,encoding) {
  const absolute=path.resolve(filename);
  const bytes=await readSavedFile(absolute);
  const sha256=createHash('sha256').update(bytes).digest('hex');
  if(loadedPins.has(absolute)) assert.equal(loadedPins.get(absolute),sha256,'loaded source changed during proof');
  loadedPins.set(absolute,sha256);
  return encoding ? bytes.toString(encoding) : bytes;
}
const finalSourceReconcile=process.argv.includes('--final-source-reconcile');
const finalSourceAcceptance=process.argv.includes('--final-source-accept') || finalSourceReconcile;
const rosterRebindProof=process.argv.includes('--roster-rebind');
const sourceAbsenceKind=process.argv.includes('--nhsp-removal') || finalSourceAcceptance ? 'NHSP' :
  process.argv.includes('--roster-removal') || rosterRebindProof ? 'ROSTER' : null;
const manualQueryProof = process.argv.includes('--manual-query') || sourceAbsenceKind!==null;
const child = spawn('docker', ['exec', '-i', container, 'psql', '-X', '-qtA',
  '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', database], { stdio: ['pipe', 'pipe', 'pipe'] });
let output = '', errors = '', sequence = 0, pending = null, childExited = false;
child.stdout.setEncoding('utf8');
child.stderr.setEncoding('utf8');
child.stdout.on('data', (chunk) => {
  output += chunk;
  while (output.includes('\n')) {
    const end = output.indexOf('\n');
    const line = output.slice(0, end).trim();
    output = output.slice(end + 1);
    if (pending && line.startsWith(pending.marker)) {
      const { resolve, timer, marker } = pending;
      pending = null;
      clearTimeout(timer);
      resolve(JSON.parse(line.slice(marker.length)));
    }
  }
});
child.stderr.on('data', (chunk) => { errors += chunk; });
child.on('exit', (code) => {
  childExited = true;
  if (pending) {
    clearTimeout(pending.timer);
    const diagnosticHeaders = errors.split(/\r?\n/)
      .filter(line => /^(ERROR|DETAIL|HINT):/.test(line)).slice(-6).join('\n');
    pending.reject(new Error(`Local SQL stopped (${code}): ${diagnosticHeaders}\n${errors.slice(-3500)}`));
    pending = null;
  }
});
function sql(batch) {
  if(childExited) return Promise.reject(new Error(`Local SQL connection already stopped: ${errors.slice(-5000)}`));
  assert.equal(pending, null, 'single sequential owner connection');
  return new Promise((resolve, reject) => {
    const marker = `BP_SOURCE_LOCAL_${++sequence}:`;
    const timer = setTimeout(() => {
      pending = null;
      child.stdin.end();
      reject(new Error(`Local SQL timeout: ${errors.slice(-2500)}`));
    }, 125000);
    pending = { marker, timer, resolve, reject };
    child.stdin.write(`${batch}\nselect '${marker}' || '{"ok":true}';\n`);
  });
}
async function query(expression) {
  if(childExited) throw new Error(`Local query connection already stopped: ${errors.slice(-5000)}`);
  assert.equal(pending, null);
  return new Promise((resolve, reject) => {
    const marker = `BP_SOURCE_LOCAL_${++sequence}:`;
    const timer = setTimeout(() => {
      pending = null;
      child.stdin.end();
      reject(new Error(`Local query timeout: ${errors.slice(-2500)}`));
    }, 125000);
    pending = { marker, timer, resolve, reject };
    child.stdin.write(`select '${marker}' || jsonb_build_object('value',(${expression}))::text;\n`);
  }).then((result) => result.value);
}
const quote = (value) => `'${JSON.stringify(value).replaceAll("'", "''")}'::jsonb`;
const names = new Set(['weekly_exceptional_pay_prepare_family_v1', 'weekly_exceptional_pay_prepare_action_v1',
  'weekly_exceptional_pay_action_context_v1', 'weekly_exceptional_pay_action_publication_status_v1',
  'weekly_source_target_managed_root_prepare_atomic_v1', 'weekly_exceptional_pay_stage_c1_request_v1',
  'weekly_exceptional_pay_complete_local_v1', 'weekly_exceptional_pay_wait_atomic_v1']);
const calls = [];
let lastContext;
let calculatorBundleSha256;
let freshAdmissionProved = false;
let genuineFinalRevisionId = null;
async function holdAdmissionElsewhere() {
  const holder = spawn('docker', ['exec','-i',container,'psql','-X','-qtA',
    '-v','ON_ERROR_STOP=1','-U','postgres','-d',database], { stdio:['pipe','pipe','pipe'] });
  holder.stdout.setEncoding('utf8');
  holder.stderr.setEncoding('utf8');
  let holderError = '';
  const ended = new Promise((resolve,reject) => {
    holder.once('error',reject);
    holder.once('exit',(code) => code===0 ? resolve() : reject(new Error(holderError)));
  });
  const held = new Promise((resolve,reject) => {
    let received = '';
    holder.stdout.on('data',(chunk) => {
      received += chunk;
      if(received.includes('SOURCE_ADMISSION_HELD')) resolve();
    });
    holder.stderr.on('data',(chunk) => { holderError += chunk; });
    holder.once('error',reject);
    holder.once('exit',(code) => { if(code!==0) reject(new Error(holderError)); });
  });
  holder.stdin.write(`begin; set local statement_timeout='10s';
    select pg_advisory_xact_lock(hashtextextended('CLOUDTMS:BPAY_NEXT:SOURCE_PAY_QUERY_ADMISSION:V2',0));
    select 'SOURCE_ADMISSION_HELD';\n`);
  await held;
  return async () => { holder.stdin.end('rollback;\n'); await ended; };
}
async function loadAttentionOwners(){
  for(const [filename,name] of [
    ['15092026_1534_weekly_source_read_projections_v1.sql','weekly_source_office_query_groups_v1'],
    ['01102026_1201_weekly_source_combined_workspace.sql','weekly_source_protected_query_rows_v1'],
    ['01102026_1201_weekly_source_combined_workspace.sql','weekly_source_combined_review_workspace_v1'],
  ]){
    const saved=await readFile(path.join(root,'supabase/repeatable',filename),'utf8');
    const defs=[...saved.matchAll(new RegExp('create or replace function (?:public|private)\\.'+name+'\\([\\s\\S]*?\\$function\\$;','gi'))];
    assert.equal(defs.length,1,'one exact attention owner '+name);
    await sql(defs[0][0]);
  }
}
const originalFetch = globalThis.fetch;
globalThis.fetch = async () => { throw new Error('LOCAL_PROOF_EXTERNAL_NETWORK_FORBIDDEN'); };
try {
  await readFile(path.join(root,'tests/run-bp-source-local-native.mjs'),'utf8');
  for (const filename of ['broker/src/weekly-source/protected-action-orchestrator.mjs',
    'broker/src/weekly-source/protected-component-identity.mjs',
    'broker/src/weekly-source/protected-target-schedule.js',
    'broker/src/banking-pay/weekly-source-c1-components.mjs',
    'broker/src/banking-pay/weekly-source-c1-publication.mjs',
    'broker/src/banking-pay/weekly-source-c1-stream.mjs']) {
    await readFile(path.join(root, filename), 'utf8');
  }
  await sql(`begin; set local statement_timeout='120s'; set local lock_timeout='5s';
    set local request.jwt.claim.role='service_role';`);
  assert.equal(await query(`current_database()='${database}'
    and current_setting('server_version_num')::integer between 170000 and 179999
    and not exists(select 1 from public.timesheets)`), true, 'own empty PG17 clone');
  const admissionSource = await readFile(path.join(bankRoot,
    'supabase/repeatable/05102026_0044_bpay_next_source_pay_query_admission_v2.sql'), 'utf8');
  const admissionMatches = [...admissionSource.matchAll(/create or replace function private\.weekly_source_pay_query_admit_v2\([\s\S]*?\$function\$;/gi)];
  assert.equal(admissionMatches.length, 1, 'one actual joint admission helper');
  assert(admissionMatches[0][0].includes('CLOUDTMS:BPAY_NEXT:SOURCE_PAY_QUERY_ADMISSION:V2'),
    'secondary connection uses the exact joint V2 key');
  await sql(`${admissionMatches[0][0]}\nrevoke all on function private.weekly_source_pay_query_admit_v2() from public,anon,authenticated,service_role;`);
  const localSource = await readFile(path.join(root,
    'supabase/repeatable/04102026_1257_weekly_source_local_protected_decision_v1.sql'), 'utf8');
  const localMatches = [...localSource.matchAll(/create or replace function public\.weekly_exceptional_pay_complete_local_v1\([\s\S]*?\$function\$;/gi)];
  assert.equal(localMatches.length, 1, 'one exact saved Source local completion');
  const bankLocalSource = await readFile(path.join(bankRoot,
    'supabase/repeatable/04102026_1257_weekly_source_local_protected_decision_v1.sql'), 'utf8');
  const bankLocalMatches = [...bankLocalSource.matchAll(/create or replace function public\.weekly_exceptional_pay_complete_local_v1\([\s\S]*?\$function\$;/gi)];
  assert.equal(bankLocalMatches.length, 1, 'one winning Banking-integrated Local completion');
  const localIdentityHunk = localMatches[0][0].match(/  -- Reconfirm the staged identity proof[\s\S]*?\n  end if;\r?\n(?=  select count\(\*\) into v_live_authorisations from public\.weekly_source_root_authorisations authority)/);
  assert(localIdentityHunk, 'one finite identity completion hunk');
  const priorContactHunk='  perform private.weekly_source_protected_contact_retire_v1(v_family.id,v_approval.work_event_id);\n';
  const contactRetirementHunk='  -- Accepted pending approval is not publication. The existing release\n'+
    '  -- trigger retires its contacts only after the deferred receipt is proved.\n'+
    '  if not v_pending then\n'+
    '    perform private.weekly_source_protected_contact_retire_v1(v_family.id,v_approval.work_event_id);\n'+
    '  end if;\n';
  assert.equal(localMatches[0][0].split(contactRetirementHunk).length,2,'one post-receipt contact retirement hook');
  const localWithoutContact=localMatches[0][0].replace(contactRetirementHunk,'');
  const bankContactCount=bankLocalMatches[0][0].split(contactRetirementHunk).length-1;
  // The old call text is a substring of the indented guarded call. Remove only
  // the complete exact current block before counting genuine old revisions.
  const bankPriorContactCount=bankLocalMatches[0][0].replace(contactRetirementHunk,'').split(priorContactHunk).length-1;
  assert(bankContactCount===0 || bankContactCount===1,'at most one exact accepted Banking contact hook');
  assert(bankPriorContactCount===0 || bankPriorContactCount===1,'at most one exact prior Banking contact hook');
  assert(bankContactCount+bankPriorContactCount<=1,'never both contact hook revisions');
  const sourceForBankComparison=bankContactCount===1 ? localMatches[0][0] : bankPriorContactCount===1
    ? localMatches[0][0].replace(contactRetirementHunk,priorContactHunk) : localWithoutContact;
  assert.equal(bankLocalMatches[0][0].includes(localIdentityHunk[0])
    ? sourceForBankComparison : sourceForBankComparison.replace(localIdentityHunk[0], ''), bankLocalMatches[0][0],
    'complete Banking Local owner matches exactly, retaining only already-integrated identity/contact hunks');
  await sql(localMatches[0][0]);
  for (const [filename, functionName] of [
    ['05102026_0201_weekly_source_local_origin_canonical_v2.sql', 'weekly_source_local_origin_canonical_v2'],
    ['05102026_0212_weekly_source_local_publication_context_v2.sql', 'weekly_source_local_publication_context_v2'],
    ['05102026_0114_weekly_source_pay_query_evidence_v1.sql', 'weekly_source_pay_query_local_receipt_v1'],
    ['05102026_1417_weekly_source_coverage_support/qualified_coverage.inc', 'weekly_source_pay_query_facts_v1'],
    ['05102026_1417_weekly_source_coverage_support/qualified_coverage.inc', 'weekly_source_covered_hours_incident_v1'],
    ['05102026_0308_weekly_source_pay_query_gate_v1.sql', 'weekly_source_pay_query_gate_v1'],
    ['05102026_0834_weekly_source_protected_contact_retirement_v1.sql', 'weekly_source_protected_contact_retire_v1'],
    ['05102026_0444_weekly_source_protected_saved_prepare_route_v1.sql', 'weekly_source_protected_saved_prepare_route_v1'],
    ['05102026_0624_weekly_source_local_saved_status_v1.sql', 'weekly_source_local_saved_status_v1'],
    ['04102026_2338_weekly_source_candidate_saved_local_hours_v2.sql', 'weekly_source_candidate_saved_local_hours_v2'],
    ['05102026_0100_weekly_source_candidate_approved_clock_row_v2.sql', 'weekly_source_candidate_approved_clock_row_v2'],
  ]) {
    const saved = await readFile(path.join(root, 'supabase/repeatable', filename), 'utf8');
    const definitions = [...saved.matchAll(new RegExp(
      `create or replace function private\\.${functionName}\\([\\s\\S]*?\\$function\\$;`, 'gi'))];
    assert.equal(definitions.length, 1, `one exact saved factual reader ${functionName}`);
    await sql(definitions[0][0]);
  }
  const stripLocal=(text)=>text.replace(/^\\set.*$/gm,'').replace(/^begin;\r?$/gmi,'').replace(/^commit;\r?$/gmi,'');
  await sql(stripLocal(await readFile(path.join(root,
    'supabase/repeatable/05102026_1210_weekly_source_office_authorise_scope_v1.sql'),'utf8')));
  await sql(stripLocal(await readFile(path.join(root,
    'supabase/repeatable/05102026_1242_weekly_source_office_next_paid_context_v1.sql'),'utf8')));
  if(process.argv.includes('--next-paid-context')) {
    await readFile(path.join(root,'broker/src/weekly-source/next-paid-evidence.mjs'),'utf8');
    const origin=await readFile(path.join(bankRoot,
      'supabase/repeatable/04102026_2106_banking_pay_next_source_origin_state_v1.sql'),'utf8');
    const originDefinitions=[...origin.matchAll(/create or replace function private\.bpay_next_source_origin_state_v1\([\s\S]*?\$function\$;/gi)];
    assert.equal(originDefinitions.length,1);await sql(originDefinitions[0][0]);
    await sql(stripLocal(await readFile(path.join(bankRoot,
      'supabase/repeatable/05102026_1112_bpay_next_source_paid_evidence_page_v1.sql'),'utf8')));
    const category=await readFile(path.join(root,
      'supabase/repeatable/05102026_0512_weekly_source_operational_category_v2.sql'),'utf8');
    const categoryDefinitions=[...category.matchAll(/create or replace function private\.weekly_source_(?:operational_empty_v1|timesheet_category_v2)\([\s\S]*?\$function\$;/gi)];
    assert.equal(categoryDefinitions.length,2);
    for(const definition of categoryDefinitions) await sql(definition[0]);
    const projection=await readFile(path.join(root,
      'supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql'),'utf8');
    for(const name of ['weekly_source_office_proposal_revision_v1','weekly_source_office_proposal_view_v1',
      'weekly_source_office_lifecycle_phase_v1','weekly_source_office_timesheet_presentation_v1']) {
      const definitions=[...projection.matchAll(new RegExp('create or replace function (?:private|public)\\.'+name+'\\([\\s\\S]*?\\$function\\$;','gi'))];
      assert.equal(definitions.length,1);await sql(definitions[0][0]);
    }
  }
  await readFile(path.join(root,'broker/src/weekly-source/authorise-routing.mjs'),'utf8');
  for(const filename of ['supabase/migrations/05102026_0428_weekly_source_manual_review_commands.sql',
    'supabase/repeatable/05102026_0428_weekly_source_manual_review_commands_v2.sql',
    'supabase/repeatable/05102026_0645_weekly_source_selected_source_witness_v1.sql',
    'supabase/repeatable/05102026_0710_weekly_source_invoice_duty_v1.sql',
    'supabase/repeatable/05102026_0740_weekly_source_approval_duty_v1.sql',
    'supabase/repeatable/05102026_0650_weekly_source_accepted_removal_query_evidence_v1.sql']) {
    await sql(stripLocal(await readFile(path.join(root,filename),'utf8')));
  }
  const cutover = await readFile(path.join(root,
    'supabase/migrations/05102026_0355_weekly_source_local_flat_origin_cutover_v1.sql'), 'utf8');
  // Execute the real read-only fence inside our existing outer rollback, never
  // its standalone transaction delimiters. No Local common publication exists.
  await sql(cutover.replace(/^\\set[^\r\n]*$/gm, '')
    .replace(/^begin;\s*$/gmi, '').replace(/^commit;\s*$/gmi, ''));
  // Use the four Banking-integrated owners, never competing whole Source
  // pages that omit Banking hooks. Selected bodies live only in this rollback.
  const contextSource = await readFile(path.join(bankRoot,
    'supabase/repeatable/15092026_1534_weekly_source_protected_action_orchestration_v1.sql'), 'utf8');
  // The current Banking factual selectors share one pure owner discriminator.
  // Load its exact saved definition, not a test substitute or a whole page.
  const protectedBankSource=await readFile(path.join(bankRoot,
    'supabase/repeatable/04102026_0345_banking_pay_next_protected_source_v1.sql'),'utf8');
  const routeBankSource=await readFile(path.join(bankRoot,
    'supabase/repeatable/includes/05102026_0659_bpay_next_protected_owner_route_v1.sqlinc'),'utf8');
  for(const [name,source] of [['bpay_next_protected_owner_route_v1',routeBankSource],
    ['bpay_next_protected_local_receipt_v1',protectedBankSource]]) {
    const definitions=[...source.matchAll(new RegExp(
      'create or replace function private\\.'+name+'\\([\\s\\S]*?\\$function\\$;','gi'))];
    assert.equal(definitions.length,1,'exact Banking read-only '+name);
    await sql(definitions[0][0]);
  }
  const contextMatches = [...contextSource.matchAll(/create or replace function public\.weekly_exceptional_pay_action_context_v1\(\s*p_request jsonb\s*\)[\s\S]*?\$function\$;/gi)];
  assert.equal(contextMatches.length, 1, 'one actual context owner');
  const waitMatches = [...contextSource.matchAll(/create or replace function public\.weekly_exceptional_pay_wait_atomic_v1\([\s\S]*?\$function\$;/gi)];
  assert.equal(waitMatches.length, 1, 'one actual Banking-integrated informational Wait owner');
  const waitSource = await readFile(path.join(root,
    'supabase/repeatable/15092026_1534_weekly_source_protected_action_orchestration_v1.sql'), 'utf8');
  const waitPendingPattern = /  update public\.weekly_exceptional_pending_reconciliation_targets target[\s\S]*?(?=  insert into public\.weekly_exceptional_payment_events\()/;
  const sourceWaitPending = waitSource.slice(waitSource.indexOf(
    'create or replace function public.weekly_exceptional_pay_wait_atomic_v1(')).match(waitPendingPattern);
  // Banking already owns a distinct NEXT Wait branch. Change ONLY its Local/
  // retained C1 branch; retain the entire NEXT branch and current-decision hook.
  const bankWaitPending = waitMatches[0][0].match(/  else\r?\n    -- Retained C1 ownership and its pending transition stay unchanged\.[\s\S]*?(?=\r?\n  end if;\r?\n  insert into public\.weekly_exceptional_payment_events\()/);
  assert(sourceWaitPending, 'exact Source pending-target hunk');
  const testedLocalWaitPending = '  else\n' + sourceWaitPending[0].trimEnd().replace(/^/gm, '  ');
  const sourceWait = waitSource.slice(waitSource.indexOf(
    'create or replace function public.weekly_exceptional_pay_wait_atomic_v1('));
  const waitAdmission = sourceWait.match(/  -- Prepare is a separate RPC\.[\s\S]*?(?=  select family\.\* into strict v_family)/)[0];
  const waitReplay = sourceWait.match(/  if v_completed_replay[\s\S]*?(?=    return pg_catalog\.jsonb_build_object\()/)[0];
  const waitDeclarations = '\n  v_completed_witness public.weekly_exceptional_orchestration_runs%rowtype;\n  v_completed_replay boolean:=false;';
  const waitLockSeam = '  select family.* into strict v_family';
  const waitReplaySeam = "  if v_run.state='COMPLETE' then\n";
  assert.equal(waitMatches[0][0].split(waitLockSeam).length,2,'one fresh Wait lock seam');
  assert.equal(waitMatches[0][0].split(waitReplaySeam).length,2,'one completed Wait return seam');
  let testedWait=waitMatches[0][0];
  if(testedWait.includes(waitDeclarations)){
    for(const hunk of [testedLocalWaitPending,waitAdmission,waitReplay])
      assert(testedWait.includes(hunk),'already-integrated Wait contains exact reviewed Source hunk');
  } else {
  assert(bankWaitPending,'exact pre-integration Banking pending hunk');
  testedWait = testedWait.replace(bankWaitPending[0], testedLocalWaitPending)
    .replace('  v_key text;', '  v_key text;'+waitDeclarations)
    .replace(waitLockSeam,waitAdmission+waitLockSeam)
    .replace(waitReplaySeam,waitReplay);
  assert.equal(testedWait.replace(testedLocalWaitPending, bankWaitPending[0])
    .replace(waitDeclarations,'').replace(waitAdmission,'').replace(waitReplay,waitReplaySeam),waitMatches[0][0],
    'removing only Source pending/admission/replay hunks restores winning Banking Wait byte-for-byte');
  }
  await sql(testedWait);
  const releaseWaitHolder = await holdAdmissionElsewhere();
  try {
    await sql(`do $direct_wait_busy$ begin
      begin
        perform public.weekly_exceptional_pay_wait_atomic_v1(jsonb_build_object(
          'schema_version','WEEKLY_PROTECTED_WAIT_V1','actor_user_id','00000000-0000-4000-8000-000000000001',
          'family_id','00000000-0000-4000-8000-000000000002',
          'orchestration_run_id','00000000-0000-4000-8000-000000000003',
          'source_cycle_id','00000000-0000-4000-8000-000000000004',
          'work_event_id','00000000-0000-4000-8000-000000000005',
          'expected_family_bound_version',1,'expected_target_vector_sha256',repeat('ab',32),
          'source_proposal','{}'::jsonb,'protected_schedule','{}'::jsonb,
          'reason','Direct fresh Wait admission proof','idempotency_key','direct-wait-admission-native-proof'));
        raise exception 'DIRECT_WAIT_FAILED_TO_ADMIT';
      exception when lock_not_available then null;
      end;
    end $direct_wait_busy$;`);
    console.log('DIRECT_FRESH_WAIT_ADMITS_BEFORE_FAMILY_LOCK_PASS');
  } finally { await releaseWaitHolder(); }
  // Finite Source identity admission on the winning Banking context. This is
  // an uninstalled integration rehearsal, not a substitute context or I1.
  const identityContextSource = await readFile(path.join(root,
    'supabase/repeatable/15092026_1534_weekly_source_protected_action_orchestration_v1.sql'),'utf8');
  const identityContextBlock = identityContextSource.match(/  -- Local\/common identity admission[\s\S]*?\n  end if;\r?\n/);
  assert(identityContextBlock,'exact saved Source admission block');
  const identityContextDeclarations = '\n  v_component_identity_inventory jsonb;\n  v_component_identity_components jsonb;';
  const identityContextField = "    'approved_component_identity_components',v_component_identity_components,\n";
  let testedContext=contextMatches[0][0];
  const finalContextReturn="  return pg_catalog.jsonb_build_object(\n    'ok',true,'contract','WEEKLY_PROTECTED_ACTION_CONTEXT_V1',";
  assert.equal(testedContext.split(finalContextReturn).length,2,'one exact final context return');
  assert.equal(testedContext.split('  v_rate_refs jsonb;').length,2,'one declaration seam');
  const financialReturn="    'requires_zero_financial',v_fin.id is null,";
  assert.equal(testedContext.split(financialReturn).length,2,'one qualified context field seam');
  if(testedContext.includes(identityContextField)){
    assert(testedContext.includes(identityContextDeclarations)&&testedContext.includes(identityContextBlock[0]),
      'already-integrated Banking context retains exact Source identity qualification');
  } else {
  testedContext=testedContext.replace('  v_rate_refs jsonb;','  v_rate_refs jsonb;'+identityContextDeclarations)
    .replace(finalContextReturn,identityContextBlock[0]+'\n'+finalContextReturn)
    .replace(financialReturn,identityContextField+financialReturn);
  assert.equal(testedContext.replace(identityContextDeclarations,'')
    .replace(identityContextBlock[0]+'\n','').replace(identityContextField,''),contextMatches[0][0],
    'removing only Source identity hunks restores winning Banking context byte-for-byte');
  }
  await sql(testedContext);
  const stageSource = await readFile(path.join(bankRoot,
    'supabase/repeatable/15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql'), 'utf8');
  const stageMatches = [...stageSource.matchAll(/create or replace function public\.weekly_exceptional_pay_stage_c1_request_v1\([\s\S]*?\$function\$;/gi)];
  assert.equal(stageMatches.length, 1, 'one exact integrated stage owner');
  let testedStage=stageMatches[0][0];
  const identityStageSource=await readFile(path.join(root,
    'supabase/repeatable/15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql'),'utf8');
  const identityStageBlock=identityStageSource.match(/  -- Identity metadata is admitted[\s\S]*?\n    'protected_component_identity_basis',v_component_identity_basis\);\r?\n/);
  assert(identityStageBlock,'exact saved Source stage identity qualification');
  const identityStageDeclarations='\n  v_component_identity_context jsonb;\n  v_component_identity_basis jsonb;';
  const inventorySeam='  -- Seal the common before-position at approval.';
  assert.equal(testedStage.split(inventorySeam).length,2,'one stage inventory seal seam');
  assert.equal(testedStage.split('  v_target_snapshot jsonb;').length,2,'one stage declaration seam');
  if(testedStage.includes(identityStageBlock[0])){
    assert(testedStage.includes(identityStageDeclarations),'integrated Banking Stage has exact Source identity declarations');
  } else {
  testedStage=testedStage.replace('  v_target_snapshot jsonb;','  v_target_snapshot jsonb;'+identityStageDeclarations)
    .replace(inventorySeam,identityStageBlock[0]+inventorySeam);
  assert.equal(testedStage.replace(identityStageDeclarations,'').replace(identityStageBlock[0],''),stageMatches[0][0],
    'removing only Source identity hunks restores winning Banking stage byte-for-byte');
  }
  if(process.argv.includes('--withdraw') || sourceAbsenceKind) {
    // Proposed EXACT Source hunks on the actual winning Banking stage, only
    // inside this rollback. This does not claim installed/merged authority.
    const proposedStage=await readFile(path.join(root,
      'supabase/repeatable/15092026_1534_weekly_source_protected_pay_c1_publication_v1.sql'),'utf8');
    const guard=proposedStage.match(/  -- Existing WITHDRAW\/RECONCILE actions[\s\S]*?\n  end if;\r?\n(?=\s*if \(v_start->>'actor_user_id')/);
    assert(guard,'exact proposed guard bounded by next unchanged validation');
    const seam="  if (v_start->>'actor_user_id')::uuid is distinct from v_actor_user_id";
    assert.equal(testedStage.split(seam).length,2,'one winning stage guard seam');
    if(!testedStage.includes('  -- Existing WITHDRAW/RECONCILE actions')) {
      assert(!testedStage.includes('v_selected_source_witness jsonb;'),'no partial prior witness admission');
      testedStage=testedStage.replace('  v_source_proposal jsonb;',
        '  v_source_proposal jsonb;\n  v_selected_source_witness jsonb;')
        .replace(seam,guard[0]+'\n'+seam);
    }
  }
  await sql(testedStage);
  const writerSource = await readFile(path.join(bankRoot,
    'supabase/repeatable/21072026_1235_37_tsfin_write_current_snapshot_single_bounded.sql'), 'utf8');
  const writerMatches = [...writerSource.matchAll(/create or replace function public\.tsfin_write_current_snapshot_single_bounded\([\s\S]*?\$function\$;/gi)];
  assert.equal(writerMatches.length, 1, 'one exact bounded TSFIN writer');
  await sql(writerMatches[0][0]);
  const extrasSource = await readFile(path.join(bankRoot, 'supabase/repeatable/19012026_extras.sql'), 'utf8');
  const extrasMatches = [...extrasSource.matchAll(/create or replace function public\.tsfin_prepare_write\([\s\S]*?\$function\$;/gi)];
  assert.equal(extrasMatches.length, 1, 'one exact guarded prepare owner');
  await sql(extrasMatches[0][0]);
  // HANDOVER 2 owns this verifier. Load its exact saved candidate definition
  // only inside this rollback proof; never copy it into Source product SQL.
  const bankStageSource = await readFile(path.join(bankRoot,
    'supabase/repeatable/03102026_1550_banking_pay_next_source_stage_v1.sql'), 'utf8');
  const bankStageMatches = [...bankStageSource.matchAll(/create or replace function private\.bpay_next_stage_source_current_v1\([\s\S]*?\$function\$;/gi)];
  assert.equal(bankStageMatches.length, 1, 'one actual Banking stage verifier');
  await sql(bankStageMatches[0][0]);
  const commonSource = await readFile(path.join(bankRoot,
    'supabase/repeatable/17092026_0300_weekly_source_entitlement_publication_v1.sql'), 'utf8');
  const commonMatches = [...commonSource.matchAll(/create or replace function private\.weekly_source_publication_request_canonical_v1\([\s\S]*?\$function\$;/gi)];
  assert.equal(commonMatches.length, 1, 'one exact integrated common request codec');
  await sql(commonMatches[0][0]);
  const detailSource = await readFile(path.join(bankRoot,
    'supabase/repeatable/05102026_0302_banking_pay_next_local_source_chosen_detail_v1.sql'), 'utf8');
  const detailMatches = [...detailSource.matchAll(/create or replace function private\.bpay_next_capture_local_source_detail_v1\([\s\S]*?\$function\$;/gi)];
  assert.equal(detailMatches.length, 1, 'one actual Local chosen-detail owner');
  await sql(detailMatches[0][0]);
  const coreSource = await readFile(path.join(bankRoot,
    'supabase/repeatable/04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql'), 'utf8');
  const coreMatches = [...coreSource.matchAll(/create or replace function private\.weekly_source_entitlement_publish_core_v1\([\s\S]*?\$function\$;/gi)];
  assert.equal(coreMatches.length, 1, 'one actual integrated common publisher');
  await sql(coreMatches[0][0]);
  for (const [filename, functionName] of [
    ['15092026_1534_weekly_source_protected_pay_publisher_v1.sql','weekly_exceptional_pay_prepare_family_v1'],
    ['15092026_1534_weekly_source_protected_action_orchestration_v1.sql','weekly_exceptional_pay_prepare_action_v1'],
    ['15092026_1534_weekly_source_protected_action_orchestration_v1.sql','weekly_exceptional_pay_action_publication_status_v1'],
  ]) {
    const saved = await readFile(path.join(root,'supabase/repeatable',filename),'utf8');
    const matches = [...saved.matchAll(new RegExp(
      `create or replace function public\\.${functionName}\\([\\s\\S]*?\\$function\\$;`,'gi'))];
    assert.equal(matches.length,1,`one actual Source outer owner ${functionName}`);
    await sql(matches[0][0]);
  }
  // Source-owned finite Local agency arm, with unchanged Final recorder branch.
  const recordSource = await readFile(path.join(root,
    'supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql'), 'utf8');
  for(const functionName of ['weekly_source_protected_component_event_v1',
    'weekly_source_protected_component_manifest_v1','weekly_source_entitlement_components_v1']) {
    const identityDefinitions=[...recordSource.matchAll(new RegExp(
      'create or replace function private\\.'+functionName+'\\([\\s\\S]*?\\$function\\$;','gi'))];
    assert.equal(identityDefinitions.length,1,'one exact saved Source '+functionName);
    await sql(identityDefinitions[0][0]);
  }
  const recordMatches = [...recordSource.matchAll(/create or replace function private\.weekly_source_entitlement_proposal_record_v1\([\s\S]*?\$function\$;/gi)];
  assert.equal(recordMatches.length, 1, 'one actual proposal recorder');
  await sql(recordMatches[0][0]);
  if(process.argv.includes('--next-paid-context')) {
    const builders=[...recordSource.matchAll(/create or replace function private\.weekly_source_entitlement_proposal_request_v1\([\s\S]*?\$function\$;/gi)];
    assert.equal(builders.length,1);await sql(builders[0][0]);
  }

  async function proveWithdrawal() {
    assert(!manualQueryProof,'no-Source withdrawal branch, not a fabricated final imported row');
    // Keep the real RPC's initially-deferred FK posture until all genuine
    // atomic owners complete; flush only at the end of this journey.
    await sql('set constraints all deferred;');
    const currentFamily=await query(`(select to_jsonb(f) from public.weekly_exceptional_pay_target_families f
      where id='${lastContext.family_id}')`);
    const withdrawal={actor_user_id:request.actor_user_id,source_cycle_id:request.source_cycle_id,
      family_id:lastContext.family_id,work_event_id:lastContext.work_event_id,
      expected_family_bound_version:currentFamily.bound_version,
      reason:'Remove protection and accept certified absent Source.',
      idempotency_key:'source-local-native-20261005-withdraw-no-source'};
    const withdrawn=await orchestrateWeeklyProtectedAction({action:'WITHDRAW_PROTECTED_HOURS',
      request:withdrawal,dependencies});
    assert.equal(withdrawn.ok,true);
    const zero=await query(`private.weekly_source_effective_inventory_v1('${rootId}')`);
    assert.equal(zero.ok,true);
    assert.equal(Number(zero.approval_basis.approved_pay_ex_vat),0);
    assert.equal(zero.components.length,0,'removed only duty has no fabricated worked-time component');
    const invoiceDuty=await query(`private.weekly_source_invoice_duty_v1('${rootId}')`);
    assert.equal(invoiceDuty.present,false,'genuine source-absent empty root owns no invoice task');
    assert.equal(invoiceDuty.discovery_complete,true,'no invoice task is a complete factual negative');
    const approvalDuty=await query(`private.weekly_source_approval_duty_v1('${rootId}')`);
    assert.equal(approvalDuty.present,false,'real completed Local withdrawal has no pending approval duty');
    assert.equal(approvalDuty.discovery_complete,true);
    assert.equal(await query(`(select archived_at_utc is null from public.timesheets where timesheet_id='${rootId}')`),true);
    assert.equal(await query(`(select state='ACCEPTED_SOURCE' from public.weekly_exceptional_pay_family_events
      where family_id='${lastContext.family_id}' order by event_sequence desc limit 1)`),true);
    const zeroBeforeReplay=await query(`jsonb_build_object('inventory',private.weekly_source_effective_inventory_v1('${rootId}'),
      'heads',(select count(*) from public.weekly_source_entitlement_heads),'bankRevisions',(select count(*) from private.bpay_next_work_revision))`);
    const replay=await orchestrateWeeklyProtectedAction({action:'WITHDRAW_PROTECTED_HOURS',request:withdrawal,dependencies});
    assert.deepEqual(replay,{...withdrawn,idempotent_replay:true});
    assert.deepEqual(await query(`jsonb_build_object('inventory',private.weekly_source_effective_inventory_v1('${rootId}'),
      'heads',(select count(*) from public.weekly_source_entitlement_heads),'bankRevisions',(select count(*) from private.bpay_next_work_revision))`),zeroBeforeReplay);
    console.log('GENUINE_CERTIFIED_NO_SOURCE_WITHDRAWAL_ZERO_AND_REPLAY_PASS');
  }
  if(process.argv.includes('--category')) {
    const manualReaders=await readFile(path.join(root,
      'supabase/repeatable/05102026_0428_weekly_source_manual_review_commands_v2.sql'),'utf8');
    for(const name of ['weekly_source_manual_review_final_rows_v2','weekly_source_manual_review_source_v2']) {
      const definitions=[...manualReaders.matchAll(new RegExp(
        'create or replace function private\\.'+name+'\\([\\s\\S]*?\\$function\\$;','gi'))];
      assert.equal(definitions.length,1);
      await sql(definitions[0][0]);
    }
    const witnessReader=await readFile(path.join(root,
      'supabase/repeatable/05102026_0645_weekly_source_selected_source_witness_v1.sql'),'utf8');
    const witnessDefinitions=[...witnessReader.matchAll(
      /create or replace function private\.weekly_source_selected_source_witness_v1\([\s\S]*?\$function\$;/gi)];
    assert.equal(witnessDefinitions.length,1);
    await sql(witnessDefinitions[0][0]);
    const originReader=await readFile(path.join(bankRoot,
      'supabase/repeatable/04102026_2106_banking_pay_next_source_origin_state_v1.sql'),'utf8');
    const originMatches=[...originReader.matchAll(/create or replace function private\.bpay_next_source_origin_state_v1\([\s\S]*?\$function\$;/gi)];
    assert.equal(originMatches.length,1);
    await sql(originMatches[0][0]);
    const category=await readFile(path.join(root,
      'supabase/repeatable/05102026_0512_weekly_source_operational_category_v2.sql'),'utf8');
    const categoryMatches=[...category.matchAll(/create or replace function private\.weekly_source_(?:operational_empty_v1|timesheet_category_v2)\([\s\S]*?\$function\$;/gi)];
    assert.equal(categoryMatches.length,2);
    for(const definition of categoryMatches) await sql(definition[0]);
    const presentation=await readFile(path.join(root,
      'supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql'),'utf8');
    const presentationMatches=[...presentation.matchAll(/create or replace function public\.weekly_source_office_timesheet_presentation_v1\([\s\S]*?\$function\$;/gi)];
    assert.equal(presentationMatches.length,1);
    await sql(presentationMatches[0][0]);
    const batchReader=await readFile(path.join(root,
      'supabase/repeatable/05102026_0559_weekly_source_office_category_batch_v1.sql'),'utf8');
    const batchMatches=[...batchReader.matchAll(/create or replace function public\.weekly_source_office_summary_rows_v1\([\s\S]*?\$function\$;/gi)];
    assert.equal(batchMatches.length,1);
    await sql(batchMatches[0][0]);
    for(const [filename,name] of [
      ['14082026_1310_timesheet_processing_status_and_authorise_authority_v1.sql','bulk_timesheet_workbench_row_source_v1'],
      ['19012026_extras.sql','timesheet_summary_lightweight_rows_v1'],
    ]) {
      const source=await readFile(path.join(root,'supabase/repeatable',filename),'utf8');
      const definitions=[...source.matchAll(new RegExp(
        'create or replace function public\\.'+name+'\\([\\s\\S]*?\\$function\\$;','gi'))];
      assert.equal(definitions.length,1,'exact canonical '+name);
      await sql(definitions[0][0]);
    }
  }

  // Reuse only the factual fixture admission (users/client/contract/group and
  // OPEN cycle). No financial certificate, Final, HEAD, approval or receipt seed.
  const fixture = await readFile(path.join(bankRoot, 'tests/fixtures/bpay-next-protected-source-real.sql'), 'utf8');
  const start = fixture.indexOf('insert into public.settings_defaults');
  const end = fixture.indexOf('create function pg_temp.bp34_assert');
  assert(start > 0 && end > start, 'factual fixture boundaries');
  let importedQuerySource;
  let importedQueryId;
  if (manualQueryProof) {
    // Genuine provisional import and the actual public query-opening owner.
    // Only parser/economic INPUTS come from the reviewed factual fixture;
    // the protected calculator, composer and SQL completion remain real.
    const importFixture=await readFile(path.join(bankRoot,
      'tests/fixtures/bpay-next-source-approved-basis-app-joined-real.sql'),'utf8');
    await sql(`create function pg_temp.bpsc_assert(p_ok boolean,p_message text)
      returns void language plpgsql as $proof_assert$ begin
      if p_ok is distinct from true then raise exception 'SOURCE_QUERY_PROTECTION_ASSERT: %',p_message; end if;
      end $proof_assert$;`);
    const setupStart=importFixture.indexOf('-- Unchanged bounded factual setup');
    const setupEnd=importFixture.indexOf('-- One bounded real upload/finalisation.');
    assert(setupStart>=0 && setupEnd>setupStart);
    await sql(importFixture.slice(setupStart,setupEnd));
    const importerStart=importFixture.indexOf('create function pg_temp.bpsc_import(');
    const importerEnd=importFixture.indexOf('end $f$;',importerStart)+'end $f$;'.length;
    let importer=importFixture.slice(importerStart,importerEnd);
    assert(importer.length>10000);
    importer=importer.replace('p_session uuid default null)',
      'p_session uuid default null,p_finalise boolean default true)');
    const stop="  if p_session is not null then return v_result||jsonb_build_object('cycle_id',v_cycle,'upload_id',v_upload,'publication_id',v_publication);end if;";
    assert.equal(importer.split(stop).length,2);
    importer=importer.replace(stop,stop+"\n  if not p_finalise then return v_result||jsonb_build_object('cycle_id',v_cycle,'upload_id',v_upload,'publication_id',v_publication);end if;");
    await sql(importer);
    if(rosterRebindProof) {
      // The second importer differs only in genuine Office-selected mapping.
      // Both eligibility candidates are obtained from the actual context owner.
      const mapping="'contract_id',v_contract,'contract_selection_method','AUTO_UNIQUE','qualifying_contract_ids',jsonb_build_array(v_contract),";
      assert.equal(importer.split(mapping).length,2);
      const rebind=importer.replace('pg_temp.bpsc_import(', 'pg_temp.bpsc_rebind(').replace(mapping,
        `'contract_id','b8550000-0000-4000-8000-000000000024'::uuid,
         'contract_selection_method','OFFICE_SELECTED','qualifying_contract_ids',
         (select jsonb_agg(c->'contract_id' order by c->>'contract_id')
          from jsonb_array_elements(public.weekly_source_upload_context_v1(jsonb_build_object(
            'operation','BUILD_PROJECTION','actor_user_id',v_actor,'upload_id',v_upload))->'rows') context_row,
          lateral jsonb_array_elements(context_row->'contracts') c
          where (context_row->>'source_row_ordinal')::integer=v_source_row.source_row_ordinal),`);
      await sql(rebind);
    }
    await query(`pg_temp.bpsc_import(${sourceAbsenceKind!=='ROSTER'},'2026-09-20',
      ${quote([{key:'manual-protection',date:sourceAbsenceKind ? '2026-08-31' : '2026-09-08',
        end:'17:00',minutes:sourceAbsenceKind ? 450 : 480,break:sourceAbsenceKind ? 30 : 0,expense:0}])},
      'SOURCE_MANUAL_PROTECTION_NATIVE',null,false)`);
    importedQuerySource=await query(`private.weekly_source_manual_review_source_v2(
      (select id from public.weekly_source_upload_rows))`);
    assert(importedQuerySource,'one real current imported shift');
    const opened=await query(`public.weekly_source_manual_review_open_v1(${quote({
      actor_user_id:'b8550000-0000-4000-8000-000000000001',
      source_row_id:importedQuerySource.upload_row_id,reason:'Worked an extra hour',
      command_id:'b8550000-0000-4000-8000-000000000911',
    })})`);
    importedQueryId=opened.review_id;
    assert.equal(opened.ok,true);
    freshAdmissionProved=true; // Empty fresh-family busy proof belongs to the default variant.
  } else await sql(fixture.slice(start, end));

  // Bundle the actual saved broker in memory and expose its existing private
  // calculator to this test. No product source is rewritten or downloaded.
  const source = await readFile(path.join(root, 'broker/src/index.js'), 'utf8');
  const bundled = await build({ stdin: { contents: `${source}\nexport { calculateWeeklyProtectedSnapshot };`,
    resolveDir: path.join(root, 'broker/src'), sourcefile: 'native-calculator-owner.js' },
    bundle: true, write: false, platform: 'node', format: 'cjs', logLevel: 'error' });
  calculatorBundleSha256 = createHash('sha256').update(bundled.outputFiles[0].contents).digest('hex');
  const calculatorModule = new Module(path.join(root, 'tests/.native-calculator-owner.cjs'));
  calculatorModule.filename = path.join(root, 'tests/.native-calculator-owner.cjs');
  calculatorModule.paths = Module._nodeModulePaths(root);
  calculatorModule._compile(bundled.outputFiles[0].text, calculatorModule.filename);
  const dependencies = {
    dataRpc: async (name, payload) => {
      assert(names.has(name), `unapproved test RPC ${name}`);
      calls.push(name);
      if(name==='weekly_exceptional_pay_prepare_family_v1' && !freshAdmissionProved) {
        const releaseHolder = await holdAdmissionElsewhere();
        try {
          await sql(`do $fresh_busy$ begin
            begin
              perform public.weekly_exceptional_pay_prepare_family_v1(${quote(payload.p_request)});
              raise exception 'FRESH_PREPARE_BYPASSED_QUERY_ADMISSION';
            exception when sqlstate '55P03' then
              if sqlerrm<>'WEEKLY_SOURCE_PAY_QUERY_ADMISSION_BUSY' then raise; end if;
            end;
            if exists(select 1 from public.timesheets)
              or exists(select 1 from public.weekly_exceptional_orchestration_runs)
              or exists(select 1 from public.weekly_exceptional_pay_target_families) then
              raise exception 'FRESH_BUSY_LEFT_BUSINESS_ROWS';
            end if;
          end; $fresh_busy$;`);
          freshAdmissionProved = true;
        } finally { await releaseHolder(); }
        console.log('GENUINE_FRESH_PREPARE_BUSY_BEFORE_BUSINESS_LOCKS_PASS');
      }
      if (name === 'weekly_exceptional_pay_stage_c1_request_v1') {
        const req = payload.p_request;
        console.log('ACTUAL_STAGE_SCOPE_CHECK', JSON.stringify(await query(`jsonb_build_object(
          'expectedBound',${quote(req)}->'expected_family_bound_version',
          'actualBound',(select f.bound_version from public.weekly_exceptional_pay_target_families f where f.id=(${quote(req)}->>'family_id')::uuid),
          'runState',(select r.state from public.weekly_exceptional_orchestration_runs r where r.id=(${quote(req)}->>'orchestration_run_id')::uuid),
          'financialExists',exists(select 1 from public.timesheets_financials f where f.id=(${quote(req)}->'c1_request'->>'financial_row_id')::uuid and f.is_current))`)));
      }
      if (name === 'weekly_source_target_managed_root_prepare_atomic_v1') {
        const snapshot = payload.p_request.service_snapshot.tsfin_snapshot_json;
        const check = await query(`(select jsonb_build_object(
          'policy',${quote(snapshot.policy_snapshot_json)}=(private._timesheet_settings_authority_frozen_v1(t.timesheet_id)->'values')-'resolved_at_utc',
          'candidate',(${quote(snapshot)}->>'candidate_id')=c.candidate_id::text,
          'client',(${quote(snapshot)}->>'client_id')=c.client_id::text,
          'role',(${quote(snapshot)}->>'role') is not distinct from c.role,
          'band',(${quote(snapshot)}->>'band') is not distinct from c.band,
          'version',(${quote(snapshot)}->>'timesheet_version')::integer=t.version,
          'payMethod',upper(${quote(snapshot)}->>'pay_method')=upper(c.pay_method_snapshot))
          from public.timesheets t join public.contracts c on c.id=t.contract_id
          where t.timesheet_id='${snapshot.timesheet_id}')`);
        console.log('ZERO_ROOT_IDENTITY_CHECK', JSON.stringify(check));
      }
      const result = await query(`public.${name}(${quote(payload.p_request)})`);
      if (name === 'weekly_exceptional_pay_action_context_v1') lastContext = result;
      return result;
    },
    calculateWeeklySnapshot: (input) => calculatorModule.exports.calculateWeeklyProtectedSnapshot({}, input),
  };
  let request = {
    actor_user_id: 'b8340000-0000-4000-8000-000000000001',
    source_cycle_id: 'b8340000-0000-4000-8000-000000000009',
    candidate_id: 'b8340000-0000-4000-8000-000000000003',
    client_id: 'b8340000-0000-4000-8000-000000000002',
    contract_id: 'b8340000-0000-4000-8000-000000000004',
    week_ending_date: '2026-09-13', work_date: '2026-09-07',
    start: '09:00', end: '17:00', break_minutes: 30,
    reason: 'Genuine saved calculator local owner proof.',
    idempotency_key: 'source-local-native-20261005-before-authorisation',
  };
  if(manualQueryProof) request={
    actor_user_id:'b8550000-0000-4000-8000-000000000001',
    source_cycle_id:importedQuerySource.source_cycle_id,
    work_event_id:importedQuerySource.work_event_id,
    candidate_id:importedQuerySource.candidate_id,client_id:importedQuerySource.client_id,
    contract_id:importedQuerySource.contract_id,
    week_ending_date:importedQuerySource.week_ending_date,work_date:importedQuerySource.work_date,
    start:'09:00',end:'18:00',break_minutes:0,reason:'Worked an extra hour',
    idempotency_key:'source-local-native-20261005-manual-query-protection',
  };
  const result = await orchestrateWeeklyProtectedAction({ action: 'APPROVE_PROTECTED_HOURS', request, dependencies });
  assert.equal(result.ok, true);
  assert.equal(result.outcome, 'PUBLISHED');
  const selectedWitness=await query(`private.weekly_source_selected_source_witness_v1(
    '${lastContext.family_id}','${request.source_cycle_id}','${lastContext.work_event_id}')`);
  assert.equal(selectedWitness?.kind,manualQueryProof ? 'CURRENT_ROW' : 'CERTIFIED_ABSENCE');
  assert.equal(selectedWitness?.basis_kind,manualQueryProof ? 'CURRENT_PROVISIONAL_ROW' : 'NO_IMPORT_YET');
  assert.match(selectedWitness.basis_sha256,/^[0-9a-f]{64}$/);
  if(!manualQueryProof) {
    await sql(`do $partial_source_negative$ declare rejected text; begin
      begin
        insert into public.weekly_source_uploads(source_cycle_id,original_filename,content_sha256,byte_count,
          source_format_profile_id,parser_version,normaliser_version,header_coordinate_map_hash,
          declared_scope_fingerprint,coverage_proof_kind,physical_row_count,uploaded_by_user_id)
        select '${request.source_cycle_id}','incomplete-selected-duty-negative.xlsx',decode(repeat('c7',32),'hex'),1,
          p.id,'NATIVE_NEGATIVE_INPUT','NATIVE_NEGATIVE_INPUT',decode(repeat('c8',32),'hex'),
          decode(repeat('c9',32),'hex'),'NHSP_TRUST_REPORT_SCOPE',0,'${request.actor_user_id}'
        from public.weekly_source_format_profiles p where p.final_authority_kind='NHSP_TRUST_BACKING_REPORT'
        order by p.id limit 1;
        if private.weekly_source_selected_source_witness_v1('${lastContext.family_id}',
          '${request.source_cycle_id}','${lastContext.work_event_id}') is not null then
          raise exception 'UNFINISHED_IMPORT_BECAME_ZERO_SOURCE'; end if;
        raise exception 'NATIVE_PARTIAL_SOURCE_NEGATIVE_ROLLBACK' using errcode='22023';
      exception when invalid_parameter_value then
        get stacked diagnostics rejected=message_text;
        if rejected<>'NATIVE_PARTIAL_SOURCE_NEGATIVE_ROLLBACK' then raise; end if;
      end;
    end $partial_source_negative$;`);
    assert.deepEqual(await query(`private.weekly_source_selected_source_witness_v1(
      '${lastContext.family_id}','${request.source_cycle_id}','${lastContext.work_event_id}')`),selectedWitness,
      'negative unfinished input was rolled back, original no-import witness unchanged');
  }
  console.log('GENUINE_SELECTED_SOURCE_WITNESS_PASS',selectedWitness.basis_kind);
  if(manualQueryProof) {
    assert.equal(await query(`(select state='RESOLVED' and resolution_kind='PROTECTED_PAY'
      from private.weekly_source_manual_reviews where id='${importedQueryId}')`),true,
      'actual protection closes the real imported-shift manual query atomically');
    assert.equal(await query(`(select count(*)=1 from private.weekly_source_manual_review_commands
      where review_id='${importedQueryId}' and command_kind='RESOLVE')`),true);
    console.log('GENUINE_IMPORTED_MANUAL_QUERY_PROTECTION_COMPLETION_PASS');
  }
  const rootId = lastContext.root_timesheet_id;
  const initialSavedRun=lastContext.orchestration_run_id;
  const savedStatus=await query(`private.weekly_source_local_saved_status_v1(
    '${lastContext.family_id}','${initialSavedRun}','${request.actor_user_id}')`);
  assert.deepEqual(Object.keys(savedStatus).sort(),['family_id','orchestration_run_id',
    'publication_request_id','request_sha256','generation_id','state',
    'requires_first_authorisation','idempotent_replay','result'].sort());
  assert.equal(savedStatus.requires_first_authorisation,true);
  assert.equal(savedStatus.state,'COMPLETE');
  assert.deepEqual(savedStatus.result,{...result,idempotent_replay:true});
  assert.equal(await query(`private.weekly_source_local_saved_status_v1(
    '${lastContext.family_id}','00000000-0000-4000-8000-000000000000','${request.actor_user_id}')`),null);
  await sql(`do $actor$ begin
    begin perform private.weekly_source_local_saved_status_v1('${lastContext.family_id}',
      '${initialSavedRun}','00000000-0000-4000-8000-000000000000');
      raise exception 'WRONG_ACTOR_ACCEPTED';
    exception when sqlstate '55000' then
      if sqlerrm<>'WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' then raise; end if;
    end;
  end $actor$;`);
  console.log('GENUINE_LOCAL_SAVED_STATUS_PREAUTHORISATION_AND_ABSENCE_PASS');
  // Each corrupt-evidence probe is a subtransaction. An immutable owner may
  // reject the write BEFORE this reader is reached; record that distinction.
  // Never disable those product guards to manufacture a contradictory receipt.
  await sql('create temp table source_saved_status_negative_results(outcome text);');
  for(const corruption of [
    `update private.weekly_source_local_protected_decision_receipts set
      request_sha256=decode(repeat('ab',32),'hex') where publication_request_id='${result.publication_request_id}'`,
    `update private.weekly_source_local_protected_decision_receipts set state='PREPARING',
      completed_at_utc=null where publication_request_id='${result.publication_request_id}'`,
    `update public.weekly_exceptional_orchestration_runs set after_state_fingerprint=decode(repeat('ab',32),'hex')
      where id='${initialSavedRun}'`,
    `update public.weekly_exceptional_pay_generations set complete_next_vector_hash=decode(repeat('ab',32),'hex')
      where id='${result.generation_id}'`,
    `update public.weekly_exceptional_orchestration_steps set owner_response_hash=decode(repeat('ab',32),'hex')
      where orchestration_run_id='${initialSavedRun}' and step_kind='COMPLETE_LOCAL_PROTECTED_DECISION'`,
    `delete from private.weekly_source_local_protected_decision_receipts
      where publication_request_id='${result.publication_request_id}'`,
  ]) {
    await sql(`do $contradiction$ declare v_outcome text; begin
      begin ${corruption};
        perform private.weekly_source_local_saved_status_v1('${lastContext.family_id}',
          '${initialSavedRun}','${request.actor_user_id}');
        raise exception 'CORRUPT_LOCAL_EVIDENCE_WAS_ACCEPTED';
      exception when sqlstate '55000' then
        if sqlerrm='WEEKLY_SOURCE_LOCAL_SAVED_STATUS_CONTRADICTION' then v_outcome:='STATUS_REJECTED';
        elsif sqlerrm='WEEKLY_PROTECTED_LOCAL_RECEIPT_IMMUTABLE' then v_outcome:='IMMUTABLE_OWNER_REJECTED';
        else raise; end if;
      end;
      insert into pg_temp.source_saved_status_negative_results values(v_outcome);
    end $contradiction$;`);
    assert.deepEqual(await query(`private.weekly_source_local_saved_status_v1(
      '${lastContext.family_id}','${initialSavedRun}','${request.actor_user_id}')`),savedStatus,
      'contradiction probe restores original accepted evidence');
  }
  console.log('GENUINE_LOCAL_SAVED_STATUS_NEGATIVES_PASS',JSON.stringify(await query(
    `(select jsonb_agg(outcome) from pg_temp.source_saved_status_negative_results)`)));
  const financial = await query(`(select jsonb_build_object('hours',f.total_hours,'pay',f.total_pay_ex_vat,
    'authorised',t.authorised_at_server is not null) from public.timesheets_financials f
    join public.timesheets t on t.timesheet_id=f.timesheet_id
    where f.timesheet_id='${rootId}' and f.is_current)`);
  assert.equal(Number(financial.hours), manualQueryProof ? 9 : 7.5);
  assert.equal(Number(financial.pay), manualQueryProof ? 90 : 75);
  assert.equal(financial.authorised, false);
  const gate = await query(`private.weekly_source_pay_query_gate_v1('${rootId}')`);
  assert.deepEqual(Object.keys(gate).sort(), ['blocked','code','ok','query_state_sha256','scope']);
  assert.equal(gate.ok, true);
  assert.equal(gate.blocked, false);
  assert.match(gate.query_state_sha256, /^[0-9a-f]{64}$/);
  const receipt = await query(`private.weekly_source_pay_query_local_receipt_v1('${rootId}',
    (select a.id from public.weekly_exceptional_payment_approvals a where
      a.creation_orchestration_run_id='${lastContext.orchestration_run_id}'),
    '${result.generation_id}')`);
  assert.equal(receipt?.lane, 'SOURCE_LOCAL');
  assert.equal(receipt?.receipt_state, 'COMPLETE');
  if (process.argv.includes('--mixed-query-gate')) {
    assert.equal(manualQueryProof, true, 'mixed gate requires the genuine provisional import variant');
    // These isolated audit vectors import another provisional source. Restore
    // the real original source before the later query/amendment journey;
    // otherwise its legitimate stale-source refusal is exercised accidentally.
    await sql('savepoint mixed_query_audits;');
    await sql(await readFile(path.join(root, 'tests/fixtures/bp-source-query-mixed-root-gate.sql'), 'utf8'));
    console.log('GENUINE_ACCEPTED_LOCAL_RECEIPT_MIXED_ROOT_QUERY_GATE_PASS');
    if(process.argv.includes('--contact-retirement')){
      await sql(await readFile(path.join(root,'tests/fixtures/bp-source-protected-contact-retirement.sql'),'utf8'));
      console.log('GENUINE_ACCEPTED_PROTECTION_CONTACT_RETIREMENT_LIFECYCLE_PASS');
    }
    if(process.argv.includes('--attention-workspace')){
      const covered=await query(`(select jsonb_agg(jsonb_build_object('id',i.id,'work_date',e.work_date,
        'covered',private.weekly_source_covered_hours_incident_v1(i.id)))
        from public.weekly_discrepancy_incidents i join public.weekly_work_events e on e.id=i.work_event_id
        where i.state='OPEN' and i.candidate_id='${request.candidate_id}')`);
      assert.equal(covered.filter(x=>x.work_date===request.work_date&&x.covered===true).length,1,
        'one genuinely accepted protected incident is covered');
      assert(covered.some(x=>x.covered===false),'another contract cannot inherit coverage');
      await loadAttentionOwners();
      const attentionRequest={actor_user_id:request.actor_user_id,source_group_id:request.source_group_id,
        client_id:request.client_id,tab:'queries',section:'protected',attention_first:true,limit:1};
      await sql(`insert into public.weekly_source_cycles(source_group_id,finalisation_week_ending,cutoff_at_utc)
        select source_group_id,finalisation_week_ending+7,cutoff_at_utc+interval '7 days'
        from public.weekly_source_cycles where id='${request.source_cycle_id}';`);
      const attention=await query(`public.weekly_source_combined_review_workspace_v1(${quote(attentionRequest)})`);
      assert.equal(attention.attention.complete,true);
      assert.equal(attention.counts.protected,1,'exact protected family/work appears once across cutoff scopes');
      assert.equal(attention.rows.length,1);
      assert.equal(attention.attention.protected,0,'provisional source is not ready for reconciliation');
      assert.equal(attention.attention.missing_source,1,'separate candidate-worked absent import row is one distinct attention job');
      const questions=await query(`public.weekly_source_combined_review_workspace_v1(${quote({...attentionRequest,section:'questions'})})`);
      assert(!questions.rows.some(row=>row.children?.some(child=>
        covered.some(fact=>fact.covered&&fact.id===child.incident_id))),
        'actual covered Hours incident omitted, not merely recoloured; unrelated missing signature stays visible');
      const missingFirst=await query(`public.weekly_source_combined_review_workspace_v1(${quote({...attentionRequest,
        section:'questions',attention_kind:'missing_source'})})`);
      assert(missingFirst.rows[0].children.some(child=>child.candidate_shift_absent_from_import===true),
        'missing-shift attention returns the exact absent-import work');
      assert(missingFirst.rows.every(row=>row.children.every(child=>child.candidate_shift_absent_from_import===true)),
        'missing-shift filter includes no other hours questions or missing-Timesheet children');
      assert.deepEqual(missingFirst.counts,attention.counts,'attention filtering never changes normal tab counts');
      const waitingFiltered=await query(`public.weekly_source_combined_review_workspace_v1(${quote({...attentionRequest,
        attention_kind:'protected'})})`);
      assert.deepEqual(waitingFiltered.rows,[],'waiting-only protection does not appear in reconciliation attention');
      assert.equal(waitingFiltered.total_count,0);
      assert.equal(waitingFiltered.counts.protected,1,'full Protected tab retains waiting shift');
      console.log('GENUINE_ATTENTION_WORKSPACE_QUALIFIED_COVERAGE_AND_WAITING_PROTECTION_PASS');
    }
  }
  if (process.argv.includes('--wait-query-gate')) {
    assert.equal(process.argv.includes('--mixed-query-gate'), true,
      'Wait proof needs the genuine protected A OPEN discrepancy');
    const originalContext = lastContext;
    const beforeWait = await query(`jsonb_build_object(
      'finance',(select jsonb_agg(to_jsonb(f) order by f.id) from public.timesheets_financials f
        where f.timesheet_id='${rootId}'),
      'generation',(select current_generation_id from public.weekly_exceptional_pay_target_families
        where id='${lastContext.family_id}'),
      'vector',(select encode(current_complete_target_vector_hash,'hex')
        from public.weekly_exceptional_pay_target_families where id='${lastContext.family_id}'))`);
    await sql('savepoint genuine_informational_wait;');
    try {
      const waitRequest = {
        actor_user_id: request.actor_user_id, source_cycle_id: request.source_cycle_id,
        family_id: lastContext.family_id, work_event_id: lastContext.work_event_id,
        expected_family_bound_version: await query(`(select bound_version::text
          from public.weekly_exceptional_pay_target_families where id='${lastContext.family_id}')`),
        reason: 'Keep approved protection while waiting for source',
        idempotency_key: 'source-local-native-20261005-genuine-informational-wait',
      };
      const waited = await orchestrateWeeklyProtectedAction({ action: 'WAIT_FOR_SOURCE',
        request: waitRequest, dependencies });
      assert.equal(waited.ok, true);
      assert.equal(waited.outcome, 'WAITING_FOR_SOURCE');
      const waitGate = await query(`private.weekly_source_pay_query_gate_v1('${rootId}')`);
      console.log('ACTUAL_INFORMATIONAL_WAIT_GATE', JSON.stringify(waitGate));
      assert.equal(waitGate.ok, true, 'genuine informational Wait retains qualified protection');
      assert.equal(waitGate.blocked, false);
      assert.deepEqual(await query(`private.weekly_source_pay_query_local_receipt_v1('${rootId}',
        (select a.id from public.weekly_exceptional_payment_approvals a where
          a.creation_orchestration_run_id='${initialSavedRun}'),'${result.generation_id}')`), receipt,
        'original immutable approval receipt survives genuine later Wait');
      const beforeWaitReplay = await query(`(select count(*) from public.weekly_exceptional_pay_target_events
        where family_id='${waitRequest.family_id}')`);
      const waitReplay = await orchestrateWeeklyProtectedAction({ action: 'WAIT_FOR_SOURCE',
        request: waitRequest, dependencies });
      assert.equal(waitReplay.idempotent_replay, true);
      assert.equal(await query(`(select count(*) from public.weekly_exceptional_pay_target_events
        where family_id='${waitRequest.family_id}')`), beforeWaitReplay,
        'same Wait replay never appends another audit');
      const secondWait = await orchestrateWeeklyProtectedAction({ action: 'WAIT_FOR_SOURCE',
        request: { ...waitRequest, expected_family_bound_version: waited.family_bound_version,
          idempotency_key: waitRequest.idempotency_key + '-second' }, dependencies });
      assert.equal(secondWait.ok, true);
      assert.equal((await query(`private.weekly_source_pay_query_gate_v1('${rootId}')`)).blocked, false,
        'multiple genuine informational audits still leave exactly one qualified financial publication');
      const twoWaitContext = lastContext;
      await sql('savepoint genuine_sibling_wait;');
      try {
        const siblingInput = await query(`(select jsonb_build_object('work_event_id',w.id,
          'work_date',to_char(w.work_date,'YYYY-MM-DD'),'source_cycle_id',i.source_cycle_id)
          from public.weekly_work_events w join public.weekly_discrepancy_incidents i on i.work_event_id=w.id
          where w.candidate_id='${request.candidate_id}' and w.client_id='${request.client_id}'
            and w.work_date='${request.work_date}'::date+1)`);
        assert(siblingInput?.work_event_id, 'genuine imported Wednesday sibling');
        const protectedSibling = await orchestrateWeeklyProtectedAction({ action: 'APPROVE_PROTECTED_HOURS',
          request: { ...request, ...siblingInput, reason: 'Genuine sibling protection isolation proof',
            idempotency_key: 'source-native-20261005-genuine-sibling-protection' }, dependencies });
        assert.equal(protectedSibling.ok, true);
        assert.equal(lastContext.root_timesheet_id, rootId, 'both protected shifts reuse the actual same root');
        const siblingWait = {
          ...waitRequest, work_event_id:siblingInput.work_event_id,
          source_cycle_id:siblingInput.source_cycle_id, expected_family_bound_version: protectedSibling.family_bound_version,
          idempotency_key: 'source-native-20261005-genuine-sibling-wait',
        };
        const siblingWaited = await orchestrateWeeklyProtectedAction({ action: 'WAIT_FOR_SOURCE',
          request: siblingWait, dependencies });
        assert.equal(siblingWaited.ok, true);
        const beforeSibling = await query(`(select to_jsonb(p)
          from public.weekly_exceptional_pending_reconciliation_targets p
          where p.family_id='${waitRequest.family_id}' and p.durable_work_event_id='${siblingInput.work_event_id}'
            and p.state='ACTIVE')`);
        assert(beforeSibling?.id, 'actual sibling Wait produced one active pending identity');
        const siblingFinance = await query(`(select jsonb_agg(to_jsonb(f) order by f.id)
          from public.timesheets_financials f where f.timesheet_id='${rootId}')`);
        const firstShiftAgain = await orchestrateWeeklyProtectedAction({ action: 'WAIT_FOR_SOURCE',
          request: { ...waitRequest, expected_family_bound_version: siblingWaited.family_bound_version,
            idempotency_key: 'source-native-20261005-first-shift-wait-with-sibling' }, dependencies });
        assert.equal(firstShiftAgain.ok, true);
        assert.deepEqual(await query(`(select to_jsonb(p)
          from public.weekly_exceptional_pending_reconciliation_targets p where p.id='${beforeSibling.id}')`),
          beforeSibling, 'Wait for Tuesday cannot supersede or alter Wednesday pending work');
        assert.deepEqual(await query(`(select jsonb_agg(to_jsonb(f) order by f.id)
          from public.timesheets_financials f where f.timesheet_id='${rootId}')`), siblingFinance,
          'Wait for either sibling changes no financial record');
        assert.equal((await query(`private.weekly_source_pay_query_gate_v1('${rootId}')`)).blocked, false);
        console.log('GENUINE_TWO_PROTECTED_SHIFTS_SAME_ROOT_WAIT_ISOLATION_PASS');
      } finally {
        await sql('rollback to savepoint genuine_sibling_wait; release savepoint genuine_sibling_wait;');
        lastContext = twoWaitContext;
      }
      // Actual prepare/context/Wait owners must refuse an already retired
      // pending identity; the raw retired state is only a negative test input.
      for (const retiredState of ['SUPERSEDED', 'CONSUMED', 'WITHDRAWN']) {
        await sql(`do $retired_wait_input$
          declare v_prepared jsonb; v_context jsonb; v_keys jsonb; v_failure text;
          begin begin
            update public.weekly_exceptional_pending_reconciliation_targets set state='${retiredState}',
              completed_at_utc=statement_timestamp() where family_id='${waitRequest.family_id}'
              and durable_work_event_id='${waitRequest.work_event_id}' and state='ACTIVE';
            v_keys:=${quote({schema_version:'WEEKLY_PROTECTED_ACTION_PREPARE_V1',
              actor_user_id:request.actor_user_id,family_id:waitRequest.family_id,
              source_cycle_id:request.source_cycle_id,work_event_id:waitRequest.work_event_id,
              action:'WAIT',protected_schedule:null,reason:'Negative retired pending input',
              idempotency_key:'source-native-retired-wait-'+retiredState})}||jsonb_build_object(
                'expected_family_bound_version',(select bound_version::text
                  from public.weekly_exceptional_pay_target_families where id='${waitRequest.family_id}'));
            v_prepared:=public.weekly_exceptional_pay_prepare_action_v1(v_keys);
            v_context:=public.weekly_exceptional_pay_action_context_v1(jsonb_build_object(
              'schema_version','WEEKLY_PROTECTED_ACTION_CONTEXT_V1',
              'actor_user_id','${request.actor_user_id}','family_id',v_prepared->'family_id',
              'orchestration_run_id',v_prepared->'orchestration_run_id',
              'source_cycle_id',v_prepared->'source_cycle_id','work_event_id',v_prepared->'work_event_id',
              'protected_schedule',v_prepared->'protected_schedule','evidence_timesheet_id',null));
            perform public.weekly_exceptional_pay_wait_atomic_v1(jsonb_build_object(
              'schema_version','WEEKLY_PROTECTED_WAIT_V1','actor_user_id','${request.actor_user_id}',
              'family_id',v_context->'family_id','orchestration_run_id',v_context->'orchestration_run_id',
              'source_cycle_id',v_context->'source_cycle_id','work_event_id',v_context->'work_event_id',
              'expected_family_bound_version',v_context->'family_bound_version',
              'expected_target_vector_sha256',v_context->'current_target_vector_sha256',
              'source_proposal',v_context->'source_proposal','protected_schedule',v_context->'protected_schedule',
              'reason',v_keys->'reason','idempotency_key',(v_keys->>'idempotency_key')||':wait'));
            raise exception 'RETIRED_WAIT_WAS_REOPENED:${retiredState}';
          exception when sqlstate '55000' then
            get stacked diagnostics v_failure=message_text;
            if v_failure<>'WEEKLY_PROTECTED_WAIT_SCOPE_INVALID' then raise; end if;
          end;
          if not exists(select 1 from public.weekly_exceptional_pending_reconciliation_targets
            where family_id='${waitRequest.family_id}' and durable_work_event_id='${waitRequest.work_event_id}'
              and state='ACTIVE' and completed_at_utc is null) then
            raise exception 'RETIRED_NEGATIVE_FAILED_TO_ROLL_BACK'; end if;
          end $retired_wait_input$;`);
      }
      console.log('GENUINE_WAIT_OWNER_REFUSES_THREE_RETIRED_PENDING_IDENTITIES_PASS');
      // Bounded raw adversarial INSERT inputs, never accepted payment authority.
      // Existing immutable rows/guards are untouched. Each inner transaction
      // rolls back; a real schema constraint may reject a bad kind earlier.
      for (const [label, change] of [
        ['wrong-kind', "v_bad.reason:='OFFICE_AMENDMENT';"],
        ['wrong-lifecycle', "v_bad.resulting_lifecycle_state:='ACTIVE';"],
        ['changed-vector', "v_bad.complete_prior_family_vector_fingerprint:=decode(repeat('d9',32),'hex');"],
        ['wrong-vector-body', "v_bad.fixed_target_component_snapshot:='{}'::jsonb;"],
        ['wrong-event-hash', ''],
        ['ambiguous-financial-link', `v_bad.financial_generation_id:='${result.generation_id}';`],
      ]) {
        await sql(`do $bad_wait_input$
          declare v_bad public.weekly_exceptional_pay_target_events%rowtype; v_gate jsonb;
          begin begin
            select t.* into strict v_bad from public.weekly_exceptional_pay_target_events t
              where t.family_id='${waitRequest.family_id}' and t.financial_generation_id is null
              order by t.event_sequence desc limit 1;
            v_bad.id:=gen_random_uuid(); v_bad.event_sequence:=v_bad.event_sequence+1;
            v_bad.idempotency_key:='native-bad-wait-${label}'; ${change}
            v_bad.event_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_PROTECTED_TARGET_EVENT_V1',
              jsonb_build_object('family_id',v_bad.family_id,'event_sequence',v_bad.event_sequence,
                'approval_id',v_bad.approval_id,'generation_id',null,
                'prior_vector_hash',encode(v_bad.complete_prior_family_vector_fingerprint,'hex'),
                'next_vector_hash',encode(v_bad.complete_next_family_vector_fingerprint,'hex'),
                'reason','WAIT','actor_user_id',v_bad.actor_user_id));
            ${label === 'wrong-event-hash' ? "v_bad.event_hash:=decode(repeat('d9',32),'hex');" : ''}
            insert into public.weekly_exceptional_pay_target_events select (v_bad).*;
            v_gate:=private.weekly_source_pay_query_gate_v1('${rootId}');
            if v_gate->>'code' is distinct from 'WEEKLY_SOURCE_PAY_QUERY_EVIDENCE_UNAVAILABLE'
              or v_gate->>'ok' is distinct from 'false' then
              raise exception 'BAD_WAIT_INPUT_ACCEPTED:${label}'; end if;
            raise exception 'NATIVE_BAD_WAIT_INPUT_ROLLBACK' using errcode='22023';
          exception when invalid_parameter_value then
            if sqlerrm<>'NATIVE_BAD_WAIT_INPUT_ROLLBACK' then raise; end if;
          when check_violation then
            if '${label}'<>'wrong-kind' then raise; end if;
          end; end $bad_wait_input$;`);
        assert.equal((await query(`private.weekly_source_pay_query_gate_v1('${rootId}')`)).blocked, false,
          label + ': adversarial input leaves genuine protection unchanged');
      }
      console.log('GENUINE_WAIT_REPLAY_MULTIPLE_AUDITS_AND_SIX_BAD_EVIDENCE_REFUSALS_PASS');
      assert.deepEqual(await query(`jsonb_build_object(
        'finance',(select jsonb_agg(to_jsonb(f) order by f.id) from public.timesheets_financials f
          where f.timesheet_id='${rootId}'),
        'generation',(select current_generation_id from public.weekly_exceptional_pay_target_families
          where id='${lastContext.family_id}'),
        'vector',(select encode(current_complete_target_vector_hash,'hex')
          from public.weekly_exceptional_pay_target_families where id='${lastContext.family_id}'))`),
        beforeWait, 'Wait writes audit only, never finance/generation/target vector');
      console.log('GENUINE_INFORMATIONAL_WAIT_RETAINS_QUALIFIED_PROTECTION_PASS');
    } finally {
      await sql('rollback to savepoint genuine_informational_wait; release savepoint genuine_informational_wait;');
      lastContext = originalContext;
    }
  }
  const firstCalls = calls.length;
  const replay = await orchestrateWeeklyProtectedAction({ action: 'APPROVE_PROTECTED_HOURS', request, dependencies });
  assert.equal(replay.outcome, 'PUBLISHED');
  assert(!calls.slice(firstCalls).includes('weekly_exceptional_pay_stage_c1_request_v1'), 'retry does not recalculate/stage');
  if(process.argv.includes('--mixed-query-gate')) {
    await sql('rollback to savepoint mixed_query_audits; release savepoint mixed_query_audits;');
  }
  if (process.argv.includes('--authorised') || process.argv.includes('--first-authorised')) {
    let originalPresentation;
    if(process.argv.includes('--authorise-scope-isolation')) {
      originalPresentation=await query(`pg_get_functiondef('public.weekly_source_office_timesheet_presentation_v1(jsonb)'::regprocedure)`);
      // Fault injection only: prove the real routing and authorise owners do
      // not depend on the informational presentation. No alternate money owner.
      await sql(`create or replace function public.weekly_source_office_timesheet_presentation_v1(p_request jsonb)
        returns jsonb language plpgsql stable security definer as $blocked_info$
        begin raise exception 'LOCAL_INFORMATIONAL_PRESENTATION_UNAVAILABLE' using errcode='55000'; end;
        $blocked_info$;`);
      const routing=await weeklySourceAuthoriseRouting(async(name,args)=>{
        assert.equal(name,'weekly_source_office_authorise_scope_v1');
        return query(`public.${name}(${quote(args.p_request)})`);
      },rootId,request.actor_user_id);
      assert.equal(routing.bound,true);
      assert.equal(routing.refusal,null);
      assert.deepEqual(routing.presentation,{contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable:true});
      // Applicability alone must leave non-Source Weekly and expense roots
      // on their existing ordinary route. Restore each fixture shape before
      // invoking the genuine first-authorisation owner below.
      for(const [label,change] of [
        ['ordinary-weekly',`update public.weekly_source_groups set active=false where id in (
          select source_group_id from public.weekly_source_group_clients where client_id=(
            select client_id from public.contracts where id=(
              select contract_id from public.timesheets where timesheet_id='${rootId}')));`],
        ['expenses',`update public.timesheets set line_type='EXPENSES' where timesheet_id='${rootId}';`],
      ]) {
        await sql('savepoint authorise_ordinary_scope;');
        try {
          await sql(change);
          assert.deepEqual(await query(`public.weekly_source_office_authorise_scope_v1(${quote({
            actor_user_id:request.actor_user_id,timesheet_id:rootId})})`),
            {contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable:false},label+' ordinary routing');
        } finally {
          await sql('rollback to savepoint authorise_ordinary_scope; release savepoint authorise_ordinary_scope;');
        }
      }
      console.log('GENUINE_NON_SOURCE_WEEKLY_AND_EXPENSE_ROUTING_ISOLATION_PASS');
      assert.equal(await query(`has_function_privilege('service_role',
        'public.weekly_source_office_authorise_scope_v1(jsonb)','EXECUTE')
        and not has_function_privilege('anon','public.weekly_source_office_authorise_scope_v1(jsonb)','EXECUTE')
        and not has_function_privilege('authenticated','public.weekly_source_office_authorise_scope_v1(jsonb)','EXECUTE')`),true);
      await sql(`do $wrong_scope_actor$ begin
        begin perform public.weekly_source_office_authorise_scope_v1(${quote({actor_user_id:'00000000-0000-4000-8000-000000000000',timesheet_id:rootId})});
          raise exception 'UNKNOWN_OFFICE_ACTOR_ACCEPTED';
        exception when insufficient_privilege then null; end;
      end $wrong_scope_actor$;`);
    }
    const authorised = await query(`public.weekly_source_first_authorise_v1('${rootId}',
      '${rootId}',null,'${request.actor_user_id}')`);
    assert.equal(authorised.ok, true, 'actual first Authorise');
    if(originalPresentation) {
      // pg_get_functiondef does not include the statement terminator. Keep
      // the restored definition separate from this runner's result marker.
      await sql(originalPresentation+';');
      console.log('GENUINE_SCOPE_ROUTING_AND_FIRST_AUTHORISE_WITH_INFORMATIONAL_PRESENTATION_UNAVAILABLE_PASS');
    }
    const approvedBefore = await query(`private.weekly_source_effective_inventory_v1('${rootId}')`);
    assert.equal(approvedBefore.approval_basis?.origin?.kind, 'INITIAL_AUTHORISED_TSFIN_V1');
    assert.equal(Number(approvedBefore.approval_basis.approved_pay_ex_vat), Number(financial.pay));
    const initialEncoding = {
      origin_kind: 'PROTECTED_LOCAL_DECISION_V1',
      publication_request_id: result.publication_request_id, generation_id: result.generation_id,
      request_sha256: receipt.request_sha256, source_qualification_digest: 'cd'.repeat(32),
      policy_fingerprint: approvedBefore.approval_basis.policy_fingerprint,
      before_origin: approvedBefore.approval_basis.origin,
      before_inventory_digest: approvedBefore.approval_basis.origin.inventory_digest,
    };
    assert.deepEqual(await query(`private.weekly_source_local_origin_canonical_v2(${quote(initialEncoding)})`),
      initialEncoding, 'encoding-only unit: genuine initial witness round trips');
    // This HEAD-reference sample tests CLOSED ENCODING ONLY. Its artificial
    // HEAD/bundle IDs are not claimed to qualify or publish a financial decision.
    const flatEncoding = { ...initialEncoding, before_origin: {
      kind: 'COMMITTED_SOURCE_HEAD_V1',
      head_id: 'e8000000-0000-4000-8000-000000000003', head_revision: '1',
      decision_bundle_id: 'e8000000-0000-4000-8000-000000000004', bundle_revision: '1',
      root_authorisation_id: initialEncoding.before_origin.root_authorisation_id,
      authorisation_generation: initialEncoding.before_origin.authorisation_generation,
      root_timesheet_id: rootId, root_version: initialEncoding.before_origin.root_version,
      source_generation_digest: 'ef'.repeat(32),
      inventory_digest: initialEncoding.before_inventory_digest,
      entitlement_digest: initialEncoding.before_origin.entitlement_digest,
    } };
    assert.deepEqual(await query(`private.weekly_source_local_origin_canonical_v2(
      private.weekly_source_local_origin_canonical_v2(${quote(flatEncoding)}))`),
      flatEncoding, 'flat HEAD encoding-only unit is idempotent and has no copied ancestry');
    await sql(`do $encoding_negative$ begin
      begin
        perform private.weekly_source_local_origin_canonical_v2(
          jsonb_set(${quote(flatEncoding)},'{before_origin,source_revision}',${quote(initialEncoding)}));
        raise exception 'LOCAL_COPIED_ANCESTRY_ENCODING_ACCEPTED';
      exception when invalid_parameter_value then null; end;
    end; $encoding_negative$;`);
    console.log('FLAT_LOCAL_CODEC_ENCODING_UNIT_PASS_NOT_PUBLICATION_AUTHORITY');
    assert.equal(await query(`(select count(*)=1 from private.bpay_next_work_revision)`), true,
      'actual first authorisation created exactly one Banking revision');
    const authoriseReplay = await query(`public.weekly_source_first_authorise_v1('${rootId}',
      '${rootId}',null,'${request.actor_user_id}')`);
    console.log('FIRST_AUTHORISATION_REPEAT', JSON.stringify(authoriseReplay));
    assert.equal(authoriseReplay.ok, false, 'a second first Authorise is correctly refused');
    assert.equal(authoriseReplay.code, 'WEEKLY_SOURCE_ROOT_ALREADY_AUTHORISED');
    assert.equal(await query(`(select count(*)=1 from private.bpay_next_work_revision)`), true,
      'authorisation replay does not duplicate the Banking revision');
    const bankRevision = await query(`(select to_jsonb(r) from private.bpay_next_work_revision r)`);
    if(process.argv.includes('--next-paid-context')) {
      const publicRequest={actor_user_id:request.actor_user_id,timesheet_id:rootId};
      assert.equal(await query('private.weekly_source_office_next_owner_v1()'),true);
      await sql(`do $next_bad_actor$ begin
        begin perform private.weekly_source_office_next_paid_evidence_page_v1(
          '00000000-0000-4000-8000-000000000000','${rootId}','COMPONENTS');
          raise exception 'NEXT_UNAUTHORISED_ACTOR_ACCEPTED';
        exception when insufficient_privilege then null; end;
      end $next_bad_actor$;`);
      await sql('savepoint next_missing_reader; drop function public.bpay_next_source_paid_evidence_page_v1(jsonb);');
      try {
        const missing=await query(`private.weekly_source_office_next_paid_evidence_page_v1(
          '${request.actor_user_id}','${rootId}','COMPONENTS')`);
        assert.deepEqual(missing,{contract:'WEEKLY_SOURCE_OFFICE_NEXT_PAGE_V1',available:false,
          reason:'NEXT_READER_UNAVAILABLE',request:null,page:null});
        assert.deepEqual(await query(`public.weekly_source_office_authorise_scope_v1(${quote(publicRequest)})`),
          {contract:'WEEKLY_SOURCE_AUTHORISE_SCOPE_V1',applicable:true});
      } finally {await sql('rollback to savepoint next_missing_reader; release savepoint next_missing_reader;');}
      console.log('GENUINE_NEXT_CONTEXT_PERMISSIONS_AND_MISSING_INFORMATION_AUTHORISE_ISOLATION_PASS');
      // Independently execute both real point readers before report
      // finalisation. The complete public presentation retains its real
      // Source publication gate; its separate proof runs after finalisation.
      const evidence=await query(`jsonb_build_object('contract','WEEKLY_SOURCE_OFFICE_NEXT_EVIDENCE_V1',
        'components',private.weekly_source_office_next_paid_evidence_page_v1('${request.actor_user_id}','${rootId}','COMPONENTS'),
        'active_holds',private.weekly_source_office_next_paid_evidence_page_v1('${request.actor_user_id}','${rootId}','ACTIVE_HOLDS'))`);
      assert.equal(evidence.contract,'WEEKLY_SOURCE_OFFICE_NEXT_EVIDENCE_V1');
      assert.equal(evidence.components.available,true,'genuine Banking reader executed, not missing-dependency fallback');
      assert.equal(evidence.active_holds.available,true);
      const owningRevision=await query(`(select coalesce(current_revision_id,applied_revision_id)::text from private.bpay_next_work)`);
      assert.equal(evidence.components.request.expected_revision_id,owningRevision);
      assert.equal(evidence.components.page.activity_coverage.root_inflight_count,null);
      for(const key of ['components','active_holds']) decodeNextPaidEvidencePage(evidence[key].page,evidence[key].request);
      assert.equal(evidence.components.page.quantity_certificate.quantity,null,'never-paid fixture has no invented paid zero');
      console.log('GENUINE_BANKING_READER_SOURCE_CONTEXT_AND_MISSING_INFO_ISOLATION_PASS');
    }
    const bankReplay = await query(`(select to_jsonb(r) from private.bpay_next_stage_source_current_v1(
      '${rootId}','${bankRevision.source_event_id}',null) r)`);
    assert.equal(bankReplay.revision_id, bankRevision.id, 'exact Banking Source-event replay returns the earlier revision');
    assert.equal(await query(`(select count(*)=1 from private.bpay_next_work_revision)`), true);
    // Isolated verifier-predicate negatives, not a mock positive publication.
    // Change only raw detail/policy evidence and retain the genuinely derived
    // earlier basis. No business row or approved money is changed by this probe.
    const changedEvidence = await query(`(select jsonb_build_object(
      'detailRefused',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_DETAIL_V2',
        jsonb_build_object('segments',f.invoice_breakdown_json->'segments',
          'additional_units',f.additional_units_json,'actual_schedule',
          f.actual_schedule_json||jsonb_build_array(jsonb_build_object('changed_evidence',true)),
          'rate_source_refs',f.rate_source_refs_json)),'hex')<>${quote(approvedBefore.approval_basis)}->>'detail_digest',
      'policyRefused',encode(private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_APPROVED_POLICY_V2',
        f.policy_snapshot_json||jsonb_build_object('changed_evidence',true)),'hex')<>
          ${quote(approvedBefore.approval_basis)}->>'policy_fingerprint')
      from public.timesheets_financials f where f.timesheet_id='${rootId}' and f.is_current)`);
    assert.deepEqual(changedEvidence, { detailRefused: true, policyRefused: true });
    await sql("set local DateStyle='SQL, DMY';");
    const retainedReceipt = await query(`private.weekly_source_pay_query_local_receipt_v1('${rootId}',
      (select a.id from public.weekly_exceptional_payment_approvals a where
        a.creation_orchestration_run_id='${lastContext.orchestration_run_id}'),
      '${result.generation_id}')`);
    assert.deepEqual(retainedReceipt, receipt, 'accepted preauthorisation receipt survives first Authorise and SQL DMY DateStyle');
    await sql("set local DateStyle='ISO, MDY';");
    console.log('GENUINE_FRACTIONAL_FIRST_AUTHORISATION_REPLAY_AND_ISOLATED_EVIDENCE_NEGATIVES_PASS');
  }
  if (process.argv.includes('--authorised')) {
    const workIdentity = await query(`(select id::text from private.bpay_next_work)`);
    const savedAmendments = [];
    let boundVersion = result.family_bound_version;
    for (const [index, end, pay] of [[1,'18:00',85],[2,'19:00',95],[3,'16:00',65]]) {
      let laterQueryId;
      if(manualQueryProof) {
        const beforeQuery=await query(`jsonb_build_object(
          'inventory',private.weekly_source_effective_inventory_v1('${rootId}'),
          'bankRevisions',(select count(*) from private.bpay_next_work_revision),
          'effects',(select count(*) from private.bpay_next_financial_effect))`);
        const opened=await query(`public.weekly_source_manual_review_open_v1(${quote({
          actor_user_id:request.actor_user_id,source_row_id:importedQuerySource.upload_row_id,
          reason:`Recheck already authorised shift ${index}`,
          command_id:`b8550000-0000-4000-8000-00000000092${index}`,
        })})`);
        laterQueryId=opened.review_id;
        assert.equal(opened.ok,true);
        assert.equal((await query(`private.weekly_source_pay_query_gate_v1('${rootId}')`)).blocked,true);
        assert.deepEqual(await query(`jsonb_build_object(
          'inventory',private.weekly_source_effective_inventory_v1('${rootId}'),
          'bankRevisions',(select count(*) from private.bpay_next_work_revision),
          'effects',(select count(*) from private.bpay_next_financial_effect))`),beforeQuery,
          'querying an authorised shift changes no approved money or Banking revision/effect');
      }
      const amendment = {
        actor_user_id: request.actor_user_id, source_cycle_id: request.source_cycle_id,
        family_id: lastContext.family_id, work_event_id: lastContext.work_event_id,
        expected_family_bound_version: boundVersion,
        work_date: request.work_date, start: '09:00', end, break_minutes: 30,
        reason: `Actual amendment ${index} after genuine first authorisation.`,
        idempotency_key: `source-local-native-20261005-after-authorisation-${index}`,
      };
      const amended = await orchestrateWeeklyProtectedAction({ action: 'AMEND_PROTECTED_HOURS',
        request: amendment, dependencies });
      assert.equal(amended.ok, true);
      assert.equal(amended.outcome, 'PUBLISHED');
      if(manualQueryProof) {
        assert.equal(await query(`(select state='RESOLVED' and resolution_kind='PROTECTED_PAY'
          from private.weekly_source_manual_reviews where id='${laterQueryId}')`),true);
        assert.equal((await query(`private.weekly_source_pay_query_gate_v1('${rootId}')`)).blocked,false);
      }
      boundVersion = amended.family_bound_version;
      savedAmendments.push({ amendment, amended });
      const approvedAfter = await query(`private.weekly_source_effective_inventory_v1('${rootId}')`);
      assert.equal(approvedAfter.approval_basis?.origin?.kind, 'COMMITTED_SOURCE_HEAD_V1');
      assert.equal(Number(approvedAfter.approval_basis.approved_pay_ex_vat), pay);
      assert.equal(await query(`(select id::text from private.bpay_next_work)`), workIdentity,
        'later financial revisions preserve one WORK identity');
      const localOrigin = approvedAfter.approval_basis.origin.source_revision;
      assert.equal(localOrigin.origin_kind, 'PROTECTED_LOCAL_DECISION_V1');
      assert.equal(Object.hasOwn(localOrigin.before_origin, 'source_revision'), false,
        'successive Local origins retain one flat before-origin witness, not recursive ancestry');
      const latestReceipt = await query(`private.weekly_source_pay_query_local_receipt_v1('${rootId}',
        (select a.id from public.weekly_exceptional_payment_approvals a where
          a.creation_orchestration_run_id='${lastContext.orchestration_run_id}'),
        '${amended.generation_id}')`);
      assert.equal(latestReceipt?.lane, 'SOURCE_LOCAL');
      assert.equal(latestReceipt?.receipt_state, 'COMPLETE');
      const historicalStatus=await query(`private.weekly_source_local_saved_status_v1(
        '${lastContext.family_id}','${lastContext.orchestration_run_id}','${request.actor_user_id}')`);
      assert.equal(historicalStatus.requires_first_authorisation,false);
      assert.deepEqual(historicalStatus.result,{...amended,idempotent_replay:true});
    }
    const sealedBeforeReplay = await query(`jsonb_build_object(
      'heads',(select count(*) from public.weekly_source_entitlement_heads),
      'bankRevisions',(select count(*) from private.bpay_next_work_revision),
      'chosen',(select jsonb_agg(to_jsonb(d) order by d.head_id,d.component_id)
        from private.bpay_next_source_chosen_detail d),
      'current',private.weekly_source_effective_inventory_v1('${rootId}'))`);
    for (const { amendment, amended } of savedAmendments) {
      const before = calls.length;
      const historical = await orchestrateWeeklyProtectedAction({ action: 'AMEND_PROTECTED_HOURS',
        request: amendment, dependencies });
      assert.deepEqual(historical, { ...amended, idempotent_replay: true },
        'original accepted amendment replays after later supersession; only the explicit replay flag differs');
      assert.deepEqual(calls.slice(before), ['weekly_exceptional_pay_prepare_action_v1',
        'weekly_exceptional_pay_action_publication_status_v1','weekly_exceptional_pay_complete_local_v1']);
    }
    const initialReplay = await orchestrateWeeklyProtectedAction({ action: 'APPROVE_PROTECTED_HOURS', request, dependencies });
    assert.deepEqual(initialReplay, { ...result, idempotent_replay: true },
      'original preauthorisation Save remains exact after three HEADs; only the explicit replay flag differs');
    const retainedSavedStatus=await query(`private.weekly_source_local_saved_status_v1(
      '${lastContext.family_id}','${initialSavedRun}','${request.actor_user_id}')`);
    assert.deepEqual(retainedSavedStatus,savedStatus,
      'initial save status remains original even after first authorisation and three later HEADs');
    const sealedAfterReplay = await query(`jsonb_build_object(
      'heads',(select count(*) from public.weekly_source_entitlement_heads),
      'bankRevisions',(select count(*) from private.bpay_next_work_revision),
      'chosen',(select jsonb_agg(to_jsonb(d) order by d.head_id,d.component_id)
        from private.bpay_next_source_chosen_detail d),
      'current',private.weekly_source_effective_inventory_v1('${rootId}'))`);
    assert.deepEqual(sealedAfterReplay, sealedBeforeReplay, 'historical replay cannot publish, backfill chosen detail or reprice');
    console.log('GENUINE_LOCAL_THREE_HEAD_AMENDMENTS_AND_HISTORICAL_REPLAY_PASS');
    if(manualQueryProof) console.log('GENUINE_AUTHORISED_QUERY_NO_MONEY_CHANGE_AND_AMENDED_PROTECTION_REMEDY_PASS');
  }
  if(process.argv.includes('--withdraw')) await proveWithdrawal();
  if(sourceAbsenceKind) {
    const finaliseRequest=await query(`(select jsonb_build_object('actor_user_id','${request.actor_user_id}',
      'source_cycle_id',p.source_cycle_id,'authority_scope_kind',p.authority_scope_kind,'report_scope_id',p.report_scope_id,
      'upload_id',p.upload_id,'projection_publication_id',p.id,'expected_authority_scope_version',p.authority_scope_version,
      'expected_row_manifest_hash',encode(u.row_manifest_hash,'hex'),
      'expected_comparison_manifest_hash',encode(p.comparison_manifest_hash,'hex'),
      'expected_issue_set_hash',encode(p.issue_set_hash,'hex'))
      from public.weekly_source_projection_publications p join public.weekly_source_uploads u on u.id=p.upload_id
      where p.source_cycle_id='${request.source_cycle_id}' and p.state='CURRENT')`);
    if(sourceAbsenceKind==='ROSTER') {
      const {actor_user_id,upload_id,projection_publication_id,expected_authority_scope_version,
        expected_row_manifest_hash}=finaliseRequest;
      const prepareRequest={actor_user_id,upload_id,projection_publication_id,expected_authority_scope_version,
        expected_row_manifest_hash};
      const importPrepared=await query(`public.weekly_source_import_prepare_atomic_v1(${quote(prepareRequest)})`);
      assert.equal(importPrepared.status,'PREPARED');
    }
    const initialFinal=await query(`public.weekly_source_finalise_atomic_v1(${quote(finaliseRequest)})`);
    assert(initialFinal.final_revision_id,'genuine initial backing Final after pre-Final protection');
    genuineFinalRevisionId = initialFinal.final_revision_id;
    if(process.argv.includes('--final-attention')){
      await loadAttentionOwners();
      await sql('savepoint protected_final_attention;');
      const retainedContext=lastContext;
      try {
        const before=await query(`jsonb_build_object(
          'finance',(select jsonb_agg(to_jsonb(f) order by f.id) from public.timesheets_financials f),
          'bankRevisions',(select count(*) from private.bpay_next_work_revision))`);
        const attentionRequest={actor_user_id:request.actor_user_id,
          client_id:request.client_id,tab:'queries',section:'protected',attention_kind:'protected',limit:50};
        const ready=await query(`public.weekly_source_combined_review_workspace_v1(${quote(attentionRequest)})`);
        assert.equal(ready.attention.protected,1,'genuine Final creates one reconciliation decision');
        assert.equal(ready.rows.length,1);
        assert.equal(ready.rows[0].requires_attention,true);
        assert.equal(ready.rows[0].status.text,'Ready to reconcile');
        const wait=await orchestrateWeeklyProtectedAction({action:'WAIT_FOR_SOURCE',request:{
          actor_user_id:request.actor_user_id,source_cycle_id:request.source_cycle_id,
          family_id:lastContext.family_id,work_event_id:lastContext.work_event_id,
          expected_family_bound_version:await query(`(select bound_version::text from
            public.weekly_exceptional_pay_target_families where id='${lastContext.family_id}')`),
          reason:'Office reviewed this Final and is retaining protection',
          idempotency_key:'source-native-20261005-attention-final-wait'},dependencies});
        assert.equal(wait.ok,true);
        const acknowledged=await query(`public.weekly_source_combined_review_workspace_v1(${quote(attentionRequest)})`);
        assert.equal(acknowledged.attention.protected,0,'acknowledged source does not remain stuck in attention');
        assert.deepEqual(acknowledged.rows,[],'attention filter hides acknowledged waiting shift');
        const full=await query(`public.weekly_source_combined_review_workspace_v1(${quote({...attentionRequest,attention_kind:''})})`);
        assert.equal(full.counts.protected,1,'normal tab retains the protected shift');
        assert.equal(full.rows[0].requires_attention,false);
        assert.deepEqual(await query(`jsonb_build_object(
          'finance',(select jsonb_agg(to_jsonb(f) order by f.id) from public.timesheets_financials f),
          'bankRevisions',(select count(*) from private.bpay_next_work_revision))`),before,
          'review acknowledgment changes no finance or Banking revisions');
        console.log('GENUINE_FINAL_PROTECTED_ATTENTION_READY_ACKNOWLEDGED_FILTERED_PASS');
      } finally {
        await sql('rollback to savepoint protected_final_attention; release savepoint protected_final_attention;');
        lastContext=retainedContext;
      }
    }
    if(rosterRebindProof) {
      await sql(`savepoint source_roster_contract_rebind;
        update public.candidates set nhsp_hr_name_aliases='["NEXT imported Source worker"]'::jsonb
          where id='${request.candidate_id}';
        insert into public.contracts(id,candidate_id,client_id,start_date,end_date,pay_method_snapshot,rates_json,
          weekly_timesheet_source,self_bill,no_timesheet_required,requires_hr,autoprocess_hr,is_nhsp,
          week_ending_weekday_snapshot)
        select 'b8550000-0000-4000-8000-000000000024',candidate_id,client_id,start_date,end_date,
          pay_method_snapshot,rates_json,weekly_timesheet_source,self_bill,no_timesheet_required,
          requires_hr,autoprocess_hr,is_nhsp,0
        from public.contracts where id='${request.contract_id}';`);
      try {
        const rebound=await query(`pg_temp.bpsc_rebind(false,'2026-09-27',
          ${quote([{key:'manual-protection',date:request.work_date,end:'17:00',minutes:450,
            break:30,expense:0,prior_event:lastContext.work_event_id}])},
          'SOURCE_RETAINED_EVENT_NEW_CONTRACT_NATIVE',null,false)`);
        const context=await query(`public.weekly_source_upload_context_v1(${quote({
          operation:'BUILD_PROJECTION',actor_user_id:request.actor_user_id,upload_id:rebound.upload_id,
        })})`);
        assert.equal(context.rows.length,1);
        assert.deepEqual(context.rows[0].contracts.map(c=>c.contract_id).sort(),
          [request.contract_id,'b8550000-0000-4000-8000-000000000024'].sort(),
          'both contracts are genuinely discovered by the normal qualification context');
        const resolution=await query(`(select jsonb_build_object('id',r.id,'work_event_id',r.work_event_id,
          'contract_id',r.contract_id,'method',r.contract_selection_method)
          from public.weekly_source_row_resolutions r join public.weekly_source_upload_rows u on u.id=r.upload_row_id
          where u.upload_id='${rebound.upload_id}' and r.mapping_state='RESOLVED')`);
        assert.equal(resolution.work_event_id,lastContext.work_event_id,'same date/key retain the actual work event');
        assert.equal(resolution.contract_id,'b8550000-0000-4000-8000-000000000024');
        assert.equal(resolution.method,'OFFICE_SELECTED','never pretend B has prior durable lineage');
        await query(`public.weekly_source_timesheet_lineage_ensure_atomic_v1('${resolution.id}','${request.actor_user_id}')`);
        const newRoot=await query(`(select jsonb_build_object('id',timesheet_id,'week',week_ending_date)
          from public.weekly_source_row_timesheet_lineages where row_resolution_id='${resolution.id}')`);
        assert.notEqual(newRoot.id,rootId);
        assert.equal(newRoot.week,'2026-09-06','Sunday contract gets its configured work-week root');
        assert.equal(await query(`(select week_ending_date::text from public.timesheets where timesheet_id='${rootId}')`),
          '2026-09-02','prior contract root keeps its Wednesday work-week');
        const priorDuty=await query(`private.weekly_source_invoice_duty_v1('${rootId}')`);
        assert.equal(priorDuty.present,true);
        assert(priorDuty.basis.some(row=>row.lane==='PREFINAL_ROSTER_PRIOR_CURRENT_UNION' && row.present===true),
          'prior Wednesday root retains invoice duty despite current retained event being bound to Sunday root');
        const currentDuty=await query(`private.weekly_source_invoice_duty_v1('${newRoot.id}')`);
        assert.equal(currentDuty.present,true,'new contract root independently owns current row duty');
        assert(currentDuty.basis.some(row=>row.lane==='PREFINAL' && row.resolution_id===resolution.id),
          'new root positive duty specifically uses the genuine new resolution');
        await sql('set constraints all immediate; set constraints all deferred;');
        console.log('GENUINE_PREFINAL_ROSTER_RETAINED_EVENT_CONTRACT_REBIND_TWO_ROOT_DUTY_PASS');
      } finally { await sql('rollback to savepoint source_roster_contract_rebind; release savepoint source_roster_contract_rebind;'); }
    }
    if(!finalSourceAcceptance) {
    const nextRows=sourceAbsenceKind==='NHSP' ? [{key:'manual-protection-reversal',date:request.work_date,
      end:'17:00',minutes:450,break:30,expense:0,sign:-1,prior_event:lastContext.work_event_id}] : [];
    const laterImport=await query(`pg_temp.bpsc_import(${sourceAbsenceKind==='NHSP'},'2026-09-27',
      ${quote(nextRows)},'SOURCE_SELECTED_ABSENCE_NATIVE',null,${sourceAbsenceKind==='NHSP'})`);
    if(sourceAbsenceKind==='ROSTER') {
      const provisionalDuty=await query(`private.weekly_source_invoice_duty_v1('${rootId}')`);
      const union=provisionalDuty.basis.filter(row=>row.lane==='PREFINAL_ROSTER_PRIOR_CURRENT_UNION');
      assert.equal(union.length,1,'complete prior/current duty includes omitted prior event without a current row');
      assert.equal(union[0].present,true,'genuine attested roster omission remains invoice work until Final');
      const laterRequest=await query(`(select jsonb_build_object('actor_user_id','${request.actor_user_id}',
        'source_cycle_id',p.source_cycle_id,'authority_scope_kind',p.authority_scope_kind,'report_scope_id',p.report_scope_id,
        'upload_id',p.upload_id,'projection_publication_id',p.id,'expected_authority_scope_version',p.authority_scope_version,
        'expected_row_manifest_hash',encode(u.row_manifest_hash,'hex'),
        'expected_comparison_manifest_hash',encode(p.comparison_manifest_hash,'hex'),
        'expected_issue_set_hash',encode(p.issue_set_hash,'hex'))
        from public.weekly_source_projection_publications p join public.weekly_source_uploads u on u.id=p.upload_id
        where p.id='${laterImport.publication_id}')`);
      const {actor_user_id,upload_id,projection_publication_id,expected_authority_scope_version,expected_row_manifest_hash}=laterRequest;
      assert.equal((await query(`public.weekly_source_import_prepare_atomic_v1(${quote({actor_user_id,upload_id,
        projection_publication_id,expected_authority_scope_version,expected_row_manifest_hash})})`)).status,'PREPARED');
      assert((await query(`public.weekly_source_finalise_atomic_v1(${quote(laterRequest)})`)).final_revision_id);
      console.log('GENUINE_PREFINAL_ROSTER_OMISSION_INVOICE_DUTY_PASS');
    }
    }
    const absent=await query(`private.weekly_source_selected_source_witness_v1('${lastContext.family_id}',
      '${request.source_cycle_id}','${lastContext.work_event_id}')`);
    console.log('GENUINE_FINAL_SELECTED_ABSENCE_DIAGNOSTIC',JSON.stringify({kind:absent?.kind,
      basis_kind:absent?.basis_kind,basis_sha256:absent?.basis_sha256,scope:absent?.scope}));
    assert.equal(absent?.kind,finalSourceAcceptance ? 'CURRENT_ROW' : 'CERTIFIED_ABSENCE');
    if(!finalSourceAcceptance) assert.equal(absent?.basis_kind,sourceAbsenceKind==='NHSP' ? 'NHSP_FULL_REVERSAL' : 'ROSTER_CANCEL');
    assert.equal(absent.source_proposal.source_present,finalSourceAcceptance);
    assert.equal(Number(absent.source_proposal.source_minutes),finalSourceAcceptance ? 450 : 0);
    let acceptanceQueryId;
    if(finalSourceAcceptance) {
      const beforeQuery=await query(`jsonb_build_object('inventory',private.weekly_source_effective_inventory_v1('${rootId}'),
        'bankRevisions',(select count(*) from private.bpay_next_work_revision))`);
      const opened=await query(`public.weekly_source_manual_review_open_v1(${quote({
        actor_user_id:request.actor_user_id,source_row_id:importedQuerySource.upload_row_id,
        reason:'Office wants to accept the genuine final Source hours',
        command_id:'b8550000-0000-4000-8000-000000000950',
      })})`);
      assert.equal(opened.ok,true);
      acceptanceQueryId=opened.review_id;
      assert.equal((await query(`private.weekly_source_pay_query_gate_v1('${rootId}')`)).blocked,true);
      assert.deepEqual(await query(`jsonb_build_object('inventory',private.weekly_source_effective_inventory_v1('${rootId}'),
        'bankRevisions',(select count(*) from private.bpay_next_work_revision))`),beforeQuery,
        're-querying final protected Source changes no money');
    } else console.log('GENUINE_LATER_CUTOFF_SELECTED_SOURCE_ABSENCE_PASS',sourceAbsenceKind);
    const bound=await query(`(select bound_version from public.weekly_exceptional_pay_target_families
      where id='${lastContext.family_id}')`);
    const remove={actor_user_id:request.actor_user_id,source_cycle_id:request.source_cycle_id,
      family_id:lastContext.family_id,work_event_id:lastContext.work_event_id,expected_family_bound_version:bound,
      reason:'Accept the genuine final removal of this Source shift',
      idempotency_key:'source-local-native-final-absence-withdraw'};
    const removalAction=finalSourceReconcile ? 'ACCEPT_SOURCE_AND_RECONCILE' : 'WITHDRAW_PROTECTED_HOURS';
    if(finalSourceReconcile) Object.assign(remove,{expected_source_hash:absent.source_proposal.source_hash,
      expected_source_revision:absent.source_proposal.source_revision});
    const removed=await orchestrateWeeklyProtectedAction({action:removalAction,request:remove,dependencies});
    assert.equal(removed.ok,true);
    const after=await query(`private.weekly_source_effective_inventory_v1('${rootId}')`);
    assert.equal(after.ok,true);
    assert.equal(after.components.length,finalSourceAcceptance ? 1 : 0);
    if(process.argv.includes('--first-authorised') || process.argv.includes('--authorised')) {
      assert.equal(Number(after.approval_basis.approved_pay_ex_vat),finalSourceAcceptance ? 75 : 0);
      assert.equal(after.approval_basis.coverage_complete,true);
    } else {
      assert.equal(after.approval_basis,null,'pre-authorisation Save is not Banking authority');
      const savedZero=await query(`(select jsonb_build_object('pay',f.total_pay_ex_vat,'hours',f.total_hours,
        'authorised',t.authorised_at_server is not null) from public.timesheets_financials f
        join public.timesheets t on t.timesheet_id=f.timesheet_id
        where f.timesheet_id='${rootId}' and f.is_current)`);
      assert.equal(Number(savedZero.pay),finalSourceAcceptance ? 75 : 0);
      assert.equal(Number(savedZero.hours),finalSourceAcceptance ? 7.5 : 0);
      assert.equal(savedZero.authorised,false);
      const displayed=await query(`private.weekly_source_candidate_saved_local_hours_v2('${rootId}')`);
      assert.equal(displayed.state,'AVAILABLE','confirmed unauthorised saved decision has reliable hours display');
      assert.equal(Number(displayed.total_hours),finalSourceAcceptance ? 7.5 : 0);
    }
    assert.deepEqual(await orchestrateWeeklyProtectedAction({action:removalAction,request:remove,dependencies}),
      {...removed,idempotent_replay:true});
    if(finalSourceAcceptance) {
      assert.equal(await query(`(select state='RESOLVED' and resolution_kind='OFFICE_ACCEPTED_SOURCE'
        from private.weekly_source_manual_reviews where id='${acceptanceQueryId}')`),true);
      assert.equal((await query(`private.weekly_source_pay_query_gate_v1('${rootId}')`)).blocked,false);
      const resolvedSnapshot=await query(`jsonb_build_object('inventory',private.weekly_source_effective_inventory_v1('${rootId}'),
        'bankRevisions',(select count(*) from private.bpay_next_work_revision))`);
      const reopened=await query(`public.weekly_source_manual_review_open_v1(${quote({
        actor_user_id:request.actor_user_id,source_row_id:importedQuerySource.upload_row_id,
        reason:'A distinct Office question after source acceptance',
        command_id:'b8550000-0000-4000-8000-000000000951',
      })})`);
      assert.equal(reopened.ok,true);
      assert.deepEqual(await orchestrateWeeklyProtectedAction({action:removalAction,request:remove,dependencies}),
        {...removed,idempotent_replay:true});
      assert.equal(await query(`(select state='OPEN' from private.weekly_source_manual_reviews where id='${reopened.review_id}')`),true,
        'original accepted-removal replay cannot close a later Office query');
      assert.deepEqual(await query(`jsonb_build_object('inventory',private.weekly_source_effective_inventory_v1('${rootId}'),
        'bankRevisions',(select count(*) from private.bpay_next_work_revision))`),resolvedSnapshot);
      console.log('GENUINE_FINAL_SOURCE_REMOVAL_RESTORES_SOURCE_AND_CLOSES_MANUAL_QUERY_PASS',removalAction);
    } else console.log('GENUINE_FINAL_ABSENCE_WITHDRAWAL_ZERO_AND_REPLAY_PASS',sourceAbsenceKind);
  }
  if(process.argv.includes('--category')) {
    assert(process.argv.includes('--authorised'),'category proof requires genuine first Authorise and three HEADs');
    const inventory=await query(`private.weekly_source_effective_inventory_v1('${rootId}')`);
    if(process.argv.includes('--withdraw')) {
      const waiting=await query(`private.weekly_source_timesheet_category_v2('${rootId}')`);
      assert.equal(waiting.presentation_category,null,'queued position is not an APPLIED empty-root certificate');
      const bankPositionProof=await readFile(path.join(bankRoot,
        'tests/fixtures/bpay-next-local-draft-cancel-real.sql'),'utf8');
      for(const name of ['ldc_assert','ldc_apply_position']) {
        const saved=[...bankPositionProof.matchAll(new RegExp(
          'create function pg_temp\\.'+name+'\\([\\s\\S]*?\\$proof\\$;','gi'))];
        assert.equal(saved.length,1,'one exact Banking local-only position proof '+name);
        await sql(saved[0][0]);
      }
      const commands=await query(`(select jsonb_agg(p.command_id order by c.agency_sequence)
        from private.bpay_next_publication p join private.bpay_next_command c on c.id=p.command_id
        join private.bpay_next_work w on w.id=p.work_id
        where w.booking_id=(select booking_id from public.timesheets where timesheet_id='${rootId}')
          and p.status='QUEUED')`);
      assert(commands.length>0 && commands.length<=10,'finite fixture-created publication scope');
      for(const command of commands) await sql(`select pg_temp.ldc_apply_position('${command}');`);
    }
    const duty=await query(`private.weekly_source_operational_empty_v1('${rootId}','${inventory.head_id}')`);
    assert.equal(duty.authorisation_state,'AUTHORISED_CURRENT');
    assert.deepEqual(duty.scope,inventory.approval_basis.scope);
    assert.deepEqual(duty.approval_origin,inventory.approval_basis.origin);
    const withdrewOnlyShift=process.argv.includes('--withdraw');
    assert.equal(duty.duties.work,!withdrewOnlyShift,'worked-time duty follows the actual accepted inventory');
    assert.equal(duty.duties.owned_adjustment,false,'complete physical-family absence census');
    assert.equal(duty.effective_empty,withdrewOnlyShift,'only a complete negative duty census is empty');
    assert.equal(duty.banking_application.source_event_id,inventory.head_id);
    const category=await query(`private.weekly_source_timesheet_category_v2('${rootId}')`);
    const expectedCategory=withdrewOnlyShift ? 'WITHDRAWN' : 'PROCESSING_DELAYED';
    if(category.presentation_category!==expectedCategory) {
      console.log('CATEGORY_BOUNDARY_DIAGNOSTIC',JSON.stringify(await query(`jsonb_build_object(
        'category',private.weekly_source_timesheet_category_v2('${rootId}'),
        'components',(select jsonb_agg(jsonb_build_object('component_id',c.value->'component_id',
          'member',c.value->'component_member_identity','hours_day',c.value->'hours_day',
          'exclude',c.value->'exclude_from_pay')) from jsonb_array_elements(
            private.weekly_source_effective_inventory_v1('${rootId}')->'components') c(value)),
        'events',(select jsonb_agg(jsonb_build_object('id',e.id,'event',e.durable_work_event_id,
          'approval',e.evidence_approval_id,'state',e.state,'sequence',e.event_sequence))
          from public.weekly_exceptional_pay_family_events e))`)));
    }
    assert.equal(category.presentation_category,expectedCategory);
    assert.equal(category.processing_reason,withdrewOnlyShift ? null : 'Awaiting a valid import for invoicing');
    assert.equal(category.is_archived,false);
    assert.equal(category.archived_at_utc,null);
    const presentation=await query(`public.weekly_source_office_timesheet_presentation_v1(
      ${quote({actor_user_id:request.actor_user_id,timesheet_id:rootId})})`);
    assert.equal(presentation.applicable,true,'genuine Source-absent detail remains readable');
    assert.deepEqual(presentation.operational_category,category,'detail and canonical category agree');
    assert.equal(presentation.freshness,'CURRENT');
    assert.match(presentation.record_version,/^[a-f0-9]{64}$/);
    const batchStart=performance.now();
    const filters={ids:[rootId],limit:1,offset:0};
    const batch=await query(`public.weekly_source_office_summary_rows_v1(
      ${quote({actor_user_id:request.actor_user_id,p_filters:filters})})`);
    assert.equal(batch.contract,'WEEKLY_SOURCE_OFFICE_SUMMARY_ROWS_V1');
    assert.equal(batch.rows.length,1);
    assert.equal(batch.rows[0].timesheet_id,rootId);
    assert.equal(batch.rows[0].weekly_source_root_version,category.category_basis.facts.root_version);
    assert.deepEqual(batch.rows[0].weekly_source_operational_category,category);
    const canonical=await query(`(select to_jsonb(r) from public.timesheet_summary_lightweight_rows_v1(${quote(filters)}) r)`);
    const {weekly_source_root_version,weekly_source_operational_category,weekly_source_processing_reason,...unchanged}=batch.rows[0];
    assert.deepEqual(unchanged,canonical,'all canonical fields remain byte-equivalent JSON values');
    console.log('GENUINE_SOURCE_SUMMARY_SAME_SNAPSHOT_ONE_ROW_PASS',JSON.stringify({wallMs:performance.now()-batchStart}));
    // Invalid requests are caught inside savepoint-equivalent PL/pgSQL blocks,
    // so they cannot abort the outer rolled-back factual proof.
    for(const invalid of [
      {actor_user_id:request.actor_user_id,p_filters:{limit:0}},
      {actor_user_id:request.actor_user_id,p_filters:{limit:201}},
      {actor_user_id:request.actor_user_id,p_filters:{limit:1,disable_paging:true}},
      {actor_user_id:request.actor_user_id,p_filters:filters,source_present:false},
    ]) {
      await sql(`do $guard$ begin
        begin perform public.weekly_source_office_summary_rows_v1(${quote(invalid)});
          raise exception 'INVALID_BATCH_WAS_ACCEPTED';
        exception when sqlstate '22023' then null; end;
      end $guard$;`);
    }
    const earlier=await query(`(select id::text from public.weekly_source_entitlement_heads where head_revision=1)`);
    const stale=await query(`private.weekly_source_operational_empty_v1('${rootId}','${earlier}')`);
    assert.equal(stale.code,'ORIGIN_CHANGED');
    assert.equal(stale.effective_empty,null);
    console.log(withdrewOnlyShift ? 'GENUINE_ZERO_AUTHORISED_WITHDRAWN_CATEGORY_NO_PHYSICAL_ARCHIVE_PASS' :
      'GENUINE_CURRENT_ORIGIN_DUTY_AND_PROTECTED_ONLY_INVOICE_DELAY_PASS');
    // Ordinary unauthorised DAILY records test pagination only. They carry no
    // fabricated financial, authorisation, Source or Banking receipt evidence.
    await sql(`insert into public.timesheets(booking_id,occupant_key_norm,hospital_norm,
      ward_norm,job_title_norm,week_ending_date,contract_id,sheet_scope,line_type)
      select 'source-summary-page-20261005-'||n,t.occupant_key_norm,t.hospital_norm,
        t.ward_norm,t.job_title_norm,t.week_ending_date,t.contract_id,'DAILY','HOURS'
      from public.timesheets t cross join generate_series(1,199) n
      where t.timesheet_id='${rootId}';`);
    const ordinaryIds=await query(`(select jsonb_agg(t.timesheet_id::text order by t.booking_id)
      from public.timesheets t where t.booking_id like 'source-summary-page-20261005-%')`);
    assert.equal(ordinaryIds.length,199);
    for(const size of [17,100,200]) {
      const pageFilters={ids:[rootId,...ordinaryIds.slice(0,size-1)],limit:size,offset:0};
      const started=performance.now();
      const page=await query(`public.weekly_source_office_summary_rows_v1(
        ${quote({actor_user_id:request.actor_user_id,p_filters:pageFilters})})`);
      assert.equal(page.rows.length,size);
      const canonicalPage=await query(`(select jsonb_agg(to_jsonb(r))
        from public.timesheet_summary_lightweight_rows_v1(${quote(pageFilters)}) r)`);
      assert.deepEqual(page.rows.map(({weekly_source_root_version,weekly_source_operational_category,
        weekly_source_processing_reason,...row})=>row),canonicalPage,
        'native mixed page preserves original membership, order and all canonical values');
      const actualSource=page.rows.find(row=>row.timesheet_id===rootId);
      assert.deepEqual(actualSource.weekly_source_operational_category,category);
      for(const row of page.rows.filter(row=>row.timesheet_id!==rootId)) {
        assert.equal(Object.hasOwn(row,'weekly_source_root_version'),false,
          'ordinary non-import row receives no Source metadata');
      }
      console.log('GENUINE_SOURCE_SUMMARY_MIXED_PAGE_PASS',JSON.stringify({size,
        wrapperAndParityWallMs:performance.now()-started}));
    }
    const ordinaryFilters={ids:ordinaryIds.slice(0,17),limit:17,offset:0};
    const ordinaryPage=await query(`public.weekly_source_office_summary_rows_v1(
      ${quote({actor_user_id:request.actor_user_id,p_filters:ordinaryFilters})})`);
    const ordinaryCanonical=await query(`(select jsonb_agg(to_jsonb(r))
      from public.timesheet_summary_lightweight_rows_v1(${quote(ordinaryFilters)}) r)`);
    assert.deepEqual(ordinaryPage.rows,ordinaryCanonical,'ordinary-only page stays exact');
    assert.equal(await query(`(select count(*) from public.timesheets t
      where t.booking_id like 'source-summary-page-20261005-%' and t.authorised_at_server is not null)`),0);
    console.log('GENUINE_SOURCE_SUMMARY_ORDINARY_ONLY_UNCHANGED_PASS');
    const offsetFilters={ids:[rootId,...ordinaryIds],limit:17,offset:13};
    const offsetPage=await query(`public.weekly_source_office_summary_rows_v1(
      ${quote({actor_user_id:request.actor_user_id,p_filters:offsetFilters})})`);
    const offsetCanonical=await query(`(select jsonb_agg(to_jsonb(r))
      from public.timesheet_summary_lightweight_rows_v1(${quote(offsetFilters)}) r)`);
    assert.deepEqual(offsetPage.rows.map(({weekly_source_root_version,weekly_source_operational_category,
      weekly_source_processing_reason,...row})=>row),offsetCanonical,'native offset is unchanged');
    await sql(`insert into public.contract_weeks(contract_id,week_ending_date,additional_seq)
      values('${request.contract_id}','${request.week_ending_date}',1);`);
    const openWeekId=await query(`(select id::text from public.contract_weeks
      where contract_id='${request.contract_id}' and week_ending_date='${request.week_ending_date}'
        and additional_seq=1)`);
    const weekFilters={contract_week_ids:[openWeekId],limit:1,offset:0};
    const weekPage=await query(`public.weekly_source_office_summary_rows_v1(
      ${quote({actor_user_id:request.actor_user_id,p_filters:weekFilters})})`);
    const weekCanonical=await query(`(select jsonb_agg(to_jsonb(r))
      from public.timesheet_summary_lightweight_rows_v1(${quote(weekFilters)}) r)`);
    assert.equal(weekPage.rows.length,1,'unsubmitted contract-week row remains discoverable');
    assert.equal(weekPage.rows[0].timesheet_id,null);
    assert.deepEqual(weekPage.rows,weekCanonical,'contract-week-only row remains exact');
    console.log('GENUINE_SOURCE_SUMMARY_OFFSET_AND_NULL_TIMESHEET_CONTRACT_WEEK_PASS');
    // Adversarial queued manifests are test inputs, not completed invoice or
    // payment authority. Real constraints/reader owners run; each case rolls
    // back before the next, and no queue consumer or external effect runs.
    const canonicalMember={source_type:'TIMESHEET',source_id:rootId,related_timesheet_id:rootId};
    for(const [label,payload,expected] of [
      ['valid-uppercase',{canonical_source_members:[{...canonicalMember,
        source_id:rootId.toUpperCase(),related_timesheet_id:rootId.toUpperCase()}]},true],
      ['invalid-source-id',{canonical_source_members:[{...canonicalMember,source_id:'bad'}]},null],
      ['contradictory-pair',{canonical_source_members:[{...canonicalMember,source_id:ordinaryIds[0]}]},null],
      ['bad-first-owned-last',{canonical_source_members:[{source_type:'TIMESHEET',source_id:'bad'},canonicalMember]},null],
      ['malformed-primary',{canonical_source_members:{bad:true},canonical_source_ids:[rootId.toUpperCase()]},null],
      ['unrelated-malformed',{canonical_source_members:[{source_type:'TIMESHEET',source_id:'bad'}]},false],
    ]) {
      await sql(`savepoint source_invoice_manifest_case;
        with operation as (insert into public.invoice_operations(operation_type,actor_user_id,idempotency_key,
          config_json) values('GENERATE_INVOICES','${request.actor_user_id}','source-duty-${label}',
          jsonb_build_object('processor_policy',private._invoice_processor_limits())) returning id)
        insert into public.invoice_operation_chunks(operation_id,chunk_type,phase,sequence_no,work_key,payload_json)
          select id,'GENERATION_GROUP','NEW',1,repeat('d5',32),${quote(payload)} from operation;`);
      try {
        const observed=await query(`private.weekly_source_invoice_duty_v1('${rootId}')`);
        assert.equal(observed.present,expected,label);
        assert.equal(observed.discovery_complete,expected!==null,label);
        assert.deepEqual(observed.scope,inventory.approval_basis.scope,label);
        if(expected!==false) {
          const guarded=await query(`private.weekly_source_operational_empty_v1('${rootId}',null)`);
          assert.equal(guarded.duties.invoice_task,expected,label);
          assert.equal((await query(`private.weekly_source_timesheet_category_v2('${rootId}')`)).presentation_category,null,
            'invoice work or uncertainty cannot become Withdrawn/awaiting-import override');
        }
      } finally { await sql('rollback to savepoint source_invoice_manifest_case; release savepoint source_invoice_manifest_case;'); }
    }
    assert.deepEqual(await query(`private.weekly_source_timesheet_category_v2('${rootId}')`),category,
      'queued-manifest negative probes leave original category/evidence unchanged');
    console.log('GENUINE_QUEUED_INVOICE_MANIFEST_TRUE_FALSE_UNKNOWN_AND_NO_FALSE_WITHDRAWN_PASS');
  }
  if(process.argv.includes('--next-paid-context')) {
    assert(finalSourceAcceptance || process.argv.includes('--category'),
      'full public Office proof needs genuine finalised source or separately qualified Source-absent category');
    const publicRequest={actor_user_id:request.actor_user_id,timesheet_id:rootId};
    const raw=await query(`public.weekly_source_office_timesheet_presentation_v1(${quote(publicRequest)})`);
    const attached=await calculatorModule.exports.weeklySourceOfficePresentationInternals
      .attachWeeklySourceOfficeTimesheetPresentation({},
        {timesheet_id:rootId,sheet_scope:'WEEKLY',is_import_authoritative:true},request.actor_user_id,
        async(_env,name,args)=>{
          assert.equal(name,'weekly_source_office_timesheet_presentation_v1');
          return query(`public.${name}(${quote(args.p_request)})`);
        });
    const presented=attached.weekly_source_presentation;
    assert.equal(raw.root_timesheet_id,rootId,'genuine Source identity, not a Banking page root');
    assert.equal(presented.record_version,raw.record_version);
    assert.equal(presented.action_state.authorise_allowed,raw.action_state.authorise_allowed);
    assert.equal(presented.lifecycle.schedules.current_paid.available,false,'no invented paid zero for never-paid genuine source');
    assert.equal(presented.lifecycle.schedules.processing.available,false);
    assert.equal(raw.lifecycle.schedules.processing.reason,'ROOT_ACTIVITY_NOT_INDEXED',
      'raw Source also keeps unknown processing distinct from no payment in flight');
    assert(!['AUTHORISED_NOT_PAID','PAID','ADJUSTMENT_SETTLED','PAYMENT_PROCESSING'].includes(presented.lifecycle.server_phase),
      'unknown NEXT root activity never becomes a legacy financial phase');
    assert.equal(presented.lifecycle.authorisation_state,'AUTHORISED');
    assert.equal(presented.lifecycle.schedules.currently_approved.available,true,'real I7 entitlement stays independent');
    assert.match(presented.next_paid_information.read_fingerprint,/^[a-f0-9]{64}$/);
    console.log('GENUINE_FINALISED_SOURCE_PUBLIC_SQL_WORKER_NEXT_INFORMATION_CONSUMER_PASS');
    if(finalSourceAcceptance) {
      const officeFile=path.resolve(root,'../office-inactive-candidate-20261003/js/weekly-source-presentation-v1.js');
      await readFile(officeFile,'utf8');
      const office=Module.createRequire(import.meta.url)(officeFile);
      await sql('savepoint source_next_proposal_visibility;');
      try {
        const finalId=genuineFinalRevisionId;
        assert(finalId,'exact genuine finalisation receipt, not the earlier provisional action cycle');
        const composed=await query(`private.weekly_source_entitlement_components_v1(
          private.weekly_source_ordinary_projection_current_segments_v1('${rootId}','${finalId}'),
          private.weekly_source_ordinary_projection_current_expenses_v1('${rootId}','${finalId}'))`);
        const proposalRequest=await query(`private.weekly_source_entitlement_proposal_request_v1(
          '${rootId}','${finalId}','LOCKED_FINAL_SOURCE',
          'd1500000-0000-4000-8000-000000000001',1,
          'd1500000-0000-4000-8000-000000000002','d1500000-0000-4000-8000-000000000003',${quote(composed)})`);
        const before=await query(`jsonb_build_object('heads',(select count(*) from public.weekly_source_entitlement_heads),
          'inventory',private.weekly_source_effective_inventory_v1('${rootId}'),
          'bankRevisions',(select count(*) from private.bpay_next_work_revision))`);
        const recorded=await query(`private.weekly_source_entitlement_proposal_record_v1(${quote(proposalRequest)},
          (select g.agency_id from public.weekly_source_groups g join public.weekly_source_cycles c
            on c.source_group_id=g.id join public.weekly_source_final_revisions r
            on r.source_cycle_id=c.id where r.id='${finalId}'),
          '${request.contract_id}',(select week_ending_date from public.timesheets where timesheet_id='${rootId}'),
          '${request.actor_user_id}')`);
        assert.equal(recorded.state,'PROPOSED');
        const proposalAttached=await calculatorModule.exports.weeklySourceOfficePresentationInternals
          .attachWeeklySourceOfficeTimesheetPresentation({},
            {timesheet_id:rootId,sheet_scope:'WEEKLY',is_import_authoritative:true},request.actor_user_id,
            async(_env,name,args)=>query(`public.${name}(${quote(args.p_request)})`));
        const genuine=proposalAttached.weekly_source_presentation;
        assert.equal(genuine.proposal.state,'PROPOSED');
        assert.equal(genuine.proposal.request_digest_verified,true,'actual current composer re-digested by actual Source reader');
        assert.equal(genuine.lifecycle.ok,false,'no invented paid/unpaid phase to expose the existing proposal');
        assert.deepEqual(genuine.lifecycle.errors.map(e=>e.code),['ROOT_ACTIVITY_NOT_INDEXED']);
        const vm=office.buildViewModel(proposalAttached);
        assert.equal(vm.independent_proposal_decision,true);
        for(const render of [office.renderSimpleLines,office.renderApprovedHours]) {
          const html=render(vm);
          assert.match(html,/data-weekly-source-decision="APPROVE_UPDATED_HOURS"/);
          assert.match(html,/data-weekly-source-decision="KEEP_CURRENTLY_APPROVED_HOURS"/);
          assert.doesNotMatch(html,/LATER_CHANGE_PENDING_PAID|LATER_CHANGE_PENDING_UNPAID/);
        }
        const command=office.buildLaterChangeDecisionCommand(vm,
          {actor_user_id:request.actor_user_id,decision:'KEEP_CURRENTLY_APPROVED_HOURS'});
        assert.deepEqual(command.body,{...genuine.proposal.decision.command_payload,
          actor_user_id:request.actor_user_id,decision:'KEEP_CURRENTLY_APPROVED_HOURS'});
        assert.deepEqual(await query(`jsonb_build_object('heads',(select count(*) from public.weekly_source_entitlement_heads),
          'inventory',private.weekly_source_effective_inventory_v1('${rootId}'),
          'bankRevisions',(select count(*) from private.bpay_next_work_revision))`),before,
          'proposal rendering/command preparation changes no entitlement or Banking revision');
        console.log('GENUINE_SOURCE_COMPOSED_VERIFIED_SINGLE_ROOT_PROPOSAL_NEXT_OFFICE_RENDER_AND_EXACT_COMMAND_PASS');
      } finally { await sql('rollback to savepoint source_next_proposal_visibility; release savepoint source_next_proposal_visibility;'); }
    }
  }
  await sql('set constraints all immediate; rollback;');
  assert.equal(await query(`not exists(select 1 from public.timesheets)
    and not exists(select 1 from private.weekly_source_local_protected_decision_receipts)`), true);
  const pins={};
  for(const [filename,sha256] of loadedPins) {
    assert.equal(createHash('sha256').update(await readSavedFile(filename)).digest('hex'),sha256,
      'proof requires unchanged loaded source at completion');
    pins[path.relative(path.dirname(root),filename).replaceAll('\\','/')]=sha256;
  }
  console.log(JSON.stringify({ result: 'GENUINE_LOCAL_PREAUTHORISATION_CALCULATOR_SAVE_REPLAY_PASS',
    hours: Number(financial.hours), approvedPay: Number(financial.pay), rolledBack: true,
    nativeImportedManualQueryProtection: manualQueryProof ? 'PASS' : 'NOT_RUN',
    nativeFirstAuthorisation: process.argv.includes('--authorised') || process.argv.includes('--first-authorised') ? 'PASS' : 'NOT_RUN',
    nativeAuthorisedPublication: process.argv.includes('--authorised') ? 'PASS' : 'NOT_RUN',
    proofFlags:process.argv.slice(2),calculatorBundleSha256,pins,calls }));
} finally {
  globalThis.fetch = originalFetch;
  if (!child.stdin.destroyed) child.stdin.end('rollback;\n');
}
