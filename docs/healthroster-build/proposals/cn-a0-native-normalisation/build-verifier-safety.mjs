import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const root=process.argv[2],here=path.dirname(fileURLToPath(import.meta.url));
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const expected='547c74da5f0a05b09297c042bc7f3cc0c0105b85';
const head=spawnSync('git',['-C',root,'rev-parse','HEAD'],{encoding:'utf8',windowsHide:true});assert.equal(head.stdout.trim(),expected);
const releaseBytes=fs.readFileSync(path.join(root,'supabase/release/current-release.json'));
const release=JSON.parse(releaseBytes);
const exclusions=new Map([
 [61,['GENUINE_APPLYING_RELEASE_REQUIRED','Nested selection fixture requires exactly one matching real unfinished APPLYING release outside banking_modal_v2_test; rollback rehearsal cannot create or impersonate that release.']],
 [66,['GENUINE_APPLYING_RELEASE_REQUIRED','Same exact nested APPLYING-release/identity/commit/mode gate as index 61. Reader calls themselves do not execute a payment.']],
 [70,['GENUINE_APPLYING_RELEASE_REQUIRED_AND_DRAFT_FINANCIAL_FINALISATION','Same nested genuine APPLYING gate, plus actual pay_batch_finalize_reservations_and_markers call on the synthetic Draft. Exclude the whole root; catalogue/pure recovery tests 69/71 remain eligible.']],
 [77,['ACTUAL_QUEUE_CLAIM','Executes settled-certificate due_claim/build_start/append/seal and certified Draft operation_start, not merely their definition inspection. Exclude this queue lifecycle; keep separate catalogue verifiers 73/74/75/76/78.']],
 [98,['ACTUAL_EMAIL_AND_RENDER_QUEUE_CLAIM','Executes email_outbox_claim_ready_batch plus candidate_expense_summary_claim_v1/render_begin/complete. A rollback does not make queue-draining commands eligible under SEND5B. Exclude the whole root.']]
]);
const notes={
56:'Synthetic empty-carrier inserts; primary-action and invocation projections only.',
57:'Synthetic expense carrier, actual database finalise/cleanup/cancel owners; no external render or send and no outbox drain. Success fixture writes canonical financial rows inside the rollback unit.',
58:'Synthetic duplicate-expense fixture invokes Office authorisation owners, not payment execution. Includes rollback-only conditional reduced-catalogue helpers and an unconditional bulk row-patch reader replacement; do not mistake this for full native reader acceptance.',
59:'Catalogue index definition only.',60:'Catalogue/ACL plus missing-session negative read.',
62:'Synthetic federated membership/session projection and authority reads.',63:'Synthetic monotonic session projection and stale/conflicting negative admission.',64:'Synthetic independent session-family projection and uniqueness/ACL checks.',
65:'Reads installed function definitions/ACLs; financial-looking source strings are not invoked.',
67:'Synthetic paper-generation cancellation and stored SENT-mail fixtures. Cancellation retires live generation; neither sends nor claims those stored delivery rows.',
68:'Synthetic no-work capability/action and no_work_atomic owners; worked case refuses, empty week is transactionally removed.',
69:'Catalogue/ACL plus pure JSON signed non-charge recovery classifier; no finalizer invocation.',
71:'Catalogue/ACL plus pure JSON recovery classifier and duplicate-component refusal.',
72:'Synthetic source-build repair/enqueue jobs; native repair terminalises/rebinds source jobs, never executes Draft/provider/payment/settlement. No source-build worker claim/drain is invoked.',
73:'Settled-certificate metadata/ACL/index catalogue; no certificate claim or settlement invocation.',
74:'Certified-Draft routine/table/index definitions and ACLs; no Draft operation invocation.',
75:'Integrity helper installed-definition/metadata/ACL checks only.',
76:'Cancellation-selection/process routine definitions and ACLs only; no selection/process execution.',
78:'Draft item writer definition/alias/ACL inspection only; no writer call.',
79:'Synthetic break-entry context read; no paper delivery.',80:'Paper mail authority definition and existing-row invariant reads only.',
81:'Synthetic certified-source preview publication and wrong-target refusal; no payment, provider, settlement, mail send or worker drain.',
82:'Synthetic pre-route break context reads.',83:'Synthetic import-source break context reads.',
84:'Finalizer function configuration catalogue only.',85:'Synthetic native expense-apply materialisation and final-state guard; database financial write only, no payment/provider/settlement/mail sender or queue drain.',
86:'Single-flight wrapper definition/ACL inspection only.',
87:'Synthetic frozen-settings authority fixture with policy/context/impact-preview/import-review/VAT-policy reads; no financial execution or delivery.',
88:'Synthetic email-admission capabilities and placement reads; no email action.',
89:'Synthetic carrier resolve/create and workflow CREATE/recovery/rejection transitions; native intent writes may occur transactionally, no mail claim/send.',
90:'Bank movement constraint and ingest definition only; no bank-event ingestion.',91:'No-money unwind definition/result-arity inspection only; no unwind invocation.',
92:'Synthetic protected additional/expense primary-action and action-contract projections.',
93:'Synthetic carrier/create workflow sequencing and primary action; no manager mail dispatch.',
94:'Synthetic targetless paper preparation/replay and manifest promotion; creates held native outbox intent without claim, lease, rendered attachment or delivery.',
95:'Route-family context reads for existing Contract Weeks (up to 500) and catalogue checks; do not retain raw payload/error data.',
96:'Unified outbox list reads and bad-sort negative admission; no claim or sender.',
97:'Synthetic native deletion/reject guards and apply/reject calls; retire workflow/queued intent only, no sender or queue draining.',
99:'Historical settings-authority/backfill absence invariants only; no backfill execution.',
100:'Receipt prepare owner definition, ACL and index inspection only.',
101:'Synthetic receipt prepare/complete and workflow submit/reuse negatives, temporary wrapper functions and dynamic EXPLAIN SELECT only. Native upload/intent metadata is transactional; no Worker render/send/drain.',
102:'Receipt reupload owner/index/trigger definitions and current-row duplicate invariants only.',
103:'Source-less cancellation routines are strings passed to catalogue readers; active cutover counts are read-only, no cancellation apply.',
104:'Canonical producer definition/ACL plus pure preview JSON contract classification.',
105:'Five intentionally invalid cancellation-notice calls: conflicting mode, half cursor, prohibited external cursor, excessive limit, unsupported template. Controlling 07092026_2301 rejects each before mode selection/first row lock/write. No valid reconciliation path is called.',
106:'Correction request-start definition/ACL/index and active legacy cutover counts only; no PREPARE/START invocation.',
107:'Dirty cohort helper/processor metadata, ACL, installed definitions/indexes only; no processor execution.',
108:'Scope invalidator/caller/trigger census only; no invalidation invocation.',
109:'Cancel-return and preview-publication routine definitions/ACL/index only.',
110:'No-money process owner definition/config/ACL only; no process invocation.',
111:'Duplicate-expense anchor owner hash/config/ACL only.'
};
function inspect(file,seen=new Set()){
 if(seen.has(file))return [];seen.add(file);
 const bytes=fs.readFileSync(path.join(root,file)),text=bytes.toString('utf8');
 // Mask comment and single-quoted strings to distinguish catalogue defstrings
 // from executable schema-qualified references; retain DO/function dollar bodies.
 const code=text.replace(/\/\*[\s\S]*?\*\//g,' ').replace(/--[^\n]*/g,' ').replace(/'(?:''|[^'])*'/g,"''");
 const refs=[...code.matchAll(/\b(public|private|pg_temp|net|http|cron)\.([a-zA-Z_0-9]+)\s*\(/g)].filter(m=>!/(?:insert\s+into|create\s+(?:or\s+replace\s+)?(?:function|table)|alter\s+function|on\s+function)\s*$/i.test(code.slice(Math.max(0,m.index-90),m.index))).map(m=>m[1]+'.'+m[2]);
 const includes=[...text.matchAll(/^\s*\\i[r]?\s+([^\r\n]+)/gm)].map(m=>path.posix.normalize(path.posix.join(path.posix.dirname(file),m[1].trim().replaceAll('\\','/'))));
 return [{file,sha256:sha(bytes),bytes:bytes.length,includes,calls:[...new Set(refs)].sort(),dynamicExecutePresent:/\bexecute\b/i.test(code),transactionDirectives:text.split('\n').filter(l=>/^\s*(begin\s*;|rollback\s*;|commit\s*;|\\set ON_ERROR|set local statement_timeout)/i.test(l)).map(l=>l.trim())},...includes.flatMap(f=>inspect(f,seen))];
}
const files=[];
for(let index=56;index<=111;index++){
 const file=release.verificationFiles[index],closure=inspect(file),excluded=exclusions.get(index);
 assert.ok(notes[index]||excluded,'missing per-file review');
 files.push({index,file,sha256:closure[0].sha256,decision:excluded?'EXCLUDE':'ELIGIBLE_ROLLBACK_REHEARSAL',category:excluded?excluded[0]:(closure[0].calls.length?'PURE_OR_TRANSACTIONAL_DATABASE_FIXTURE':'CATALOGUE_OR_READ_INVARIANT'),reason:excluded?.[1]??notes[index],closure});
}
const calleeInputs=[
'supabase/repeatable/07092026_2301_banking_pay_payment_cancellation_notice_reconcile_v1.sql',
'supabase/repeatable/04092026_1420_candidate_finalisation_single_flight_v1.sql',
'supabase/repeatable/10102026_1347_import_expense_qr_approval_v1.sql',
'supabase/repeatable/01092026_1647_banking_pay_signed_recovery_classifier_v1.sql',
'supabase/repeatable/04102026_0558_banking_pay_next_legacy_email_claim_boundary_v1.sql'
].map(file=>({file,sha256:sha(fs.readFileSync(path.join(root,file)))}));
const result={contract:'HEALTHROSTER_VERIFIER_REHEARSAL_SAFETY_V1',status:'SOURCE_SAFETY_REVIEW_ONLY',rootCommit:expected,release:{file:'supabase/release/current-release.json',sha256:sha(releaseBytes)},range:{first:56,last:111,count:56},counts:{eligible:files.filter(f=>f.decision!=='EXCLUDE').length,excluded:exclusions.size},method:'Full source bodies and nested include loaded; executable control flow/call references distinguished from quoted function-definition inspections. Concrete queue/APPLYING exclusions retained. No SQL or hosted action executed. This is rehearsal eligibility, not runtime acceptance.',harnessRequirements:['Expand includes preserving file boundaries/source hashes.','Suppress only outer BEGIN/COMMIT/ROLLBACK and ON_ERROR_STOP directives when joining the single owned transaction; preserve PL/pgSQL BEGIN blocks and nested exception/savepoint semantics.','Reapply jit=off, pg_show_plans.is_enabled=off, statement_timeout=0 after verifier-local settings as required by SEND5B; do not introduce a harness time limit.','Never fabricate APPLYING context, substitute a real release or weaken a verifier to qualify it.','Redact result/error payloads; retain only data-free file/statement/object/error code and approved bounded diagnosis.','Fresh connection catalogue/ledger/marker/LEGACY:1 rollback verification remains root-owned.'],calleeInputs,files};
fs.writeFileSync(path.join(here,'VERIFIER_SAFETY_56_111.json'),JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify({files:files.length,...result.counts,sha256:sha(fs.readFileSync(path.join(here,'VERIFIER_SAFETY_56_111.json')))}));
