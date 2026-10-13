import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
const out = dirname(fileURLToPath(import.meta.url));
const census = JSON.parse(readFileSync(resolve(out, 'VERIFIER_EXECUTION_CENSUS_112_167.json')));
const catalogue = new Set([112,132,151,154,155]);
const pure = new Set([114,158,159,161,162,163,164,165,166,167]);
const excluded = new Map([
  [118, 'Executed pg_temp claim loops invoke actual Source message command and target claim owners; they can lease real hosted rows before filtering to fixture. This is dispatch queue processing, although SQL rollback prevents durable leases and no Worker sends mail.'],
  [119, 'Executed paid-fixture helper calls public.pay_workbench_dirty_apply_jobs_chunk, the actual job claim/apply processor. Candidate-bound fixture filtering does not change the prohibited queue-processing action.'],
  [121, 'Includes the complete ordinary-pay-projection verifier (119), which calls the actual Workbench dirty job processor; exclude the containing root too.'],
  [122, 'Includes the complete ordinary-pay-projection verifier (119), which calls the actual Workbench dirty job processor; exclude the containing root too.'],
  [125, 'Executed paid-fixture preparation calls public.pay_workbench_dirty_apply_jobs_chunk; this is the actual queue processor, not a seeded settled evidence row or a read-only phase projection.'],
  [140, 'Includes the complete ordinary-pay-projection verifier (119), which calls the actual Workbench dirty job processor; exclude the containing root too.'],
  [144, 'Executes private.weekly_source_pending_entitlement_release_claim_page_v1 and release apply owners. The claim first runs the native watch page, then leases due native PENDING/RELEASING bundles with SKIP LOCKED and changes state/lease. It is pending-publication queue processing.'],
  [147, 'Executed paid-fixture preparation calls public.pay_workbench_dirty_apply_jobs_chunk, the actual queue claim/apply processor.'],
  [148, 'Executed paid-fixture preparation calls public.pay_workbench_dirty_apply_jobs_chunk. The separately named pg_temp drain helper is only fixture DML, but it does not remove this actual native queue-processor invocation.'],
  [152, 'Executes public.email_outbox_claim_ready_batch for forged/paper/valid copy probes. Even with fixture-first ordering and a sentinel, this is the real generic mail claim owner and therefore excluded dispatch queue processing.'],
]);
const reasons = new Map([
  [113, 'Isolated Candidate account/workflow/Contract-week fixture; executes exact preview/reject/delete SQL owners. The rejection inserts a Candidate notification record; SQL does not send email or perform R2 deletion.'],
  [115, 'Rollback upload/stage/seal/publication fixtures and exact replay/CAS refusals. Native projection publication writes Source comparison/query intent data; no dispatch claim/send or payment/settlement executor is invoked.'],
  [116, 'Rollback Source context, stage/rows/seal/publication and Office recheck fixtures; no queue claim/drain, provider or financial payment execution.'],
  [117, 'Rollback projection mapping/issue/lineage fixtures. Included key capture installs temporary counter/key triggers; no external effects or queue processor.'],
  [120, 'Rollback Source preparation/finalisation/charge-accept fixtures. Source economic and billing authority plus entitlement preparation are transaction-contained SQL; no payment-provider submission, settlement recording, message claim or job drain is called.'],
  [123, 'Included isolated invoice fixture has scoped native Source prepare/finalise/ordinary projection; root tests read the batch generation classifier/line probes. It does not include verifier 119 or invoke the dirty job processor.'],
  [124, 'Includes finalisation fixture 120 and calls Source finalisation-pay task state/receipt open/start/finish/unknown/recover owners. This is transaction-contained materialisation orchestration metadata, not payment execution, provider submission or queue drain.'],
  [126, 'Rollback policy/Contract/group fixtures and effective-policy reads/hashes only.'],
  [127, 'Rollback global/group/Client/Contract settings owners and open-cycle helper. No payment, provider, settlement, delivery claim or queue processor.'],
  [128, 'Rollback native Client creation/replay and effective-settings read; no external action.'],
  [129, 'Rollback protected family/action/context/WAIT and Source finalisation fixtures. WAIT is an audited retained decision, not a financial payment/provider action; no C1 transport invocation or queue processor.'],
  [130, 'Rollback protected family preparation/editor context and installed-definition checks. The publisher bodies are inspected as strings; no publish/transport/payment owner is executed.'],
  [131, 'Rollback Candidate request/draft/submit/materialisation/workflow transitions and query intent SQL. Notification/outbox insertions remain uncommitted; no sender, dispatch claim or queue processor is invoked.'],
  [133, 'Rollback exact Correct Final open/cancel/CAS fixtures, including a temporary swallowing trigger negative control. No payment/provider/settlement/queue owner.'],
  [134, 'ACL/type/immutability fixtures plus temporary trigger targets invoke immutable guard functions. No financial or communication business executor.'],
  [135, 'Schema/constraint/immutable entitlement fixtures, native canonical codecs and deferred-failure SQL. Seeded financial evidence is data, not executed payment/provider/settlement owners.'],
  [136, 'Rollback family identity/serial/row locks and Source lineage guards; dynamic expect-failure statements call lineage assertions only. No payment/provider/settlement executor or queue drain.'],
  [137, 'Rollback common entitlement proposal/publication/first-authorisation fixtures and native chosen-detail capture. This publishes approved entitlement metadata under native serial/freeze guards, without payment-provider or settlement execution; dynamic assertions call only lifecycle signature.'],
  [138, 'Rollback seeded batch/reservation/transfer evidence exercises freeze census and rail-state classifier reads. No native transfer, provider, payment or settlement writer is called.'],
  [139, 'Rollback Mode A validation/reference/lineage owners and fixture row accounting; no provider/payment/settlement executor or dispatch claim.'],
  [141, 'Rollback first Authorise/withdrawal/refusal fixtures. pg_temp.drain_workbench_jobs only updates IDs captured as inserted fixture keys; it never invokes the real queue processor. Batch/rail/settled rows are seeded data, not actual payment/settlement actions.'],
  [142, 'Rollback first-authorisation withdrawal/supersession fixtures. pg_temp drain is a direct UPDATE fenced by a fixture dedupe prefix; provider-attempt/transfer scope rows are synthetic evidence, not provider submission.'],
  [143, 'Rollback absence declaration/clear/branch probes, freeze reads and first Authorise guards. pg_temp drain only marks captured fixture job IDs complete; no actual job processor or payment executor.'],
  [145, 'Rollback immutable head/workbench seam fixtures. pg_temp.walk and dynamic expected-failure calls invoke the paged economic occurrence reader/fingerprint, not a job processor or payment execution. Temporary constraint removal is restored by outer rollback.'],
  [146, 'Rollback seeded settled-batch/snapshot/history fixtures invoke Source settlement allocation/position READERS and negative STABLE write/rowlock controls. No actual settlement-record/apply/provider owner is executed; the word settlement describes the evidence being read.'],
  [149, 'Rollback native Authorise/Unauthorise guards and Workbench reader fixtures. pg_temp.drain_jobs is only a direct fixture-prefix UPDATE. Dynamic assertions invoke ordinary Authorise/Unauthorise owners, without payment/provider/settlement or queue processor.'],
  [150, 'Rollback upload admission/cutoff/overlap stage/seal fixtures and fixture key capture; no financial/communication executor.'],
  [153, 'Rollback family invoice-census guard fixtures invoke ordinary Unauthorise single/bulk on fixture identities. No payment/provider/settlement or message/queue execution.'],
  [156, 'Rollback orphan item/advance/batch/finance-case evidence fixtures invoke entitlement and settled-scope READERS only; no actual payment, provider submission or settlement writer.'],
  [157, 'Rollback actor/Client/group/cycle authority and Office workspace-read fixtures; no protected pay action or monetary writer is executed.'],
  [160, 'Rollback immutable rule version/assignment/settings/feature-owner fixtures. Dynamic SQL is explicit expected-failure rule/assignment/update input. Positive feature activation uses only the genuine latest VERIFIED matched-contract release when present; otherwise verifies refusal. No genuine APPLYING release is required or fabricated; no external action.'],
]);
const entries = census.verifiers.map(v => ({ index: v.index, path: v.path, bytes: v.bytes, sha256: v.sha256,
  decision: excluded.has(v.index) ? 'EXCLUDE' : 'INCLUDE',
  category: excluded.has(v.index) ? 'ACTUAL_QUEUE_PROCESSING' : catalogue.has(v.index) ? 'CATALOGUE_ONLY' : pure.has(v.index) ? 'CATALOGUE_OR_DATA_FREE_HELPER_NEGATIVES' : 'TRANSACTION_CONTAINED_FIXTURES',
  reason: excluded.get(v.index) ?? reasons.get(v.index) ?? (catalogue.has(v.index)
    ? 'Only catalogue, definition-string/constraint/ACL/trigger metadata inspection and hashes; referenced business functions are not invoked.'
    : 'Data-free helper vectors or null/authenticated negative admission and catalogue/ACL checks. Browser/invalid-input calls refuse before business owners; no actual financial or communications effect.'),
  calls: v.calls.filter(call => !call.startsWith('pg_catalog.')),
  dynamicSqlCalleeNames: [...new Set(v.dynamicOrInspectedSqlLiterals.map(d => d.call))], includes: v.includes,
  requiresGenuineApplyingRelease: false,
}));
const report = { formatVersion: 1, sourceCommit: census.sourceCommit, status: 'SOURCE_SAFETY_CLASSIFICATION_NOT_EXECUTED',
  releaseSha256: census.releaseSha256, range: { firstIndex:112, lastIndex:167, count:56, zeroBased:true },
  method: 'Read complete byte bodies and recursively resolve every include, produce a complete quote/comment-aware call census, then distinguish executed calls/dynamic SQL from pg_get_functiondef/to_regprocedure string inspection and follow the actual critical native claim/drain/reader bodies. This is a source safety decision, not native runtime acceptance.',
  harnessRequirements: ['One outer BEGIN with REPEATABLE READ for snapshot-guard includes, ROLLBACK and a fresh absence/fingerprint connection.',
    'Repository release expansion must remove nested verifier transaction boundaries and conflicting psql ON_ERROR_STOP controls; never execute raw included ROLLBACK as the outer close.',
    'Exclude containing roots whenever an included body is excluded; do not approve a root by checking only its own visible calls.',
    'Fixture-only direct DML/mocks do not prove real queue/settlement/payment business behavior; retain those acceptance gates separately.'],
  counts: { include: entries.filter(e=>e.decision==='INCLUDE').length, exclude: entries.filter(e=>e.decision==='EXCLUDE').length },
  verifiers: entries, includeBodies: census.includeBodies.map(({path,bytes,sha256,includes,calls})=>({path,bytes,sha256,includes,calls})),
  noDatabaseCalls:true, noNetworkOrSecrets:true, noBankingWorktreeRead:true,
};
const body = JSON.stringify(report,null,2)+'\n'; writeFileSync(resolve(out,'VERIFIER_SAFETY_112_167.json'),body);
console.log(JSON.stringify({ sha256:createHash('sha256').update(body).digest('hex'),counts:report.counts,
  excludedIndices:entries.filter(e=>e.decision==='EXCLUDE').map(e=>e.index) }));
