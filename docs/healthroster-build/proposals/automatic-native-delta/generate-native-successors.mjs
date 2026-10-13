import { readFileSync, writeFileSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';

// Offline proposal generation only. Root owns the reviewed installation closure.
const root = resolve(process.argv[2]);
const out = dirname(fileURLToPath(import.meta.url));
const hash = bytes => createHash('sha256').update(bytes).digest('hex');
const pins = [];
function read(path) {
  const bytes = readFileSync(resolve(root, path));
  pins.push({ path, bytes: bytes.length, sha256: hash(bytes) });
  return bytes.toString('utf8').replace(/\r\n/g, '\n');
}
function changeOnce(text, from, to) {
  if (text.split(from).length !== 2) throw new Error(`Unique native anchor unavailable: ${from.slice(0, 80)}`);
  return text.replace(from, to);
}
function definition(text, name) {
  const start = text.indexOf(`create or replace function ${name}(`);
  if (start < 0) throw new Error(`Missing complete owner: ${name}`);
  const end = text.indexOf('$function$;', text.indexOf('as $function$', start));
  if (end < 0) throw new Error(`Incomplete owner: ${name}`);
  return text.slice(start, end + '$function$;'.length);
}
const receiptSources = [
  ['local', 'supabase/repeatable/05102026_0114_weekly_source_pay_query_evidence_v1.sql'],
  ['c1', 'supabase/repeatable/05102026_0245_weekly_source_pay_query_c1_receipt_v1.sql'],
  ['next', 'supabase/repeatable/05102026_0258_weekly_source_pay_query_next_receipt_v1.sql'],
];
let receipts = '-- Complete native receipt successors. Ordinary calls retain the latest-event path.\n'
  + '-- Explicit historical events are private and must be qualified as the exact own-stage predecessor.\n';
for (const [lane, path] of receiptSources) {
  const originalName = `private.weekly_source_pay_query_${lane}_receipt_v1`;
  const coreName = `private.weekly_source_pay_query_${lane}_receipt_event_v1`;
  let core = definition(read(path), originalName);
  core = changeOnce(core, `${originalName}(`, `${coreName}(`);
  core = changeOnce(core, 'p_root_timesheet_id uuid,p_approval_id uuid,p_generation_id uuid',
    'p_root_timesheet_id uuid,p_approval_id uuid,p_generation_id uuid,p_family_event_id uuid');
  const anchor = lane === 'next'
    ? 'where e.family_id=f.id and e.durable_work_event_id=a.work_event_id'
    : lane === 'c1'
      ? 'where e.family_id=v_approval.pay_target_family_id and e.durable_work_event_id=v_approval.work_event_id'
      : 'and e.durable_work_event_id=v_approval.work_event_id';
  core = changeOnce(core, anchor, anchor + '\n      and (p_family_event_id is null or e.id=p_family_event_id)');
  receipts += '\n' + core + `\nalter function ${coreName}(uuid,uuid,uuid,uuid) owner to postgres;\n`
    + `revoke all on function ${coreName}(uuid,uuid,uuid,uuid) from public,anon,authenticated,service_role;\n`
    + `create or replace function ${originalName}(p_root_timesheet_id uuid,p_approval_id uuid,p_generation_id uuid)\n`
    + `returns jsonb language sql stable security definer set search_path to 'pg_catalog','pg_temp'\n`
    + `as $function$ select ${coreName}(p_root_timesheet_id,p_approval_id,p_generation_id,null); $function$;\n`
    + `alter function ${originalName}(uuid,uuid,uuid) owner to postgres;\n`
    + `revoke all on function ${originalName}(uuid,uuid,uuid) from public,anon,authenticated,service_role;\n`;
}
writeFileSync(resolve(out, '04_native_receipt_successors.sqlinc'), receipts);

let priorWait = definition(read('supabase/repeatable/13102026_1438_weekly_source_protection_queries/01_wait_receipt.sqlinc'),
  'private.weekly_source_protected_wait_receipt_v1');
priorWait = changeOnce(priorWait, 'private.weekly_source_protected_wait_receipt_v1(\n  p_root_timesheet_id uuid,p_family_event_id uuid',
  'private.weekly_source_staged_prior_wait_receipt_v1(\n  p_request_id uuid,p_actor_id uuid');
priorWait = changeOnce(priorWait, '  v_scope jsonb;', '  v_stage jsonb; v_wait jsonb; p_root_timesheet_id uuid; p_family_event_id uuid;\n  v_scope jsonb;');
priorWait = changeOnce(priorWait, '  v_scope:=private.weekly_source_pay_query_scope_v1(p_root_timesheet_id);',
  `  v_stage:=private.weekly_source_staged_publication_context_v1(p_request_id,p_actor_id);\n`
  + `  select generation.complete_next_vector_json#>'{automatic_reconcile_proof,qualified_wait}' into strict v_wait\n`
  + `    from public.weekly_exceptional_c1_publication_requests request\n`
  + `    join public.weekly_exceptional_pay_generations generation on generation.id=request.generation_id and generation.family_id=request.family_id\n`
  + `    where request.id=p_request_id;\n`
  + `  p_root_timesheet_id:=(v_stage#>>'{scope,root_timesheet_id}')::uuid; p_family_event_id:=(v_wait->>'event_id')::uuid;\n`
  + `  if v_wait->>'state' is distinct from 'QUALIFIED_WAIT' or v_wait->>'family_id' is distinct from v_stage->>'family_id'\n`
  + `    or exists(select 1 from public.weekly_exceptional_pay_family_events e where e.family_id=(v_stage->>'family_id')::uuid\n`
  + `      and e.durable_work_event_id=(v_stage->>'work_event_id')::uuid and e.event_sequence>(v_wait->>'event_sequence')::bigint\n`
  + `      and e.id is distinct from (v_stage->>'family_event_id')::uuid) then\n`
  + `    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_PRIOR_WAIT_CHANGED' using errcode='40001'; end if;\n`
  + '  v_scope:=private.weekly_source_pay_query_scope_v1(p_root_timesheet_id);');
priorWait = changeOnce(priorWait, `    and event.state='WAIT' and not exists(select 1 from public.weekly_exceptional_pay_family_events newer\n`
  + `      where newer.family_id=event.family_id and newer.durable_work_event_id=event.durable_work_event_id\n`
  + `        and newer.event_sequence>event.event_sequence);`, `    and event.state='WAIT'\n`
  + `    and event.durable_work_event_id=(v_stage->>'work_event_id')::uuid\n`
  + `    and event.evidence_approval_id=(v_wait->>'approval_id')::uuid\n`
  + `    and event.event_sequence=(v_wait->>'event_sequence')::bigint;`);
for (const lane of ['local','c1','next']) priorWait = changeOnce(priorWait,
  `private.weekly_source_pay_query_${lane}_receipt_v1(p_root_timesheet_id,\n          v_latest.evidence_approval_id,v_generation)`,
  `private.weekly_source_pay_query_${lane}_receipt_event_v1(p_root_timesheet_id,\n          v_latest.evidence_approval_id,v_generation,v_latest.id)`);
priorWait = changeOnce(priorWait, '  if v_unavailable then return null; end if;',
  `  if v_unavailable or v_exception is null or v_exception->'accepted_receipt' is distinct from v_wait->'accepted_receipt'\n`
  + `    or v_exception#>>'{accepted_receipt,generation_id}' is distinct from v_wait->>'accepted_generation_id'\n`
  + `    or coalesce(v_exception#>>'{accepted_receipt,receipt_state}','') not in ('COMPLETE','PUBLISHED') then\n`
  + `    raise exception 'WEEKLY_SOURCE_AUTOMATIC_RECONCILE_PRIOR_WAIT_UNAVAILABLE' using errcode='55000'; end if;`);
writeFileSync(resolve(out,'05_staged_prior_wait_receipt.sqlinc'), '-- Complete native receipt count and transport qualification; exact own-stage predecessor only.\n'
  + priorWait + '\nalter function private.weekly_source_staged_prior_wait_receipt_v1(uuid,uuid) owner to postgres;\n'
  + 'revoke all on function private.weekly_source_staged_prior_wait_receipt_v1(uuid,uuid) from public,anon,authenticated,service_role;\n');

const localSourcePath = 'supabase/repeatable/13102026_1113_weekly_source_protected_pay_authority_v1.sql';
let local = definition(read(localSourcePath), 'public.weekly_exceptional_pay_complete_local_v1');
local = changeOnce(local, '  v_components jsonb;', '  v_components jsonb;\n  v_automatic_components jsonb;');
local = changeOnce(local, '  select count(*) into v_live_authorisations from public.weekly_source_root_authorisations authority',
  `  -- A manually invoked native Local owner cannot bypass the sealed automatic\n`
  + `  -- request's per-component zero-delta proof. This is before PREPARING insert.\n`
  + `  if v_generation.complete_next_vector_json ? 'automatic_reconcile_proof' then\n`
  + `    perform private.weekly_source_automatic_reconcile_delta_assert_v1(v_request.id,v_actor);\n`
  + `  end if;\n`
  + '  select count(*) into v_live_authorisations from public.weekly_source_root_authorisations authority');
const compositionStart = local.indexOf("    select coalesce(jsonb_agg(jsonb_build_object('work_event_id',expense.work_event_id,");
const compositionEnd = local.indexOf('    -- Source expenses must be the same approved observations', compositionStart);
if (compositionStart < 0 || compositionEnd < 0) throw new Error('Native complete component producer anchors unavailable');
const ordinary = local.slice(compositionStart, compositionEnd);
local = local.slice(0, compositionStart)
  + `    if v_generation.complete_next_vector_json ? 'automatic_reconcile_proof' then\n`
  + `      -- Exactly the producer used by delta_assert; no separately guessed vector.\n`
  + `      v_automatic_components:=private.weekly_source_staged_entitlement_components_v1(v_request.id,v_actor);\n`
  + `      v_components:=v_automatic_components->'components';\n`
  + `      v_expenses:=v_automatic_components->'expenses';\n`
  + `    else\n` + ordinary + `    end if;\n` + local.slice(compositionEnd);
writeFileSync(resolve(out, '09_complete_local_successor.sqlinc'), '-- Complete native owner successor; only sealed automatic requests use the common producer.\n'
  + local + '\nalter function public.weekly_exceptional_pay_complete_local_v1(jsonb) owner to postgres;\n'
  + 'revoke all on function public.weekly_exceptional_pay_complete_local_v1(jsonb) from public,anon,authenticated;\n'
  + 'grant execute on function public.weekly_exceptional_pay_complete_local_v1(jsonb) to service_role;\n');

// These current complete owners already call the certificate at the correct
// points. Copy unchanged, rather than introduce a competing stage or route.
for (const [output, path] of [
  ['10_complete_stage_request.sqlinc', 'supabase/repeatable/13102026_1438_weekly_source_protection_queries/20_stage_request.sqlinc'],
  ['11_complete_automatic_reconcile.sqlinc', 'supabase/repeatable/13102026_1438_weekly_source_protection_queries/27_automatic_reconcile.sqlinc'],
]) writeFileSync(resolve(out, output), read(path));

for (const path of [
  'supabase/repeatable/13102026_1438_weekly_source_protection_queries/01_wait_receipt.sqlinc',
  'supabase/repeatable/13102026_1438_weekly_source_protection_queries/02_wait_witness.sqlinc',
  'supabase/repeatable/13102026_1438_weekly_source_protection_queries/19_action_context.sqlinc',
  'supabase/repeatable/13102026_1438_weekly_source_protection_queries/25_bulk_stage_proof.sqlinc',
  'supabase/repeatable/15092026_1534_weekly_source_ordinary_pay_projection_v1.sql',
  'supabase/repeatable/04102026_2120_weekly_source_approval_basis_v2.sql',
  'supabase/repeatable/05102026_0212_weekly_source_local_publication_context_v2.sql',
  'supabase/repeatable/15092026_1534_weekly_source_protected_action_orchestration_v1.sql',
  'supabase/repeatable/05102026_0302_banking_pay_next_local_source_chosen_detail_v1.sql',
  'supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql',
  'supabase/repeatable/17092026_0300_weekly_source_entitlement_publication_v1.sql',
  'broker/src/banking-pay/weekly-source-c1-components.mjs',
  'broker/src/banking-pay/weekly-source-c1-stream.mjs',
]) read(path);
writeFileSync(resolve(out, 'NATIVE_INPUT_PINS.json'), JSON.stringify({ schema_version: 'AUTOMATIC_NATIVE_DELTA_INPUT_PINS_V1', inputs: pins }, null, 2) + '\n');
