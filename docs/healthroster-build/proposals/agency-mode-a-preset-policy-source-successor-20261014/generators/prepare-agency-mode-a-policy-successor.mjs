import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const repo = path.resolve(here, '../../../..');
const root = path.resolve(process.argv[2] || '');
const dir = path.join(here, 'mode-a-policy-construction');
const pin = 'fa66183f01913e86651e82816f5e88f97e07123c';
const sha = b => crypto.createHash('sha256').update(b).digest('hex');
const reverses = [];
const inputs = [];
const write = (name, b) => {
  const p = path.join(dir, name);
  if (fs.existsSync(p)) {
    assert.ok(fs.readFileSync(p).equals(Buffer.from(b)), `Refuse differing saved bytes: ${p}`);
    return;
  }
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, b);
};
const read = (p, commit = null) => {
  const base = commit ? root : repo;
  const b = fs.readFileSync(path.join(base, p));
  if (commit) assert.ok(b.equals(execFileSync('git', ['-c', 'gc.auto=0', '-c', 'maintenance.auto=false', 'show', `${commit}:${p}`], { cwd: root, maxBuffer: 64e6 })));
  inputs.push({ path: p, commit, bytes: b.length, sha256: sha(b) });
  return b.toString('utf8');
};
function successor(name, input, before, changes) {
  let forward = before;
  const hunks = [];
  for (const [a0, b0] of changes) {
    const lf = a0.replace(/\r?\n/g, '\n');
    const crlf = a0.replace(/\r?\n/g, '\r\n');
    const nl = forward.includes(lf) ? '\n' : '\r\n';
    const a = a0.replace(/\r?\n/g, nl), b = b0.replace(/\r?\n/g, nl);
    assert.equal(forward.split(a).length, 2, `${name}: unique before hunk`);
    forward = forward.replace(a, () => b);
    assert.equal(forward.split(b).length, 2, `${name}: unique after hunk`);
    hunks.push({ before: a, after: b });
  }
  let reverse = forward;
  for (const h of [...hunks].reverse()) reverse = reverse.replace(h.after, () => h.before);
  assert.equal(reverse, before);
  write(name, forward);
  write('before/' + name + '.txt', before);
  reverses.push({ input, input_sha256: sha(before), output: name, forward_sha256: sha(forward), hunks, complete_reverse: true });
  return forward;
}

// One native, bounded import-preview census supplies the same complete
// Candidate/Client units used by the existing import apply guard. No selector,
// caller membership, stored selection mutation or partial-envelope hash exists.
write('20_native_mode_a_unit_census.sqlinc', `create or replace function private.weekly_source_agency_mode_a_units_v1(p_import_id uuid)
returns table(candidate_id uuid,client_id uuid,has_preset boolean,timesheet_ids uuid[],week_endings date[])
language plpgsql stable security definer
set search_path to 'pg_catalog','public','private','pg_temp'
as $function$
declare hi public.hr_imports%rowtype; preview jsonb;
begin
 select * into strict hi from public.hr_imports where id=p_import_id and pruned_at is null;
 if hi.source_system::text<>'HEALTHROSTER' or upper(coalesce(hi.import_scope::text,''))<>'HR_WEEKLY' then return;end if;
 preview:=public.hr_weekly_validation_preview(hi.id);
 if preview->>'import_id' is distinct from hi.id::text
  or preview->>'client_id' is distinct from hi.client_id::text
  or jsonb_typeof(preview->'rows') is distinct from 'array'
  or jsonb_array_length(preview->'rows')>5000 then
  raise exception 'HR_VALIDATION_NATIVE_TARGETS_UNAVAILABLE' using errcode='55000';end if;
 if exists(select 1 from jsonb_array_elements(preview->'rows') r
  where jsonb_typeof(r) is distinct from 'object'
   or jsonb_typeof(r->'candidate_id') is distinct from 'string'
   or (r->>'candidate_id')!~'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
   or jsonb_typeof(r->'client_id') is distinct from 'string'
   or r->>'client_id' is distinct from hi.client_id::text
   or jsonb_typeof(r->'week_ending_date') is distinct from 'string'
   or not pg_input_is_valid(r->>'week_ending_date','date')
   or (r->>'week_ending_date')!~'^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
   or (jsonb_typeof(r->'timesheet_id') is distinct from 'null' and
    (jsonb_typeof(r->'timesheet_id') is distinct from 'string' or
     (r->>'timesheet_id')!~'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'))) then
  raise exception 'HR_VALIDATION_NATIVE_TARGETS_UNAVAILABLE' using errcode='55000';end if;
 return query
 with native_rows as materialized (
  select (r->>'candidate_id')::uuid candidate,(r->>'client_id')::uuid client,
   (r->>'timesheet_id')::uuid ts,(r->>'week_ending_date')::date own_week
  from jsonb_array_elements(preview->'rows') r
 ), classified as (
  select n.*,coalesce(private.weekly_source_agency_invoice_check_only_v1(n.ts),false)
   or exists(select 1 from public.weekly_source_group_clients m
    join public.weekly_source_groups g on g.id=m.source_group_id
    where m.client_id=n.client and n.own_week between m.valid_from and coalesce(m.valid_to,'infinity'::date)
     and private.weekly_source_configured_agency_invoice_v1(n.client,g.id,g.agency_id,g.environment,n.own_week) is true) preset
  from native_rows n
 )
 select n.candidate,n.client,bool_or(n.preset),
  coalesce(array_agg(distinct n.ts order by n.ts) filter(where n.ts is not null),array[]::uuid[]),
  array_agg(distinct n.own_week order by n.own_week)
 from classified n group by n.candidate,n.client order by n.candidate,n.client;
end;
$function$;
alter function private.weekly_source_agency_mode_a_units_v1(uuid) owner to postgres;
revoke all on function private.weekly_source_agency_mode_a_units_v1(uuid) from public,anon,authenticated,service_role;
`);

const corePath = 'supabase/repeatable/21072026_1820_00_import_review_internal_core.sql';
const core = read(corePath, pin);
const start = core.indexOf('create or replace function public._import_review_ready_action_ids_core_v1(');
const end = core.indexOf('$function$;', start) + '$function$;'.length;
assert.ok(start >= 0 && end > start);
const ready = core.slice(start, end);
successor('21_complete_native_ready_units_with_preset_exclusion.sqlinc', corePath + '#public._import_review_ready_action_ids_core_v1(uuid)', ready, [
  ['  with selected_units as (', `  with preset_units as materialized (
    select n.candidate_id,n.client_id
    from private.weekly_source_agency_mode_a_units_v1(p_import_id) n where n.has_preset
  ), selected_units as (`],
  [`    from selected_units u
    where not exists (`, `    from selected_units u
    -- Preserve the native COMPLETE Candidate/Client unit boundary. A unit with
    -- a dated preset week is reserved for its sole completeness writer; other
    -- candidates/clients remain the existing native ready set.
    where not exists(select 1 from preset_units p
      where p.candidate_id=u.candidate_id and p.client_id=u.client_id)
      and not exists (`],
]);

const hrPath = 'docs/healthroster-build/proposals/agency-native-whole-week-validation-runtime-forward-20261014/construction/71_complete_current_mode_a_with_preset_refusal.sqlinc';
const hr = read(hrPath);
successor('22_complete_native_mode_a_with_selected_preset_refusal.sqlinc', hrPath, hr, [
  [`begin
  -- Mode A/reference apply cannot feed a second validation writer for a preset.
  perform private.weekly_source_agency_legacy_import_validation_admit_v1(p_import_id);
  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','START'));`, `begin
  v_steps := v_steps || jsonb_build_array(jsonb_build_object('step','START'));`],
  [`  v_review_operation_id:=(v_review_contract->>'operation_id')::uuid;`, `  -- This is refusal only: claimed action IDs resolve back to real current
  -- decisions and the native preview census. No payload grants membership.
  if exists(select 1 from private.weekly_source_agency_mode_a_units_v1(p_import_id) n
    join public.import_review_decisions d on d.import_id=p_import_id and d.is_current
      and d.candidate_id=n.candidate_id and d.client_id=n.client_id
    where n.has_preset and d.action_id in (select jsonb_array_elements_text(v_review_selected_ids))) then
    raise exception 'AGENCY_INVOICE_USES_COMPLETENESS_WRITER' using errcode='55000';end if;
  v_review_operation_id:=(v_review_contract->>'operation_id')::uuid;`],
  [`  if not exists(select 1 from tmp_review_batch_units) then`, `  -- Recheck the actual guard-admitted complete units before ANY legacy
  -- validation, manager email, mirror or import auto-authorisation owner.
  if exists(select 1 from private.weekly_source_agency_mode_a_units_v1(p_import_id) n
    join pg_temp.tmp_review_batch_units u on u.candidate_id=n.candidate_id and u.client_id=n.client_id
    where n.has_preset) then
    raise exception 'AGENCY_INVOICE_USES_COMPLETENESS_WRITER' using errcode='55000';end if;
  if not exists(select 1 from tmp_review_batch_units) then`],
]);

const refPath = 'docs/healthroster-build/proposals/install2-agency-current-native-20261014/issue-support-construction/before/06_public_weekly_source_mode_a_reference_apply_atomic_v1.sql.txt';
const ref = read(refPath);
successor('23_complete_native_reference_apply_with_preset_row_refusal.sqlinc', refPath, ref, [
  [`  v_applied_ids uuid[];`, `  v_applied_ids uuid[];
  v_native_units jsonb;
  v_preset_refused integer:=0;`],
  [`    select * into strict v_state from public.import_review_states where import_id=v_import.id;`, `    select * into strict v_state from public.import_review_states where import_id=v_import.id;
    select coalesce(jsonb_agg(to_jsonb(n) order by n.candidate_id,n.client_id),'[]'::jsonb)
      into v_native_units from private.weekly_source_agency_mode_a_units_v1(v_import.id) n;`],
  [`      select * into strict v_timesheet from public.timesheets where timesheet_id=v_timesheet_id;`, `      select * into strict v_timesheet from public.timesheets where timesheet_id=v_timesheet_id;
      if exists(select 1 from jsonb_to_recordset(v_native_units) n(candidate_id uuid,client_id uuid,has_preset boolean)
        join public.contracts c on c.id=v_timesheet.contract_id and c.candidate_id=n.candidate_id and c.client_id=n.client_id
        where n.has_preset) then
        perform private.weekly_source_mode_a_refuse_operation_v1(v_cycle.id,v_upload.id,
          v_publication_id,v_timesheet_id,v_actor,'AGENCY_INVOICE_USES_COMPLETENESS_WRITER');
        v_preset_refused:=v_preset_refused+1;
        continue;
      end if;`],
  [`    'superseded_heads_refused',v_refused`, `    'superseded_heads_refused',v_refused,
    'preset_units_refused',v_preset_refused`],
]);

const dispPath = 'docs/healthroster-build/proposals/install2-agency-current-native-20261014/mode-a-policy-construction/19_complete_native_mode_a_dispatch_with_preset_selection.sqlinc';
const disp = read(dispPath);
successor('24_complete_native_dispatch_with_separate_completeness_continuation.sqlinc', dispPath, disp, [
  [`  v_dispatched boolean:=false;`, `  v_dispatched boolean:=false;
  v_reference_apply_required boolean:=false;
  v_preset_units integer:=0;`],
  [`      'publication_id',v_publication_id,'clients','[]'::jsonb`, `      'publication_id',v_publication_id,'clients','[]'::jsonb,
      'reference_apply_required',false,'preset_units',0`],
  [`    v_dispatched:=true;`, `    -- Native units are read AFTER the unchanged comparison/bridge writer.
    -- The caller gets only the owner's continuation choice, never raw units.
    v_reference_apply_required:=v_reference_apply_required or exists(
      select 1 from private.weekly_source_agency_mode_a_units_v1(v_import_id) n where not n.has_preset);
    v_preset_units:=v_preset_units+(select count(*)::integer
      from private.weekly_source_agency_mode_a_units_v1(v_import_id) n where n.has_preset);
    v_dispatched:=true;`],
  [`  return pg_catalog.jsonb_build_object(
    'ok',true,'dispatched',v_dispatched`, `  -- Comparison staging remains Mode A's. Validation is the one typed
  -- whole-week writer and observes the actual bridge/comparisons now present.
  perform private.weekly_source_agency_publication_validation_v1(v_publication_id);
  return pg_catalog.jsonb_build_object(
    'ok',true,'dispatched',v_dispatched`],
  [`    'clients',v_client_results`, `    'clients',v_client_results,
    'reference_apply_required',v_reference_apply_required,'preset_units',v_preset_units`],
]);

const routePath = 'broker/src/weekly-source/routes.js';
const route = read(routePath, pin);
successor('25_complete_current_routes_with_native_mode_a_continuation.js', routePath, route, [
  [`  if (dispatched.dispatched !== true) return null;
  const applied = await modeARpc(`, `  if (dispatched.dispatched !== true) return null;
  if (typeof dispatched.reference_apply_required !== 'boolean'
    || !Number.isSafeInteger(dispatched.preset_units) || dispatched.preset_units < 0) {
    fail('WEEKLY_SOURCE_MODE_A_CONTINUATION_UNAVAILABLE',
      'The current checking continuation is unavailable.', 503,
      { publication_id: publicationId, publication_state: 'CURRENT' });
  }
  // Primary26.3: preset units use the actual native completeness writer.
  // The native owner derives this choice; Office supplies no mode/selector.
  if (!dispatched.reference_apply_required) return Object.freeze({
    dispatched: true,
    timesheet_authority_rows: dispatched.timesheet_authority_rows ?? 0,
    comparisons_written: dispatched.comparisons_written ?? 0,
    preset_units: dispatched.preset_units,
    reference_apply_operations: 0,
    auto_authorise_requested: 0,
    superseded_heads_refused: 0,
    auto_authorise_follow_up: Object.freeze({ required: false, completed: 0 }),
  });
  const applied = await modeARpc(`],
  [`    reference_apply_operations: operations.length,`, `    reference_apply_operations: operations.length,
    preset_units: dispatched.preset_units,
    preset_units_refused: applied.preset_units_refused ?? 0,`],
]);

write('MODE_A_POLICY_COMPLETE_REVERSES.json', JSON.stringify({ source_only: true, native: false, pin, inputs, reverses,
  new_private_signatures: ['private.weekly_source_agency_mode_a_units_v1(uuid)'],
  public_signature_changes: [], caller_selectors: [], stored_selection_mutations: [],
  rejected_source45_outputs: ['11_native_mode_a_preset_reference_admission.sqlinc','12_complete_native_mode_a_with_preset_reference_only.sqlinc'],
  complete_native_unit_boundary_preserved: true, mount_ready: false, independent_review_pending: true }, null, 2) + '\n');
console.log(JSON.stringify({ source_only: true, native: false, outputs: reverses.map(r => ({ path: r.output, sha256: r.forward_sha256, hunks: r.hunks.length })), new_private_signatures: 1 }));
