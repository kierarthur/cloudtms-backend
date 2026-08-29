const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const root = path.resolve(__dirname, '..');
const read = file => fs.readFileSync(path.join(root, file), 'utf8').replaceAll('\r\n', '\n');
const legacy = read('supabase/repeatable/20072026_0117_banking_pay_preview_selection_revision.sql');
const projection = read('supabase/repeatable/28082026_1232_banking_pay_modal_certified_projection.sql');
test('warm-read planning fix is scoped to the new eligible reader only',()=>{
 const eligible=projection.slice(0,projection.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_modal_row_payload_v2'));
 assert.match(eligible,/SET plan_cache_mode TO 'force_custom_plan'/);
 assert.match(eligible,/LANGUAGE plpgsql STABLE SECURITY INVOKER/);
 assert.match(eligible,/RETURN QUERY\s*WITH session_preview_rows/);
 assert.equal((projection.match(/SET plan_cache_mode/g)||[]).length,1);
 assert.doesNotMatch(projection,/ALTER (?:SYSTEM|DATABASE|ROLE)|set_config\('plan_cache_mode'/i);
 assert.doesNotMatch(legacy,/force_custom_plan/);
});
test('eligible reader restores caller planning mode and retains its private invoker boundary',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const query=require('./fixtures/banking-pay-local-query.cjs');
 const result=query(`DO $proof$
 DECLARE m text;cfg text[];mode text;
 BEGIN
  SELECT proconfig INTO cfg FROM pg_proc WHERE oid='private.pay_workbench_modal_eligible_rows_v2(uuid,bigint,text)'::regprocedure;
  IF NOT cfg@>ARRAY['plan_cache_mode=force_custom_plan','search_path=""'] OR cardinality(cfg)<>2 THEN
   RAISE EXCEPTION 'UNEXPECTED_READER_CONFIGURATION';END IF;
  IF EXISTS(SELECT 1 FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
   WHERE p.oid='private.pay_workbench_modal_eligible_rows_v2(uuid,bigint,text)'::regprocedure
    AND (p.prosecdef OR l.lanname<>'plpgsql')) THEN
   RAISE EXCEPTION 'ELIGIBLE_READER_PRIVILEGE_CHANGED';END IF;
  FOREACH mode IN ARRAY ARRAY['auto','force_generic_plan','force_custom_plan'] LOOP
   PERFORM set_config('plan_cache_mode',mode,true);
   PERFORM * FROM private.pay_workbench_modal_eligible_rows_v2(NULL::uuid,1,'canonical_preview_lines');
   IF current_setting('plan_cache_mode')<>mode THEN RAISE EXCEPTION 'READER_CHANGED_CALLER_CONFIGURATION';END IF;
  END LOOP;
 END;
 $proof$;
 SELECT jsonb_build_object('configuration_restored',true);`);
 assert.deepEqual(result,[{configuration_restored:true}]);
});
function between(source, start, end) {
  const a = source.indexOf(start), b = source.indexOf(end, a);
  assert.ok(a >= 0 && b > a, 'certified source extraction boundaries must remain exact');
  return source.slice(a, b);
}
test('v2 eligibility is byte-equivalent to the complete certified reader predicate', () => {
  const old = between(legacy, '  WITH session_preview_rows AS MATERIALIZED (', '\n  SELECT\n    COALESCE(ARRAY_AGG(eligible_row.id');
  const actual = between(projection, '  WITH session_preview_rows AS MATERIALIZED (', '\n  SELECT source_row.*');
  const barrier = '), eligible_rows AS MATERIALIZED (';
  assert.equal(actual.split(barrier).length, 2, 'only the exact final eligible-set evaluation boundary may differ');
  assert.equal(actual.replace(barrier, '), eligible_rows AS ('), old.replaceAll('v_session_row.version', 'p_session_version').replaceAll('v_resolved_section', 'p_section'));
});
test('v2 payload retains every canonical normalization and metadata field', () => {
  const old = between(legacy, 'normalised_rows AS (', '\n  SELECT\n    COALESCE(jsonb_agg(');
  const actual = between(projection, 'normalised_rows AS MATERIALIZED (', '\n  SELECT\n      normalised_rows.base_json');
  const barrier='      -- Single-row evaluation barrier; not list pagination.\n      OFFSET 0\n';
  assert.equal(actual.split(barrier).length,2,'only the exact constant single-row barrier may differ');
  assert.equal(actual.replace(barrier,'').replace('normalised_rows AS MATERIALIZED (','normalised_rows AS ('), old);
  assert.match(projection, /WITH limited_rows AS MATERIALIZED/);
  const oldExpression = between(legacy, '      normalised_rows.base_json', '\n      ORDER BY normalised_rows.row_ordinal');
  const newExpression = between(projection, '      normalised_rows.base_json', '\n  FROM normalised_rows;');
  assert.equal(newExpression, oldExpression);
});
test('private projection does not write, widen grants, or invent SQL economics', () => {
  assert.doesNotMatch(projection, /\b(?:INSERT INTO|UPDATE public\.|DELETE FROM|SECURITY DEFINER)\b/i);
  assert.doesNotMatch(projection, /pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(projection, /GRANT\s+EXECUTE/i);
  assert.match(projection, /FROM PUBLIC, anon, authenticated, service_role/);
  for (const fence of ['exact_active_item_overlap', 'strict_recovery_siblings', 'recovery_residual_is_current', 'post_draft_unavailable']) assert.ok(projection.includes(fence));
});

test('summary eligibility is byte-equivalent to the existing server selection owner', () => {
  const source = read('supabase/repeatable/09082026_1727_pay_workbench_session_set_selected_rows_semantic_overlay.sql');
  const start = source.indexOf('      SELECT preview_row.id,', source.indexOf('CREATE TEMP TABLE _tmp_pay_wb_global_selection_rows'));
  const end = source.indexOf('\n\n      WITH updated_rows', start);
  assert.ok(start>0 && end>start);
  const helper = read('supabase/repeatable/28082026_1308_banking_pay_modal_ready_members.sql');
  assert.equal(between(helper, '      SELECT preview_row.id,', '\n$function$;'),
    source.slice(start,end).replaceAll('v_session_row.version','p_session_version'));
});
