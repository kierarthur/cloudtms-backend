import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';

const root = new URL('../../', import.meta.url);
const read = file => fs.readFileSync(new URL(file, root), 'utf8');
const finalisation = read('supabase/repeatable/13102026_1433_weekly_source_invoice_money_finalisation_v1.sql');
const owners = read('supabase/repeatable/13102026_1431_weekly_source_invoice_money_owners_v1.sql');
const body = (source, name) => {
  const start = source.indexOf(`create or replace function ${name}(`);
  assert.notEqual(start, -1, name);
  return source.slice(start, source.indexOf('$function$;', start));
};
const plainSql = text => text.replace(/--[^\n]*|'(?:''|[^'])*'/g, match => ' '.repeat(match.length));

// Mechanical control-flow qualification, not a substitute for hosted native
// races. Track phase predicates through nested PL/pgSQL IF and SQL CASE blocks.
function lockPhases(text, variable) {
  const sql = plainSql(text);
  const token = /\bend\s+if\s*;|\belsif\b([\s\S]*?)\bthen\b|\bif\b([\s\S]*?)\bthen\b|\bcase\b|\bend\b|\belse\b|\bfor\s+(update|share)\b|private\.(bpay_next_source_current_lock_inventory_v1|weekly_source_lock_family_rows_v1)\(/gi;
  const stack = [], events = [];
  const allowed = () => [1, 2].filter(n => stack.every(frame => frame.kind === 'case' || frame.allowed.includes(n)));
  const predicate = condition => {
    const match = new RegExp(`\\b${variable}\\s*=\\s*([12])\\b`).exec(condition);
    return {yes: match ? [Number(match[1])] : [1, 2], no: match && condition.trim() === match[0] ? [3 - Number(match[1])] : [1, 2]};
  };
  for (const match of sql.matchAll(token)) {
    const word = match[0].toLowerCase();
    if (word.startsWith('end if')) { assert.equal(stack.pop()?.kind, 'if'); }
    else if (word.startsWith('elsif')) {
      const frame = stack.at(-1); assert.equal(frame.kind, 'if');
      const p = predicate(match[1]); Object.assign(frame, {allowed: p.yes, otherwise: p.no});
    } else if (word.startsWith('if')) {
      const p = predicate(match[2]); stack.push({kind: 'if', allowed: p.yes, otherwise: p.no});
    } else if (word === 'case') stack.push({kind: 'case'});
    else if (word === 'end' && stack.at(-1)?.kind === 'case') stack.pop();
    else if (word === 'else' && stack.at(-1)?.kind === 'if') stack.at(-1).allowed = stack.at(-1).otherwise;
    else if (word.startsWith('for ') || word.startsWith('private.')) events.push({operation: match[3] || match[4], phases: allowed(), index: match.index});
  }
  assert.equal(stack.length, 0);
  return events;
}

test('FIN complete native family inventory precedes every Source row lock in both retained owner paths', () => {
  const fn = body(finalisation, 'private.weekly_source_finalise_engine_v1');
  const phase = fn.slice(fn.indexOf('for v_lock_phase in 1..2 loop'), fn.indexOf('-- One transaction at a time'));
  const events = lockPhases(phase, 'v_lock_phase');
  const rows = events.filter(e => ['update', 'share'].includes(e.operation));
  // The native control row SHARE is part of the unchanged inventory admission.
  const business = rows.filter(e => !phase.slice(Math.max(0, e.index - 100), e.index).includes('private.bpay_next_module_control'));
  assert.ok(business.length >= 6);
  for (const lock of business) assert.deepEqual(lock.phases, [2]);
  const families = events.filter(e => !['update', 'share'].includes(e.operation));
  assert.deepEqual(families.map(e => e.operation).sort(), ['bpay_next_source_current_lock_inventory_v1', 'weekly_source_lock_family_rows_v1']);
  for (const lock of families) assert.deepEqual(lock.phases, [1]);
  assert.match(phase, /weekly_source_groups[\s\S]*?for update[\s\S]*?weekly_source_cycles[\s\S]*?for update/);
  assert.match(phase, /bpay_next_source_current_lock_recheck_v1\(v_finalisation_lock_operation\)/);
  assert.match(phase, /ROOT_SET_CHANGED_DURING_LOCK'[\s\S]*?errcode='40001'/);
  assert.ok(fn.indexOf('end loop;') < fn.indexOf('insert into public.weekly_source_final_revisions'));
});

test('standalone lineage discovers I-1 first and fences null to nonnull or rotated Contract Week roots before DML', () => {
  const fn = body(finalisation, 'public.weekly_source_timesheet_lineage_ensure_atomic_v1');
  const phase = fn.slice(fn.indexOf('for v_lineage_lock_phase in 1..2 loop'), fn.indexOf('-- A missing base Contract Week'));
  const events = lockPhases(phase, 'v_lineage_lock_phase');
  for (const event of events) assert.deepEqual(event.phases, event.operation === 'weekly_source_lock_family_rows_v1' ? [1] : [2]);
  assert.match(phase, /v_known_root_timesheet_id is distinct from[\s\S]*?root_timesheet_id[\s\S]*?errcode='40001'/);
  assert.ok(fn.indexOf('end loop;') < fn.indexOf('insert into public.contract_weeks'));
  const guardStart = phase.indexOf('v_finalisation_root_set:=nullif');
  const guard = phase.slice(guardStart, phase.indexOf('if v_known_root_timesheet_id is not null and', guardStart));
  assert.match(guard, /v_finalisation_root_set='EMPTY_LOCKED_SET'/);
  assert.doesNotMatch(guard, /v_finalisation_root_set\s*::\s*uuid/);
  assert.match(guard, /ROOT_SET_CHANGED_DURING_LOCK'[\s\S]*?errcode='40001'/);
});

test('FIN restores exact caller lock context for replay, ordinary/prepared completion and both exception handlers', () => {
  const fn = body(finalisation, 'private.weekly_source_finalise_engine_v1');
  assert.equal((fn.match(/v_prior_finalisation_root_set:=/g) || []).length, 1);
  assert.equal((fn.match(/set_config\('cloudtms.weekly_source_finalisation_root_set',v_prior_finalisation_root_set,true\)/g) || []).length, 4);
  const replay = fn.slice(fn.indexOf('-- Native operation is transaction-local'), fn.indexOf("'idempotent',true"));
  assert.match(replay, /bpay_next_source_current_lock_release_v1/);
  assert.match(replay, /if v_finalisation_lock_context then/);
  assert.match(fn, /array_to_string\(v_finalisation_root_ids,','\),'EMPTY_LOCKED_SET'/);
});

test('source contract preserves rule null path and pay selection independent of invoice hold readiness', () => {
  const fn = body(finalisation, 'private.weekly_source_finalise_engine_v1');
  assert.match(fn, /effective_rule_set_v1\(v_client_id,v_cycle.id\)/);
  assert.match(fn, /case when v_rule_version is null then '\{\}'::jsonb/);
  assert.match(fn, /'s25_pay_selected'/);
  assert.doesNotMatch(fn, /->>'invoice_ready'/);
  const shift = body(owners, 'private.weekly_source_shift_on_self_bill_v1');
  for (const key of ['on_accepted_report', 'bound_to_finalised_export', 'invoice_ready', 's25_pay_selected']) assert.ok(shift.includes(`'${key}'`));
  assert.match(shift, /select \* into event_fact from public\.weekly_work_events where id=p_work_event_id/);
  assert.match(shift, /effective_rule_set_v1\(event_fact.client_id,c.id\)/);
  assert.match(shift, /event_fact.first_source_group_id is distinct from c.source_group_id/);
  assert.match(shift, /SELF_BILL_SCOPE_NOT_FOUND'[\s\S]*?errcode='22023'/);
  assert.match(shift, /SELF_BILL_SCOPE_MISMATCH'[\s\S]*?errcode='22023'/);
  assert.match(shift, /self_bill_source'='SEPARATE_FINAL_REPORT'[\s\S]*?return private\.weekly_source_report_shift_facts_v1\(event_fact.id,p_cycle_or_report\)/);
});

test('registered pay census marks every native Banking owner read-only and contains all scoped pay successors', () => {
  const census = JSON.parse(read('docs/healthroster-build/F5_PAY_SOURCE_CENSUS.json'));
  assert.equal(census.functions.length, census.function_count);
  assert.equal(new Set(census.functions.map(f => f.name)).size, census.function_count);
  for (const fn of census.functions.filter(f => f.name.startsWith('private.bpay_next_'))) assert.equal(fn.classification, 'BANK_OWNER_CALL_ONLY');
  const pay = read('supabase/repeatable/13102026_1435_weekly_source_invoice_only_pay_isolation_v1.sql');
  const selectors = census.functions.filter(f => f.classification === 'PAY_SELECTOR_CHANGED' && f.name !== 'private.weekly_source_finalise_engine_v1');
  assert.equal(selectors.length, 11);
  for (const fn of selectors) {
    const definition = body(pay, fn.name);
    assert.ok(definition.includes('SELF_BILL_REPORT'));
    for (const kind of ['SELF_BILL_REPORT_LINE','INVOICE_VAT_ROUNDING','SOURCE_REBILL_LINE','SOURCE_INVOICE_CORRECTION']) assert.ok(definition.includes(kind), `${fn.name}: ${kind}`);
  }
});
