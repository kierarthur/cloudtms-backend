'use strict';
const assert = require('node:assert/strict');
const test = require('node:test');
const fs = require('node:fs');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
const read = name => fs.readFileSync(path.join(root, 'supabase/repeatable', name), 'utf8').replaceAll('\r\n','\n');
function declaration(source, name) {
  const start = source.indexOf(`CREATE OR REPLACE FUNCTION ${name}(`);
  assert.ok(start>=0);
  const end=source.indexOf('\n$function$;',start);
  assert.ok(end>start);
  return source.slice(start,end+'\n$function$;'.length);
}
const strip = (sql, name) => sql.replace(new RegExp(` *-- BEGIN ${name}\\n[\\s\\S]*? *-- END ${name}\\n`),'');
test('ALL recovery logic and all financial expressions remain byte-identical', () => {
  const name='private.pay_workbench_recovery_selection_overlay_apply_v1';
  const original=declaration(read('09082026_0712_banking_pay_semantic_ready_helpers.sql'),name);
  let actual=declaration(read('28082026_1427_banking_pay_modal_recovery_channel_scope.sql'),name);
  actual=strip(strip(actual,'BANKING_PAY_MODAL_WRITE_CHANNEL_VALIDATION_V2'),'BANKING_PAY_MODAL_WRITE_CHANNEL_FILTER_V2');
  actual=actual.replace("supplied_option.option_key NOT IN ('force_v3', 'reason', 'pay_channel_scope')","supplied_option.option_key NOT IN ('force_v3', 'reason')");
  assert.equal(actual,original);
});
test('three-argument revalidator overload preserves the original owner body and ALL branch', () => {
  const name='public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1';
  const original=declaration(read('19072026_1405_revalidate_recovery_headroom_after_materialisation.sql'),name)
    .replace(/^SET plpgsql_check\.[^\n]+\n/gm, '');
  let actual=declaration(read('28082026_1427_banking_pay_modal_recovery_channel_scope.sql'),name);
  actual=strip(strip(actual,'BANKING_PAY_MODAL_RECOVERY_SCOPE_VALIDATION_V2'),'BANKING_PAY_MODAL_NON_V3_SCOPE_FENCE_V2');
  actual=actual.replace('  p_candidate_id uuid,\n  p_options_json jsonb\n)','  p_candidate_id uuid\n)');
  actual=actual.replace("jsonb_build_object('reason', 'SELECTION_REVALIDATION') || p_options_json","jsonb_build_object('reason', 'SELECTION_REVALIDATION')");
  assert.equal(actual,original);
});
test('write-channel filter is closed, not SQL interpolation or a new allocation predicate', () => {
  const sql=read('28082026_1427_banking_pay_modal_recovery_channel_scope.sql');
  assert.match(sql,/overlay_row\.pay_channel=p_options_json->>'pay_channel_scope'/);
  assert.match(sql,/NOT IN \('ALL','PAYE','UMBRELLA'\)/);
  assert.doesNotMatch(sql,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
  assert.doesNotMatch(sql,/GRANT[^;]*\bTO\s+[^;]*\b(?:anon|authenticated|PUBLIC)\b/i);
  assert.doesNotMatch(
    sql,
    /(?:SET|ALTER\s+FUNCTION[^;]*\s+SET)\s+plpgsql_check\./i,
    'the provider-neutral Banking definition must not require permission to set plpgsql_check GUCs',
  );
});
