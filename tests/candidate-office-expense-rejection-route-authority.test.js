import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';

const source = readFileSync(
  new URL(
    '../supabase/repeatable/06092026_1636_candidate_advanced_expense_component_policy_v1.sql',
    import.meta.url
  ),
  'utf8'
);
const completionSource = readFileSync(
  new URL(
    '../supabase/repeatable/06092026_2355_candidate_advanced_expense_completion_v1.sql',
    import.meta.url
  ),
  'utf8'
);

const context = source.match(
  /create or replace function private\._candidate_office_expense_rejection_context_v1\([\s\S]*?\n\$function\$;/
)?.[0] || '';
const financialRemoval = source.match(
  /create or replace function private\._candidate_expense_financial_remove_v1\([\s\S]*?\n\$function\$;/
)?.[0] || '';
const emptyCarrierDelete = completionSource.match(
  /create or replace function private\._candidate_zero_expense_carrier_delete_v1\([\s\S]*?\n\$function\$;/
)?.[0] || '';

test('Office category rejection uses the immutable route of an exact Candidate expense carrier', () => {
  assert.match(
    context,
    /v_is_candidate_expense_carrier:=v_timesheet_id is not null[\s\S]*?v_workflow\.workflow_kind='CONTRACT_EXPENSE'/
  );
  assert.match(
    context,
    /v_timesheet\.sheet_scope='WEEKLY'::public\.timesheet_scope_enum[\s\S]*?line_type::text,''\)\) in \('EXPENSES','MILEAGE'\)/
  );
  assert.match(
    context,
    /v_timesheet\.candidate_workflow_id=v_workflow\.id[\s\S]*?v_workflow\.target_timesheet_id=v_timesheet_id[\s\S]*?v_component\.owning_timesheet_id=v_timesheet_id/
  );
  assert.match(
    context,
    /if v_is_candidate_expense_carrier then[\s\S]*?v_route_kind:=v_workflow_route_kind;[\s\S]*?authority_basis','CANDIDATE_EXPENSE_WORKFLOW_ROUTE'/
  );
  assert.match(
    context,
    /elsif v_timesheet_id is not null[\s\S]*?_candidate_route_family_v1/
  );
  assert.match(
    context,
    /v_fin\.nhsp_import_id is not null[\s\S]*?v_route_kind not in \('ELECTRONIC','QR'\)/
  );
});

test('payment alone does not hide Office category rejection', () => {
  assert.match(
    context,
    /agency_authorisation_state not in \('NOT_AUTHORISED','PAID'\)/
  );
  assert.doesNotMatch(
    context,
    /upper\(coalesce\(v_timesheet\.status::text,''\)\) in \('AUTHORISED','AUTHORIZED','INVOICED','PAID'\)/
  );
  assert.doesNotMatch(context, /or v_fin\.paid_at_utc is not null then/);
  assert.match(
    context,
    /agency_authorisation_state='PAID'[\s\S]*?payment_only_eligible/
  );
});

test('expense removal subtracts the owned category without treating rate snapshots as money', () => {
  assert.match(
    financialRemoval,
    /v_total_pay:=round\([\s\S]*?v_fin\.total_pay_ex_vat,0\)-v_financial_category_amount,2/
  );
  assert.match(
    financialRemoval,
    /v_total_charge:=round\([\s\S]*?v_fin\.total_charge_ex_vat,0\)-v_financial_category_charge,2/
  );
  assert.doesNotMatch(
    financialRemoval,
    /v_total_pay:=coalesce\(v_fin\.pay_day,0\)/
  );
  assert.doesNotMatch(
    financialRemoval,
    /zero_expense_carrier[\s\S]*?coalesce\(v_fin\.pay_day,0\)=0/
  );
  assert.doesNotMatch(
    context,
    /v_will_delete:=[\s\S]*?coalesce\(v_fin\.pay_day,0\)=0/
  );
  assert.doesNotMatch(
    emptyCarrierDelete,
    /coalesce\(v_fin\.pay_day,0\)<>0/
  );
  assert.doesNotMatch(
    emptyCarrierDelete,
    /coalesce\(v_fin\.charge_day,0\)<>0/
  );
  assert.match(emptyCarrierDelete, /coalesce\(v_fin\.total_pay_ex_vat,0\)<>0/);
  assert.match(emptyCarrierDelete, /coalesce\(v_fin\.total_charge_ex_vat,0\)<>0/);
});
