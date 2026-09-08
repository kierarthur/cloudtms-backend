-- Repeatable CloudTMS function/view authority: candidate_duplicate_expense_anchor_inclusion_v1
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

-- A later expense-only workflow deliberately shares the worked Timesheet
-- anchor with the original combined claim.  That shared anchor must not hide
-- an already-approved category from the final duplicate confirmation.
create or replace function private._expense_duplicate_review_v1(
  p_workflow_id uuid,
  p_required_categories text[] default array[]::text[]
)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public, private, pg_temp
as $function$
declare
  v_workflow public.candidate_submission_workflows%rowtype;
  v_client_id uuid;
  v_client_name text;
  v_requested_categories text[]:=array[]::text[];
  v_duplicate_categories text[]:=array[]::text[];
  v_prior_claim_count integer:=0;
  v_prior_claims jsonb:='[]'::jsonb;
  v_review_identity jsonb;
  v_confirmation_digest text;
begin
  select workflow_row.*
  into v_workflow
  from public.candidate_submission_workflows workflow_row
  where workflow_row.id=p_workflow_id
  for update;

  if not found then
    raise exception 'CANDIDATE_WORKFLOW_NOT_FOUND' using errcode='P0002';
  end if;
  select contract_row.client_id,client_row.name
  into v_client_id,v_client_name
  from public.contracts contract_row
  join public.clients client_row on client_row.id=contract_row.client_id
  where contract_row.id=v_workflow.contract_id;
  if v_client_id is null then
    raise exception 'CANDIDATE_WORKFLOW_CLIENT_NOT_FOUND' using errcode='P0002';
  end if;
  if v_workflow.workflow_kind not in ('CONTRACT_COMBINED','CONTRACT_EXPENSE')
     or v_workflow.week_ending_date is null then
    return jsonb_build_object(
      'required',false,'categories','[]'::jsonb,'prior_claim_count',0,
      'confirmation_digest',null,'client_name',v_client_name,
      'week_ending_date',v_workflow.week_ending_date
    );
  end if;

  select coalesce(array_agg(category order by category),array[]::text[])
  into v_requested_categories
  from (
    select distinct upper(btrim(category)) as category
    from unnest(coalesce(p_required_categories,array[]::text[])) category
    where upper(btrim(category)) in ('MILEAGE','TRAVEL','ACCOMMODATION','OTHER')
  ) requested;

  if cardinality(v_requested_categories)=0 then
    return jsonb_build_object(
      'required',false,'categories','[]'::jsonb,'prior_claim_count',0,
      'confirmation_digest',null,'client_name',v_client_name,
      'week_ending_date',v_workflow.week_ending_date
    );
  end if;

  -- Candidate, Client and week ending form one duplicate-expense boundary.
  -- Serialising that boundary prevents two separate claims being submitted at
  -- the same instant and both incorrectly observing an empty prior set.
  perform pg_advisory_xact_lock(hashtextextended(
    'expense-duplicate:'||v_workflow.candidate_id::text||':'||v_client_id::text
      ||':'||v_workflow.week_ending_date::text,
    0
  ));

  with requested as (
    select unnest(v_requested_categories) as category
  ), prior_component_claims as (
    select distinct
      case when coalesce(prior_workflow.target_timesheet_id,prior_workflow.anchor_timesheet_id) is null
        then 'workflow:'||prior_workflow.id::text
        else 'timesheet:'||coalesce(prior_workflow.target_timesheet_id,prior_workflow.anchor_timesheet_id)::text
      end as claim_key,
      case
        when component.component_kind='MILEAGE_FORM' then 'MILEAGE'
        else upper(component.expense_category)
      end as category
    from public.candidate_submission_workflows prior_workflow
    join public.contracts prior_contract on prior_contract.id=prior_workflow.contract_id
    join public.candidate_submission_components component
      on component.workflow_id=prior_workflow.id
     and component.manager_approved_at_utc is not null
    where prior_workflow.id<>v_workflow.id
      and prior_workflow.candidate_id=v_workflow.candidate_id
      and prior_contract.client_id=v_client_id
      and prior_workflow.week_ending_date=v_workflow.week_ending_date
      and prior_workflow.workflow_kind in ('CONTRACT_COMBINED','CONTRACT_EXPENSE')
      and prior_workflow.worker_submitted_at_utc is not null
      and prior_workflow.state not in ('CREATED','WORKER_DRAFT','CANCELLED','EXPIRED','SUPERSEDED')
      -- A later claim must warn when the same category was approved on the
      -- worked Timesheet that anchors this new expense-only workflow. The
      -- different workflow ID already excludes the claim currently being
      -- submitted, so excluding the shared Timesheet anchor hid real matches.
      and component.component_kind in ('MILEAGE_FORM','EXPENSE_EVIDENCE')
      and component.state not in ('SUPERSEDED','REJECTED')
      and case when component.component_kind='MILEAGE_FORM' then 'MILEAGE'
        else upper(component.expense_category) end
          in ('MILEAGE','TRAVEL','ACCOMMODATION','OTHER')
  ), prior_financial_claims as (
    select distinct 'timesheet:'||prior_timesheet.timesheet_id::text as claim_key,category.category
    from public.timesheets prior_timesheet
    join public.timesheets_financials prior_financial
      on prior_financial.timesheet_id=prior_timesheet.timesheet_id
     and prior_financial.is_current=true
    cross join lateral (values
      ('MILEAGE',abs(coalesce(prior_financial.mileage_units,0))
        +abs(coalesce(prior_financial.mileage_pay_ex_vat,0))
        +abs(coalesce(prior_financial.mileage_charge_ex_vat,0))),
      ('TRAVEL',abs(coalesce(prior_financial.travel_pay_ex_vat,0))
        +abs(coalesce(prior_financial.travel_charge_ex_vat,0))),
      ('ACCOMMODATION',abs(coalesce(prior_financial.accommodation_pay_ex_vat,0))
        +abs(coalesce(prior_financial.accommodation_charge_ex_vat,0))),
      ('OTHER',abs(coalesce(prior_financial.other_pay_ex_vat,0))
        +abs(coalesce(prior_financial.other_charge_ex_vat,0))
        +case when abs(coalesce(prior_financial.expenses_pay_ex_vat,0))
                       +abs(coalesce(prior_financial.expenses_charge_ex_vat,0))>0
                    and abs(coalesce(prior_financial.travel_pay_ex_vat,0))
                       +abs(coalesce(prior_financial.travel_charge_ex_vat,0))
                       +abs(coalesce(prior_financial.accommodation_pay_ex_vat,0))
                       +abs(coalesce(prior_financial.accommodation_charge_ex_vat,0))
                       +abs(coalesce(prior_financial.other_pay_ex_vat,0))
                       +abs(coalesce(prior_financial.other_charge_ex_vat,0))=0
          then 1 else 0 end)
    ) category(category,amount)
    where prior_timesheet.is_current=true
      and prior_timesheet.archived_at_utc is null
      and prior_timesheet.timesheet_id is distinct from v_workflow.target_timesheet_id
      and prior_timesheet.timesheet_id is distinct from v_workflow.anchor_timesheet_id
      and prior_timesheet.week_ending_date=v_workflow.week_ending_date
      and prior_financial.candidate_id=v_workflow.candidate_id
      and prior_financial.client_id=v_client_id
      and category.amount>0
  ), prior_claims as (
    select * from prior_component_claims
    union
    select * from prior_financial_claims
  ), matched as (
    select prior_claims.claim_key,prior_claims.category
    from prior_claims join requested using(category)
  )
  select
    coalesce(array_agg(distinct category order by category),array[]::text[]),
    count(distinct claim_key)::integer,
    coalesce(jsonb_agg(distinct jsonb_build_object('claim_key',claim_key,'category',category)),'[]'::jsonb)
  into v_duplicate_categories,v_prior_claim_count,v_prior_claims
  from matched;

  v_review_identity:=jsonb_build_object(
    'contract_version','CANDIDATE_DUPLICATE_EXPENSE_REVIEW_V1',
    'workflow_id',v_workflow.id,
    'candidate_id',v_workflow.candidate_id,
    'client_id',v_client_id,
    'week_ending_date',v_workflow.week_ending_date,
    'categories',to_jsonb(v_duplicate_categories),
    'prior_claims',v_prior_claims
  );
  v_confirmation_digest:=case when cardinality(v_duplicate_categories)>0
    then encode(private._candidate_sha256_jsonb_v1(v_review_identity),'hex') else null end;

  return jsonb_build_object(
    'required',cardinality(v_duplicate_categories)>0,
    'categories',to_jsonb(v_duplicate_categories),
    'prior_claim_count',v_prior_claim_count,
    'confirmation_digest',v_confirmation_digest,
    'client_name',v_client_name,
    'week_ending_date',v_workflow.week_ending_date
  );
end;
$function$;

revoke all on function private._expense_duplicate_review_v1(uuid,text[])
  from public,anon,authenticated,service_role;

commit;
