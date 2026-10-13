-- WB-45 / BD153: independent immutable treatment versions and append-only events.
-- Empty private schema only; WB-29 owns commands, publication and captured joins.

\set ON_ERROR_STOP on

begin;

create table private.bpay_next_component_resolution (
 id uuid primary key default gen_random_uuid(),
 work_id uuid not null,candidate_id uuid not null,
 based_on_revision_id uuid not null,approved_line_id uuid not null,
 physical_timesheet_id uuid not null references public.timesheets(timesheet_id) on delete restrict,
 component_key text not null check(length(component_key) between 1 and 256),
 rate_family text not null check(rate_family in ('STANDARD','ADDITIONAL','ADJUSTMENT')),
 rate_code text not null check(length(rate_code) between 1 and 256),
 decision_version bigint not null check(decision_version>0 and decision_version<9223372036854775807),
 original_basis jsonb not null check(jsonb_typeof(original_basis)='object' and octet_length(original_basis::text)<=8192),
 published_revision_id uuid,published_line_id uuid,published_basis jsonb,
 state text not null default 'ACTIVE' check(state in ('ACTIVE','CLEARED')),
 command_id uuid not null references private.bpay_next_command(id) on delete restrict,
 actor_user_id uuid not null references public.tms_users(id) on delete restrict,
 request_body jsonb not null check(jsonb_typeof(request_body)='object' and octet_length(request_body::text)<=8192),
 target_pay_channel text not null check(target_pay_channel in ('PAYE','UMBRELLA')),
 target_umbrella_id uuid references public.umbrellas(id) on delete restrict,
 target_umbrella_vat_chargeable boolean,
 valuation_policy_id uuid not null references private.bpay_next_valuation_policy(id) on delete restrict,
 valuation_date date not null check(isfinite(valuation_date) and valuation_date between date '0001-01-01' and date '9999-12-31'),
 resolution_mode public.pay_finance_component_resolution_mode_enum not null,
 -- Preserve input precision until admission; a typmod must not round a seventh decimal.
 manual_replacement_rate numeric,manual_amount_ex_vat private.bpay_next_penny_amount,
 reason text check(octet_length(reason)<=2000),
 saved_at_utc timestamptz not null default transaction_timestamp() check(isfinite(saved_at_utc)),
 unique(id,decision_version),
 foreign key(work_id,candidate_id) references private.bpay_next_work(id,candidate_id) on delete restrict,
 foreign key(work_id,based_on_revision_id) references private.bpay_next_work_revision(work_id,id) on delete restrict,
 foreign key(based_on_revision_id,approved_line_id,component_key)
  references private.bpay_next_approved_line(revision_id,id,component_key) on delete restrict,
 foreign key(work_id,published_revision_id) references private.bpay_next_work_revision(work_id,id) on delete restrict,
 foreign key(published_revision_id,published_line_id,component_key)
  references private.bpay_next_approved_line(revision_id,id,component_key) on delete restrict,
 foreign key(command_id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
 check((published_revision_id is null and published_line_id is null and published_basis is null)
  or(published_revision_id is not null and published_revision_id<>based_on_revision_id
   and published_line_id is not null and published_basis is not null
   and jsonb_typeof(published_basis)='object' and octet_length(published_basis::text)<=8192)),
 check((target_pay_channel='PAYE' and target_umbrella_id is null and target_umbrella_vat_chargeable is null)
  or(target_pay_channel='UMBRELLA' and target_umbrella_id is not null and target_umbrella_vat_chargeable is not null)),
 check((resolution_mode='SUGGESTED_EQUIVALENT_BASIS' and manual_replacement_rate is null and manual_amount_ex_vat is null)
  or(resolution_mode='MANUAL_REPLACEMENT_RATE' and manual_replacement_rate is not null
   and manual_replacement_rate::text not in ('NaN','Infinity','-Infinity') and manual_replacement_rate>=0
   and manual_replacement_rate<1000000000000 and manual_replacement_rate=trunc(manual_replacement_rate,6)
   and manual_amount_ex_vat is null and nullif(btrim(reason),'') is not null)
  or(resolution_mode='MANUAL_AMOUNT' and manual_amount_ex_vat is not null and manual_amount_ex_vat>=0
   and manual_amount_ex_vat<10000000000000000
   and manual_replacement_rate is null and nullif(btrim(reason),'') is not null))
);
-- Native immutable line identity supplies the physical/economic signature; no JSON hash index.
create unique index bpay_next_component_resolution_version_idx on private.bpay_next_component_resolution
 (approved_line_id,rate_family,rate_code,target_pay_channel,target_umbrella_id,
  valuation_policy_id,valuation_date,decision_version) nulls not distinct;
create unique index bpay_next_component_resolution_active_idx on private.bpay_next_component_resolution
 (approved_line_id,rate_family,rate_code,target_pay_channel,target_umbrella_id,
  valuation_policy_id,valuation_date) nulls not distinct where state='ACTIVE';
create index bpay_next_component_resolution_boundary_idx on private.bpay_next_component_resolution
 (work_id,based_on_revision_id,approved_line_id,rate_family,rate_code) where state='ACTIVE';
create index bpay_next_component_resolution_published_idx on private.bpay_next_component_resolution
 (work_id,published_revision_id,published_line_id,rate_family,rate_code) where state='ACTIVE';
create unique index bpay_next_component_resolution_published_active_idx on private.bpay_next_component_resolution
 (published_line_id,rate_family,rate_code,target_pay_channel,target_umbrella_id,
  valuation_policy_id,valuation_date) nulls not distinct where state='ACTIVE' and published_line_id is not null;

create table private.bpay_next_component_resolution_event (
 id uuid primary key default gen_random_uuid(),
 event_kind text not null check(event_kind in ('NEED','SAVED','CLEARED')),
 work_id uuid not null,candidate_id uuid not null,revision_id uuid not null,approved_line_id uuid not null,
 rate_family text not null check(rate_family in ('STANDARD','ADDITIONAL','ADJUSTMENT')),
 rate_code text not null check(length(rate_code) between 1 and 256),
 decision_id uuid,decision_version bigint,
 need_operation_id uuid,need_member_key text,need_version bigint,
 command_id uuid references private.bpay_next_command(id) on delete restrict,
 actor_user_id uuid not null references public.tms_users(id) on delete restrict,
 reason text not null check(octet_length(reason) between 1 and 2000),
 occurred_at_utc timestamptz not null default transaction_timestamp() check(isfinite(occurred_at_utc)),
 foreign key(work_id,candidate_id) references private.bpay_next_work(id,candidate_id) on delete restrict,
 foreign key(work_id,revision_id) references private.bpay_next_work_revision(work_id,id) on delete restrict,
 foreign key(revision_id,approved_line_id) references private.bpay_next_approved_line(revision_id,id) on delete restrict,
 foreign key(decision_id,decision_version) references private.bpay_next_component_resolution(id,decision_version) on delete restrict,
 foreign key(need_operation_id,need_member_key) references private.bpay_next_pay_method_change_member(operation_id,member_key) on delete restrict,
 foreign key(command_id,candidate_id) references private.bpay_next_command_member(command_id,candidate_id) on delete restrict,
 check((event_kind='NEED' and decision_id is null and decision_version is null and command_id is null
   and need_operation_id is not null and need_member_key is not null and need_version is not null and need_version>0)
  or(event_kind in ('SAVED','CLEARED') and decision_id is not null and decision_version is not null
   and command_id is not null and need_operation_id is null and need_member_key is null and need_version is null))
);
create unique index bpay_next_component_resolution_event_version_idx
 on private.bpay_next_component_resolution_event(decision_id,decision_version,event_kind) where decision_id is not null;
create unique index bpay_next_component_resolution_need_idx
 on private.bpay_next_component_resolution_event(need_operation_id,need_member_key,approved_line_id,rate_family,rate_code,need_version)
 where event_kind='NEED';
create unique index bpay_next_component_resolution_event_request_idx
 on private.bpay_next_component_resolution_event(command_id,event_kind,approved_line_id,rate_family,rate_code)
 where command_id is not null;
create index bpay_next_component_resolution_event_work_idx
 on private.bpay_next_component_resolution_event(work_id,revision_id,approved_line_id,rate_family,rate_code);

alter table private.bpay_next_component_resolution owner to postgres;
alter table private.bpay_next_component_resolution_event owner to postgres;
alter table private.bpay_next_component_resolution enable row level security;
alter table private.bpay_next_component_resolution force row level security;
alter table private.bpay_next_component_resolution_event enable row level security;
alter table private.bpay_next_component_resolution_event force row level security;
create policy service_owner on private.bpay_next_component_resolution to CURRENT_USER using(true) with check(true);
create policy service_owner on private.bpay_next_component_resolution_event to CURRENT_USER using(true) with check(true);
revoke all on private.bpay_next_component_resolution,private.bpay_next_component_resolution_event
 from public,anon,authenticated,service_role;

commit;
