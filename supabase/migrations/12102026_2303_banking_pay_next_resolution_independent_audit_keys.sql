-- One-time CloudTMS schema/data migration: banking_pay_next_resolution_independent_audit_keys
-- WB29 / BD161: retain every exact independent decision through one publication.
-- Four audited private indexes only; no row rewrite, public trigger or second store.

\set ON_ERROR_STOP on

begin;

do $function$
declare v_name text;v_definition text;
begin
 for v_name,v_definition in select * from (values
  ('bpay_next_component_resolution_active_idx',$expr$CREATE UNIQUE INDEX bpay_next_component_resolution_active_idx ON private.bpay_next_component_resolution USING btree (approved_line_id, rate_family, rate_code, target_pay_channel, target_umbrella_id, valuation_policy_id, valuation_date) NULLS NOT DISTINCT WHERE (state = 'ACTIVE'::text)$expr$),
  ('bpay_next_component_resolution_event_request_idx',$expr$CREATE UNIQUE INDEX bpay_next_component_resolution_event_request_idx ON private.bpay_next_component_resolution_event USING btree (command_id, event_kind, approved_line_id, rate_family, rate_code) WHERE (command_id IS NOT NULL)$expr$),
  ('bpay_next_component_resolution_published_active_idx',$expr$CREATE UNIQUE INDEX bpay_next_component_resolution_published_active_idx ON private.bpay_next_component_resolution USING btree (published_line_id, rate_family, rate_code, target_pay_channel, target_umbrella_id, valuation_policy_id, valuation_date) NULLS NOT DISTINCT WHERE ((state = 'ACTIVE'::text) AND (published_line_id IS NOT NULL))$expr$),
  ('bpay_next_component_resolution_version_idx',$expr$CREATE UNIQUE INDEX bpay_next_component_resolution_version_idx ON private.bpay_next_component_resolution USING btree (approved_line_id, rate_family, rate_code, target_pay_channel, target_umbrella_id, valuation_policy_id, valuation_date, decision_version) NULLS NOT DISTINCT$expr$)
 ) expected(name,definition) loop
  if pg_catalog.pg_get_indexdef(pg_catalog.to_regclass('private.'||v_name)) is distinct from v_definition then
   raise exception using errcode='23514',message='BPAY_NEXT_RESOLUTION_INDEPENDENT_INDEX_CHANGED';end if;
 end loop;
end
$function$;

drop index private.bpay_next_component_resolution_event_request_idx;
create unique index bpay_next_component_resolution_event_request_idx
 on private.bpay_next_component_resolution_event
 (command_id,event_kind,approved_line_id,rate_family,rate_code,decision_id,decision_version)
 where command_id is not null;

drop index private.bpay_next_component_resolution_version_idx;
create unique index bpay_next_component_resolution_version_idx on private.bpay_next_component_resolution
 (approved_line_id,rate_family,rate_code,target_pay_channel,target_umbrella_id,target_umbrella_vat_chargeable,
 valuation_policy_id,valuation_date,decision_version) nulls not distinct;
drop index private.bpay_next_component_resolution_active_idx;
create unique index bpay_next_component_resolution_active_idx on private.bpay_next_component_resolution
 (approved_line_id,rate_family,rate_code,target_pay_channel,target_umbrella_id,target_umbrella_vat_chargeable,
 valuation_policy_id,valuation_date) nulls not distinct where state='ACTIVE';
drop index private.bpay_next_component_resolution_published_active_idx;
create unique index bpay_next_component_resolution_published_active_idx on private.bpay_next_component_resolution
 (published_line_id,rate_family,rate_code,target_pay_channel,target_umbrella_id,target_umbrella_vat_chargeable,
 valuation_policy_id,valuation_date) nulls not distinct where state='ACTIVE' and published_line_id is not null;
-- Per-version SAVED/CLEARED, NEED, native mapping indexes, guards and ACLs retained.

commit;
