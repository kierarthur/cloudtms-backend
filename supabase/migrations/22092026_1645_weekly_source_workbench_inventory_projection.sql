-- One-time CloudTMS schema/data migration: weekly_source_workbench_inventory_projection
-- State the exact authority, safety boundary, and verification before implementation.

\set ON_ERROR_STOP on

begin;

-- Derived validation metadata only. Source heads/components remain financial
-- authority. Existing heads are NOT silently certified by this migration.
create table private.weekly_source_workbench_inventory_v1 (
  head_id uuid primary key references public.weekly_source_entitlement_heads(id) on delete cascade,
  component_count bigint not null default 0 check(component_count>=0),
  emittable_count bigint not null default 0 check(emittable_count>=0),
  invalid_count bigint not null default 0 check(invalid_count>=0),
  complete boolean not null default false,
  bootstrap_last_ordinal integer not null default 0 check(bootstrap_last_ordinal>=0),
  check(emittable_count<=component_count and invalid_count<=component_count)
);
alter table private.weekly_source_workbench_inventory_v1 owner to postgres;
revoke all on private.weekly_source_workbench_inventory_v1 from public,anon,authenticated,service_role;
alter table private.weekly_source_workbench_inventory_v1 enable row level security;
alter table private.weekly_source_workbench_inventory_v1 force row level security;
create policy weekly_source_workbench_inventory_owner on private.weekly_source_workbench_inventory_v1
  to current_user using(true) with check(true);

-- One permanent installation-progress row. This is not a business activation
-- flag and is never read by a payment or Source publication owner.
create table private.weekly_source_workbench_inventory_install_v1 (
  singleton boolean primary key default true check(singleton),
  scan_through uuid,
  last_head uuid,
  complete boolean not null default false,
  page_calls bigint not null default 0 check(page_calls>=0),
  check(last_head is null or (scan_through is not null and last_head<=scan_through))
);
alter table private.weekly_source_workbench_inventory_install_v1 owner to postgres;
revoke all on private.weekly_source_workbench_inventory_install_v1 from public,anon,authenticated,service_role;
alter table private.weekly_source_workbench_inventory_install_v1 enable row level security;
alter table private.weekly_source_workbench_inventory_install_v1 force row level security;
create policy weekly_source_workbench_inventory_install_owner on private.weekly_source_workbench_inventory_install_v1
  to current_user using(true) with check(true);

-- Exact monetary-emission predicate; suppressed inventory remains in Source.
-- Numeric ordinal is the seek key, never the rendered source-key expression.
create index weekly_source_head_components_emittable_ordinal_idx
 on public.weekly_source_entitlement_head_components(head_id,component_ordinal)
 where component_kind='WORKED_TIME'
    or round(case when exclude_from_pay then 0::numeric else coalesce(pay_ex_vat,0) end,2)<>0;

commit;
