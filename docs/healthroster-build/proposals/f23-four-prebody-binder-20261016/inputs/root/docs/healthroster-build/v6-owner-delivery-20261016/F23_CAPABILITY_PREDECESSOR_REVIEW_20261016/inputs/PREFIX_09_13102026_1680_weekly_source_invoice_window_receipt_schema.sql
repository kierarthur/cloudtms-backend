-- Genuine Source identity retained; additive invoice window/receipt projection.
\set ON_ERROR_STOP on
begin;
-- Isolated complete source proposal. No installation/runtime acceptance.
-- Additive schema proposal over reviewed native Plan6 + frozen Money1430.
-- No new relation, origin or policy/pay hash. A NULL window is the old scope.
alter table public.weekly_source_client_manifests add column invoice_window_cycle_id uuid
  references public.weekly_source_cycles(id) on delete restrict;
alter table public.weekly_source_billing_movements add column invoice_window_cycle_id uuid
  references public.weekly_source_cycles(id) on delete restrict,
  add constraint weekly_source_round_window_kind_chk check(invoice_window_cycle_id is null or
    (source_line_kind='INVOICE_VAT_ROUNDING' and invoice_window_cycle_id<>finalisation_cycle_id));
alter table public.weekly_source_invoice_presentation_lines add column invoice_rounding_authority_json jsonb,
  add constraint weekly_source_round_authority_kind_chk check(invoice_rounding_authority_json is null or
    (line_kind='INVOICE_VAT_ROUNDING' and origin_kind='INVOICE_VAT_ROUNDING'
      and jsonb_typeof(invoice_rounding_authority_json)='object'
      and invoice_rounding_authority_json->>'contract'='WEEKLY_SOURCE_INVOICE_ROUNDING_AUTHORITY_V1'));
-- Discover only the exact native two-column unique key, asserting its census,
-- instead of guessing PostgreSQL's truncated generated constraint name.
do $constraint$
declare names text[]; actual text;
begin
  select array_agg(c.conname order by c.conname) into names from pg_catalog.pg_constraint c
    where c.conrelid='public.weekly_source_client_manifests'::regclass and c.contype='u'
      and (select array_agg(a.attname::text order by k.ordinality) from unnest(c.conkey) with ordinality k(attnum,ordinality)
        join pg_catalog.pg_attribute a on a.attrelid=c.conrelid and a.attnum=k.attnum)=array['final_revision_id','client_id'];
  if cardinality(names) is distinct from 1 then
    raise exception 'WEEKLY_SOURCE_MANIFEST_NATIVE_UNIQUE_KEY_UNEXPECTED' using errcode='55000'; end if;
  actual:=names[1];
  execute format('alter table public.weekly_source_client_manifests drop constraint %I',actual);
end;
$constraint$;
create unique index weekly_source_client_manifests_revision_client_window_uq
  on public.weekly_source_client_manifests(final_revision_id,client_id,coalesce(invoice_window_cycle_id,source_cycle_id));
-- Exact native1430 explicit index: its definition is checked before replacement.
do $index$
begin
  if not exists(select 1 from pg_catalog.pg_index i
    where i.indexrelid=to_regclass('public.weekly_source_billing_movements_vat_rounding_client_uq')
      and i.indrelid='public.weekly_source_billing_movements'::regclass and i.indisunique
      and pg_get_expr(i.indpred,i.indrelid)='(vat_rounding_revision_id IS NOT NULL)'
      and (select array_agg(a.attname::text order by key.ordinality) from unnest(i.indkey) with ordinality key(attnum,ordinality)
        join pg_catalog.pg_attribute a on a.attrelid=i.indrelid and a.attnum=key.attnum)=array['vat_rounding_revision_id','actual_client_id']) then
    raise exception 'WEEKLY_SOURCE_ROUND_NATIVE_UNIQUE_KEY_UNEXPECTED' using errcode='55000'; end if;
end;
$index$;
drop index public.weekly_source_billing_movements_vat_rounding_client_uq;
create unique index weekly_source_billing_movements_vat_rounding_client_window_uq
  on public.weekly_source_billing_movements(vat_rounding_revision_id,actual_client_id,coalesce(invoice_window_cycle_id,finalisation_cycle_id))
  where vat_rounding_revision_id is not null;
-- Existing generic immutable/lifecycle guards compare complete row JSON, so the
-- three new fields retain their existing relation classes and RLS/direct ACL.


commit;
