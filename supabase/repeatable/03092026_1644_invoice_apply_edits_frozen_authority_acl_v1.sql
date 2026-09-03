-- The current invoice editor is re-included by later July repeatables. Reassert
-- its final service-only ACL after the frozen-settings source change so NEW and
-- UPGRADE installations end with the same browser-isolated authority.

alter function public.invoice_apply_edits(uuid,jsonb,uuid) owner to postgres;

revoke all on function public.invoice_apply_edits(uuid,jsonb,uuid)
  from public,anon,authenticated,service_role,authenticator,supabase_admin;

grant execute on function public.invoice_apply_edits(uuid,jsonb,uuid)
  to postgres,service_role;

alter function public.invoice_detail_get(uuid,uuid) owner to postgres;

revoke all on function public.invoice_detail_get(uuid,uuid)
  from public,anon,authenticated,service_role,authenticator,supabase_admin;

grant execute on function public.invoice_detail_get(uuid,uuid)
  to postgres,service_role;
