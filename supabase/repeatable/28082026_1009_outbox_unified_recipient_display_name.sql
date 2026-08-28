-- Repeatable CloudTMS function/view authority: outbox_unified_recipient_display_name
-- Use CREATE OR REPLACE and preserve owner, security, search_path, and ACL contracts.

\set ON_ERROR_STOP on

begin;

create or replace view public.v_outbox_unified with (security_invoker=true) as
with unified as (
  select
    'EMAIL'::text as channel,
    o.id as outbox_id,
    o.type as outbox_type,
    o.status::text as status,
    o.provider_status as delivery_status,
    o.created_at_utc,
    o.sent_at,
    o.delivered_at,
    o.read_at,
    o.failed_at,
    o."to" as to_address,
    o.cc,
    o.bcc,
    o.reply_to,
    o.importance,
    o.email_type,
    o.subject,
    o.body_text,
    o.body_html,
    o.attachments,
    o.reference,
    o.provider_message_id,
    o.last_error,
    o.created_by,
    o.recipient_kind,
    o.recipient_id,
    o.context_kind,
    o.context_id,
    o.mailshot_run_id,
    o.document_template_id,
    o.scheduled_for_utc,
    o.next_attempt_at_utc
  from public.mail_outbox as o

  union all

  select
    c.channel,
    c.id as outbox_id,
    null::text as outbox_type,
    c.status,
    case
      when c.read_at is not null then 'READ'::text
      when c.delivered_at is not null then 'DELIVERED'::text
      when c.sent_at is not null then 'SENT'::text
      when c.failed_at is not null then 'FAILED'::text
      else null::text
    end as delivery_status,
    c.created_at_utc,
    c.sent_at,
    c.delivered_at,
    c.read_at,
    c.failed_at,
    c.to_address,
    null::text as cc,
    null::text as bcc,
    null::text as reply_to,
    null::text as importance,
    null::text as email_type,
    null::text as subject,
    c.message_text as body_text,
    null::text as body_html,
    null::jsonb as attachments,
    null::text as reference,
    c.provider_message_id,
    c.last_error,
    c.created_by,
    c.recipient_kind,
    c.recipient_id,
    c.context_kind,
    c.context_id,
    c.mailshot_run_id,
    c.document_template_id,
    c.scheduled_for_utc,
    c.next_attempt_at_utc
  from public.comms_outbox as c
)
select
  u.channel,
  u.outbox_id,
  u.outbox_type,
  u.status,
  u.delivery_status,
  u.created_at_utc,
  u.sent_at,
  u.delivered_at,
  u.read_at,
  u.failed_at,
  u.to_address,
  u.cc,
  u.bcc,
  u.reply_to,
  u.importance,
  u.email_type,
  u.subject,
  u.body_text,
  u.body_html,
  u.attachments,
  u.reference,
  u.provider_message_id,
  u.last_error,
  u.created_by,
  u.recipient_kind,
  u.recipient_id,
  u.context_kind,
  u.context_id,
  u.mailshot_run_id,
  u.document_template_id,
  u.scheduled_for_utc,
  u.next_attempt_at_utc,
  coalesce(
    case
      when lower(coalesce(u.recipient_kind, '')) = 'candidate' then
        coalesce(
          nullif(btrim(c_rec.display_name), ''),
          nullif(btrim(concat_ws(' ', c_rec.first_name, c_rec.last_name)), ''),
          nullif(btrim(c_rec.email), ''),
          nullif(btrim(c_rec.phone), ''),
          nullif(btrim(u.to_address), '')
        )
      when lower(coalesce(u.recipient_kind, '')) = 'client' then
        nullif(btrim(coalesce(cl_rec.name, cl_rec.primary_invoice_email, cl_rec.contact_email, u.to_address)), '')
      when lower(coalesce(u.recipient_kind, '')) = 'umbrella' then
        nullif(btrim(coalesce(um_rec.name, um_rec.remittance_email, u.to_address)), '')
      else null
    end,
    nullif(btrim(coalesce(u.to_address, '')), '')
  ) as recipient_display_name
from unified as u
left join public.candidates as c_rec
  on lower(coalesce(u.recipient_kind, '')) = 'candidate'
 and u.recipient_id = c_rec.id
left join public.clients as cl_rec
  on lower(coalesce(u.recipient_kind, '')) = 'client'
 and u.recipient_id = cl_rec.id
left join public.umbrellas as um_rec
  on lower(coalesce(u.recipient_kind, '')) = 'umbrella'
 and u.recipient_id = um_rec.id;

alter view public.v_outbox_unified owner to postgres;

notify pgrst, 'reload schema';

commit;
