create unique index if not exists ux_mail_outbox_deterministic_outbox_key_full
on public.mail_outbox using btree (deterministic_outbox_key);
