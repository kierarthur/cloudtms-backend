ALTER TABLE public.tms_users
  ADD COLUMN IF NOT EXISTS email_signature_html text;

COMMENT ON COLUMN public.tms_users.email_signature_html
  IS 'Per-user editable HTML email signature used for default injection into email editors.';
