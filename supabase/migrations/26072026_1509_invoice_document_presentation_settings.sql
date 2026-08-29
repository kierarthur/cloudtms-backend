BEGIN;

-- Preserve the current downstream phase implementation before the replacement
-- BUILD_MANIFEST authority is installed. This is an internal helper only.
DO $preserve_downstream$
DECLARE
  v_definition text;
BEGIN
  IF to_regprocedure(
      'private._invoice_document_advance_batch_v6_downstream(jsonb,timestamp with time zone)'
    ) IS NULL THEN
    IF to_regprocedure(
        'private._invoice_document_advance_batch(jsonb,timestamp with time zone)'
      ) IS NULL THEN
      RAISE EXCEPTION USING
        ERRCODE = '42883',
        MESSAGE = 'INVOICE_DOCUMENT_ADVANCE_AUTHORITY_MISSING';
    END IF;

    SELECT pg_get_functiondef(
      'private._invoice_document_advance_batch(jsonb,timestamp with time zone)'::regprocedure
    )
    INTO v_definition;

    v_definition := replace(
      v_definition,
      'FUNCTION private._invoice_document_advance_batch(',
      'FUNCTION private._invoice_document_advance_batch_v6_downstream('
    );

    IF v_definition NOT LIKE
        '%FUNCTION private._invoice_document_advance_batch_v6_downstream(%' THEN
      RAISE EXCEPTION USING
        ERRCODE = '55000',
        MESSAGE = 'INVOICE_DOCUMENT_ADVANCE_PRESERVATION_FAILED';
    END IF;

    EXECUTE v_definition;
  END IF;

  EXECUTE
    'REVOKE ALL ON FUNCTION private._invoice_document_advance_batch_v6_downstream(jsonb,timestamptz) FROM public,anon,authenticated';
  EXECUTE
    'GRANT EXECUTE ON FUNCTION private._invoice_document_advance_batch_v6_downstream(jsonb,timestamptz) TO service_role';
END;
$preserve_downstream$;

-- -------------------------------------------------------------------------
-- CloudTMS invoice document presentation settings
-- -------------------------------------------------------------------------
-- Purpose:
--   Adds one presentation-only configuration JSON column used by the invoice
--   document presentation snapshot functions.
--
-- Safety:
--   - Rerunnable.
--   - Does not change invoice economics.
--   - Does not change VAT calculation.
--   - Does not change payment routing.
--   - Does not seed invented legal wording, VAT numbers, or logo hashes.
--   - Only creates/validates schema for presentation configuration.
-- -------------------------------------------------------------------------

ALTER TABLE public.settings_defaults
  ADD COLUMN IF NOT EXISTS invoice_document_presentation_json jsonb;

UPDATE public.settings_defaults
SET invoice_document_presentation_json = '{}'::jsonb
WHERE invoice_document_presentation_json IS NULL;

ALTER TABLE public.settings_defaults
  ALTER COLUMN invoice_document_presentation_json SET DEFAULT '{}'::jsonb,
  ALTER COLUMN invoice_document_presentation_json SET NOT NULL;

COMMENT ON COLUMN public.settings_defaults.invoice_document_presentation_json IS
'Presentation-only invoice document configuration. Expected optional keys include: branding.logo {r2_key, sha256, size_bytes, media_type}, self_bill_legal_wording, legal_wording, hide_bank_footer_default, payment_instructions, remittance_email, locale, page_geometry. This column must not contain invoice economics, VAT rates, pay-side values, settlement routing, or remittance authority.';

ALTER TABLE public.settings_defaults
  DROP CONSTRAINT IF EXISTS settings_defaults_invoice_document_presentation_json_chk;

ALTER TABLE public.settings_defaults
  ADD CONSTRAINT settings_defaults_invoice_document_presentation_json_chk
  CHECK (
    invoice_document_presentation_json IS NOT NULL
    AND jsonb_typeof(invoice_document_presentation_json) = 'object'

    -- branding, if present, must be an object or JSON null.
    AND (
      invoice_document_presentation_json->'branding' IS NULL
      OR invoice_document_presentation_json->'branding' = 'null'::jsonb
      OR jsonb_typeof(invoice_document_presentation_json->'branding') = 'object'
    )

    -- branding.logo, if present, must be either JSON null or a complete immutable asset identity.
    AND (
      invoice_document_presentation_json #> '{branding,logo}' IS NULL
      OR invoice_document_presentation_json #> '{branding,logo}' = 'null'::jsonb
      OR (
        jsonb_typeof(invoice_document_presentation_json #> '{branding,logo}') = 'object'
        AND nullif(btrim(coalesce(invoice_document_presentation_json #>> '{branding,logo,r2_key}', '')), '') IS NOT NULL
        AND coalesce(invoice_document_presentation_json #>> '{branding,logo,sha256}', '') ~ '^[0-9a-f]{64}$'
        AND coalesce(invoice_document_presentation_json #>> '{branding,logo,size_bytes}', '') ~ '^[1-9][0-9]{0,18}$'
        AND lower(coalesce(invoice_document_presentation_json #>> '{branding,logo,media_type}', '')) IN (
          'image/jpeg',
          'image/png'
        )
      )
    )

    -- self-bill wording, if present, must be text or JSON null.
    AND (
      invoice_document_presentation_json->'self_bill_legal_wording' IS NULL
      OR invoice_document_presentation_json->'self_bill_legal_wording' = 'null'::jsonb
      OR jsonb_typeof(invoice_document_presentation_json->'self_bill_legal_wording') = 'string'
    )

    -- legal wording, if present, must be an array or JSON null.
    AND (
      invoice_document_presentation_json->'legal_wording' IS NULL
      OR invoice_document_presentation_json->'legal_wording' = 'null'::jsonb
      OR jsonb_typeof(invoice_document_presentation_json->'legal_wording') = 'array'
    )

    -- hide_bank_footer_default, if present, must be boolean or JSON null.
    AND (
      invoice_document_presentation_json->'hide_bank_footer_default' IS NULL
      OR invoice_document_presentation_json->'hide_bank_footer_default' = 'null'::jsonb
      OR jsonb_typeof(invoice_document_presentation_json->'hide_bank_footer_default') = 'boolean'
    )

    -- Optional text fields.
    AND (
      invoice_document_presentation_json->'payment_instructions' IS NULL
      OR invoice_document_presentation_json->'payment_instructions' = 'null'::jsonb
      OR jsonb_typeof(invoice_document_presentation_json->'payment_instructions') = 'string'
    )
    AND (
      invoice_document_presentation_json->'remittance_email' IS NULL
      OR invoice_document_presentation_json->'remittance_email' = 'null'::jsonb
      OR jsonb_typeof(invoice_document_presentation_json->'remittance_email') = 'string'
    )
    AND (
      invoice_document_presentation_json->'locale' IS NULL
      OR invoice_document_presentation_json->'locale' = 'null'::jsonb
      OR jsonb_typeof(invoice_document_presentation_json->'locale') = 'string'
    )
    AND (
      invoice_document_presentation_json->'page_geometry' IS NULL
      OR invoice_document_presentation_json->'page_geometry' = 'null'::jsonb
      OR jsonb_typeof(invoice_document_presentation_json->'page_geometry') = 'string'
    )
  ) NOT VALID;

ALTER TABLE public.settings_defaults
  VALIDATE CONSTRAINT settings_defaults_invoice_document_presentation_json_chk;

COMMIT;
