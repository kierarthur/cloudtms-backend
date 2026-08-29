-- CloudTMS Banking Pay cancellation — Stage 1.
-- Immutable SHA-256 helper for Banking Pay correction contracts.

CREATE OR REPLACE FUNCTION private.pay_payment_correction_sha256_v1(
    p_value jsonb
)
RETURNS text
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
SECURITY INVOKER
SET search_path TO pg_catalog, extensions, pg_temp
SET statement_timeout TO '1000ms'
AS $function$
    SELECT CASE
        WHEN p_value IS NULL THEN
            pg_catalog.current_setting('PAYMENT_CORRECTION_HASH_INPUT_REQUIRED')
        ELSE
            pg_catalog.encode(
                extensions.digest(
                    pg_catalog.convert_to(p_value::text, 'UTF8'),
                    'sha256'
                ),
                'hex'
            )
    END
$function$;

ALTER FUNCTION private.pay_payment_correction_sha256_v1(jsonb) OWNER TO postgres;
REVOKE ALL ON FUNCTION private.pay_payment_correction_sha256_v1(jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION private.pay_payment_correction_sha256_v1(jsonb) FROM anon;
REVOKE ALL ON FUNCTION private.pay_payment_correction_sha256_v1(jsonb) FROM authenticated;
REVOKE ALL ON FUNCTION private.pay_payment_correction_sha256_v1(jsonb) FROM service_role;
