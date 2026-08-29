BEGIN;

CREATE TABLE IF NOT EXISTS public.banking_alert_acknowledgements (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_fingerprint text NOT NULL,
    alert_kind text NOT NULL,
    entity_kind text NOT NULL DEFAULT 'pay_batch',
    entity_id uuid NOT NULL,
    acknowledged_by_user_id uuid NOT NULL,
    acknowledged_at_utc timestamptz NOT NULL DEFAULT now(),
    acknowledge_scope text NOT NULL DEFAULT 'USER',
    note text NULL,
    alert_payload_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    resolved_at_ack boolean NOT NULL DEFAULT false
);

ALTER TABLE public.banking_alert_acknowledgements
    ADD COLUMN IF NOT EXISTS id uuid DEFAULT gen_random_uuid();

ALTER TABLE public.banking_alert_acknowledgements
    ADD COLUMN IF NOT EXISTS alert_fingerprint text;

ALTER TABLE public.banking_alert_acknowledgements
    ADD COLUMN IF NOT EXISTS alert_kind text;

ALTER TABLE public.banking_alert_acknowledgements
    ADD COLUMN IF NOT EXISTS entity_kind text DEFAULT 'pay_batch';

ALTER TABLE public.banking_alert_acknowledgements
    ADD COLUMN IF NOT EXISTS entity_id uuid;

ALTER TABLE public.banking_alert_acknowledgements
    ADD COLUMN IF NOT EXISTS acknowledged_by_user_id uuid;

ALTER TABLE public.banking_alert_acknowledgements
    ADD COLUMN IF NOT EXISTS acknowledged_at_utc timestamptz DEFAULT now();

ALTER TABLE public.banking_alert_acknowledgements
    ADD COLUMN IF NOT EXISTS acknowledge_scope text DEFAULT 'USER';

ALTER TABLE public.banking_alert_acknowledgements
    ADD COLUMN IF NOT EXISTS note text;

ALTER TABLE public.banking_alert_acknowledgements
    ADD COLUMN IF NOT EXISTS alert_payload_json jsonb DEFAULT '{}'::jsonb;

ALTER TABLE public.banking_alert_acknowledgements
    ADD COLUMN IF NOT EXISTS resolved_at_ack boolean DEFAULT false;

ALTER TABLE public.banking_alert_acknowledgements
    ALTER COLUMN id SET DEFAULT gen_random_uuid(),
    ALTER COLUMN entity_kind SET DEFAULT 'pay_batch',
    ALTER COLUMN acknowledged_at_utc SET DEFAULT now(),
    ALTER COLUMN acknowledge_scope SET DEFAULT 'USER',
    ALTER COLUMN alert_payload_json SET DEFAULT '{}'::jsonb,
    ALTER COLUMN resolved_at_ack SET DEFAULT false;

UPDATE public.banking_alert_acknowledgements AS baa
SET
    entity_kind = COALESCE(NULLIF(BTRIM(baa.entity_kind), ''), 'pay_batch'),
    acknowledge_scope = COALESCE(NULLIF(BTRIM(baa.acknowledge_scope), ''), 'USER'),
    alert_payload_json = COALESCE(baa.alert_payload_json, '{}'::jsonb),
    resolved_at_ack = COALESCE(baa.resolved_at_ack, false),
    acknowledged_at_utc = COALESCE(baa.acknowledged_at_utc, now())
WHERE
    baa.entity_kind IS NULL
    OR BTRIM(baa.entity_kind) = ''
    OR baa.acknowledge_scope IS NULL
    OR BTRIM(baa.acknowledge_scope) = ''
    OR baa.alert_payload_json IS NULL
    OR baa.resolved_at_ack IS NULL
    OR baa.acknowledged_at_utc IS NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS c
        WHERE c.conrelid = 'public.banking_alert_acknowledgements'::regclass
          AND c.conname = 'banking_alert_acknowledgements_pkey'
    ) THEN
        ALTER TABLE public.banking_alert_acknowledgements
            ADD CONSTRAINT banking_alert_acknowledgements_pkey PRIMARY KEY (id);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS c
        WHERE c.conrelid = 'public.banking_alert_acknowledgements'::regclass
          AND c.conname = 'banking_alert_acknowledgements_ack_user_fkey'
    ) THEN
        ALTER TABLE public.banking_alert_acknowledgements
            ADD CONSTRAINT banking_alert_acknowledgements_ack_user_fkey
            FOREIGN KEY (acknowledged_by_user_id)
            REFERENCES auth.users(id)
            ON DELETE CASCADE;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS c
        WHERE c.conrelid = 'public.banking_alert_acknowledgements'::regclass
          AND c.conname = 'banking_alert_acknowledgements_fingerprint_chk'
    ) THEN
        ALTER TABLE public.banking_alert_acknowledgements
            ADD CONSTRAINT banking_alert_acknowledgements_fingerprint_chk
            CHECK (BTRIM(alert_fingerprint) <> '');
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS c
        WHERE c.conrelid = 'public.banking_alert_acknowledgements'::regclass
          AND c.conname = 'banking_alert_acknowledgements_kind_chk'
    ) THEN
        ALTER TABLE public.banking_alert_acknowledgements
            ADD CONSTRAINT banking_alert_acknowledgements_kind_chk
            CHECK (BTRIM(alert_kind) <> '');
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS c
        WHERE c.conrelid = 'public.banking_alert_acknowledgements'::regclass
          AND c.conname = 'banking_alert_acknowledgements_entity_kind_chk'
    ) THEN
        ALTER TABLE public.banking_alert_acknowledgements
            ADD CONSTRAINT banking_alert_acknowledgements_entity_kind_chk
            CHECK (BTRIM(entity_kind) <> '');
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS c
        WHERE c.conrelid = 'public.banking_alert_acknowledgements'::regclass
          AND c.conname = 'banking_alert_acknowledgements_scope_chk'
    ) THEN
        ALTER TABLE public.banking_alert_acknowledgements
            ADD CONSTRAINT banking_alert_acknowledgements_scope_chk
            CHECK (BTRIM(acknowledge_scope) <> '');
    END IF;
END $$;

ALTER TABLE public.banking_alert_acknowledgements
    ALTER COLUMN id SET NOT NULL,
    ALTER COLUMN alert_fingerprint SET NOT NULL,
    ALTER COLUMN alert_kind SET NOT NULL,
    ALTER COLUMN entity_kind SET NOT NULL,
    ALTER COLUMN entity_id SET NOT NULL,
    ALTER COLUMN acknowledged_by_user_id SET NOT NULL,
    ALTER COLUMN acknowledged_at_utc SET NOT NULL,
    ALTER COLUMN acknowledge_scope SET NOT NULL,
    ALTER COLUMN alert_payload_json SET NOT NULL,
    ALTER COLUMN resolved_at_ack SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_banking_alert_ack_user_fingerprint_scope
ON public.banking_alert_acknowledgements (
    alert_fingerprint,
    acknowledged_by_user_id,
    acknowledge_scope
);

CREATE INDEX IF NOT EXISTS idx_banking_alert_ack_user_scope
ON public.banking_alert_acknowledgements (
    acknowledged_by_user_id,
    acknowledge_scope,
    acknowledged_at_utc DESC
);

CREATE INDEX IF NOT EXISTS idx_banking_alert_ack_entity
ON public.banking_alert_acknowledgements (
    entity_kind,
    entity_id,
    acknowledged_at_utc DESC
);

CREATE INDEX IF NOT EXISTS idx_banking_alert_ack_kind
ON public.banking_alert_acknowledgements (
    alert_kind,
    acknowledged_at_utc DESC
);

COMMENT ON TABLE public.banking_alert_acknowledgements IS
'Stores per-user acknowledgement of active Banking alert fingerprints. Acknowledgement only clears the Banking nav/popover attention state for that user; it does not resolve or mutate the underlying payment issue.';

COMMENT ON COLUMN public.banking_alert_acknowledgements.alert_fingerprint IS
'Durable fingerprint for the exact Banking alert state. If the issue changes materially, a new fingerprint should be generated.';

COMMENT ON COLUMN public.banking_alert_acknowledgements.acknowledge_scope IS
'Initial scope is USER. Future scopes may support admin/global acknowledgement, but default behaviour is per-user only.';

COMMENT ON COLUMN public.banking_alert_acknowledgements.resolved_at_ack IS
'Snapshot flag only. True means the alert was already resolved when acknowledgement was recorded; it must not be used as the source of payment resolution truth.';

COMMIT;
