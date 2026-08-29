alter table public.pay_bank_transfer_events
drop constraint if exists pay_bank_transfer_events_mapping_method_chk;

alter table public.pay_bank_transfer_events
add constraint pay_bank_transfer_events_mapping_method_chk
check (
  mapping_method is null
  or mapping_method = any (
    array[
      'TRANSFER_ID'::text,
      'PROVIDER_EVENT_ID'::text,
      'PROVIDER_REFERENCE'::text,
      'PROVIDER_TRANSACTION_ID'::text,
      'REQUEST_ID'::text,
      'RAIL_TX_ID'::text,
      'PAYMENT_REFERENCE'::text,
      'MANUAL_TRANSFER_SELECTION'::text,
      'AMOUNT_ONLY_UNIQUE'::text,
      'UNMATCHED'::text,
      'AMBIGUOUS'::text,
      'LEGACY_NO_ARTIFACT'::text
    ]
  )
);
