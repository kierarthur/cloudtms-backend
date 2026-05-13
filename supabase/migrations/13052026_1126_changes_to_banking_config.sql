update public.banking_pay_operation_config cfg
set
  max_chunk_size = 250,
  updated_at_utc = now()
where cfg.enabled is true
  and cfg.max_chunk_size = 500
  and (
    (cfg.operation_type = 'PAYMENT_EXECUTE' and cfg.phase in (
      'APPLY_RAIL_UPDATES',
      'PREPARE_TRANSFER_CHUNKS',
      'PREPARE_TRANSFER_SCOPE'
    ))
    or (cfg.operation_type = 'PAYMENT_SETTLEMENT' and cfg.phase = 'APPLY_SETTLEMENT_CHUNKS')
    or (cfg.operation_type = 'REMITTANCE_QUEUE' and cfg.phase in (
      'QUEUE_PAYOUT_NOTICE_CHUNKS',
      'QUEUE_REMITTANCE_CHUNKS'
    ))
    or (cfg.operation_type = 'DRAFT_CREATE' and cfg.phase in (
      'BUILD_ITEM_BREAKDOWNS',
      'CREATE_TIMESHEET_SNAPSHOTS',
      'DRAIN_TSFIN',
      'FINALISE_RESERVATIONS',
      'INSERT_CANDIDATES',
      'INSERT_ITEMS',
      'POPULATE_CANDIDATE_SUMMARIES',
      'SEED_DRAFT_CHUNKS'
    ))
  );

update public.banking_pay_operation_config cfg
set
  default_chunk_size = 50,
  max_chunk_size = 250,
  updated_at_utc = now()
where cfg.enabled is true
  and cfg.operation_type = 'DRAFT_CREATE'
  and cfg.phase = 'APPLY_FINANCE_ADJUSTMENTS'
  and cfg.chunk_type = 'CANDIDATE_SCOPE';
