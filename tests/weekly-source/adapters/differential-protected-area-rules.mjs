// Weekly Source Plan 6.2 - the protected-area owner rules for the differential phase (WP-16d).
//
// WHY THIS FILE EXISTS.
//
// WP-16c built `differential-phase-adapter.mjs` and reached 11 of the 29 controlling
// protected areas. The other 18 were recorded `NOT_CAPTURED` with a reason of the form
// "browser phase" or "service phase". Executed against a real pair of builds, that reason is
// not the cause: every one of the 18 has installed database owners in the committed baseline.
// The cause is that `WEEKLY_SOURCE_PROTECTED_OWNERS` enumerated owners for 11 areas only.
// This file supplies an owner rule for all 29 so the database surface can be measured for
// every controlling protected ID.
//
// HOW AN OWNER SET IS CHOSEN, AND WHY IT IS NOT CHERRY-PICKED.
//
// Each area names a rule, not a hand-written list of survivors. The rule is a PostgreSQL
// regular expression over `nspname.proname` (and, where the area owns rows, over
// `nspname.relname`), or a rule over the installed definition text, or a rule over the
// privilege catalogue. THE SAME RULE IS EVALUATED ON BOTH SIDES. An owner that exists on one
// side only therefore changes the measured set and shows as a difference; it cannot be
// silently dropped to make a comparison come out clean.
//
// THE ONE EXCLUSION, STATED ONCE.
//
// The protected matrix protects ORDINARY, pre-existing behaviour ("Ordinary Timesheet
// rotation outside the Weekly-Source-managed scope", "Ordinary Unauthorise", "Ordinary
// Weekly non-source Timesheets", "All ordinary receipt/mileage expenses remain completely
// unchanged"). Plan 6.2 places every new owner it creates under the `weekly_source_` or
// `weekly_exceptional_` name family. For an area whose protected subject is the ordinary
// route, a new owner in that family is by definition not a member of the protected set, so
// `excludeNewSourceOwners` removes it - and the adapter REPORTS how many were excluded for
// each area, so the exclusion is visible rather than hidden.
//
// The exclusion is deliberately NOT applied to the four areas whose forbidden change is
// precisely that a new source owner reaches them: PROT-SEC-001 (a new browser-reachable
// RPC), PROT-ADV-001 (a new consumer of the `ADVANCE_THIS_PAYMENT` marker),
// PROT-BANKALERT-001 (Weekly Source notifications stored in Banking alerts) and
// PROT-INFRA-001 (a widened timeout or lease). For those four the new owners are inside the
// measured set and any arrival is a difference.

/** The new-owner name family Plan 6.2 creates. Stated once, used by every area rule. */
export const WEEKLY_SOURCE_NEW_OWNER_PATTERN =
  '^(public|private)\\._?weekly_(source|exceptional)_|^public\\.tsfin_weekly_source_hours_v1$';

/**
 * The 29 controlling protected IDs of `annexes/protected-functionality-matrix.csv`.
 *
 * `protects` and `requiredProof` are the matrix's own words, abbreviated only where the cell
 * is long. The owner rule is what this phase measures. `unmeasurable` is set only where no
 * installed database object owns the protected behaviour at all; the reason says what the
 * behaviour lives in instead and what would be needed to measure it.
 */
export const WEEKLY_SOURCE_PROTECTED_AREA_RULES = Object.freeze([
  {
    protectionId: 'PROT-WB-001',
    protects: 'Workbench calculation and Ready/Action Required/Blocked ownership',
    requiredProof: 'Ordinary baseline plus the six Workbench money cases',
    routineRegex: '^(public|private)\\._?pay_workbench|^public\\._pay_timesheet_rotation_scope$',
    relationRegex: '^public\\.pay_workbench',
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-DRAFT-001',
    protects: 'Create Draft, allocations, reservations and frozen evidence',
    requiredProof: 'Create/cancel/recreate and concurrency differential',
    routineRegex: '^(public|private)\\._?pay_(batch|create_draft|build_batch_artifacts|preview|prepare_draft|set_paye|settle_rail)',
    relationRegex: '^public\\.pay_batch',
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-BP-001',
    protects: 'Banking Pay UI, Case/Resolution, execution, provider and settlement',
    requiredProof: 'Every section/action and no-provider fixture',
    routineRegex: '^(public|private)\\._?(banking_pay|pay_execute|pay_payment|pay_finance|pay_operation|pay_settle|pay_rail|pay_bank)',
    relationRegex: '^public\\.(banking_pay|pay_payment|pay_settlement|pay_rail)',
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-CANCEL-001',
    protects: 'Existing Draft/payment cancellation and rebuild',
    requiredProof: 'Cancel/reconcile/rebuild/repeat differential',
    routineRegex: '^(public|private)\\._?pay_[a-z_]*(cancel|correction|abort)',
    relationRegex: null,
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-REM-001',
    protects: 'PAYE payout notice and Umbrella remittance',
    requiredProof: 'Positive/recovery/channel advice golden output',
    routineRegex: '^(public|private)\\._?pay_[a-z_]*(remittance|payout_notice)',
    relationRegex: null,
    excludeNewSourceOwners: true,
    partial: 'This measures the installed notice and remittance builders. The rendered '
      + 'document golden is the service phase.',
  },
  {
    protectionId: 'PROT-BANKALERT-001',
    protects: 'Existing Banking alert ledger/badge/popover',
    requiredProof: 'Store/API/UI byte and count differential',
    routineRegex: '^(public|private)\\._?banking_alert',
    relationRegex: '^public\\.banking_alert',
    excludeNewSourceOwners: false,
  },
  {
    protectionId: 'PROT-INV-001',
    protects: 'Source-only self-bill invoice authority',
    requiredProof: 'Invoice set equality across every protected/query/pay state',
    routineRegex: '^(public|private)\\._?invoice_(batch_generate|batch_issue|autoinvoice|reference_rows)|^private\\._invoice_batch_generate_classification',
    relationRegex: '^public\\.invoice_lines$',
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-INVDOC-001',
    protects: 'Invoice detail, PDF and export totals',
    requiredProof: 'Exact pence golden documents equal to the self-bill and report number',
    routineRegex: '^public\\.(invoice_issue_one|invoice_detail_get|invoice_reference_rows)$|^private\\._invoice_(document|delivery|dispatch)',
    relationRegex: '^public\\.invoice_lines$',
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-ISSUED-001',
    protects: 'Issued invoice immutability',
    requiredProof: 'Issued correction and audit-history differential',
    routineRegex: '^public\\.(invoice_apply_edits|invoice_unissue_one|invoice_unissue_batch)$|^public\\._ctms_assert_invoice_can_unissue',
    relationRegex: '^public\\.invoices$',
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-INVMOVE-001',
    protects: 'Existing guarded unissue/reissue and segment placement',
    requiredProof: 'Unissue/move/reissue plus Draft-to-Draft fixtures',
    routineRegex: '^(public|private)\\._?(invoice_unissue|invoice_operation)|^public\\._ctms_assert_invoice_can_unissue',
    relationRegex: null,
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-ORDINV-001',
    protects: 'Ordinary evidence-required invoicing/consolidation',
    requiredProof: 'Existing consolidation and whole-week evidence suite',
    routineRegex: '^(public|private)\\._?(invoice_batch|invoice_operation|invoice_generation|invoice_issue|invoice_outbox)',
    relationRegex: null,
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-DAILY-001',
    protects: 'Daily Timesheets and every Daily rate/pay/invoice path',
    requiredProof: 'API/database/UI byte-equivalence',
    routineRegex: '(\\.|_)daily(_|$)',
    relationRegex: null,
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-ORDW-001',
    protects: 'Ordinary Weekly non-source Timesheets',
    requiredProof: 'Full ordinary Weekly workflow differential',
    routineRegex: '^(public|private)\\._?(contract_week|hr_weekly|nhsp_weekly|weekly_import|timesheet_weekly|wkimp)',
    relationRegex: '^public\\.contract_weeks$',
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-PAY-001',
    protects: 'Existing Timesheet authorisation and ordinary pay-eligibility rules',
    requiredProof: 'Ordinary authorised/unauthorised, paid/unpaid and invoice-locked differential',
    routineRegex: '^(public|private)\\._?(timesheet_authorise|timesheet_pay_state|timesheet_payment_override|tsfin_)',
    relationRegex: '^public\\.timesheets_financials$',
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-ADV-001',
    protects: 'Existing ADVANCE_THIS_PAYMENT timing override',
    requiredProof: 'Marker absent/present differential',
    routineRegex: null,
    definitionContains: 'ADVANCE_THIS_PAYMENT',
    relationRegex: null,
    excludeNewSourceOwners: false,
  },
  {
    protectionId: 'PROT-EXP-001',
    protects: 'Ordinary receipt/mileage expense route',
    requiredProof: 'Existing expense suite plus route-isolation tests',
    routineRegex: '^(public|private)\\.[a-z_]*expense',
    relationRegex: null,
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-CONTRACT-001',
    protects: 'Existing Contract creation and valid selection',
    requiredProof: 'Zero/one/many plus ordinary Contract workflow',
    routineRegex: '(\\.|_)contracts?(_|$)',
    relationRegex: '^public\\.contracts$',
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-RATE-001',
    protects: 'Existing rate engine outputs and stored legacy segment shape',
    requiredProof: 'Golden byte-equivalence and rounding boundaries',
    // Word-boundary rule. A loose `[a-z_]*rate` matched "generate" and "federate" and put
    // three invoice-batch routines inside the rate engine's protected set; corrected here.
    routineRegex: '(\\.|_)rates?(_|$)',
    relationRegex: '^public\\.rates?(_|$)',
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-APP-001',
    protects: 'Existing MyTMS layout and navigation',
    requiredProof: 'Visual/interaction golden tests',
    unmeasurable: 'No installed database object owns MyTMS layout or navigation. The owner is '
      + 'TEST-Frontend/js and TEST-Frontend/css in the separate frontend repository, which is '
      + 'not present in this worktree. Measuring it needs a rendered browser golden of the '
      + 'baseline frontend and of the changed frontend, which is the browser phase.',
  },
  {
    protectionId: 'PROT-SUMMARY-001',
    protects: 'Existing Timesheet Summary modal behavior',
    requiredProof: 'Screenshot and interaction differential',
    routineRegex: '^(public|private)\\._?[a-z_]*timesheet_summary',
    relationRegex: null,
    excludeNewSourceOwners: true,
    partial: 'The modal itself is frontend. This measures only the installed read and refresh '
      + 'owners the modal calls; the screenshot and interaction differential is the browser phase.',
  },
  {
    protectionId: 'PROT-EXPORT-001',
    protects: 'Existing reports and exports',
    requiredProof: 'Header/row/order/golden-file differential',
    routineRegex: '(\\.|_)(report|export|csv)(s|ed)?(_|$)',
    relationRegex: null,
    excludeNewSourceOwners: true,
    partial: 'This measures the installed report composers, their declared result type and '
      + 'their definition. The rendered CSV or XLSX golden file is the service phase.',
  },
  {
    protectionId: 'PROT-SETTINGS-001',
    protects: 'Existing settings layout and behavior',
    requiredProof: 'Four toggle combinations and ordinary settings differential',
    routineRegex: '^(public|private)\\._?[a-z_]*settings',
    relationRegex: '^public\\.(client_settings|settings_|contract_settings)',
    excludeNewSourceOwners: true,
    partial: 'This measures the installed settings owners and settings relations. The rendered '
      + 'settings layout is the browser phase.',
  },
  {
    protectionId: 'PROT-NOTIFY-001',
    protects: 'Existing notification delivery semantics',
    requiredProof: 'Boundary-time/replay and unrelated notification differential',
    routineRegex: '^(public|private)\\._?[a-z_]*(outbox|notification|email_delivery)',
    relationRegex: '^public\\.([a-z_]*outbox|notifications)',
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-SEC-001',
    protects: 'Existing tenant/security boundaries',
    requiredProof: 'Catalog/grant/API negative matrix',
    browserReachable: true,
    excludeNewSourceOwners: false,
  },
  {
    protectionId: 'PROT-AUDIT-001',
    protects: 'Existing immutable financial/source history',
    requiredProof: 'Replay/concurrency/hash and legacy census',
    routineRegex: '(\\.|_)audit(_|$)',
    relationRegex: '^public\\.audit_events$',
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-LEGACY-001',
    protects: 'Existing legacy Timesheets and invoices',
    requiredProof: 'Legacy snapshot before/after equality',
    routineRegex: '^(public|private)\\.[a-z_]*legacy',
    relationRegex: '^public\\.[a-z_]*legacy',
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-INFRA-001',
    protects: 'Existing production timing/resource limits',
    requiredProof: 'Configuration diff plus PostgreSQL-major refusal',
    configurationSurface: true,
    excludeNewSourceOwners: false,
    partial: 'This measures the installed configuration diff only: every routine that carries '
      + 'a timeout, lock or lease setting, with that setting, and every per-database and '
      + 'per-role setting. Worker budgets and provider timeouts are Worker configuration, not '
      + 'database configuration, and are not measured here.',
  },
  {
    protectionId: 'PROT-ROTATION-001',
    protects: 'Ordinary Timesheet rotation outside the Weekly-Source-managed scope',
    requiredProof: 'ROT-010 before/after differential through every entry point E1-E11',
    requiredDifferential: 'ROT-010',
    // The matrix row enumerates this area's entry points itself: "route conversion, candidate
    // rejection and resubmission, QR refuse and restore, manual upsert, bulk decisions, TSFIN
    // current-row writes, paid-uninvoiced rollover, import review refresh, withdrawal and
    // receipt resets, expense edit shell". The rule is that enumeration, plus the rotation
    // scope resolver and the Timesheet delete-apply owners the same guard reaches. A narrower
    // name rule left six guarded owners inside no protected area at all.
    routineRegex: '(\\.|_)timesheet_(route|qr|rotation)|rotation_scope|reject_rotate'
      + '|contract_week_manual_(upsert|unprocess)|bulk_process'
      + '|tsfin_(prepare_write|mark_revoked|write_)'
      + '|paid_uninvoiced_rollover|import_review_(refresh|create|action_catalog)'
      + '|(withdrawal|receipt|submission)_reset|expense_payment_edit_shell'
      + '|delete_apply',
    relationRegex: '^public\\.timesheets$',
    excludeNewSourceOwners: true,
  },
  {
    protectionId: 'PROT-UNAUTH-001',
    protects: 'Ordinary Unauthorise for non-import Timesheets and the Banking Pay cancellation process',
    requiredProof: 'UNA-012 before/after differential; owner definition hashes unchanged',
    requiredDifferential: 'UNA-012',
    routineRegex: '^public\\.(timesheet_unauthorise_atomic|timesheet_unauthorise_bulk_atomic|timesheet_authorise_generic_atomic)$',
    relationRegex: '^public\\.timesheets_financials$',
    excludeNewSourceOwners: true,
  },
]);

export const WEEKLY_SOURCE_PROTECTED_AREA_COUNT = WEEKLY_SOURCE_PROTECTED_AREA_RULES.length;
