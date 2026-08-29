import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repeatableDir = path.join(root, 'supabase', 'repeatable');
const outputPath = path.join(
  repeatableDir,
  '29082026_0326_banking_pay_release_authority_repair_v1.sql',
);

const targets = [
  {
    name: 'bulk_authorise_dataset_v1',
    identity: 'public.bulk_authorise_dataset_v1(jsonb)',
    source: '14082026_1310_timesheet_processing_status_and_authorise_authority_v1.sql',
  },
  {
    name: 'bulk_process_dataset_v1',
    identity: 'public.bulk_process_dataset_v1(jsonb)',
    source: '07082026_2224_candidate_app_weekly_office_replacements_v1.sql',
  },
  {
    name: 'bulk_timesheet_row_patch_v1',
    identity: 'public.bulk_timesheet_row_patch_v1(jsonb)',
    source: '07082026_2224_candidate_app_weekly_office_replacements_v1.sql',
  },
  {
    name: 'contract_week_manual_upsert_atomic',
    identity: 'public.contract_week_manual_upsert_atomic(uuid,uuid,jsonb,jsonb,jsonb,jsonb,jsonb,uuid,boolean,timestamptz,text,jsonb)',
    source: '27082026_2205_candidate_weekly_manager_finalisation_authority_v1.sql',
  },
  {
    name: 'pay_workbench_mark_finance_case_dirty',
    identity: 'public.pay_workbench_mark_finance_case_dirty()',
    source: '04082026_1219_pay_workbench_mark_finance_case_dirty.sql',
  },
  {
    name: 'timesheet_daily_manual_process_atomic',
    identity: 'public.timesheet_daily_manual_process_atomic(uuid,uuid,uuid,jsonb,jsonb,timestamptz,text)',
    source: '07082026_2224_candidate_app_weekly_office_replacements_v1.sql',
  },
];

const bankingV2PublicServiceIdentities = [
  'public.pay_workbench_revalidate_zero_retained_recovery_headroom_v1(uuid,uuid,jsonb)',
  'public.pay_workbench_session_get_action_required_detail_v1(uuid,jsonb,uuid,text,text,integer)',
  'public.pay_workbench_session_get_action_required_page_v1(uuid,jsonb,uuid,text,text,text,integer,text,text)',
  'public.pay_workbench_session_get_blocked_detail_v1(uuid,jsonb,uuid,text,text,integer)',
  'public.pay_workbench_session_get_blocked_page_v1(uuid,jsonb,uuid,text,text,text,integer,text)',
  'public.pay_workbench_session_get_candidate_ready_page_v1(uuid,uuid,jsonb,uuid,text,integer)',
  'public.pay_workbench_session_get_candidate_summary_page_v1(uuid,jsonb,uuid,text,text,text,integer)',
  'public.pay_workbench_session_get_selected_ready_timesheets_v1(uuid,uuid,jsonb,uuid,text)',
  'public.pay_workbench_session_set_candidate_ready_selection_v1(uuid,uuid,jsonb,uuid,text,uuid,text,jsonb)',
  'public.pay_workbench_session_set_filtered_ready_selection_v1(uuid,jsonb,uuid,text,uuid,text)',
  'public.pay_workbench_session_set_ready_group_v1(uuid,uuid,jsonb,uuid,text,text,boolean,uuid,text,jsonb)',
  'public.pay_workbench_session_set_ready_rows_v1(uuid,uuid,jsonb,uuid,jsonb,boolean,uuid,text,jsonb)',
];

const normalizeLf = (value) => String(value || '').replaceAll('\r\n', '\n');

function extractDefinition(target) {
  const source = normalizeLf(fs.readFileSync(path.join(repeatableDir, target.source), 'utf8'));
  const token = `CREATE OR REPLACE FUNCTION public.${target.name}(`;
  const start = source.indexOf(token);
  if (start < 0) throw new Error(`Missing ${target.name} in ${target.source}`);
  if (source.indexOf(token, start + token.length) >= 0) {
    throw new Error(`Ambiguous ${target.name} in ${target.source}`);
  }
  const terminator = '\n$function$;';
  const end = source.indexOf(terminator, start);
  if (end < 0) throw new Error(`Incomplete ${target.name} in ${target.source}`);
  const definition = source.slice(start, end + terminator.length).trim();
  if (/^SET plpgsql_check\./mi.test(definition)) {
    throw new Error(`Provider-specific plpgsql_check setting in ${target.name}`);
  }
  return definition;
}

const wrapper = `CREATE OR REPLACE FUNCTION public.timesheet_daily_manual_unprocess_atomic(
  p_timesheet_id uuid,
  p_expected_timesheet_id uuid,
  p_actor_user_id uuid DEFAULT NULL::uuid,
  p_now_utc timestamp with time zone DEFAULT now()
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path TO 'public'
AS $function$
BEGIN
  RETURN public.timesheet_daily_manual_unprocess_atomic(
    p_timesheet_id => p_timesheet_id,
    p_expected_timesheet_id => p_expected_timesheet_id,
    p_actor_user_id => p_actor_user_id,
    p_now_utc => p_now_utc,
    p_expected_row_signature => NULL::text
  );
END;
$function$;`;

const blocks = targets.map((target) => `${extractDefinition(target)}

ALTER FUNCTION ${target.identity} OWNER TO postgres;
REVOKE ALL ON FUNCTION ${target.identity} FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION ${target.identity} TO service_role;`);

blocks.push(`${wrapper}

ALTER FUNCTION public.timesheet_daily_manual_unprocess_atomic(uuid,uuid,uuid,timestamptz)
  OWNER TO postgres;
REVOKE ALL ON FUNCTION public.timesheet_daily_manual_unprocess_atomic(uuid,uuid,uuid,timestamptz)
  FROM PUBLIC, anon, authenticated, service_role;
GRANT EXECUTE ON FUNCTION public.timesheet_daily_manual_unprocess_atomic(uuid,uuid,uuid,timestamptz)
  TO service_role;`);

blocks.push(`-- Upgrade-history convergence: interrupted installs can create a
-- public RPC before the later browser-isolation verifier runs. Reassert the
-- exact final service-only ACL for every additive Banking v2 RPC.
REVOKE ALL ON FUNCTION
  ${bankingV2PublicServiceIdentities.join(',\n  ')}
FROM PUBLIC, anon, authenticated, service_role;

GRANT EXECUTE ON FUNCTION
  ${bankingV2PublicServiceIdentities.join(',\n  ')}
TO service_role;`);

const output = `-- Exact provider-neutral authority repair for Banking Pay modal v2.
--
-- The immutable historical compatibility replay was accidentally changed by
-- the first capability-off release. Its partial hosted replay replaced seven
-- current routine bodies before failing. This closure restores only those
-- seven certified definitions and their service-only ACLs. It deliberately
-- contains no broad include, provider setting, economic rewrite or alternate
-- Draft/selection owner.
--
-- Generated from the named current authoritative source files by:
--   node scripts/generate-banking-pay-release-authority-repair.mjs

\\set ON_ERROR_STOP on

begin;

${blocks.join('\n\n')}

NOTIFY pgrst, 'reload schema';

commit;
`;

if (process.argv.includes('--check')) {
  const current = normalizeLf(fs.readFileSync(outputPath, 'utf8'));
  if (current !== output) {
    console.error('Banking Pay authority-repair repeatable is stale. Regenerate it.');
    process.exit(1);
  }
  console.log('Banking Pay authority-repair repeatable is current.');
} else {
  fs.writeFileSync(outputPath, output, 'utf8');
  console.log(`Wrote ${path.relative(root, outputPath).replaceAll('\\', '/')}`);
}
