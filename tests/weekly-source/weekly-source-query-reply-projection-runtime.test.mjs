import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';

test('query replies use actual current-episode responses, not delivery or an older answer', {
  skip: !process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,
}, () => {
  const fixture = readFileSync('supabase/verification/15092026_1534_weekly_source_read_projections_v1.sql', 'utf8');
  const marker = "'three current incidents were not created');";
  assert.equal(fixture.split(marker).length, 2);
  const proof = `
  declare
    target public.weekly_discrepancy_incidents%rowtype;
    publication_id uuid;
    child jsonb;
  begin
    select * into strict target from public.weekly_discrepancy_incidents
      where work_event_id='d9000000-0000-4000-8000-000000000001' and state='OPEN';
    select projection_publication_id into publication_id from public.weekly_issue_comparison_revisions
      where id=target.current_comparison_revision_id;
    update public.weekly_discrepancy_incidents set candidate_action_state='RESPONDED',manager_action_state='RESPONDED'
      where id=target.id;
    insert into public.weekly_discrepancy_events(incident_id,issue_episode,event_kind,actor_kind,
      bounded_payload_json,idempotency_key,occurred_at_utc)
    values(target.id,target.episode_number,'CANDIDATE_RESPONDED','CANDIDATE',
      '{"choice":"CANDIDATE_CORRECT"}','reply-projection-candidate','2026-09-03T10:00:00Z'),
      (target.id,target.episode_number,'MANAGER_RESPONDED','MANAGER',
      '{"response_kind":"SYSTEM_CORRECT"}','reply-projection-manager','2026-09-03T11:00:00Z');
    select item into strict child from private.weekly_source_office_query_groups_v1(target.source_cycle_id,publication_id,'{}') groups
      cross join lateral jsonb_array_elements(groups.children) item where item->>'incident_id'=target.id::text;
    perform pg_temp.assert_true(child->>'candidate_response'='My hours are correct','candidate actual choice missing');
    perform pg_temp.assert_true(child->>'candidate_responded_at'='3 Sep 2026, 11:00','candidate reply time missing');
    perform pg_temp.assert_true(child->>'manager_response'='System hours are correct','manager actual choice missing');
    perform pg_temp.assert_true(child->>'manager_responded_at'='3 Sep 2026, 12:00','manager reply time missing');
    update public.weekly_discrepancy_incidents set episode_number=episode_number+1 where id=target.id;
    select item into strict child from private.weekly_source_office_query_groups_v1(target.source_cycle_id,publication_id,'{}') groups
      cross join lateral jsonb_array_elements(groups.children) item where item->>'incident_id'=target.id::text;
    perform pg_temp.assert_true(child->>'candidate_response' is null and child->>'manager_response' is null,
      'prior episode answer leaked into changed question');
    raise exception 'reply projection proof complete' using errcode='ZX001';
  exception when sqlstate 'ZX001' then null;
  end;
`;
  const sql = fixture.replace(marker, marker + proof);
  const result = spawnSync(process.env.CLOUDTMS_TEST_PSQL || 'psql', ['-X','-h','127.0.0.1','-p',
    process.env.CLOUDTMS_PROTECTED_EDITOR_LOCAL_PORT,'-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],
    { input: sql, encoding: 'utf8', env: { ...process.env, PGOPTIONS: '-c jit=off' }, windowsHide: true });
  assert.equal(result.status, 0, result.stderr || result.error?.message);
});
