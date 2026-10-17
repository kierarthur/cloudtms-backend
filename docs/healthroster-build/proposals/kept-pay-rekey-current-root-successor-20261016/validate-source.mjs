import assert from'node:assert/strict';
export function validateContext(source){
 const s=source.replaceAll('\r\n','\n'),at=s.indexOf('  select audit.* into v_ack from public.audit_events audit');assert(at>=0);
 const query=s.slice(at,s.indexOf(';',at));
 assert(!query.includes('v_reason'),'CURRENT_REASON_HISTORY_FILTER');assert(!/facts_fingerprint|v_facts/.test(query),'CURRENT_HASH_HISTORY_FILTER');
 assert(query.includes("audit.object_id_text=v_event.id::text"));assert(query.includes("audit.action='WEEKLY_SOURCE_KEEP_AS_PAID_ACKNOWLEDGED'"));
 assert(query.includes("order by (audit.after_json->>'acknowledgement_sequence')::bigint desc,audit.id desc limit 1"));
 assert(at<s.indexOf('  if v_reason is not null then'),'LATEST_ACK_MUST_PRECEDE_CURRENT_REASON');
 assert(s.includes('select * into strict v_decision from public.weekly_source_kept_as_paid_decisions'));
 assert(s.includes('v_decision.reason=v_reason'));assert(s.includes('v_decision.facts_fingerprint=v_facts'));
 assert(s.includes("'acknowledged_at_utc',v_ack.after_json->>'acknowledged_at_utc'"));
}
export function validateWriter(source){
 const s=source.replaceAll('\r\n','\n'),replays=[...s.matchAll(/select audit\.\* into v_audit from public\.audit_events audit/g)].map(m=>m.index);
 assert.equal(replays.length,2);let previous=0;
 for(const at of replays){const prefix=s.slice(previous,at);assert(prefix.includes('perform private.weekly_source_kept_as_paid_read_admit_v1(v_event_id,v_actor);'),'FRESH_READ_BEFORE_EVERY_REPLAY');previous=at+20;}
 assert(s.indexOf('weekly_source_lock_and_resolve_families_v1')<s.indexOf('weekly-source-kept-as-paid-request:'));
 assert(s.indexOf('weekly-source-kept-as-paid-event:')<s.indexOf('max((audit.after_json'));
 assert(!/(?:update|delete\s+from)\s+public\.weekly_source_kept_as_paid_decisions/i.test(s));
 assert(s.includes("'WEEKLY_SOURCE_KEEP_AS_PAID_ACKNOWLEDGED',null,v_audit_payload"));
 assert(s.includes('on conflict do nothing'));assert(s.includes("'pay_changed',false,'invoice_changed',false"));
}
export function validateCredit(apply,preflight,invoice){
 const s=apply.replaceAll('\r\n','\n'),p=preflight.replaceAll('\r\n','\n'),i=invoice.replaceAll('\r\n','\n');
 const census=s.indexOf("for v_credit_context in select value from jsonb_array_elements(v_credit_frame->'native_contexts')"),firstDelete=s.indexOf('delete from public.invoice_lines'),firstInsert=s.indexOf('insert into public.invoice_lines');
 assert(census>=0&&census<firstDelete&&census<firstInsert,'FULL_CENSUS_BEFORE_OWN_DML');
 assert(!s.slice(s.indexOf('for item in')).includes('weekly_source_kept_as_paid_facts_v1'),'NO_PER_ITEM_SELF_CENSUS_RECHECK');
 assert(s.includes('select saved into strict v_credit_context'));
 for(const x of[s,i])assert(x.includes("removed.meta_json ? 'credited_work_event_id'"),'REMOVAL_ONLY_FRAME_REQUIRED');
 assert(p.includes('select unnest(v_add_ids) target'));assert(p.includes("removed.meta_json->>'credited_work_event_id'"));
 assert(p.indexOf('weekly_source_lock_and_resolve_families_v1')<p.indexOf('where id=v_group for update'));
 assert(i.indexOf('weekly_source_invoice_credited_targets_admit_v1')<i.indexOf('perform public._ctms_assert_invoice_mutable_draft_v1('));
 assert(i.includes('private.invoice_contract_move_selected_financial_authorise_v1('),'CURRENT_V6_FINANCIAL_UNLOCK_GUARD_REQUIRED');
}
