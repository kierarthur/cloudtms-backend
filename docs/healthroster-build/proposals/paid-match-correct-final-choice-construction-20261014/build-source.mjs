import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
const dir = path.dirname(fileURLToPath(import.meta.url)), root = path.resolve(dir, '../../../..');
const sha = b => createHash('sha256').update(b).digest('hex');
const base = '65c82dcc6d8f2748fef7c4209d96a673f7ec8756';
const native = 'fa66183f01913e86651e82816f5e88f97e07123c';
assert.ok(!fs.existsSync(path.join(dir, 'FROZEN_ARTIFACT_MANIFEST.json')));
const proposal = 'docs/healthroster-build/proposals/';
const legacy = 'paid-match-legacy-native-consumer-construction-20261014';
const writer = 'paid-shift-match-writer-none-construction-20261014';
const sources = [], reverses = [];
for (const name of ['before','input','forward/broker/src/weekly-source']) fs.mkdirSync(path.join(dir,name),{recursive:true});
const legacyManifestRaw = fs.readFileSync(path.join(root,proposal,legacy,'FROZEN_ARTIFACT_MANIFEST.json'));
const legacyManifest = JSON.parse(legacyManifestRaw);
function read(source) {
  const b = fs.readFileSync(path.join(root,source));
  if (source.startsWith(proposal+legacy+'/')) {
    const relative = source.slice((proposal+legacy+'/').length);
    const entry = legacyManifest.artifacts.find(a => a.file===relative);
    assert.ok(entry, relative);assert.equal(entry.sha256,sha(b));assert.equal(entry.bytes,b.length);
  } else assert.deepEqual(b,execFileSync('git',['show',base+':'+source],{cwd:root,maxBuffer:4e6}));
  return b;
}
function compose(source,file,hunks) {
  const b=read(source), before=b.toString(),eol=before.includes('\r\n')?'\r\n':'\n';
  hunks=hunks.map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));
  let after=before;
  for(const h of hunks){assert.equal(after.split(h.before).length,2,h.name);after=after.replace(h.before,()=>h.after);}
  let reverse=after;
  for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,h.name);reverse=reverse.replace(h.after,()=>h.before);}
  assert.equal(reverse,before);
  fs.mkdirSync(path.dirname(path.join(dir,'before',file)),{recursive:true});
  fs.mkdirSync(path.dirname(path.join(dir,'forward',file)),{recursive:true});
  fs.writeFileSync(path.join(dir,'before',file),b);fs.writeFileSync(path.join(dir,'forward',file),after);
  reverses.push({source,file,input_authority:source.startsWith(proposal+legacy+'/')?'FROZEN_UNREVIEWED_LEGACY30':'COMMITTED_65C',
    before_bytes:b.length,before_sha256:sha(b),after_bytes:Buffer.byteLength(after),after_sha256:sha(after),hunks,whole_reverse_verified:true});
}
compose(proposal+legacy+'/forward/05_complete_overlap_inputs.sqlinc','01_complete_overlap_inputs.sqlinc',[
 {name:'actual correction scope before candidate discovery',before:`  if v_upload.report_scope_id is not null or v_upload.purpose<>'ORDINARY'`,after:`  if v_upload.purpose='FINAL_SOURCE_CORRECTION' then
    perform private.weekly_source_paid_match_correction_scope_v1(v_upload.id,p_actor_user_id,false);
  end if;
  if v_upload.report_scope_id is not null or v_upload.purpose not in ('ORDINARY','FINAL_SOURCE_CORRECTION')`},
 {name:'immutable correction identity bound in the same facts domain',before:`    'candidate_id',p_candidate_id,'client_id',p_client_id,'overlaps',v_items);
  return v_result`,after:`    'candidate_id',p_candidate_id,'client_id',p_client_id,'overlaps',v_items);
  if v_upload.purpose='FINAL_SOURCE_CORRECTION' then
    v_result:=v_result||jsonb_build_object('correction_scope',
      private.weekly_source_paid_match_correction_scope_v1(v_upload.id,p_actor_user_id,false));
  end if;
  return v_result`}
]);
compose(proposal+writer+'/forward/04_complete_upload_context.sqlinc','02_complete_upload_context.sqlinc',[
 {name:'same actual blocker in genuine correction publication context',before:`    if v_candidate_id is not null and v_row_client_id is not null and v_upload.purpose='ORDINARY'`,after:`    if v_candidate_id is not null and v_row_client_id is not null and v_upload.purpose in ('ORDINARY','FINAL_SOURCE_CORRECTION')`}
]);
compose(proposal+legacy+'/forward/08_complete_office_question.sqlinc','03_complete_paid_match_question.sqlinc',[
 {name:'real correction replacement pointer instead of ordinary current pointer',before:`  perform private.weekly_source_query_current_publication_v1(v_cycle.id,v_publication.id);`,after:`  if v_upload.purpose='FINAL_SOURCE_CORRECTION' then
    perform private.weekly_source_paid_match_correction_scope_v1(v_upload.id,p_actor_user_id,false);
    if v_publication.state is distinct from 'CORRECTION_READY'
      or v_publication.id is distinct from (select replacement_projection_publication_id
        from public.weekly_final_source_correction_sessions where id=v_upload.correction_session_id) then
      raise exception 'WEEKLY_SOURCE_PAID_MATCH_CORRECTION_SCOPE_STALE' using errcode='40001';end if;
  else
    perform private.weekly_source_query_current_publication_v1(v_cycle.id,v_publication.id);
  end if;`},
 {name:'closed question preserves ordinary availability and exact correction READY scope',before:`    or v_upload.report_scope_id is not null or v_upload.purpose<>'ORDINARY'
    or v_cycle.state<>'OPEN' or v_resolution.candidate_id is null`,after:`    or v_upload.report_scope_id is not null or v_upload.purpose not in ('ORDINARY','FINAL_SOURCE_CORRECTION')
    or (v_upload.purpose='ORDINARY' and v_cycle.state<>'OPEN')
    or (v_upload.purpose='FINAL_SOURCE_CORRECTION' and not exists(select 1
      from public.weekly_final_source_correction_sessions session where session.id=v_upload.correction_session_id
        and session.state in ('READY','REVIEWED') and session.actor_user_id=p_actor_user_id))
    or v_resolution.candidate_id is null`}
]);
compose(proposal+writer+'/forward/02_complete_office_recheck.sqlinc','04_complete_office_recheck.sqlinc',[
 {name:'same owner holds real correction session and rebuild DTO',before:`  v_begin jsonb;v_paid_discovery jsonb;v_paid_choice jsonb;`,after:`  v_begin jsonb;v_paid_discovery jsonb;v_paid_choice jsonb;
  v_correction public.weekly_final_source_correction_sessions%rowtype;v_correct boolean:=false;`},
 {name:'finite correction CAS input fields only',before:`       'pay_effect','paid_overlap_target_work_event_id','expected_paid_overlap_inputs_sha256'))`,after:`       'pay_effect','paid_overlap_target_work_event_id','expected_paid_overlap_inputs_sha256',
       'correction_session_id','expected_correction_session_version','expected_comparison_manifest_hash','expected_issue_set_hash'))`},
 {name:'correction keys cannot bypass ordinary or nonpaid branch',before:`  v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_OFFICE_RECHECK_V1',p_request);
  select * into strict v_upload from public.weekly_source_uploads where id=(p_request->>'upload_id')::uuid;`,after:`  v_hash:=private.weekly_source_sha256_jsonb_v1('WEEKLY_SOURCE_OFFICE_RECHECK_V1',p_request);
  select * into strict v_upload from public.weekly_source_uploads where id=(p_request->>'upload_id')::uuid;
  v_correct:=v_upload.purpose='FINAL_SOURCE_CORRECTION';
  if v_correct then
    if not p_request ?& array['pay_effect','correction_session_id','expected_correction_session_version',
      'expected_comparison_manifest_hash','expected_issue_set_hash']
      or (p_request->>'correction_session_id')::uuid is distinct from v_upload.correction_session_id then
      raise exception 'WEEKLY_SOURCE_PAID_MATCH_CORRECTION_REQUEST_INVALID' using errcode='22023';end if;
    perform private.weekly_source_paid_match_correction_scope_v1(v_upload.id,v_actor,false);
  elsif p_request ?| array['correction_session_id','expected_correction_session_version',
    'expected_comparison_manifest_hash','expected_issue_set_hash'] then
    raise exception 'WEEKLY_SOURCE_RECHECK_REQUEST_INVALID' using errcode='22023';
  end if;`},
 {name:'correction scope locks after I1 and cycle and preserves prior ordinary heads',before:`  if v_upload.report_scope_id is null then
    v_version:=v_cycle.version; v_current_upload:=v_cycle.current_complete_upload_id;`,after:`  if v_correct then
    select * into strict v_correction from public.weekly_final_source_correction_sessions
      where id=v_upload.correction_session_id for update;
    perform private.weekly_source_paid_match_correction_scope_v1(v_upload.id,v_actor,false);
    v_version:=v_cycle.version;v_current_upload:=v_upload.id;
    v_current_publication:=v_correction.replacement_projection_publication_id;
  elsif v_upload.report_scope_id is null then
    v_version:=v_cycle.version; v_current_upload:=v_cycle.current_complete_upload_id;`},
 {name:'ordinary current admission unchanged outside the finite paid correction arm',before:`  if v_upload.purpose<>'ORDINARY' or v_upload.state<>'CURRENT'
    or v_current_upload is distinct from v_upload.id or v_cycle.state in ('FINALISING','FINALISED')
    or v_scope.state in ('FINALISING','FINALISED') then`,after:`  if not v_correct and (v_upload.purpose<>'ORDINARY' or v_upload.state<>'CURRENT'
    or v_current_upload is distinct from v_upload.id or v_cycle.state in ('FINALISING','FINALISED')
    or v_scope.state in ('FINALISING','FINALISED')) then`},
 {name:'exact journal replay includes genuine rebuilt correction result',before:`    if v_publication.authority_scope_version<>v_version or v_publication.state not in ('BUILDING','CURRENT') then`,after:`    if v_publication.authority_scope_version<>v_version
      or (not v_correct and v_publication.state not in ('BUILDING','CURRENT'))
      or (v_correct and (v_publication.state not in ('BUILDING','CORRECTION_READY')
        or v_publication.correction_session_id is distinct from v_correction.id
        or (v_publication.state='CORRECTION_READY' and v_correction.replacement_projection_publication_id is distinct from v_publication.id))) then`},
 {name:'replay returns real session scope without fabricating current publication',before:`      'publication_id',v_publication.id,'upload_id',v_upload.id,'authority_scope_version',v_version,'idempotent',true);`,after:`      'publication_id',v_publication.id,'upload_id',v_upload.id,'authority_scope_version',v_version,'idempotent',true)
      ||case when not v_correct then '{}'::jsonb else jsonb_build_object('purpose','FINAL_SOURCE_CORRECTION',
        'correction_session_id',v_correction.id,'ready_session_version',v_publication.ready_session_version) end;`},
 {name:'fresh confirmation binds full actual session and native publication hashes',before:`  if v_version is distinct from (p_request->>'expected_authority_scope_version')::bigint`,after:`  if v_correct then
    perform private.weekly_source_paid_match_correction_scope_v1(v_upload.id,v_actor,true);
    if v_correction.version is distinct from (p_request->>'expected_correction_session_version')::bigint then
      raise exception 'WEEKLY_SOURCE_PAID_MATCH_CORRECTION_SCOPE_STALE' using errcode='40001';end if;
  end if;
  if v_version is distinct from (p_request->>'expected_authority_scope_version')::bigint`},
 {name:'replacement CORRECTION_READY admitted only with actual locked session',before:`  if v_publication.state<>'CURRENT' or v_publication.upload_id<>v_upload.id then`,after:`  if v_publication.upload_id<>v_upload.id
    or (not v_correct and v_publication.state<>'CURRENT')
    or (v_correct and (v_publication.state<>'CORRECTION_READY'
      or v_publication.correction_session_id is distinct from v_correction.id
      or v_publication.comparison_manifest_hash is distinct from private.weekly_source_hex32_v1(
        p_request->>'expected_comparison_manifest_hash','WEEKLY_SOURCE_COMPARISON_MANIFEST_HASH_INVALID')
      or v_publication.issue_set_hash is distinct from private.weekly_source_hex32_v1(
        p_request->>'expected_issue_set_hash','WEEKLY_SOURCE_ISSUE_SET_HASH_INVALID'))) then`},
 {name:'use original native correction rebuild owner before ordinary cycle mutation',before:`  update public.weekly_source_projection_publications set state='STALE' where id=v_publication.id;`,after:`  if v_correct then
    -- Existing native rebuild derives its own next generation, owns session
    -- CAS/review invalidation, and leaves the actual CURRENT cycle heads alone.
    v_begin:=public.weekly_source_projection_begin_atomic_v1(jsonb_build_object(
      'actor_user_id',v_actor,'upload_id',v_upload.id,'expected_authority_scope_version',v_version,
      'correction_session_id',v_correction.id,'expected_correction_session_version',v_correction.version,
      'expected_projection_publication_id',v_publication.id,
      'expected_row_manifest_hash',encode(v_upload.row_manifest_hash,'hex'),
      'expected_comparison_manifest_hash',encode(v_publication.comparison_manifest_hash,'hex'),
      'expected_issue_set_hash',encode(v_publication.issue_set_hash,'hex'),
      'rebuild_idempotency_key',v_request_id::text));
    if v_begin->>'status' is distinct from 'BUILDING'
      or v_begin->>'publication_id' is not distinct from v_publication.id::text then
      raise exception 'WEEKLY_SOURCE_RECHECK_BEGIN_FAILED' using errcode='55000';end if;
    insert into private.weekly_source_office_rechecks(request_id,actor_user_id,upload_id,prior_publication_id,publication_id,request_hash,request_json)
      values(v_request_id,v_actor,v_upload.id,v_publication.id,(v_begin->>'publication_id')::uuid,v_hash,p_request);
    return v_begin||jsonb_build_object('purpose','FINAL_SOURCE_CORRECTION','correction_session_id',v_correction.id);
  end if;
  update public.weekly_source_projection_publications set state='STALE' where id=v_publication.id;`}
]);
compose(proposal+writer+'/forward/broker/src/weekly-source/upload-publication-owner.mjs','broker/src/weekly-source/upload-publication-owner.mjs',[
 {name:'literal native correction status only when returned owner scope binds request',before:`      if (!begun?.ok || !['BUILDING', 'CURRENT'].includes(begun.status)) {`,after:`      const correction = begun?.purpose === 'FINAL_SOURCE_CORRECTION';
      if (correction !== Object.hasOwn(request,'correction_session_id')) {
        fail('WEEKLY_SOURCE_CORRECTION_REBUILD_IDENTITY_MISMATCH', 'The saved source purpose changed.', 409);
      }
      if (correction && (begun.correction_session_id !== request.correction_session_id
          || begun.upload_id !== request.upload_id)) {
        fail('WEEKLY_SOURCE_CORRECTION_REBUILD_IDENTITY_MISMATCH', 'The saved correction scope changed.', 409);
      }
      if (!begun?.ok || !(correction ? ['BUILDING', 'CORRECTION_READY'] : ['BUILDING', 'CURRENT']).includes(begun.status)) {`},
 {name:'exact native completed correction replay',before:`      if (begun.status === 'CURRENT') return begun;`,after:`      if (begun.status === 'CURRENT' || (correction && begun.status === 'CORRECTION_READY')) return begun;`},
 {name:'context uses actual purpose and session derived by native owner',before:`          ...scopeRequest({}, request.actor_user_id, 'BUILD_PROJECTION', begun.upload_id),`,after:`          ...scopeRequest(correction ? { purpose: 'FINAL_SOURCE_CORRECTION',
            correction_session_id: begun.correction_session_id } : {}, request.actor_user_id, 'BUILD_PROJECTION', begun.upload_id),`},
 {name:'no CURRENT relabel of genuinely inactive replacement comparison',before:`        if (!result?.ok || result.status !== 'CURRENT') {`,after:`        if (!result?.ok || result.status !== (correction ? 'CORRECTION_READY' : 'CURRENT')) {`}
]);
const pins=[];
for(const file of [
 'supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_projection_begin.sqlinc',
 'supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_projection_publish.sqlinc',
 'supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/10_complete_source_readers_correction.sqlinc',
 'supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql'
]) {
 const b=execFileSync('git',['show',native+':'+file],{cwd:root,maxBuffer:4e6});
 const saved='input/'+path.basename(file);fs.writeFileSync(path.join(dir,saved),b);
 pins.push({commit:native,file,saved,bytes:b.length,sha256:sha(b)});
}
fs.writeFileSync(path.join(dir,'COMPLETE_CURRENT_REVERSES.json'),JSON.stringify({source_only:true,base,native,
 legacy_unpublished_manifest_sha256:sha(legacyManifestRaw),legacy_independent_review:'PENDING',reverses,native_inputs:pins},null,2)+'\n');
console.log(JSON.stringify({whole_owners:reverses.length,unique_hunks:reverses.reduce((n,r)=>n+r.hunks.length,0),native_pins:pins.length,source_only:true}));

// The additional consumers use the same complete files, not a second owner.
function amend(file,hunks){
 const entry=reverses.find(r=>r.file===file);assert.ok(entry,file);
 const original=fs.readFileSync(path.join(dir,'before',file)).toString();
 let after=fs.readFileSync(path.join(dir,'forward',file)).toString();
 const eol=after.includes('\r\n')?'\r\n':'\n';
 const normalized=hunks.map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));
 for(const h of normalized){assert.equal(after.split(h.before).length,2,h.name);after=after.replace(h.before,()=>h.after);}
 entry.hunks.push(...normalized);let reverse=after;
 for(const h of [...entry.hunks].reverse()){assert.equal(reverse.split(h.after).length,2,h.name);reverse=reverse.replace(h.after,()=>h.before);}
 assert.equal(reverse,original);entry.after_bytes=Buffer.byteLength(after);entry.after_sha256=sha(after);
 fs.writeFileSync(path.join(dir,'forward',file),after);
}
const reviewFrame=`jsonb_build_object('schema_version','WEEKLY_SOURCE_PAID_MATCH_CORRECTION_RECHECK_CONTEXT_V1',
          'correction_session_id',v_correction.id,'source_cycle_id',v_cycle.id,
          'expected_current_final_revision_id',v_correction.expected_current_final_revision_id,
          'expected_final_manifest_hash',encode(v_correction.expected_final_manifest_hash,'hex'))`;
amend('04_complete_office_recheck.sqlinc',[
 {name:'actual source identity for genuine same owner review after saved replay',before:`        'correction_session_id',v_correction.id,'ready_session_version',v_publication.ready_session_version) end;`,after:`        'correction_session_id',v_correction.id,'ready_session_version',v_publication.ready_session_version,
        'comparison_manifest_hash',encode(v_publication.comparison_manifest_hash,'hex'),
        'issue_set_hash',encode(v_publication.issue_set_hash,'hex'),
        'correction_review_identity',${reviewFrame}) end;`},
 {name:'actual source identity for genuine same owner review after fresh native publish',before:`    return v_begin||jsonb_build_object('purpose','FINAL_SOURCE_CORRECTION','correction_session_id',v_correction.id);`,after:`    return v_begin||jsonb_build_object('purpose','FINAL_SOURCE_CORRECTION','correction_session_id',v_correction.id,
      'correction_review_identity',${reviewFrame});`}
]);
amend('broker/src/weekly-source/upload-publication-owner.mjs',[
 {name:'sealed native context is the same real correction scope',before:`        const reportRecheckRowsReceipt = await rpc(dependencies, 'weekly_source_projection_rows_apply_atomic_v1', {`,after:`        if (correction && (context.upload_id !== begun.upload_id
          || context.correction_session_id !== begun.correction_session_id
          || context.source_cycle_id !== begun.correction_review_identity?.source_cycle_id)) {
          fail('WEEKLY_SOURCE_CORRECTION_REBUILD_IDENTITY_MISMATCH','The saved correction context changed.',409);
        }
        const reportRecheckRowsReceipt = await rpc(dependencies, 'weekly_source_projection_rows_apply_atomic_v1', {`},
 {name:'literal finite review import',before:`import { REPORT_PROFILE,`,after:`import { reviewWeeklyCorrectFinalPaidMatchRecheck } from './correct-final-source-orchestrator.mjs';
import { REPORT_PROFILE,`},
 {name:'complete native saved-recheck replay recovers original review',before:`      if (begun.status === 'CURRENT' || (correction && begun.status === 'CORRECTION_READY')) return begun;`,after:`      if (begun.status === 'CURRENT') return begun;
      if (correction && begun.status === 'CORRECTION_READY') return {
        ...begun, correction_review: await reviewWeeklyCorrectFinalPaidMatchRecheck(dependencies,request,begun,begun),
      };`},
 {name:'complete native fresh correction review only after actual publish',before:`        return result;
      } catch {
        // Begin is a separate committed transaction.`,after:`        return correction ? { ...result,
          correction_review: await reviewWeeklyCorrectFinalPaidMatchRecheck(dependencies,request,begun,result),
        } : result;
      } catch {
        // Begin is a separate committed transaction.`}
]);
function nativeOwner(file,signature,output,hunks){
 const raw=execFileSync('git',['show',native+':'+file],{cwd:root,maxBuffer:4e6}),text=raw.toString();
 const marker='create or replace function '+signature;
 assert.equal(text.split(marker).length,2);const start=text.indexOf(marker),end=text.indexOf('$function$;',start)+'$function$;'.length;
 assert.ok(end>start);const b=Buffer.from(text.slice(start,end)+'\n');
 const source='NATIVE_GIT:'+file+':'+signature;
 let after=b.toString(),normalized=hunks.map(h=>({...h,before:h.before.replaceAll('\n',after.includes('\r\n')?'\r\n':'\n'),after:h.after.replaceAll('\n',after.includes('\r\n')?'\r\n':'\n')}));
 for(const h of normalized){assert.equal(after.split(h.before).length,2,h.name);after=after.replace(h.before,()=>h.after);}
 let reverse=after;for(const h of [...normalized].reverse()){assert.equal(reverse.split(h.after).length,2,h.name);reverse=reverse.replace(h.after,()=>h.before);}assert.equal(reverse,b.toString());
 fs.writeFileSync(path.join(dir,'before',output),b);fs.writeFileSync(path.join(dir,'forward',output),after);
 reverses.push({source,file:output,input_authority:native,before_bytes:b.length,before_sha256:sha(b),after_bytes:Buffer.byteLength(after),after_sha256:sha(after),hunks:normalized,whole_reverse_verified:true});
 sources.push({commit:native,file,signature,whole_input_bytes:raw.length,whole_input_sha256:sha(raw),definition_sha256:sha(b)});
}
nativeOwner('supabase/repeatable/13102026_1700_weekly_source_invoice_window_receipt/10c_complete_root_report_consumer_union.sqlinc',
 'private.weekly_source_correct_final_office_preview_v1(', '05_complete_correct_final_office_preview.sqlinc',[
 {name:'existing preview owns one paid question set',before:`  v_preview jsonb;
begin`,after:`  v_preview jsonb;v_paid_questions jsonb:='[]'::jsonb;
begin`},
 {name:'genuine replacement rows and existing native question producer only',before:`  if pg_catalog.jsonb_array_length(v_blockers)=0 then`,after:`  if v_session.authority_scope_kind='CYCLE' and v_session.state in ('READY','REVIEWED')
    and exists(select 1 from public.weekly_source_cycles cycle join public.weekly_source_groups source_group
      on source_group.id=cycle.source_group_id and source_group.source_family='ROSTER'
      where cycle.id=v_session.source_cycle_id) then
    select coalesce(jsonb_agg(jsonb_build_object(
      'candidate',coalesce(candidate.display_name,source_row.source_candidate_identity,'Candidate'),
      'client',coalesce(client.name,'Client'),'shift',to_char(source_row.work_date,'Dy FMDD Mon YYYY'),
      'system_hours',to_char(source_row.start_at_local,'HH24:MI')||'-'||to_char(source_row.end_at_local,'HH24:MI')
        ||case when source_row.break_minutes is null then ' (break not recorded)'
          else ' ('||source_row.break_minutes::text||' min break)' end,
      'paid_match_question',question.value) order by source_row.source_row_ordinal,source_row.id),'[]'::jsonb)
    into v_paid_questions
    from public.weekly_source_upload_rows source_row
    join public.weekly_source_row_resolutions resolution on resolution.upload_row_id=source_row.id
      and resolution.generation=coalesce(v_publication.projection_generation,v_publication.authority_scope_version::integer)
    left join public.candidates candidate on candidate.id=resolution.candidate_id
    left join public.clients client on client.id=resolution.client_id
    cross join lateral(select private.weekly_source_paid_match_question_v1(
      v_publication.id,source_row.id,v_session.actor_user_id) value) question
    where source_row.upload_id=v_session.replacement_correction_upload_id
      and resolution.blocker_code='PAID_SHIFT_OVERLAP_CONFIRMATION_REQUIRED'
      and question.value->>'state'='READY';
  end if;
  if pg_catalog.jsonb_array_length(v_blockers)=0 then`},
 {name:'native preview SHA binds genuine questions without changing empty preview',before:`  return v_preview||pg_catalog.jsonb_build_object(
    'preview_hash'`,after:`  if jsonb_array_length(v_paid_questions)>0 then
    v_preview:=v_preview||jsonb_build_object('paid_match_questions',v_paid_questions);
  end if;
  return v_preview||pg_catalog.jsonb_build_object(
    'preview_hash'`}
]);
const orchestratorPath='broker/src/weekly-source/correct-final-source-orchestrator.mjs';
const orchestratorRaw=execFileSync('git',['show',native+':'+orchestratorPath],{cwd:root,maxBuffer:2e6});
const orchestrator=orchestratorRaw.toString();
const old=`    changes: Object.freeze(preview.changes.map((row) => plainPreviewRow(
      row,
      ['candidate', 'day_date', 'current_final', 'replacement', 'result'],
      'Correction change',
    ))),`;
const after=`    changes: Object.freeze(preview.changes.map((row) => {
      const fields=['candidate','day_date','current_final','replacement','result',
        'current_final_amount','replacement_amount','amount_changed'];
      if (!row || typeof row!=='object' || Array.isArray(row)
        || !sameJson(Object.keys(row).sort(),fields.sort()) || typeof row.amount_changed!=='boolean'
        || fields.filter(key=>key!=='amount_changed').some(key=>typeof row[key]!=='string')) {
        fail('WEEKLY_SOURCE_CORRECTION_PREVIEW_RESULT_INVALID','Correction change is invalid.',502);
      }
      const strings=plainPreviewRow(Object.fromEntries(fields.filter(key=>key!=='amount_changed').map(key=>[key,row[key]])),
        fields.filter(key=>key!=='amount_changed'),'Correction change');
      return Object.freeze({...strings,amount_changed:row.amount_changed});
    })),
    paid_match_questions: normaliseCorrectFinalPaidQuestions(preview.paid_match_questions),`;
const fragment=fs.readFileSync(path.join(dir,'fragments/review-paid-recheck.mjs')).toString();
const questionDecoder=fs.readFileSync(path.join(dir,'fragments/question-decoder.mjs')).toString();
const eol=orchestrator.includes('\r\n')?'\r\n':'\n';
const orchHunks=[{name:'actual native eight-key changes preserve amount and Boolean fields',before:old,after},
 {name:'same finite question decoder and actual saved native review consumer',before:'function normaliseOfficePreview(value) {',after:questionDecoder+'\n'+fragment+'\nfunction normaliseOfficePreview(value) {'},
 {name:'original recheck preview exposes real question set',before:'      changes: reviewed.office_preview.changes,',after:'      paid_match_questions: reviewed.office_preview.paid_match_questions,\n      changes: reviewed.office_preview.changes,'},
 {name:'original upload preview exposes real question set',before:'    changes: reviewed.office_preview.changes,\n    blockers: reviewed.office_preview.blockers,',after:'    paid_match_questions: reviewed.office_preview.paid_match_questions,\n    changes: reviewed.office_preview.changes,\n    blockers: reviewed.office_preview.blockers,'}].map(h=>({...h,before:h.before.replaceAll('\n',eol),after:h.after.replaceAll('\n',eol)}));
let orchAfter=orchestrator;
for(const h of orchHunks){assert.equal(orchAfter.split(h.before).length,2,h.name);orchAfter=orchAfter.replace(h.before,()=>h.after);}
let orchReverse=orchAfter;for(const h of [...orchHunks].reverse()){assert.equal(orchReverse.split(h.after).length,2,h.name);orchReverse=orchReverse.replace(h.after,()=>h.before);}assert.equal(orchReverse,orchestrator);
fs.writeFileSync(path.join(dir,'before',orchestratorPath),orchestratorRaw);fs.writeFileSync(path.join(dir,'forward',orchestratorPath),orchAfter);
reverses.push({source:'NATIVE_GIT:'+orchestratorPath,file:orchestratorPath,input_authority:native,before_bytes:orchestratorRaw.length,before_sha256:sha(orchestratorRaw),after_bytes:Buffer.byteLength(orchAfter),after_sha256:sha(orchAfter),hunks:orchHunks,whole_reverse_verified:true});
fs.writeFileSync(path.join(dir,'COMPLETE_CURRENT_REVERSES.json'),JSON.stringify({source_only:true,base,native,
 legacy_unpublished_manifest_sha256:sha(legacyManifestRaw),legacy_independent_review:'PENDING',reverses,native_inputs:pins,native_definition_inputs:sources},null,2)+'\n');
console.log(JSON.stringify({whole_owners:reverses.length,unique_hunks:reverses.reduce((n,r)=>n+r.hunks.length,0),source_only:true}));
