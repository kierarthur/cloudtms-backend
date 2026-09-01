import assert from 'node:assert/strict';
import test from 'node:test';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {spawnSync} from 'node:child_process';
import {validateBankingPayModalEnvelope} from '../broker/src/banking-pay-modal-v2.js';

const root=path.resolve(path.dirname(fileURLToPath(import.meta.url)),'..');
const id=n=>`00000000-0000-4000-8000-${String(n).padStart(12,'0')}`;
const args={p_session_id:id(1),p_candidate_id:id(2),p_cursor:null,p_limit:100,
 p_options_json:{expected_session_version:2,expected_progress_counter_version:3,scope_hash:'a'.repeat(64),pay_channel_scope:'ALL'}};
function page(){return {ok:true,contract:'BANKING_PAY_MODAL_STRUCTURE_V2',contract_version:1,session_id:id(1),
 session_version:2,progress_counter_version:3,scope_hash:'a'.repeat(64),candidate_id:id(2),
 candidate:{candidate_id:id(2),candidate_name:'Synthetic',candidate_reference:'FIXTURE',candidate_sort_name:'synthetic',candidate_sort_reference:'fixture',
 child_revision:'current',facts_digest:'b'.repeat(64),selectable_ready_count:2,selected_ready_count:0,selection_state:'NONE',selected_display_amount:'0.00',
 selected_deduction_exists:false,selected_timesheet_count:0,selected_timesheet_ids:[],selected_timesheet_scope_token:null},
 rows:[3,4].map(n=>({identity:id(n),candidate_id:id(2),effective_section:'canonical_preview_lines',selected:false,
  presentation_group_kind:'ROW',presentation_group_key:`ROW|${id(n)}`,presentation_group_row_count:1,
  selection_group_kind:null,selection_group_key:null,selection_group_member_count:0,selection_group_selected_count:0,
  selection_group_state:null,selection_group_display_amount:null,selection_group_selected_display_amount:null})),
 total_count:2,ready_row_count:2,page_number:1,has_more:false,next_cursor:null,has_previous:false,previous_cursor:null,page_anchor:'current_anchor'};}
for(const [name,change] of Object.entries({
 missingPageNumber:p=>delete p.page_number, zeroPage:p=>p.page_number=0, fractionalPage:p=>p.page_number=1.5,
 missingPrevious:p=>delete p.has_previous, wrongPrevious:p=>p.has_previous=true,
 missingPreviousCursor:p=>delete p.previous_cursor, unexpectedPreviousCursor:p=>p.previous_cursor='previous',
 missingAnchor:p=>delete p.page_anchor, invalidAnchor:p=>p.page_anchor='../invalid',
 truncatedLaterPage:p=>{p.page_number=2;p.has_previous=true;p.total_count=105;},
 firstReadReturnsLaterPage:p=>{p.page_number=2;p.has_previous=true;p.total_count=102;},
 oversizedUtf8:p=>p.rows[0].breakdown_note='£'.repeat(262145)
}))test('direct Ready boundary rejects '+name,()=>{
 const p=page();change(p);
 assert.throws(()=>validateBankingPayModalEnvelope(p,'ready',args),{code:'BANKING_PAY_V2_INVALID_RESPONSE'});
});
test('direct Ready accepts complete first, later, third and departed pages',()=>{
 validateBankingPayModalEnvelope(page(),'ready',args);
 const later=page();Object.assign(later,{page_number:2,has_previous:true,total_count:102,ready_row_count:102});
 validateBankingPayModalEnvelope(later,'ready',{...args,p_cursor:'exact_next'});
 Object.assign(later,{page_number:3,previous_cursor:'exact_previous',total_count:202,ready_row_count:202});
 validateBankingPayModalEnvelope(later,'ready',{...args,p_cursor:'exact_next'});
 validateBankingPayModalEnvelope({...page(),candidate:null,rows:[],total_count:0,ready_row_count:0,page_number:0,page_anchor:null},'ready',args);
});
test('direct Ready SQL rejects oversized complete payload rather than truncating',
 {skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const fixture=fs.readFileSync(path.join(root,'tests/28082026_1254_banking_pay_modal_ready_page_runtime.sql'),'utf8');
 const source=fs.readFileSync(path.join(root,'supabase/repeatable/28082026_1159_banking_pay_modal_structure_v2.sql'),'utf8');
 const start=source.indexOf('CREATE OR REPLACE FUNCTION public.pay_workbench_session_get_candidate_ready_page_v1(');
 const reader=source.slice(start,source.indexOf('$function$;',start)+'$function$;'.length);
 assert.ok(start>=0);assert.doesNotMatch(reader,/^\s*(?:begin|commit|rollback)\s*;/im);
 const sql=fixture.slice(0,fixture.indexOf('DO $ready_read_proof$'))+reader+`
 DO $proof$
 DECLARE s public.banking_pay_workbench_sessions%ROWTYPE; options jsonb; p jsonb;
 BEGIN
  SELECT * INTO s FROM public.banking_pay_workbench_sessions WHERE id='00000000-0000-4000-8000-000000000005';
  options:=jsonb_build_object('expected_session_version',s.version,'expected_progress_counter_version',s.progress_counter_version,
   'pay_channel_scope','ALL','scope_hash',private.pay_workbench_modal_scope_hash_v2(s,'ALL'));
  p:=public.pay_workbench_session_get_candidate_ready_page_v1(s.id,'00000000-0000-4000-8000-000000000002',options,s.actor_user_id,NULL,100);
  IF jsonb_array_length(p->'rows') IS DISTINCT FROM 100 OR (p->>'total_count')::int IS DISTINCT FROM 105 THEN RAISE EXCEPTION 'NORMAL_COMPLETE_PAGE_LOST'; END IF;
  UPDATE public.banking_pay_workbench_preview_rows SET row_json=row_json||jsonb_build_object('fixture_oversize_note',repeat('£',262145))
   WHERE session_id=s.id AND row_ordinal=3;
  BEGIN
   PERFORM public.pay_workbench_session_get_candidate_ready_page_v1(s.id,'00000000-0000-4000-8000-000000000002',options,s.actor_user_id,NULL,100);
   RAISE EXCEPTION 'OVERSIZED_DIRECT_READY_ACCEPTED';
  EXCEPTION WHEN SQLSTATE 'P0001' THEN IF SQLERRM<>'BANKING_PAY_V2_READY_TOO_LARGE' THEN RAISE; END IF; END;
 END $proof$;
 ROLLBACK;`;
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres',
  '-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{input:sql,cwd:root,encoding:'utf8',timeout:30000,maxBuffer:2*1024*1024});
 assert.equal(r.status,0,r.error?.message||r.stderr);
});
