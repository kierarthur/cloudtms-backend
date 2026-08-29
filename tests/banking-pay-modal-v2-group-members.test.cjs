const assert=require('node:assert/strict');const fs=require('node:fs');const path=require('node:path');const test=require('node:test');
const {spawnSync}=require('node:child_process');const root=path.resolve(__dirname,'..');
const file=path.join(root,'supabase/repeatable/29082026_0108_banking_pay_modal_group_members_v2.sql');const source=()=>fs.readFileSync(file,'utf8');
test('complete group projection is private read-only identity work',()=>{const sql=source();
 for(const name of ['js_string','js_first_truthy_text','ready_group_key','ready_group_members'])assert.ok(sql.includes(`private.pay_workbench_modal_${name}_v2`));
 assert.doesNotMatch(sql,/\b(?:GRANT|INSERT INTO|UPDATE public\.|DELETE FROM|pay_workbench_prepare_draft|SUM\s*\(|amount_)\b/i);
 assert.doesNotMatch(sql,/pg_catalog\.(?:coalesce|nullif|least|greatest)\s*\(/i);
 assert.match(sql,/private\.pay_workbench_modal_eligible_rows_v2/);assert.match(sql,/private\.pay_workbench_modal_selection_rows_v2/);
});
test('SQL identity keys match the frozen existing Timesheet and overpayment grouping helpers',{skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const oracle=require('./fixtures/banking-pay-legacy-oracle.cjs')();const candidate='10000000-0000-4000-8000-000000000002';const timesheet='10000000-0000-4000-8000-000000000010';
 const fixtures=[{candidate_id:candidate,timesheet_id:timesheet},{candidateId:candidate,timesheetId:timesheet},
  {candidate_id:candidate,linkedTimesheetId:timesheet},{candidate_id:candidate,economic_key:{timesheet_id:timesheet}},
  {row_json:{candidate_id:candidate,linked_timesheet_id:timesheet}},{candidate_id:' ',timesheet_id:' ',row_json:{candidate_id:candidate,timesheet_id:timesheet}}];
 const over=[{line_type:'OVERPAYMENT_RECOVERY',finance_case_id:'case-a'},{lineType:'overpayment_recovery',financeCaseId:'case-b'},
  {row_json:{lineType:'OVERPAYMENT_RECOVERY',financeCaseId:'case-c'}},{line_type:'OVERPAYMENT_RECOVERY',presentation_parent_line_id:'parent-a'},
  {line_type:'OVERPAYMENT_RECOVERY',presentationParentLineId:'parent-b'},
  {line_type:'OVERPAYMENT_RECOVERY',finance_case_id:' ',presentation_parent_line_id:'parent-c'}];
 const literal=v=>`'${JSON.stringify(v).replaceAll("'","''")}'::jsonb`;
 const definition=source().slice(source().indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_modal_js_string_v2'),source().lastIndexOf('commit;'));
 const sql=`BEGIN;${definition}SELECT jsonb_agg(private.pay_workbench_modal_ready_group_key_v2(value,'TIMESHEET') ORDER BY ord) FROM jsonb_array_elements(${literal(fixtures)}) WITH ORDINALITY a(value,ord);SELECT jsonb_agg(private.pay_workbench_modal_ready_group_key_v2(value,'OVERPAYMENT') ORDER BY ord) FROM jsonb_array_elements(${literal(over)}) WITH ORDINALITY a(value,ord);ROLLBACK;`;
 const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-A','-t','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{input:sql,encoding:'utf8',timeout:30000});
 assert.equal(r.status,0,r.stderr||r.error?.message);const actual=r.stdout.trim().split(/\r?\n/).map(JSON.parse);
 assert.deepEqual(actual[0],fixtures.map(v=>oracle.getReadyTimesheetGroupKey(v)));
 assert.deepEqual(actual[1],over.map(v=>oracle.getOverpaymentRecoveryPresentationGroupKey(v)));
});
test('actual complete group membership crosses two child pages without changing selection',{skip:!process.env.BANKING_MODAL_LOCAL_PSQL},()=>{
 const sql=source();const start=sql.indexOf('CREATE OR REPLACE FUNCTION private.pay_workbench_modal_js_string_v2');
 const definitions=sql.slice(start,sql.lastIndexOf('commit;'));
 const setup=fs.readFileSync(path.join(__dirname,'fixtures/28082026_1429_banking_pay_selection_setup.sql'),'utf8');
 const runtime=`BEGIN;DO $g$ BEGIN IF current_database()<>'banking_modal_v2_test' OR current_setting('server_version_num')::int NOT BETWEEN 170000 AND 179999 THEN RAISE EXCEPTION 'LOCAL_PG17_ONLY';END IF;END $g$;${definitions}${setup}
 INSERT INTO public.timesheets(timesheet_id,booking_id,occupant_key_norm,hospital_norm,ward_norm,job_title_norm,week_ending_date)
 VALUES('10000000-0000-4000-8000-000000000010','rollback-group-timesheet','rollback candidate','rollback hospital','rollback ward','rollback role','2026-08-23');
 UPDATE public.banking_pay_workbench_preview_rows SET timesheet_id='10000000-0000-4000-8000-000000000010',row_json=row_json||jsonb_build_object('timesheet_id','10000000-0000-4000-8000-000000000010') WHERE session_id='10000000-0000-4000-8000-000000000005';
 INSERT INTO public.banking_pay_workbench_preview_rows(id,session_id,candidate_id,timesheet_id,section,row_key,row_ordinal,row_json,key_type,key_value,selected,selection_state,status,session_version)
 SELECT ('10000000-0000-4000-8000-'||lpad((9000+n)::text,12,'0'))::uuid,'10000000-0000-4000-8000-000000000005','10000000-0000-4000-8000-000000000002','10000000-0000-4000-8000-000000000010','canonical_preview_lines','group-cross-page:'||n,500+n,
 jsonb_build_object('candidate_id','10000000-0000-4000-8000-000000000002','timesheet_id','10000000-0000-4000-8000-000000000010','pay_channel','PAYE','line_type','TIMESHEET_PAYMENT','amount_display','1.00','selection_allowed',true,'draftable',true,'is_ready_for_draft',true),'SOURCE_REF','group-cross-page:'||n,false,'UNSELECTED','READY',1 FROM generate_series(1,104)n;
 DO $p$ DECLARE s public.banking_pay_workbench_sessions%ROWTYPE;c bigint;chosen bigint;k text;before_state text;after_state text;e bigint;sel bigint;types jsonb;keys jsonb;BEGIN SELECT * INTO STRICT s FROM public.banking_pay_workbench_sessions WHERE id='10000000-0000-4000-8000-000000000005';SELECT md5(jsonb_agg(jsonb_build_array(id,selected,selection_state)ORDER BY id)::text)INTO before_state FROM public.banking_pay_workbench_preview_rows WHERE session_id=s.id;SELECT count(*)INTO e FROM private.pay_workbench_modal_eligible_rows_v2(s.id,s.version,'canonical_preview_lines')r WHERE r.candidate_id='10000000-0000-4000-8000-000000000002';SELECT count(*)INTO sel FROM private.pay_workbench_modal_selection_rows_v2(s.id,s.version)r JOIN public.banking_pay_workbench_preview_rows p ON p.id=r.id WHERE p.candidate_id='10000000-0000-4000-8000-000000000002' AND r.is_selectable;SELECT jsonb_agg(DISTINCT private.pay_workbench_modal_row_payload_v2(r)->'line_type'),jsonb_agg(DISTINCT private.pay_workbench_modal_ready_group_key_v2(private.pay_workbench_modal_row_payload_v2(r),'TIMESHEET'))INTO types,keys FROM public.banking_pay_workbench_preview_rows r WHERE r.session_id=s.id AND r.candidate_id='10000000-0000-4000-8000-000000000002';SELECT count(*),count(*)FILTER(WHERE selected),min(group_key)INTO c,chosen,k FROM private.pay_workbench_modal_ready_group_members_v2(s,'ALL','10000000-0000-4000-8000-000000000002');IF c<>107 OR chosen<>107 OR k<>'READY_TO_PAY|10000000-0000-4000-8000-000000000002|10000000-0000-4000-8000-000000000010' THEN RAISE EXCEPTION 'GROUP_SCOPE_INCOMPLETE group %, selected %, key %, eligible %, selectable %, types %, keys %',c,chosen,k,e,sel,types,keys;END IF;SELECT md5(jsonb_agg(jsonb_build_array(id,selected,selection_state)ORDER BY id)::text)INTO after_state FROM public.banking_pay_workbench_preview_rows WHERE session_id=s.id;IF before_state<>after_state THEN RAISE EXCEPTION 'GROUP_READ_CHANGED_SELECTION';END IF;END $p$;ROLLBACK;`;
 assert.doesNotMatch(runtime,/^\s*COMMIT;/im);const r=spawnSync(process.env.BANKING_MODAL_LOCAL_PSQL,['-X','-q','-h','127.0.0.1','-p','55441','-U','postgres','-d','banking_modal_v2_test','-v','ON_ERROR_STOP=1'],{input:runtime,encoding:'utf8',timeout:45000,maxBuffer:2*1024*1024});assert.equal(r.status,0,r.stderr||r.error?.message);
});
