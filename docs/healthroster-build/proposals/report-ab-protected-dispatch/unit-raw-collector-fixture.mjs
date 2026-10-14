// FABRICATED UNIT_ONLY codec/session vectors. Never save as native acceptance.
import {sha256,domainHash} from './weekly-source-protected-release-qualification.mjs';
import {collectorSchema,reportFrozenSemantics,deriveReportNativeReceipt,reportNativeRequestHash} from './weekly-source-report-fin-native-collector.mjs';
export function unitRawTrace(execution,{caseId='D_ZERO',variant='A',complete=true}={}){
 const id=execution.installation_id,hash='a'.repeat(64),fixture={group_code:'UNIT_ONLY',client_ids:[id],contract_ids:[id],candidate_ids:[id],client_refs:['UNIT_ONLY'],candidate_refs:['UNIT_ONLY']};
 const request={actor_user_id:id,source_cycle_id:id,authority_scope_kind:'SELF_BILL_REPORT_SCOPE',report_scope_id:id,upload_id:id,
  projection_publication_id:id,expected_authority_scope_version:1,expected_row_manifest_hash:hash,expected_comparison_manifest_hash:hash,
  expected_issue_set_hash:hash,exclude_unfinalised_acknowledged:false};
 const row={id,upload_id:id,row_finalisation_state:'SOURCE_REPORT_LINE',source_row_ordinal:1,external_source_key:'0007',work_date:'2026-10-02',start_at_local:'2026-10-02T09:00:00',
  end_at_local:'2026-10-02T17:00:00',source_shift_charge_pence:100,source_vat_pence:20,source_gross_pence:120};
 const facts={schema_version:'WEEKLY_SOURCE_SELF_BILL_REPORT_FACTS_V1',report_scope_id:id,self_bill_report_id:id,upload_row_id:id,rule_set_version_id:id,
  request_id:row.external_source_key,source_row_ordinal:1,work_date:row.work_date,start_at_local:row.start_at_local,end_at_local:row.end_at_local,
  source_net_pence:100,source_vat_pence:20,source_gross_pence:120};
 const sourceHash=sha256('UNIT_ONLY source');
 const basis={cycle:{id,source_group_id:id,finalisation_week_ending:'2026-10-04'},scope:{id,self_bill_report_id:id,source_cycle_id:id,source_group_id:id,client_id:id,current_final_revision_id:null,
  current_complete_upload_id:id,current_projection_publication_id:id,projection_state:'CURRENT',version:1},
  report:{id,source_cycle_id:id,source_group_id:id,client_id:id,line_count:1,content_sha256:'\\x'+sourceHash,row_manifest_hash:'\\x'+hash,net_pence:100,vat_pence:20,gross_pence:120},
  upload:{id,source_cycle_id:id,report_scope_id:id,state:'CURRENT',content_sha256:'\\x'+sourceHash,row_manifest_hash:'\\x'+hash},
  publication:{id,upload_id:id,source_cycle_id:id,report_scope_id:id,authority_scope_kind:'SELF_BILL_REPORT_SCOPE',authority_scope_version:1,
   comparison_manifest_hash:'\\x'+hash,issue_set_hash:'\\x'+hash,state:'CURRENT'},authorization:{schema_version:'WEEKLY_SOURCE_OFF_RULE_TEST_AUTHORIZATION_V1',
   purpose:'SOURCE_FIXTURE_SETUP',release_id:execution.release_id,source_commit:execution.source_commit,contract_sha256:execution.contract_sha256,
   scope_id:id,source_cycle_id:id,source_group_id:id,journey_actor_user_id:id,agency_id:id,authorization_sha256:hash,rule_set_version_id:id,
   rule_values_sha256:hash,week_ending:'2026-10-04',fixture_manifest:fixture},rows:[row],bindings:[]};
 const f={revision:{id,authority_scope_kind:'SELF_BILL_REPORT_SCOPE',report_scope_id:id,source_cycle_id:id,upload_id:id,rule_set_version_id:id},
  manifest:{id,final_revision_id:id,client_id:id,source_cycle_id:id,source_group_id:id,movement_count:1},
  invoice:{id,client_id:id,invoice_no:'UNIT_ONLY_NEVER_NATIVE',header_snapshot_json:{meta:{final_revision_id:id}}},
  movements:[{id,final_revision_id:id,finalisation_cycle_id:id,movement_economic_hash:'\\x'+hash,self_bill_report_row_id:id,source_line_kind:'SELF_BILL_REPORT_LINE',source_profile_kind:'SELF_BILL_REPORT',source_facts_json:facts,
   canonical_pay_vector_json:{schema_version:'WEEKLY_SOURCE_RATE_VECTOR_V1',kind:'PAY',source_mode:'HEALTHROSTER_WEEKLY',rate_method:'SPLIT_RATE_WINDOWS',
    row_sign:1,paid_minutes:0,break_minutes:0,hours:{day:0,night:0,sat:0,sun:0,bh:0},rates:{day:0,night:0,sat:0,sun:0,bh:0},total_pence:0},total_pay_ex_vat:0,actual_client_id:id,invoice_presentation_charge_pence:100,
   movement_role:'POSITIVE',candidate_id:id,contract_id:id,work_event_id:id,invoice_timesheet_id:id}],
  membership:[{client_manifest_id:id,billing_movement_id:id,movement_hash:'\\x'+hash}],
  invoice_lines:[{id,invoice_id:id,source_key:'UNIT_ONLY_NATIVE_LINE'}],invoice_bindings:[{id,invoice_id:id,invoice_line_id:id,billing_movement_id:id,presentation_line_id:id,state:'CURRENT'}],
  presentation_lines:[{id,billing_movement_id:id,client_manifest_id:id,final_revision_id:id,candidate_display_snapshot:'UNIT_ONLY',booking_reference_snapshot:'0007'}],
  allocation:{ok:true,is_source_invoice:true,invoice_id:id},validation:{ok:true,is_source_invoice:true,invoice_id:id},net_pence:'100',vat_pence:'20',gross_pence:'120',rounding_pence:'0',snapshot_count:'0',transition_count:'0'};
 const reference={schema_version:'WEEKLY_SOURCE_REPORT_FIN_FIXTURE_REFERENCE_V1',case_id:caseId,variant,source_cycle_id:id,report_scope_id:id,client_id:id,
  rule_set_version_id:id,rule_set_values_sha256:hash,source_census_sha256:hash,native_request_sha256:reportNativeRequestHash(request)};
 const finBytes=JSON.stringify({ok:true,status:'FINALISED',invoice_only:true,idempotent:false,final_revision_id:id,source_cycle_id:id,report_scope_id:id});
 const referenceFrame={schema_version:'WEEKLY_SOURCE_REPORT_FIN_FIXTURE_FRAME_V1',scope_id:id,authorization_sha256:hash,transaction_id:'999',backend_pid:'123',
  reference,source_cas:{source_cas_sha256:hash,UNIT_ONLY:'never native'},operational_ready:false,frame_sha256:hash};
 const commandFrame={schema_version:'WEEKLY_SOURCE_OFF_RULE_COMMAND_V1',command_id:id,transaction_id:'999',backend_pid:'123',source_cycle_id:id,actor_user_id:id,
  authorization:basis.authorization,native_operation:'REPORT_FIN_AB_FINALISE',native_request_sha256:reportNativeRequestHash(request,'WEEKLY_SOURCE_OFF_RULE_NATIVE_REQUEST_V1'),
  operational_ready:false,command_sha256:hash};
 const {frame_sha256:ignoredReference,...referenceBase}=referenceFrame,{command_sha256:ignoredCommand,...commandBase}=commandFrame;
 const referenceBaseBytes=JSON.stringify(referenceBase),commandBaseBytes=JSON.stringify(commandBase);
 referenceFrame.frame_sha256=sha256('WEEKLY_SOURCE_REPORT_FIN_FIXTURE_FRAME_V1\x1f'+referenceBaseBytes);
 commandFrame.command_sha256=sha256('WEEKLY_SOURCE_OFF_RULE_COMMAND_V1\x1f'+commandBaseBytes);
 const audit=(n,objectType,action,before,after,objectId=id)=>({id:'00000000-0000-4000-8000-00000000000'+n,object_type:objectType,object_id_text:objectId,
  action,event_sequence:String(n),event_sequence_is_authoritative:true,actor_user_id:id,before_bytes:before===null?null:JSON.stringify(before),after_bytes:JSON.stringify(after),
  frame_base_bytes:n===1?referenceBaseBytes:n===2?commandBaseBytes:null});
 f.off_audit_frames=[audit(1,'weekly_source_off_rule_report_reference','WEEKLY_SOURCE_REPORT_FIN_FIXTURE_BOUND',null,referenceFrame),
  audit(2,'weekly_source_off_rule_command','WEEKLY_SOURCE_OFF_RULE_COMMAND_ADMITTED',null,commandFrame),
  audit(3,'weekly_source_off_rule_command','WEEKLY_SOURCE_OFF_RULE_COMMAND_COMPLETED',commandFrame,{command_id:id,command_sha256:commandFrame.command_sha256,
   native_result_sha256:sha256('WEEKLY_SOURCE_OFF_RULE_NATIVE_RESULT_V1\x1f'+finBytes),operational_ready:false})];
 const model={schema_version:'INVOICE_RENDER_MODEL_V1',purpose:'DRAFT',document_type:'SELF_BILL_INVOICE',invoice_id:id,invoice_number:'UNIT_ONLY_NEVER_NATIVE',
  supplier:{legal_name:'UNIT_ONLY Supplier',registered_address:['UNIT_ONLY'],vat_registration_number:'UNIT_ONLY'},customer:{legal_name:'UNIT_ONLY Client',billing_address:['UNIT_ONLY']},
  references:{},totals:{net:'1',vat:'0.2',gross:'1.2'},payment:{terms_text:'UNIT_ONLY',remittance_reference:'UNIT_ONLY_NEVER_NATIVE'},credit_note:{},self_bill:{legal_wording:'UNIT_ONLY'},currency:'GBP',
  lines:[{row_key:'UNIT_ONLY_NATIVE_LINE:residual',source_invoice_line_id:id,source_key:'UNIT_ONLY_NATIVE_LINE',worker:'UNIT_ONLY',reference:'0007',description:'UNIT_ONLY',unit:'shift',quantity:'1',unit_price:'1',
   net_amount:'1',vat_rate:'20',vat_amount:'0.2',gross_amount:'1.2',display_order:1}],vat_breakdown:[{rate:'20',net_amount:'1',vat_amount:'0.2',gross_amount:'1.2'}],legal_wording:[]};
 const frozen=JSON.stringify(model),semantic=reportFrozenSemantics(frozen,f),pdf=Buffer.from('%PDF-UNIT_ONLY_NOT_A_REAL_DOCUMENT');
 const document={invoice_id:id,document_bytes_base64:pdf.toString('base64'),document_sha256:sha256(pdf),frozen_model_bytes:frozen,semantic_bytes:semantic,
  render_receipt_bytes:JSON.stringify({invoice_id:id,document_sha256:sha256(pdf),frozen_model_sha256:sha256(frozen),semantic_sha256:sha256(semantic)})};
 const needed={native_candidate_i1:['complete_original_root_set','empty_set_guard','candidate_gate_order','hooks_body'],ordinary_nhsp:['ordinary_owners','nhsp_owners','ordinary_result','nhsp_result'],
  origin_authority:['prepared','c1','withheld','headless',...(caseId==='NEXT_ACTIVE'?['next']:[])],no_external_effects:['provider','settlement','email','network']};
 const nativeGateFacts=Object.fromEntries(Object.entries(needed).map(([key,ids])=>{const input=JSON.stringify({schema_version:'WEEKLY_SOURCE_REPORT_FIN_NATIVE_GATE_CAPTURE_REQUEST_V1',execution,
  actor_user_id:id,source_cycle_id:id,report_scope_id:id,invoice_id:id,case_id:caseId,variant,native_owner_request:{UNIT_ONLY:'never native'}}),
  observations=ids.map(identity=>({identity,expected_bytes:'{"UNIT_ONLY":"never native"}',observed_bytes:'{"UNIT_ONLY":"never native"}'}));
  return [key,{owner_signature:'private.unit_only_not_native_owner_v1(jsonb)',owner_body_sha256:hash,request_bytes:input,
   result_bytes:JSON.stringify({schema_version:'WEEKLY_SOURCE_REPORT_FIN_NATIVE_GATE_OBSERVATION_V1',execution,native_request_sha256:sha256(input),
    native_result_sha256:domainHash('WEEKLY_SOURCE_REPORT_FIN_NATIVE_GATE_OBSERVATIONS_V1',observations),observations})}];}));
 const isolation=pid=>JSON.stringify({database:'cloudtms_test_clone',backend_pid:String(pid),pay:hash,expenses:hash}),basisBytes=JSON.stringify(basis);
 return {schema_version:collectorSchema,execution,case_id:caseId,variant,native_request:request,fixture_manifest_sha256:sha256(JSON.stringify(fixture,Object.keys(fixture).sort())),
  original_report_sha256:sha256('UNIT_ONLY source'),basis_bytes:basisBytes,reference_bytes:JSON.stringify(reference),fin_bytes:finBytes,
  admission_bytes:JSON.stringify({ok:true,status:'ADMITTED',invoice_ids:[id],client_manifest_id:id}),financial_bytes:JSON.stringify(f),before_bytes:isolation(123),after_bytes:isolation(123),fresh_bytes:isolation(124),
  fresh_basis_bytes:basisBytes,fresh_execution_bytes:JSON.stringify(execution),document:complete?document:null,native_gate_facts:complete?nativeGateFacts:null,
  session_receipt:{database:'cloudtms_test_clone',backend_pid:'123',fresh_backend_pid:'124',rolled_back:true,turn_backend_pid:125,original_transaction_closed:true,new_financial_rows:'0',
   turn_lock_result_bytes:'t',managed_writer_lock_result_bytes:'t'}};
}
export function unitRawInput(execution,variant='A'){
 const bytes=JSON.stringify(unitRawTrace(execution,{variant})),derived=deriveReportNativeReceipt(bytes),receiptId='UNIT_ONLY_RAW/D_ZERO/'+variant;
 return {input:{receipt_id:receiptId,receipt:derived.receipt,receipt_sha256:derived.receipt_sha256},artifact:{receipt_id:receiptId,collector_bytes:bytes,collector_sha256:sha256(bytes)}};
}
