import{parsePushTimingJson}from'./push-timing-office.mjs';
const UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/,HASH=/^[0-9a-f]{64}$/;
const id=v=>typeof v==='string'&&UUID.test(v),hash=v=>typeof v==='string'&&HASH.test(v);
const closed=(v,keys)=>v!==null&&typeof v==='object'&&!Array.isArray(v)&&Object.keys(v).length===keys.length&&keys.every(k=>Object.hasOwn(v,k));
const nullable=(v,p)=>v===null||p(v),bool=v=>typeof v==='boolean';
const counter=v=>typeof v==='string'&&/^[1-9][0-9]*$/.test(v)&&(v.length<19||(v.length===19&&v<='9223372036854775807'));
const date=v=>typeof v==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(v)&&v.slice(0,4)!=='0000'&&Number.isFinite(Date.parse(v+'T00:00:00Z'))&&new Date(v+'T00:00:00Z').toISOString().slice(0,10)===v;
const messages=Object.freeze({
 CURRENT_SOURCE_PAY_BASIS_UNAVAILABLE:'The current Source pay basis is not yet available. Refresh this shift before deciding.',
 SOURCE_PRESENT:'This shift is present in the latest source file.',
 ACCEPTED_SOURCE_REMOVED:'Source hours have already been accepted for this shift.',
 CURRENT_ACKNOWLEDGEMENT:'Keep as paid is already recorded for these Source facts.',
 SOURCE_FACTS_UNAVAILABLE:'The current Source facts are not yet available. Refresh this shift before deciding.',
 CREDIT_SELECTION_REQUIRED:'Open the credited Source line before recording Keep as paid.',
 NO_DECISION_REQUIRED:'No Keep as paid decision is needed for these Source facts.'
});
function fail(code,status=400){throw Object.assign(new Error(code),{code,status});}
export function validateKeptPayContextCommand(body){
 if(!closed(body,['action','payload'])||body.action!=='KEEP_AS_PAID_CONTEXT'
  ||!(closed(body.payload,['schema_version','work_event_id'])||closed(body.payload,['schema_version','work_event_id','credit_invoice_line_id']))
  ||(Object.hasOwn(body.payload??{},'credit_invoice_line_id')&&!id(body.payload.credit_invoice_line_id))
  ||body.payload.schema_version!=='WEEKLY_SOURCE_KEEP_AS_PAID_CONTEXT_REQUEST_V1'||!id(body.payload.work_event_id))fail('WEEKLY_SOURCE_KEEP_AS_PAID_CONTEXT_REQUEST_INVALID');
 return body.payload;
}
export function parseKeptPayContextCommand(text){let body;try{body=parsePushTimingJson(text);}catch{fail('WEEKLY_SOURCE_KEEP_AS_PAID_CONTEXT_REQUEST_INVALID');}validateKeptPayContextCommand(body);return body;}
export function decodeKeptPayContext(value,payload){
 const keys=['schema_version','work_event_id','candidate_id','client_id','contract_id','source_group_id','source_cycle_id',
  'finalisation_week_ending','projection_publication_id','upload_id','source_present','invoiced','paid','paid_evidence_state',
  'root_timesheet_ids','credit_lines','reason','kept_by_default','decision_available','decision_needed','facts_fingerprint',
  'decision_applies','latest_acknowledgement','holds_pay','holds_invoice','accepted_source_removed','disabled_reason'];
 const invalid=()=>fail('WEEKLY_SOURCE_KEEP_AS_PAID_CONTEXT_RESULT_INVALID',502);
 if(!closed(value,keys)||value.schema_version!=='WEEKLY_SOURCE_KEPT_AS_PAID_CONTEXT_READ_V1'||value.work_event_id!==payload.work_event_id
  ||!['work_event_id','candidate_id','client_id','contract_id','source_group_id','source_cycle_id','projection_publication_id','upload_id'].every(k=>id(value[k]))
  ||!date(value.finalisation_week_ending)||!['source_present','invoiced','kept_by_default','decision_available','decision_applies','accepted_source_removed'].every(k=>bool(value[k]))
  ||!['paid','decision_needed','holds_pay','holds_invoice'].every(k=>nullable(value[k],bool))
  ||!['UNKNOWN','NOT_SETTLED','SETTLED_ORIGINAL','SETTLED_SOURCE_SEGMENT'].includes(value.paid_evidence_state)
  ||!nullable(value.facts_fingerprint,hash)||!nullable(value.reason,v=>['CREDITED','NOT_INVOICED_DISAPPEARED'].includes(v)))invalid();
 if(!Array.isArray(value.root_timesheet_ids)||!value.root_timesheet_ids.every(id)||new Set(value.root_timesheet_ids).size!==value.root_timesheet_ids.length
  ||!Array.isArray(value.credit_lines)||!value.credit_lines.every(v=>closed(v,['invoice_line_id'])&&id(v.invoice_line_id))
  ||new Set(value.credit_lines.map(v=>v.invoice_line_id)).size!==value.credit_lines.length)invalid();
 const selected=payload.credit_invoice_line_id;
 if(selected!==undefined&&(!id(selected)||!value.credit_lines.some(v=>v.invoice_line_id===selected)))invalid();
 if(value.reason==='CREDITED'&&value.credit_lines.length>1&&selected===undefined
  &&value.decision_available&&value.decision_needed===true&&!value.decision_applies&&value.disabled_reason?.code!=='CREDIT_SELECTION_REQUIRED')invalid();
 const ack=value.latest_acknowledgement;
 if(ack!==null&&(!closed(ack,['audit_id','decision_id','actor_user_id','actor_display','acknowledged_at_utc','acknowledgement_sequence'])
  ||!['audit_id','decision_id','actor_user_id'].every(k=>id(ack[k]))||!nullable(ack.actor_display,v=>typeof v==='string')
  ||typeof ack.acknowledged_at_utc!=='string'||!/^\d{4}-\d{2}-\d{2}T(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:[.]\d{1,6})?(?:Z|[+]00:00)$/.test(ack.acknowledged_at_utc)
  ||!date(ack.acknowledged_at_utc.slice(0,10))||!Number.isFinite(Date.parse(ack.acknowledged_at_utc))||!counter(ack.acknowledgement_sequence)))invalid();
 const disabled=value.disabled_reason;
 if(disabled!==null&&(!closed(disabled,['code','message'])||!Object.hasOwn(messages,disabled.code)||messages[disabled.code]!==disabled.message))invalid();
 if(disabled===null&&!(value.decision_available&&value.decision_needed===true&&hash(value.facts_fingerprint)
  &&['CREDITED','NOT_INVOICED_DISAPPEARED'].includes(value.reason)
  &&(value.reason!=='CREDITED'||value.credit_lines.length===1||selected!==undefined)&&!value.source_present&&!value.accepted_source_removed))invalid();
 if(value.decision_available&&(!value.root_timesheet_ids.length||value.source_present||value.accepted_source_removed))invalid();
 if(value.decision_applies&&ack===null||value.accepted_source_removed&&value.decision_applies)invalid();
 return value;
}
