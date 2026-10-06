// Informational Source consumer of the closed Banking -> Source V3 contract.
// No command admission, payment arithmetic, cash allocation or hours inference.
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DECIMAL = /^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$/;
const COUNT = /^(?:0|[1-9][0-9]*)$/;
const UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$/;
const REASONS = new Set(['CURRENT_ORIGIN_PENDING','CURRENT_ORIGIN_UNAVAILABLE',
  'NO_QUALIFIED_ORIGINAL','ORIGINAL_POSTING_PENDING','RETURN_POSTING_PENDING',
  'REISSUE_PENDING_OR_HELD','CASH_ALREADY_REPAID','ADDITIONAL_POSITIVE_PAYROLL',
  'QUANTITY_MAPPING_UNSUPPORTED','APPROVAL_UUID_ORIGIN_NOT_YET_QUALIFIED',
  'COMPONENT_SET_NOT_ONE_SNAPSHOT','ROOT_ACTIVITY_NOT_INDEXED']);
const CASH_STATES = ['RETURN_POSTING_PENDING','UNSUPPORTED_BENEFICIARY',
  'REISSUE_REQUESTED','CASH_AVAILABLE','CASH_RESERVED','CASH_REPAID'];
const RUN_STATES = ['PREPARING','REVIEW','DRAFT','CANCELLING','CANCELLED','EXECUTING','COMPLETE'];
const WORKER_STATES = ['PREPARING','REVIEW','READY','DRAFT','CANCELLING','CANCELLED','ISSUED','COMPLETE'];
const TRANSFER_STATES = ['BUILDING','MEMBERS_READY','DRAFT','SCHEDULED','ISSUED_CSV',
  'SUBMITTED','UNKNOWN','SETTLED','RETURNED','REFUSED','CANCELLED','INTERNAL_PROCESSING','INTERNAL_SETTLED'];
const CASH_KEYS = ['transfer_id','transfer_no','scope','original_cash_amount',
  'original_settlement_posting_complete','original_return_command_id','return_posting_complete',
  'return_cash_id','cash_owed','cash_held','cash_repaid','cash_available',
  'pending_reissue_command_id','action_state'];
const COMPONENT_KEYS = ['component_key','source_component_id','detail_kind','quantity_subject',
  'applied_revision_id','collection_id','qualification_state','approved_source_ex_vat',
  'realised_source_disposition_ex_vat','active_source_hold_ex_vat','actual_recovered_source_ex_vat',
  'written_off_source_ex_vat','active_recovery_hold_source_ex_vat','original_effect_id',
  'original_transfer_id','original_run_worker_id','original_positive_source_ex_vat',
  'only_original_positive_payroll','paid_quantity_certificate','paid_quantity','reason','original_cash'];
const HOLD_KEYS = ['hold_id','component_key','run_id','run_worker_id','captured_revision_id',
  'quantity_subject','source_held_ex_vat','target_held_ex_vat','target_held_vat','target_held_inc_vat',
  'run_status','worker_status','preparation_created_at_utc','preparation_deadline_utc',
  'confirmed_at_utc','captured_line_count','active_case_hold_count'];
const WORKER_KEYS = ['run_id','run_worker_id','run_status','worker_status',
  'preparation_created_at_utc','preparation_deadline_utc','confirmed_at_utc',
  'captured_line_count','captured_position_count','realised_effect_count','active_case_hold_count'];
const TRANSFER_KEYS = ['transfer_id','transfer_no','original_transfer_id','return_cash_id',
  'execution_kind','status','cash_scope','cash_amount','outcome_posting_complete',
  'return_posting_complete','internal_posting_complete','reissue_action_state'];
function require(value) {
  if (!value) throw Object.assign(new Error('WEEKLY_SOURCE_NEXT_PAID_CONTRACT_INVALID'),
    {code:'WEEKLY_SOURCE_NEXT_PAID_CONTRACT_INVALID'});
}
function object(value, keys) {
  require(value && typeof value==='object' && !Array.isArray(value));
  require(Object.keys(value).length===keys.length && keys.every(key=>Object.hasOwn(value,key)));
}
const text = value => typeof value==='string';
const pattern = (value,re) => text(value) && re.test(value);
const oneOf = (value,values) => values.includes(value);
const nullable = (value,check) => value===null || check(value);
const boolean = value => typeof value==='boolean';
const uuid = value => pattern(value,UUID);
const count = value => pattern(value,COUNT);
const money = value => pattern(value,DECIMAL);
const timestamp = value => pattern(value,UTC) && Number.isFinite(Date.parse(value));
const reason = value => value===null || REASONS.has(value);
const positive = value => money(value) && !value.startsWith('-') && /[1-9]/.test(value);
const zero = value => money(value) && /^-?0(?:\.0+)?$/.test(value);
const componentKey = value => text(value) && value.length>=1 && value.length<=256
  && new TextEncoder().encode(value).length<=1024;
function cursor(value,kind) {
  if (value===null) return;
  const keys=kind==='COMPONENTS' ? ['after_component_key'] : kind==='ACTIVE_HOLDS'
    ? ['after_component_key','after_hold_id'] : ['after_transfer_no'];
  object(value,keys);
  if(kind==='WORKER_TRANSFERS') require(count(value.after_transfer_no)
    && BigInt(value.after_transfer_no)>0n && BigInt(value.after_transfer_no)<=2147483647n);
  else require(componentKey(value.after_component_key));
  if(kind==='ACTIVE_HOLDS') require(uuid(value.after_hold_id));
}
function worker(value,keys) {
  object(value,keys);
  require(uuid(value.run_id) && uuid(value.run_worker_id)
    && oneOf(value.run_status,RUN_STATES) && oneOf(value.worker_status,WORKER_STATES)
    && timestamp(value.preparation_created_at_utc)
    && nullable(value.preparation_deadline_utc,timestamp) && nullable(value.confirmed_at_utc,timestamp));
  for(const key of keys.filter(key=>key.endsWith('_count'))) require(nullable(value[key],count));
}
function cash(value) {
  object(value,CASH_KEYS);
  require(uuid(value.transfer_id) && count(value.transfer_no) && BigInt(value.transfer_no)>0n
    && value.scope==='WHOLE_ORIGINAL_TRANSFER' && money(value.original_cash_amount)
    && boolean(value.original_settlement_posting_complete)
    && nullable(value.original_return_command_id,uuid) && nullable(value.return_posting_complete,boolean)
    && nullable(value.return_cash_id,uuid) && nullable(value.pending_reissue_command_id,uuid)
    && nullable(value.action_state,value=>oneOf(value,CASH_STATES)));
  for(const key of ['cash_owed','cash_held','cash_repaid','cash_available']) require(nullable(value[key],money));
}
function component(value) {
  object(value,COMPONENT_KEYS);
  require(componentKey(value.component_key) && uuid(value.applied_revision_id)
    && nullable(value.detail_kind,text) && nullable(value.quantity_subject,boolean)
    && oneOf(value.qualification_state,['READY','UNBOUND','NO_QUALIFIED_ORIGINAL'])
    && nullable(value.only_original_positive_payroll,boolean) && reason(value.reason));
  for(const key of ['source_component_id','collection_id','original_effect_id',
    'original_transfer_id','original_run_worker_id']) require(nullable(value[key],uuid));
  for(const key of ['approved_source_ex_vat','realised_source_disposition_ex_vat','active_source_hold_ex_vat']) require(money(value[key]));
  for(const key of ['actual_recovered_source_ex_vat','written_off_source_ex_vat',
    'active_recovery_hold_source_ex_vat','original_positive_source_ex_vat']) require(nullable(value[key],money));
  require(oneOf(value.paid_quantity_certificate,['ORIGINAL_RETURN_ZERO','POSITION_WITHHELD']));
  require(value.paid_quantity_certificate==='ORIGINAL_RETURN_ZERO' ? value.paid_quantity==='0' : value.paid_quantity===null);
  if(value.original_cash!==null) {cash(value.original_cash);require(value.original_cash.transfer_id===value.original_transfer_id);}
  if(value.paid_quantity_certificate==='ORIGINAL_RETURN_ZERO') {
    require(value.quantity_subject===true && value.qualification_state==='READY' && value.reason===null
      && value.only_original_positive_payroll===true && uuid(value.original_effect_id)
      && positive(value.original_positive_source_ex_vat)
      && value.original_cash?.original_settlement_posting_complete===true
      && value.original_cash.return_posting_complete===true && uuid(value.original_cash.return_cash_id)
      && zero(value.original_cash.cash_held) && zero(value.original_cash.cash_repaid)
      && value.original_cash.pending_reissue_command_id===null);
  }
}

export function decodeNextPaidEvidencePage(page,request) {
  object(request,['version','actor_user_id','root_timesheet_id','work_id','expected_revision_id','kind','context_id','after','limit']);
  require(request.version==='SOURCE_PAID_EVIDENCE_V1' && uuid(request.actor_user_id)
    && uuid(request.root_timesheet_id) && uuid(request.work_id) && uuid(request.expected_revision_id)
    && oneOf(request.kind,['COMPONENTS','ACTIVE_HOLDS','WORKER_TRANSFERS'])
    && Number.isInteger(request.limit) && request.limit>=1 && request.limit<=100);
  require(request.kind==='WORKER_TRANSFERS' ? uuid(request.context_id) : request.context_id===null);
  cursor(request.after,request.kind);
  object(page,['version','ok','kind','header','rows','complete','next_cursor','quantity_certificate','activity_coverage']);
  require(page.version===request.version && page.ok===true && page.kind===request.kind
    && Array.isArray(page.rows) && page.rows.length<=request.limit && boolean(page.complete));
  require(new TextEncoder().encode(JSON.stringify(page)).length<=120000);
  const header=page.header;
  object(header,['root_timesheet_id','work_id','expected_revision_id','module_epoch',
    'current_origin_state','financial_view_revision','scope','quantity_authority_scope','context']);
  require(['root_timesheet_id','work_id','expected_revision_id'].every(key=>header[key]===request[key])
    && header.scope==='EXACT_SOURCE_WORK' && count(header.module_epoch) && BigInt(header.module_epoch)>0n
    && nullable(header.financial_view_revision,count)
    && oneOf(header.current_origin_state,['APPLIED','PENDING','UNAVAILABLE','INCOMPATIBLE'])
    && oneOf(header.quantity_authority_scope,['CURRENT','HISTORICAL_ONLY','NONE']));
  if(request.kind==='WORKER_TRANSFERS') {worker(header.context,WORKER_KEYS);require(header.context.run_worker_id===request.context_id);}
  else require(header.context===null);
  cursor(page.next_cursor,page.kind);
  require(page.complete ? page.next_cursor===null : page.next_cursor!==null);
  require(page.complete || page.rows.length>0);
  const pageKeys=new Set();
  for(const row of page.rows) {
    if(page.kind==='COMPONENTS') component(row);
    else if(page.kind==='ACTIVE_HOLDS') {
      worker(row,HOLD_KEYS);
      require(uuid(row.hold_id) && uuid(row.captured_revision_id) && componentKey(row.component_key)
        && nullable(row.quantity_subject,boolean));
      for(const key of ['source_held_ex_vat','target_held_ex_vat','target_held_vat','target_held_inc_vat']) require(money(row[key]));
    } else {
      object(row,TRANSFER_KEYS);
      require(uuid(row.transfer_id) && count(row.transfer_no) && BigInt(row.transfer_no)>0n
        && nullable(row.original_transfer_id,uuid) && nullable(row.return_cash_id,uuid)
        && oneOf(row.execution_kind,['BANK','INTERNAL_ZERO']) && oneOf(row.status,TRANSFER_STATES)
        && row.cash_scope==='WHOLE_WORKER_LEG' && money(row.cash_amount)
        && nullable(row.reissue_action_state,value=>oneOf(value,CASH_STATES)));
      for(const key of ['outcome_posting_complete','return_posting_complete','internal_posting_complete']) require(nullable(row[key],boolean));
      require(row.execution_kind==='BANK' ? positive(row.cash_amount)
        && !['INTERNAL_PROCESSING','INTERNAL_SETTLED'].includes(row.status)
        : zero(row.cash_amount) && ['BUILDING','MEMBERS_READY','CANCELLED','INTERNAL_PROCESSING','INTERNAL_SETTLED'].includes(row.status));
    }
    const rowKey=page.kind==='COMPONENTS' ? row.component_key : page.kind==='ACTIVE_HOLDS' ? row.hold_id : row.transfer_id;
    require(!pageKeys.has(rowKey));pageKeys.add(rowKey);
  }
  if(!page.complete) {
    const last=page.rows.at(-1);
    if(page.kind==='WORKER_TRANSFERS') require(page.next_cursor.after_transfer_no===last.transfer_no);
    else require(page.next_cursor.after_component_key===last.component_key
      && (page.kind!=='ACTIVE_HOLDS' || page.next_cursor.after_hold_id===last.hold_id));
  }
  const certificate=page.quantity_certificate;
  object(certificate,['scope','state','quantity','unit','reason','component_set_complete']);
  require(oneOf(certificate.scope,['EXACT_ORIGINAL_COMPONENTS','EXACT_SOURCE_WORK','NONE'])
    && oneOf(certificate.state,['ORIGINAL_RETURN_ZERO','ROOT_RETURN_ZERO','POSITION_WITHHELD'])
    && certificate.unit==='HOURS' && boolean(certificate.component_set_complete) && reason(certificate.reason));
  require(certificate.state==='POSITION_WITHHELD' ? certificate.quantity===null : certificate.quantity==='0');
  if(certificate.state==='ROOT_RETURN_ZERO') {
    require(page.kind==='COMPONENTS' && request.after===null && page.complete
      && header.quantity_authority_scope==='CURRENT' && header.current_origin_state==='APPLIED'
      && certificate.scope==='EXACT_SOURCE_WORK' && certificate.component_set_complete && certificate.reason===null
      && page.rows.every(row=>row.quantity_subject!==null
        && (row.quantity_subject===false || row.paid_quantity_certificate==='ORIGINAL_RETURN_ZERO'))
      && page.rows.some(row=>row.quantity_subject===true && row.qualification_state==='READY'
        && row.paid_quantity_certificate==='ORIGINAL_RETURN_ZERO' && row.only_original_positive_payroll===true
        && uuid(row.original_effect_id) && positive(row.original_positive_source_ex_vat)
        && row.original_cash?.original_settlement_posting_complete===true
        && row.original_cash.return_posting_complete===true && uuid(row.original_cash.return_cash_id)
        && zero(row.original_cash.cash_held) && zero(row.original_cash.cash_repaid)
        && row.original_cash.pending_reissue_command_id===null));
  }
  const activity=page.activity_coverage;
  object(activity,['scope','complete_for_scope','page_row_count','root_preparing_count',
    'root_frozen_draft_count','root_inflight_count','reason']);
  require(activity.scope===(page.kind==='COMPONENTS' ? 'COMPONENT_ORIGINALS' : page.kind==='ACTIVE_HOLDS'
    ? 'ACTIVE_HOLDS_ONLY' : 'EXACT_RUN_WORKER') && boolean(activity.complete_for_scope)
    && activity.complete_for_scope===page.complete && activity.page_row_count===String(page.rows.length) && reason(activity.reason));
  for(const key of ['root_preparing_count','root_frozen_draft_count','root_inflight_count']) require(nullable(activity[key],count));
  return page;
}

export function nextPaidSchedule(page,request) {
  decodeNextPaidEvidencePage(page,request);
  if(page.quantity_certificate.state==='ROOT_RETURN_ZERO') return {
    available:true,reason:null,source:'NEXT_ROOT_RETURN_ZERO',total_hours:'0',row_count:0,rows:[]};
  return {available:false,reason:page.quantity_certificate.reason || 'QUANTITY_MAPPING_UNSUPPORTED',
    unavailable_class:'POSITION_WITHHELD',source:null,row_count:0,rows:[]};
}

export function nextOriginalCashObligations(page,request) {
  decodeNextPaidEvidencePage(page,request);
  require(page.kind==='COMPONENTS');
  const found=new Map();
  for(const row of page.rows) {
    if(row.original_cash===null) continue;
    const value=row.original_cash;
    const previous=found.get(value.transfer_id);
    if(previous) {
      require(CASH_KEYS.every(key=>previous.cash[key]===value[key]));
      previous.component_keys.push(row.component_key);
    } else found.set(value.transfer_id,{cash:value,component_keys:[row.component_key]});
  }
  return [...found.values()]; // bounded page only: never a root cash total.
}

const REASON_DETAILS=Object.freeze({
  CURRENT_ORIGIN_PENDING:'The latest approved position is still being applied.',
  CURRENT_ORIGIN_UNAVAILABLE:'The current approved position cannot yet be checked against payment evidence.',
  NO_QUALIFIED_ORIGINAL:'There is no fully verified original payment evidence for these hours.',
  ORIGINAL_POSTING_PENDING:'The original payment outcome is still being recorded.',
  RETURN_POSTING_PENDING:'The returned payment outcome is still being recorded.',
  REISSUE_PENDING_OR_HELD:'Returned money is reserved or awaiting reissue.',
  CASH_ALREADY_REPAID:'Returned money has been paid again; the paid-hours position cannot yet be stated.',
  ADDITIONAL_POSITIVE_PAYROLL:'Further payments prevent a reliable paid-hours figure.',
  QUANTITY_MAPPING_UNSUPPORTED:'Payment evidence does not yet provide a reliable paid-hours figure.',
  APPROVAL_UUID_ORIGIN_NOT_YET_QUALIFIED:'Payment evidence for these protected hours is not yet fully verified.',
  COMPONENT_SET_NOT_ONE_SNAPSHOT:'A complete paid-hours position cannot be established from this bounded read.',
  ROOT_ACTIVITY_NOT_INDEXED:'The complete payment-processing status is not yet available.',
  NEXT_NOT_ACTIVE:'Payment information is not available from the current payment owner.',
  NEXT_READER_UNAVAILABLE:'Payment information is temporarily unavailable.'
});

function officePage(value,actor,root,kind) {
  object(value,['contract','available','reason','request','page']);
  require(value.contract==='WEEKLY_SOURCE_OFFICE_NEXT_PAGE_V1' && boolean(value.available));
  if(!value.available) {
    require(Object.hasOwn(REASON_DETAILS,value.reason) && value.request===null && value.page===null);
    return value;
  }
  require(value.reason===null);
  decodeNextPaidEvidencePage(value.page,value.request);
  require(value.request.actor_user_id===actor && value.request.root_timesheet_id===root
    && value.request.kind===kind && value.request.context_id===null && value.request.after===null && value.request.limit===100);
  return value;
}

// The public Office reader supplies both bounded pages in one STABLE snapshot.
// This consumer independently checks every closed field, not only a successful
// HTTP result, and binds informational freshness separately from command version.
export async function applyNextPaidEvidence(presentation,actor,root) {
  if(presentation.next_paid_evidence===undefined) return presentation; // retained legacy owner
  const evidence=presentation.next_paid_evidence;
  object(evidence,['contract','components','active_holds']);
  require(evidence.contract==='WEEKLY_SOURCE_OFFICE_NEXT_EVIDENCE_V1');
  const components=officePage(evidence.components,actor,root,'COMPONENTS');
  const holds=officePage(evidence.active_holds,actor,root,'ACTIVE_HOLDS');
  if(components.available && holds.available) {
    for(const key of ['root_timesheet_id','work_id','expected_revision_id','module_epoch',
      'current_origin_state','financial_view_revision','quantity_authority_scope'])
      require(components.page.header[key]===holds.page.header[key]);
  }
  const paid=components.available ? nextPaidSchedule(components.page,components.request) : {
    available:false,reason:components.reason,unavailable_class:'POSITION_WITHHELD',source:null,row_count:0,rows:[]};
  if(!paid.available) paid.reason_detail=REASON_DETAILS[paid.reason];
  const processing={available:false,reason:'ROOT_ACTIVITY_NOT_INDEXED',unavailable_class:'POSITION_WITHHELD',
    reason_detail:REASON_DETAILS.ROOT_ACTIVITY_NOT_INDEXED,source:null,row_count:0,rows:[]};
  require(presentation.lifecycle && typeof presentation.lifecycle==='object' && !Array.isArray(presentation.lifecycle));
  const lifecycle={...presentation.lifecycle,schedules:{...presentation.lifecycle.schedules,
    paid_to_date:paid,current_paid:paid,processing},informational:{
      contract:'WEEKLY_SOURCE_OFFICE_NEXT_INFORMATION_V1',read_only:true,
      approved_caption:'Currently approved hours',paid_caption:'Hours paid',processing_caption:'Payment processing hours'}};
  const bytes=new TextEncoder().encode(JSON.stringify({contract:evidence.contract,actor_user_id:actor,
    root_timesheet_id:root,components,active_holds:holds}));
  const digest=await crypto.subtle.digest('SHA-256',bytes);
  const read_fingerprint=Array.from(new Uint8Array(digest),n=>n.toString(16).padStart(2,'0')).join('');
  return {...presentation,lifecycle,next_paid_information:{contract:'WEEKLY_SOURCE_OFFICE_NEXT_INFORMATION_READ_V1',
    read_fingerprint,components:components.available ? components.page : null,
    active_holds:holds.available ? holds.page : null,
    original_cash:components.available ? nextOriginalCashObligations(components.page,components.request) : []}};
}
