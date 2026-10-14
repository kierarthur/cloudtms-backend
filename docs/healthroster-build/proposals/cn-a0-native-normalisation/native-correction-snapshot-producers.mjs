import { normaliseRole, normaliseBand, parseCandidateRoleCodes } from './native-daily-role-helpers.mjs';
import { deriveNativeWeeklyCorrectionRoute } from './native-weekly-correction-dispatch.mjs';

const own=(o,k)=>Object.prototype.hasOwnProperty.call(o,k);
const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
const uuid=v=>typeof v==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(v);
const same=(a,b)=>uuid(a)&&uuid(b)&&a.toLowerCase()===b.toLowerCase();
function refuse(code){const e=new Error(code);e.code=code;e.status=409;throw e;}
function required(value,code){if(!value)refuse(code);}

// Match ordinary JSON transport's omission of object undefined fields (the
// native ONE_PER_WEEK additional owner emits days:undefined). Do not coerce
// money, stringify unknown objects, accept non-finite numbers, or fill arrays.
export function canonicalCorrectionWire(value,depth=0){
  if(depth>48)refuse('CORRECTION_CANONICAL_WIRE_DEPTH_INVALID');
  if(value===null||typeof value==='string'||typeof value==='boolean')return value;
  if(typeof value==='number'){required(Number.isFinite(value),'CORRECTION_CANONICAL_WIRE_NUMBER_INVALID');return value;}
  if(Array.isArray(value))return value.map(item=>{
    required(item!==undefined,'CORRECTION_CANONICAL_WIRE_ARRAY_INVALID');return canonicalCorrectionWire(item,depth+1);
  });
  required(object(value)&&(Object.getPrototypeOf(value)===Object.prototype||Object.getPrototypeOf(value)===null),'CORRECTION_CANONICAL_WIRE_SHAPE_INVALID');
  const out={};
  for(const k of Object.keys(value).sort()){
    required(!['__proto__','prototype','constructor'].includes(k),'CORRECTION_CANONICAL_WIRE_KEY_INVALID');
    if(value[k]!==undefined)out[k]=canonicalCorrectionWire(value[k],depth+1);
  }
  return out;
}
const jsonEqual=(a,b)=>JSON.stringify(canonicalCorrectionWire(a))===JSON.stringify(canonicalCorrectionWire(b));
function jsonObject(value,code){
  let out=value;
  if(typeof out==='string'){try{out=JSON.parse(out);}catch{refuse(code);}}
  required(object(out),code);return out;
}
function finiteFact(value){return (typeof value==='number'&&Number.isFinite(value))||
  (typeof value==='string'&&/^-?\d+(?:\.\d+)?$/.test(value)&&Number.isFinite(Number(value)));}

function weeklyRateAuthority(snapshot,ts,contract,anyMissingRates){
  const buckets=['day','night','sat','sun','bh'];
  const hours={},pay={},charge={};
  for(const b of buckets){
    required(finiteFact(snapshot['hours_'+b]),'CORRECTION_CANONICAL_HOURS_AUTHORITY_INVALID');
    hours[b]=Math.abs(Number(snapshot['hours_'+b]));
    pay[b]=snapshot['pay_'+b];charge[b]=snapshot['charge_'+b];
    if(hours[b]>0&&(!finiteFact(pay[b])||!finiteFact(charge[b])))return true;
  }
  const missing=anyMissingRates(hours,pay,charge);
  required(typeof missing==='boolean','CORRECTION_CANONICAL_RATE_AUTHORITY_INVALID');
  if(missing)return true;
  // Qualify actual additional rate facts without calculating any amount.
  // Missing configs must never turn an existing nonzero claim into £0.
  const claimed=new Set();
  for(const field of ['additional_units_week','additional_units_per_day']){
    if(ts[field]==null)continue;
    const units=jsonObject(ts[field],'CORRECTION_CANONICAL_ADDITIONAL_AUTHORITY_INVALID');
    for(const [code,value] of Object.entries(units)){
      const vals=field==='additional_units_per_day'?Object.values(jsonObject(value,'CORRECTION_CANONICAL_ADDITIONAL_AUTHORITY_INVALID')):[value];
      for(const v of vals){required(finiteFact(v),'CORRECTION_CANONICAL_ADDITIONAL_AUTHORITY_INVALID');if(Number(v)!==0)claimed.add(code.toUpperCase());}
    }
  }
  const produced=jsonObject(snapshot.additional_units_json??{},'CORRECTION_CANONICAL_ADDITIONAL_AUTHORITY_INVALID');
  for(const code of Object.keys(produced))claimed.add(code.toUpperCase());
  if(!claimed.size)return false;
  required(Array.isArray(contract.additional_rates_json),'CORRECTION_CANONICAL_ADDITIONAL_AUTHORITY_INVALID');
  for(const code of claimed){
    const rates=contract.additional_rates_json.filter(r=>object(r)&&String(r.code??'').toUpperCase()===code);
    if(rates.length!==1||!finiteFact(rates[0].pay_rate)||!finiteFact(rates[0].charge_rate))return true;
  }
  return false;
}

/** Read-only original/P0 producer binding. All calculations are native owners.
 * No queue preparation, dequeue, snapshot write, financial recalculation RPC,
 * manager renderer, notification, payment, provider or settlement is invoked.
 */
export function createNativeCorrectionSnapshotProducers(deps){
  for(const name of ['readContext','readWeeklyContext','readShifts','readWeeklyValidation','resolveRates',
    'buildWeekly','deriveDaily','buildDaily','toLocalParts','subtractBreak','classifyMinutes','resolveEffectivePayChannel','anyMissingRates']){
    required(typeof deps?.[name]==='function','CORRECTION_CANONICAL_NATIVE_OWNER_UNAVAILABLE');
  }
  async function build({prepare,actor},which){
    required(actor?.environment==='TEST'&&uuid(actor?.actor_user_id),'CORRECTION_CANONICAL_ACTOR_INVALID');
    const a=prepare?.authority,original=which==='ORIGINAL';
    required(object(a)&&uuid(prepare.candidate_id)&&uuid(prepare.client_id),'CORRECTION_CANONICAL_CONTEXT_INVALID');
    if(original)required(prepare.original_materialisation_required===true&&prepare.original_authorisation_required===true&&
      prepare.original_current_snapshot===null&&a.original_financial_id===null&&a.original_financial_authority_hash===null,
      'CORRECTION_CANONICAL_ORIGINAL_CONTEXT_INVALID');
    const expectedTs=original?prepare.frozen_original?.timesheet:prepare.replacement_timesheet;
    const id=original?a.original_timesheet_id:a.replacement_timesheet_id;
    const version=original?a.original_timesheet_version:a.replacement_version;
    required(uuid(id)&&Number.isSafeInteger(version)&&version>0&&object(expectedTs)&&same(expectedTs.timesheet_id,id)&&
      expectedTs.version===version&&typeof expectedTs.booking_id==='string'&&expectedTs.booking_id.length>0,'CORRECTION_CANONICAL_TARGET_INVALID');
    const rows=await deps.readContext([id]);
    required(Array.isArray(rows)&&rows.length===1,'CORRECTION_CANONICAL_CONTEXT_AMBIGUOUS');
    const ctx=rows[0],ts=ctx?.out_timesheet;
    required(same(ctx?.effective_timesheet_id,id)&&same(ts?.timesheet_id,id)&&ts.version===version&&ts.booking_id===expectedTs.booking_id&&
      ts.is_current===true&&ts.archived_at_utc==null&&ts.authorised_at_server==null&&same(ctx?.out_candidate?.id,prepare.candidate_id)&&
      same(ctx?.out_client_id,prepare.client_id)&&object(ctx?.out_policy)&&object(ctx?.out_effective_flags),'CORRECTION_CANONICAL_TARGET_CHANGED');
    required(jsonEqual(ts.actual_schedule_json??null,original?expectedTs.actual_schedule_json??null:a.replacement_schedule??null)&&
      jsonEqual(ts.settings_authority_json??null,expectedTs.settings_authority_json??null), 'CORRECTION_CANONICAL_SOURCE_CHANGED');
    for(const f of ['worked_start_iso','worked_end_iso','break_start_iso','break_end_iso','break_minutes','additional_units_week','additional_units_per_day']){
      required(jsonEqual(ts[f]??null,expectedTs[f]??null),'CORRECTION_CANONICAL_SOURCE_CHANGED');
    }
    if(original)required(ctx.out_cur_fin==null,'CORRECTION_CANONICAL_ORIGINAL_ALREADY_MATERIALISED');
    else{
      required(ts.correction_id===expectedTs.correction_id&&ts.correction_kind==='WHOLE_TIMESHEET_REPLACEMENT'&&
        same(ts.parent_timesheet_id,a.root_timesheet_id),'CORRECTION_CANONICAL_P0_CONTEXT_INVALID');
      const fin=ctx.out_cur_fin;
      required((a.replacement_financial_id==null&&fin==null)||(same(fin?.id,a.replacement_financial_id)&&same(fin?.timesheet_id,id)&&
        fin.timesheet_version===version&&same(fin.candidate_id,prepare.candidate_id)&&same(fin.client_id,prepare.client_id)&&
        fin.is_current===true&&fin.authorised_at_utc==null&&fin.paid_at_utc==null&&fin.locked_by_invoice_id==null),'CORRECTION_CANONICAL_P0_FINANCE_CHANGED');
    }
    let snapshot;
    if(ts.sheet_scope==='WEEKLY'){
      const weekly=await deps.readWeeklyContext([id]);
      required(Array.isArray(weekly)&&weekly.length===1&&same(weekly[0]?.timesheet_id,id),'CORRECTION_CANONICAL_WEEKLY_CONTEXT_AMBIGUOUS');
      const cw=weekly[0].out_cw,contract=weekly[0].out_contract;
      required(object(cw)&&object(contract)&&same(cw.timesheet_id,id)&&same(cw.contract_id,contract.id)&&same(ts.contract_id,contract.id)&&
        same(contract.candidate_id,prepare.candidate_id)&&same(contract.client_id,prepare.client_id),'CORRECTION_CANONICAL_WEEKLY_TARGET_CHANGED');
      const shiftRows=await deps.readShifts([id]);
      required(Array.isArray(shiftRows)&&shiftRows.length===1&&same(shiftRows[0]?.timesheet_id,id),'CORRECTION_CANONICAL_IMPORTED_CONTEXT_AMBIGUOUS');
      let shifts=shiftRows[0].shifts;
      if(typeof shifts==='string'){try{shifts=JSON.parse(shifts);}catch{refuse('CORRECTION_CANONICAL_IMPORTED_CONTEXT_INVALID');}}
      required(Array.isArray(shifts),'CORRECTION_CANONICAL_IMPORTED_CONTEXT_INVALID');
      const validations=await deps.readWeeklyValidation(id);
      required(Array.isArray(validations)&&validations.length<=2&&validations.every(v=>same(v.timesheet_id,id)),'CORRECTION_CANONICAL_VALIDATION_AMBIGUOUS');
      if(validations.length===2){
        // The native enrichment owner orders validation by updated_at DESC.
        // Read one extra row to refuse an ambiguous latest timestamp; an older
        // history row is not itself a contradiction or a fabricated status.
        const first=Date.parse(validations[0].updated_at),second=Date.parse(validations[1].updated_at);
        required(Number.isFinite(first)&&Number.isFinite(second)&&first>second,'CORRECTION_CANONICAL_VALIDATION_AMBIGUOUS');
      }
      const route=deriveNativeWeeklyCorrectionRoute(ts,ctx.out_cur_fin,ctx.out_effective_flags);
      snapshot=await deps.buildWeekly({env:deps.env,ts,curFin:ctx.out_cur_fin??null,effFlags:ctx.out_effective_flags,cw,contract,
        policySql:ctx.out_policy,shifts,validation:validations[0]??null,...route,
        // Server-only purpose/context; never forwarded from the Office payload.
        correctionPurpose:original?'MISSING_ORIGINAL_A0':'REPLACEMENT_P0',authority:a,
        frozenOriginal:original?prepare.frozen_original:null,candidateId:prepare.candidate_id,clientId:prepare.client_id});
      required(object(snapshot),'CORRECTION_CANONICAL_SNAPSHOT_INVALID');
      const hasRateIssue=weeklyRateAuthority(snapshot,ts,contract,deps.anyMissingRates);
      const channel=deps.resolveEffectivePayChannel({pay_method:snapshot.pay_method,candidate:ctx.out_candidate,umbrella:ctx.out_umbrella??null});
      required(object(channel)&&typeof channel.ok==='boolean','CORRECTION_CANONICAL_PAY_CHANNEL_AUTHORITY_INVALID');
      required(!own(snapshot,'has_rate_issue')||typeof snapshot.has_rate_issue==='boolean','CORRECTION_CANONICAL_RATE_AUTHORITY_INVALID');
      required(!own(snapshot,'has_pay_channel_issue')||typeof snapshot.has_pay_channel_issue==='boolean','CORRECTION_CANONICAL_PAY_CHANNEL_AUTHORITY_INVALID');
      snapshot={...snapshot,has_rate_issue:hasRateIssue||snapshot.has_rate_issue===true,
        has_pay_channel_issue:!channel.ok||snapshot.has_pay_channel_issue===true};
    }else if(ts.sheet_scope==='DAILY'){
      const daily=deps.deriveDaily({effectiveTimesheetId:id,context:ctx,toLocalParts:deps.toLocalParts,parseCandidateRoleCodes,
        normaliseRole,normaliseBand});
      required(daily?.key===id&&same(daily.candidateId,prepare.candidate_id)&&same(daily.clientId,prepare.client_id)&&
        typeof daily.workedDateYmd==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(daily.workedDateYmd),'CORRECTION_CANONICAL_DAILY_CONTEXT_INVALID');
      const rates=await deps.resolveRates([daily.rateRequest]);
      required(Array.isArray(rates)&&rates.length===1&&rates[0]?.k===id,'CORRECTION_CANONICAL_DAILY_RATES_AMBIGUOUS');
      snapshot=deps.buildDaily({env:deps.env,timesheet:ts,currentFinancials:ctx.out_cur_fin??null,policy:daily.policy,candidate:daily.candidate,
        umbrella:ctx.out_umbrella??null,candidateId:daily.candidateId,candidateAssignment:daily.candidateAssignment,clientId:daily.clientId,
        role:daily.roleForRates,band:daily.bandForRates,rateType:daily.rateType,ratesRow:rates[0],effectiveFlags:ctx.out_effective_flags,
        preserveUnprocessed:String(ctx.out_cur_fin?.processing_status??'').toUpperCase()==='UNPROCESSED',subtractBreak:deps.subtractBreak,
        classifyMinutes:deps.classifyMinutes,resolveEffectivePayChannel:deps.resolveEffectivePayChannel});
    }else refuse('CORRECTION_CANONICAL_SCOPE_UNSUPPORTED');
    required(object(snapshot)&&same(snapshot.timesheet_id,id)&&snapshot.timesheet_version===version&&same(snapshot.candidate_id,prepare.candidate_id)&&
      same(snapshot.client_id,prepare.client_id)&&snapshot.has_rate_issue===false&&snapshot.has_pay_channel_issue===false,
      'CORRECTION_CANONICAL_FINANCIAL_ADMISSION_BLOCKED');
    required(!own(snapshot,'is_stale')||snapshot.is_stale===false,'CORRECTION_CANONICAL_FINANCIAL_ADMISSION_BLOCKED');
    return canonicalCorrectionWire(snapshot);
  }
  return Object.freeze({buildOriginalSnapshot:input=>build(input,'ORIGINAL'),buildReplacementSnapshot:input=>build(input,'REPLACEMENT')});
}
