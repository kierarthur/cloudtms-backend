/* DR23: read-only advisory continuation; no money value enters financial intake. */
const PN_UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const PN_INT=/^(0|[1-9]\d{0,18})$/;
const PN_MONEY=/^(0|[1-9]\d{0,15})\.\d{2}$/;
const PN_CARRY=/^-?(0|[1-9]\d{0,35})\.\d{2}$/;
const pnRecord=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
const pnExact=(v,k)=>pnRecord(v)&&Object.keys(v).length===k.length&&k.every(k=>Object.hasOwn(v,k));
const pnBytes=v=>new TextEncoder().encode(JSON.stringify(v)).byteLength;
const pnSame=(a,b)=>a===b||pnRecord(a)&&pnRecord(b)&&Object.keys(a).length===Object.keys(b).length&&Object.keys(a).every(k=>Object.hasOwn(b,k)&&pnSame(a[k],b[k]));
const PN_CONTEXT=['actor_user_id','owner_epoch','run_id','run_worker_id','candidate_id','preparation_revision','selection_revision','expected_projection_revision','financial_view_revision','net_request_revision','net_request_high_water','capacity_revision','week_revision','draft_state_id','projection_id','entered_paye_net'];
const PN_CARRY_KEYS=['requires_week','prechecked','processed','e','h','net_additions','gross_additions','gross_deductions','recoveries','before_advance','after_advance','before_overpayment','after_overpayment','before_debt','after_debt'];
const PN_SUMMARY=['accepted_net','accepted_recoveries','accepted_cash','proposed_net','proposed_recoveries','proposed_cash','net_additions','gross_additions','gross_deductions','advance_before','advance_after','overpayment_before','overpayment_after','debt_before','debt_after'];
const pnInteger=v=>typeof v==='string'&&PN_INT.test(v)&&BigInt(v)<=9223372036854775807n;
const pnUuid=v=>typeof v==='string'&&PN_UUID.test(v);
const pnMoney=v=>typeof v==='string'&&PN_MONEY.test(v);
function pnNet(v){if(typeof v!=='string'||!/^(0|[1-9]\d{0,15})(?:\.\d{1,2})?$/.test(v))return null;const [w,f='']=v.split('.');return w+'.'+f.padEnd(2,'0');}
function pnContext(v){
 if(!pnExact(v,PN_CONTEXT)||!['actor_user_id','run_id','run_worker_id','candidate_id'].every(k=>pnUuid(v[k]))
  ||!['owner_epoch','preparation_revision','selection_revision','expected_projection_revision','financial_view_revision','net_request_revision','net_request_high_water','capacity_revision'].every(k=>pnInteger(v[k]))
  ||v.owner_epoch==='0'||v.week_revision!==null&&!pnInteger(v.week_revision)
  ||v.draft_state_id!==null&&!pnUuid(v.draft_state_id)||v.projection_id!==null&&!pnUuid(v.projection_id)||!pnMoney(v.entered_paye_net)
  ||v.selection_revision!=='0'&&BigInt(v.net_request_high_water)<BigInt(v.expected_projection_revision)
  ||v.net_request_revision!==v.expected_projection_revision
  ||(v.expected_projection_revision==='0')!==(v.projection_id===null)
  ||(v.selection_revision==='0')!==(v.draft_state_id===null)
  ||(v.selection_revision==='0')!==(v.week_revision===null))return false;
 return true;
}
function pnCursor(v){
 if(!pnExact(v,['context','stage','instruction_after','run_after','carry'])||pnBytes(v)>8192||!pnContext(v.context)
  ||!['PRECHECK','PRIOR_RUNS','WEEK_CAPTURE','ALLOCATE'].includes(v.stage)||!pnExact(v.carry,PN_CARRY_KEYS)
  ||typeof v.carry.requires_week!=='boolean'||!pnInteger(v.carry.prechecked)||!pnInteger(v.carry.processed)
  ||!PN_CARRY_KEYS.slice(3).every(k=>typeof v.carry[k]==='string'&&PN_CARRY.test(v.carry[k])&&(k==='h'||!v.carry[k].startsWith('-')))
  ||BigInt(v.carry.processed)>BigInt(v.carry.prechecked))return false;
 const a=v.instruction_after,r=v.run_after;
 if(a!==null&&(!pnExact(a,['age_key','case_id','component_ordinal','case_component_id'])
  ||typeof a.age_key!=='string'||! /^-?(0|[1-9]\d{0,18})$/.test(a.age_key)
  ||BigInt(a.age_key)<-9223372036854775808n||BigInt(a.age_key)>9223372036854775807n
  ||!pnInteger(a.component_ordinal)||a.component_ordinal==='0'||BigInt(a.component_ordinal)>2147483647n
  ||!pnUuid(a.case_id)||!pnUuid(a.case_component_id)))return false;
 if(r!==null&&(!pnExact(r,['pay_date','created_at_utc','id'])||typeof r.pay_date!=='string'
  ||!/^\d{4}-\d{2}-\d{2}$/.test(r.pay_date)||!Number.isFinite(Date.parse(r.pay_date))
  ||typeof r.created_at_utc!=='string'||!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$/.test(r.created_at_utc)
  ||!Number.isFinite(Date.parse(r.created_at_utc))||!pnUuid(r.id)))return false;
 if(['PRECHECK','ALLOCATE'].includes(v.stage)?r!==null:a!==null)return false;
 if(['PRIOR_RUNS','WEEK_CAPTURE'].includes(v.stage)&&!v.carry.requires_week)return false;
 return true;
}
function payeNetPreviewInput(v){
 return pnExact(v,['run_worker_id','entered_paye_net','after','limit'])&&pnUuid(v.run_worker_id)&&pnNet(v.entered_paye_net)!==null
  &&Number.isInteger(v.limit)&&v.limit>=1&&v.limit<=100&&(v.after===null||pnCursor(v.after))
  &&(v.after===null||v.after.context.run_worker_id===v.run_worker_id.toLowerCase()&&v.after.context.entered_paye_net===pnNet(v.entered_paye_net));
}
function payeNetPreviewResult(v,args,actor){
 if(!payeNetPreviewInput(args)||!pnExact(v,['context','complete','rows_visited','recovery_rows','next_cursor','summary'])
  ||!pnContext(v.context)||v.context.run_worker_id!==args.run_worker_id.toLowerCase()||v.context.actor_user_id!==actor.toLowerCase()
  ||v.context.entered_paye_net!==pnNet(args.entered_paye_net)||args.after!==null&&!pnSame(v.context,args.after.context)
  ||typeof v.complete!=='boolean'||!pnInteger(v.rows_visited)||BigInt(v.rows_visited)>BigInt(args.limit)
  ||!Array.isArray(v.recovery_rows)||v.recovery_rows.length>Number(v.rows_visited)||pnBytes(v)>120*1024
  ||v.complete!==(v.next_cursor===null)||v.complete!==(v.summary!==null))return false;
 const ids=new Set();
 for(const row of v.recovery_rows){if(!pnExact(row,['instruction_id','case_id','case_component_id','case_kind','before','after'])
  ||!['instruction_id','case_id','case_component_id'].every(k=>pnUuid(row[k]))
  ||!['LOAN','ADVANCE','MANUAL_DEBT','OVERPAYMENT'].includes(row.case_kind)
  ||!pnMoney(row.after)||row.before!==null&&!pnMoney(row.before)
  ||(v.context.projection_id===null)!==(row.before===null)||ids.has(row.instruction_id))return false;ids.add(row.instruction_id);}
 if(!v.complete){
  const n=v.next_cursor;if(!pnCursor(n)||!pnSame(n.context,v.context)||args.after!==null&&pnSame(n,args.after))return false;
  if(args.after!==null){const stages=['PRECHECK','PRIOR_RUNS','WEEK_CAPTURE','ALLOCATE'];
   if(stages.indexOf(n.stage)<stages.indexOf(args.after.stage)||BigInt(n.carry.prechecked)<BigInt(args.after.carry.prechecked)
    ||BigInt(n.carry.processed)<BigInt(args.after.carry.processed))return false;}
 }else{
  const s=v.summary;if(!pnExact(s,PN_SUMMARY)||!PN_SUMMARY.every(k=>s[k]===null?['accepted_net','accepted_recoveries','accepted_cash','advance_before','overpayment_before','debt_before'].includes(k):pnMoney(s[k]))
   ||s.proposed_net!==v.context.entered_paye_net||BigInt(s.proposed_cash.replace('.',''))!==BigInt(s.proposed_net.replace('.',''))+BigInt(s.net_additions.replace('.',''))-BigInt(s.proposed_recoveries.replace('.',''))
   ||!['accepted_net','accepted_recoveries','accepted_cash','advance_before','overpayment_before','debt_before'].every(k=>v.context.projection_id===null?s[k]===null:pnMoney(s[k])))return false;
  const cents=v=>BigInt(v.replace('.',''));
  if(cents(s.advance_after)+cents(s.overpayment_after)+cents(s.debt_after)!==cents(s.proposed_recoveries)
   ||v.context.projection_id!==null&&(cents(s.advance_before)+cents(s.overpayment_before)+cents(s.debt_before)!==cents(s.accepted_recoveries)
     ||cents(s.accepted_cash)!==cents(s.accepted_net)+cents(s.net_additions)-cents(s.accepted_recoveries)))return false;
 }
 return true;
}

export {payeNetPreviewInput,payeNetPreviewResult};
