// Call only after the real Daily business save/recalculation has succeeded.
// This follow-on prepares native metadata; it never saves or retries a Timesheet.
const UUID=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const HASH=/^[0-9a-f]{64}$/;
const CONTRACT='WEEKLY_SOURCE_SUBMITTED_TIMESHEET_PREPARE_V1';
const record=value=>value!==null&&typeof value==='object'&&!Array.isArray(value);
const closed=(value,keys)=>record(value)&&Object.keys(value).length===keys.length&&keys.every(key=>Object.hasOwn(value,key));
const uuid=value=>typeof value==='string'&&UUID.test(value);
function validate(value,actorUserId,timesheetId){
  if(!closed(value,['ok','contract','actor_user_id','timesheet_id','result'])||value.ok!==true||value.contract!==CONTRACT
    ||value.actor_user_id!==actorUserId||value.timesheet_id!==timesheetId)throw Error('SUBMISSION_CYCLE_PREPARE_RESPONSE_INVALID');
  const result=value.result;
  if(result?.applies===false){
    if(!closed(result,['applies','reason'])||!['NO_TIMESHEET','NO_ACTUAL_SUBMISSION','NO_SIGNED_GROUP'].includes(result.reason))
      throw Error('SUBMISSION_CYCLE_PREPARE_RESPONSE_INVALID');
  }else{
    if(!closed(result,['applies','source_group_id','client_id','week_ending','source_cycle_id','timesheet_id','submission_witness_sha256'])
      ||result.applies!==true||!uuid(result.source_group_id)||!uuid(result.client_id)||!uuid(result.source_cycle_id)
      ||result.timesheet_id!==timesheetId||typeof result.week_ending!=='string'||!/^\d{4}-\d{2}-\d{2}$/.test(result.week_ending)
      ||typeof result.submission_witness_sha256!=='string'||!HASH.test(result.submission_witness_sha256))throw Error('SUBMISSION_CYCLE_PREPARE_RESPONSE_INVALID');
    const date=new Date(`${result.week_ending}T00:00:00Z`);
    if(!Number.isFinite(date.getTime())||date.toISOString().slice(0,10)!==result.week_ending)throw Error('SUBMISSION_CYCLE_PREPARE_RESPONSE_INVALID');
  }
  return value;
}
export async function prepareSubmittedDailyCycleAfterSave({rpc,actorUserId,timesheetId}){
  const actualTimesheetId=uuid(timesheetId)?timesheetId.toLowerCase():null;
  try{
    if(typeof rpc!=='function'||!uuid(actorUserId)||!actualTimesheetId)throw Error('SUBMISSION_CYCLE_PREPARE_CONTEXT_INVALID');
    const actualActorId=actorUserId.toLowerCase();
    // Closed native input: the saved identity and authenticated actor only.
    // Dates, Client, group, cycle, authority and submission all come from SQL.
    const value=await rpc('weekly_source_submitted_timesheet_prepare_atomic_v1',{
      p_request:{actor_user_id:actualActorId,timesheet_id:actualTimesheetId}
    });
    return validate(value,actualActorId,actualTimesheetId);
  }catch{
    // PGRST202, stale witness, permission changes and transport failures have
    // the same truthful boundary: the earlier business save is already done.
    // Do not expose native/private error frames or retry that business save.
    return {ok:false,saved:true,source_readiness:false,
      error_code:'WEEKLY_SOURCE_SUBMISSION_CYCLE_READINESS_FAILED',
      message:'Timesheet saved. Checklist readiness could not be prepared. Refresh the row; do not repeat the save.',
      timesheet_id:actualTimesheetId,current_timesheet_id:actualTimesheetId,
      refresh_required:true,requires_affected_row_refresh:true};
  }
}
