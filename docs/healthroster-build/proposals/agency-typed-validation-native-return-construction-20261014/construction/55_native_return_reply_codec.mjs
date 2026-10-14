const UUID=/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/,SHA=/^[0-9a-f]{64}$/;
const keys=['schema_version','request_id','request_version','phase','view_actor_user_id','timesheet_id',
 'timesheet_version','completed','last_refusal_code','completed_actor_user_id','return_invoice_no','completion_sha256'];
const own=v=>v!==null&&typeof v==='object'&&!Array.isArray(v)&&Object.getPrototypeOf(v)===Object.prototype;
const fail=()=>{throw Object.assign(new Error('The same-draft action could not be confirmed.'),{code:'AGENCY_CHANGE_NATIVE_RETURN_RESULT_UNCONFIRMED'});};
// Transport decoder only. It accepts the literal native mechanical RPC reply;
// it cannot generate validation, Return, completion or financial qualification.
export function decodeAgencyReturnStep(value,expected){
 if(!own(expected)||Object.keys(expected).sort().join(',')!==['actor_user_id','request_id','timesheet_id','timesheet_version'].sort().join(',')
  ||typeof expected.actor_user_id!=='string'||typeof expected.request_id!=='string'||typeof expected.timesheet_id!=='string'
  ||!UUID.test(expected.actor_user_id)||!UUID.test(expected.request_id)||!UUID.test(expected.timesheet_id)
  ||!Number.isSafeInteger(expected.timesheet_version)||expected.timesheet_version<1)fail();
 if(!own(value)||Object.keys(value).sort().join(',')!==[...keys].sort().join(',')
  ||new TextEncoder().encode(JSON.stringify(value)).byteLength>8192||value.schema_version!=='WEEKLY_SOURCE_AGENCY_NATIVE_RETURN_STEP_RESULT_V1'
  ||value.request_id!==expected.request_id||value.timesheet_id!==expected.timesheet_id
  ||value.timesheet_version!==expected.timesheet_version||value.view_actor_user_id!==expected.actor_user_id
  ||!Number.isSafeInteger(value.request_version)||value.request_version<1)fail();
 if(value.phase==='AUTHORISED'){
  if(value.completed!==false||typeof value.last_refusal_code!=='string'||!/^[A-Z][A-Z0-9_]{0,199}$/.test(value.last_refusal_code)
   ||value.completed_actor_user_id!==null||value.return_invoice_no!==null||value.completion_sha256!==null)fail();
 }else if(value.phase==='COMPLETED'){
  if(value.completed!==true||value.last_refusal_code!==null||typeof value.completed_actor_user_id!=='string'||!UUID.test(value.completed_actor_user_id)
   ||typeof value.completion_sha256!=='string'||!SHA.test(value.completion_sha256)
   ||(value.return_invoice_no!==null&&(typeof value.return_invoice_no!=='string'||value.return_invoice_no.length<1||value.return_invoice_no.length>200)))fail();
 }else fail();
 return JSON.parse(JSON.stringify(value));
}
