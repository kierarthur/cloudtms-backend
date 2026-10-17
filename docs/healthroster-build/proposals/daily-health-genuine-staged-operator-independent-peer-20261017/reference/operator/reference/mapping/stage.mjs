// Exact existing API request specification only. No executable transport or admission stub.
export const STAGE=Object.freeze({version:'DAILY_GENUINE_HOSPITAL_MAPPING_S1A_V1',status:'SOURCE_ONLY_ROOT_REVIEW_REQUIRED',
 after:'S1_CLIENT_AND_INITIAL_SETTINGS_READBACK',before:'S4_MANUAL_DAILY_CREATE',method:'POST',
 path:'/api/clients/f717d043-0000-4000-8000-000000000010/hospitals',
 body:Object.freeze({id:'f717d043-0000-4000-8000-000000000020',hospital_name_norm:Object.freeze(['daily_health_basis_20261017_client']),ward_hint:null}),
 executionEntryPoints:Object.freeze([]),native:'NOT_RUN'});
export function assertExactRequest(value){
 if(!value||typeof value!=='object'||Object.keys(value).sort().join(',')!=='body,method,path'||value.method!==STAGE.method||value.path!==STAGE.path
  ||!value.body||typeof value.body!=='object'||Object.keys(value.body).sort().join(',')!=='hospital_name_norm,id,ward_hint'
  ||value.body.id!==STAGE.body.id||value.body.ward_hint!==null||!Array.isArray(value.body.hospital_name_norm)||value.body.hospital_name_norm.length!==1||value.body.hospital_name_norm[0]!==STAGE.body.hospital_name_norm[0])throw Error('S1A_EXACT_REVIEWED_REQUEST_REQUIRED');
 return true;
}
