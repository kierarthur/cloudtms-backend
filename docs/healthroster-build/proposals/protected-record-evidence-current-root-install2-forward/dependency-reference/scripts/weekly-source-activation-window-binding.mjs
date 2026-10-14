import {exact,domainHash} from './weekly-source-protected-release-qualification.mjs';
const fail=()=>{throw Error('OPERATIONAL_ACTIVATION_WINDOW_BINDING_INVALID');};
const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,hex=/^[0-9a-f]{64}$/;
const sunday=v=>typeof v==='string'&&/^\d{4}-\d{2}-\d{2}$/.test(v)&&Number.isFinite(Date.parse(v+'T00:00:00Z'))
 &&new Date(v+'T00:00:00Z').toISOString().slice(0,10)===v&&new Date(v+'T00:00:00Z').getUTCDay()===0;
export const activationBindingSchema='WEEKLY_SOURCE_ACTIVATION_WINDOW_BINDING_V1';
export const activationBindingKeys=Object.freeze(['schema_version','assignment_id','assignment_version','assignment_sha256','source_group_id','feature_purpose',
 'rule_set_version_id','rule_values_sha256','effective_from_week_ending','effective_to_week_ending','native_effective_from_week_ending',
 'native_effective_to_week_ending','fixture_cycle_id','fixture_rule_set_version_id','fixture_rule_values_sha256',
 'scope_id','scope_sha256','original_census_sha256','scope_binding_sha256']);
export function assertActivationWindowBinding(value,context,native,closure){
 exact(value,activationBindingKeys,'OPERATIONAL_ACTIVATION_WINDOW_BINDING_INVALID');
 if(value.schema_version!==activationBindingSchema||!['assignment_id','source_group_id','fixture_cycle_id','scope_id'].every(k=>uuid.test(value[k]))
  ||typeof value.assignment_version!=='string'||!/^[1-9][0-9]*$/.test(value.assignment_version)
  ||!['assignment_sha256','scope_sha256','original_census_sha256','scope_binding_sha256'].every(k=>hex.test(value[k]))
  ||!['RULE_SET','PROTECTED_SOURCE'].includes(value.feature_purpose)
  ||!sunday(value.effective_from_week_ending)||!sunday(value.native_effective_from_week_ending)
  ||[value.effective_to_week_ending,value.native_effective_to_week_ending].some(v=>v!==null&&!sunday(v))
  ||(value.effective_to_week_ending!==null&&value.effective_to_week_ending<value.effective_from_week_ending)
  ||(value.native_effective_to_week_ending!==null&&value.native_effective_to_week_ending<value.native_effective_from_week_ending)
  ||value.source_group_id!==context.source_group_id||value.effective_from_week_ending!==context.effective_from_week_ending
  ||value.effective_to_week_ending!==context.effective_to_week_ending
  ||value.native_effective_from_week_ending!==native.effective_from_week_ending||value.native_effective_to_week_ending!==native.effective_to_week_ending
  ||value.effective_from_week_ending<value.native_effective_from_week_ending
  ||value.fixture_cycle_id!==closure.source_cycle_id||value.scope_id!==closure.scope_id||value.scope_sha256!==closure.scope_sha256
  ||value.original_census_sha256!==closure.original_census_sha256)fail();
 if(value.feature_purpose==='PROTECTED_SOURCE'){
  if(value.rule_set_version_id!==null||value.rule_values_sha256!==null
   ||value.fixture_rule_set_version_id!==null||value.fixture_rule_values_sha256!==null
   ||context.capabilities.includes('AUTOMATIC_RECONCILE'))fail();
 }else if(!uuid.test(value.rule_set_version_id)||!hex.test(value.rule_values_sha256)
  ||value.rule_set_version_id!==value.fixture_rule_set_version_id||value.rule_values_sha256!==value.fixture_rule_values_sha256)fail();
 const {scope_binding_sha256,...body}=value;if(domainHash(activationBindingSchema,body)!==scope_binding_sha256)fail();
 return value;
}
export function buildActivationWindowBinding(input,context,native,closure){
 exact(input,activationBindingKeys.filter(k=>!['schema_version','scope_binding_sha256'].includes(k)),'OPERATIONAL_ACTIVATION_WINDOW_BINDING_INVALID');
 const body={schema_version:activationBindingSchema,...input};return assertActivationWindowBinding({...body,scope_binding_sha256:domainHash(activationBindingSchema,body)},context,native,closure);
}
