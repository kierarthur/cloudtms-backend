import {gates,validateQualificationContext,exact,sha256,domainHash} from '../protected-source-capability/release-qualification.mjs';
const fail=code=>{throw Error(code);};
const negativeSecurity=Object.freeze(gates.SECURITY.filter(key=>key!=='active_admin_activation'));

// A probe input never contains or manufactures the missing activation positive.
// Only actual byte-preserved native envelopes can supply the other gates; the
// root trusted collector still establishes that their journeys happened.
export function nativeInput(context,input,gate,assertionKeys,journeyId){
 exact(input,['gate','receipt_id','receipt_bytes','expected_receipt_sha256'],'ACTIVATION_PROBE_NATIVE_INPUT_INVALID');
 if(input.gate!==gate||typeof input.receipt_id!=='string'||!/^[A-Za-z0-9_./:@-]{1,256}$/.test(input.receipt_id)
  ||!(Buffer.isBuffer(input.receipt_bytes)||typeof input.receipt_bytes==='string')
  ||sha256(input.receipt_bytes)!==input.expected_receipt_sha256)fail('ACTIVATION_PROBE_NATIVE_ARTIFACT_INVALID');
 const row=JSON.parse(input.receipt_bytes.toString('utf8'));
 exact(row,['schema_version','gate','release_id','source_commit','contract_sha256','agency_id','environment','database','source_group_id',
  'effective_from_week_ending','effective_to_week_ending','execution','native_execution_receipt','assertions'],'ACTIVATION_PROBE_NATIVE_RECEIPT_INVALID');
 if(row.schema_version!=='WEEKLY_SOURCE_NATIVE_JOURNEY_RECEIPT_V1'||row.gate!==gate||row.execution!=='AGENCY_TEST_NATIVE'
  ||row.release_id!==context.release_id||row.source_commit!==context.git_commit||row.contract_sha256!==context.contract_sha256
  ||['agency_id','environment','database','source_group_id','effective_from_week_ending','effective_to_week_ending'].some(key=>row[key]!==context[key]))
  fail('ACTIVATION_PROBE_NATIVE_SCOPE_INVALID');
 exact(row.native_execution_receipt,['accepted','database','rolledBack','connectedFresh','tool_receipt_sha256','native_journey_id'],'ACTIVATION_PROBE_EXECUTION_INVALID');
 const execution=row.native_execution_receipt;
 if(execution.accepted!==true||execution.database!==context.database||execution.rolledBack!==true||execution.connectedFresh!==true
  ||!/^[0-9a-f]{64}$/.test(execution.tool_receipt_sha256)||execution.native_journey_id!==journeyId)fail('ACTIVATION_PROBE_EXECUTION_INVALID');
 exact(row.assertions,assertionKeys,'ACTIVATION_PROBE_ASSERTION_CENSUS_INVALID');
 if(Object.values(row.assertions).some(value=>value!==true))fail('ACTIVATION_PROBE_NATIVE_ASSERTION_FAILED');
 return {gate,status:gate==='SECURITY_NEGATIVES'?'NEGATIVE_PASS':'PASS',execution:'AGENCY_TEST_NATIVE',database:context.database,
  receipt_id:input.receipt_id,receipt_sha256:input.expected_receipt_sha256,source_commit:context.git_commit,contract_sha256:context.contract_sha256,
  source_group_id:context.source_group_id,agency_id:context.agency_id,environment:context.environment};
}
export function buildActivationProbeNative(context,inputs,securityInput){
 validateQualificationContext(context);
 const needed=Object.keys(gates).filter(gate=>gate!=='SECURITY'&&(gate!=='AUTOMATIC_COMPONENT_DELTA'||context.capabilities.includes('AUTOMATIC_RECONCILE')));
 if(!Array.isArray(inputs)||inputs.length!==needed.length||new Set(inputs.map(row=>row.gate)).size!==needed.length
  ||inputs.some(row=>!needed.includes(row.gate)))fail('ACTIVATION_PROBE_OTHER_GATE_CENSUS_INVALID');
 const rows=inputs.map(input=>nativeInput(context,input,input.gate,gates[input.gate],'PROTECTED_CAPABILITY_'+input.gate)).sort((a,b)=>a.gate.localeCompare(b.gate));
 const security=nativeInput(context,securityInput,'SECURITY_NEGATIVES',negativeSecurity,'PROTECTED_CAPABILITY_SECURITY_NEGATIVES');
 const body={schema_version:'WEEKLY_SOURCE_ACTIVATION_PROBE_NATIVE_OTHER_GATES_V1',release_id:context.release_id,git_commit:context.git_commit,
  contract_sha256:context.contract_sha256,agency_id:context.agency_id,environment:context.environment,source_group_id:context.source_group_id,
  effective_from_week_ending:context.effective_from_week_ending,effective_to_week_ending:context.effective_to_week_ending,
  capabilities:[...context.capabilities].sort(),gate_receipts:rows,negative_security_receipt:security};
 return {...body,other_gates_sha256:domainHash(body.schema_version,body)};
}
export function validateProbeAssignment(value,context,closure,nativeContext){
 exact(value,['scope_id','scope_sha256','original_census_sha256','setup_actor_user_id','assignment_id','expected_assignment_version',
  'expected_assignment_sha256','source_group_id','effective_from_week_ending','effective_to_week_ending','rule_set_version_id',
  'native_effective_from_week_ending','native_effective_to_week_ending'],'ACTIVATION_PROBE_ASSIGNMENT_SCOPE_INVALID');
 const uuid=/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
 if(!uuid.test(value.scope_id)||!uuid.test(value.setup_actor_user_id)||!uuid.test(value.assignment_id)
  ||!/^\d+$/.test(value.expected_assignment_version)||BigInt(value.expected_assignment_version)<1n
  ||!['scope_sha256','original_census_sha256','expected_assignment_sha256'].every(key=>/^[0-9a-f]{64}$/.test(value[key]))
  ||value.rule_set_version_id!==null||value.source_group_id!==context.source_group_id
  ||value.effective_from_week_ending!==context.effective_from_week_ending||value.effective_to_week_ending!==context.effective_to_week_ending
  ||value.native_effective_from_week_ending!==nativeContext.effective_from_week_ending
  ||value.native_effective_to_week_ending!==nativeContext.effective_to_week_ending
  ||context.effective_from_week_ending<nativeContext.effective_from_week_ending
  ||value.original_census_sha256!==closure.original_census_sha256)fail('ACTIVATION_PROBE_ASSIGNMENT_SCOPE_INVALID');
 return value;
}
