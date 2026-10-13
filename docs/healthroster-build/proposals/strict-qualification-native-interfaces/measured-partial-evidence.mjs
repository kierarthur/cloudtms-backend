import {gates,validateQualificationContext,domainHash} from '../protected-source-capability/release-qualification.mjs';
import {nativeInput} from './probe-native-evidence.mjs';
// Byte-preserved actual receipts only. Omitted origin-dependent gates stay
// explicitly HELD; no partial shape is an operational capability certificate.
export function buildMeasuredNativePartial(context,inputs){
 validateQualificationContext(context);
 const needed=Object.keys(gates).filter(gate=>gate!=='AUTOMATIC_COMPONENT_DELTA'||context.capabilities.includes('AUTOMATIC_RECONCILE')).sort();
 if(!Array.isArray(inputs)||!inputs.length||inputs.length>needed.length||new Set(inputs.map(row=>row.gate)).size!==inputs.length
  ||inputs.some(row=>!needed.includes(row.gate)))throw Error('PARTIAL_NATIVE_GATE_CENSUS_INVALID');
 const measured=inputs.map(input=>nativeInput(context,input,input.gate,gates[input.gate],'PROTECTED_CAPABILITY_'+input.gate)).sort((a,b)=>a.gate.localeCompare(b.gate));
 const body={schema_version:'WEEKLY_SOURCE_PROTECTED_NATIVE_PARTIAL_EVIDENCE_V1',phase:'MEASURED_PARTIAL_NON_OPERATIONAL',operational_ready:false,
  release_id:context.release_id,git_commit:context.git_commit,contract_sha256:context.contract_sha256,agency_id:context.agency_id,environment:context.environment,
  source_group_id:context.source_group_id,effective_from_week_ending:context.effective_from_week_ending,effective_to_week_ending:context.effective_to_week_ending,
  capabilities:[...context.capabilities].sort(),required_gate_census:needed,gate_receipts:measured,held_gates:needed.filter(gate=>!measured.some(row=>row.gate===gate))};
 return {...body,partial_evidence_sha256:domainHash(body.schema_version,body)};
}
