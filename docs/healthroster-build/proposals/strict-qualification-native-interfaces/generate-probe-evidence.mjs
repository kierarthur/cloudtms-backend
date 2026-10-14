import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),old=path.resolve(out,'../protected-source-qualification-operational-gate');
const source=fs.readFileSync(path.join(old,'operational-evidence.mjs'),'utf8');
const start=source.indexOf('export function buildOperationalQualification('),end=source.indexOf('\nexport function operationalCertificate(');
assert.ok(start>0&&end>start);
let prefix=source.slice(0,start).replace("from './source-equivalence.mjs'","from '../strict-qualification-git-path-parser-forward/source-equivalence.mjs'");
prefix=prefix.replace('export const operationalSchema=', 'export const probeSchema=').replace("'WEEKLY_SOURCE_PROTECTED_CAPABILITY_OPERATIONAL_QUALIFICATION_V1'","'WEEKLY_SOURCE_ACTIVATION_PROBE_AUTHORIZATION_V1'");
prefix+="import {buildActivationProbeNative,validateProbeAssignment} from './probe-native-evidence.mjs';\n";
let body=source.slice(start,end);
const replace=(before,after)=>{assert.equal(body.split(before).length-1,1);body=body.replace(before,()=>after);};
replace('buildOperationalQualification(context,nativeContext,nativeInputs,remoteInputs,setupClosure,sourceProof)',
 'buildActivationProbeAuthorization(context,nativeContext,nativeInputs,securityInput,remoteInputs,setupClosure,sourceProof,assignmentScope)');
replace("if(nativeContext.release_id===context.release_id||nativeContext.contract_sha256!==context.contract_sha256",
 "if((nativeContext.release_id===context.release_id&&nativeContext.git_commit!==context.git_commit)||nativeContext.contract_sha256!==context.contract_sha256");
replace('const native=buildQualification(nativeContext,nativeInputs); // Original 9/10 gates retained; no aliases for CLIENT_API/DOCUMENT.',
 'const native=buildActivationProbeNative(nativeContext,nativeInputs,securityInput); // Missing activation positive remains missing.');
replace("['agency_id','environment','database','source_group_id','effective_from_week_ending','effective_to_week_ending'].some(k=>nativeContext[k]!==context[k])",
 "['agency_id','environment','database','source_group_id'].some(k=>nativeContext[k]!==context[k])");
replace('setupClosure.week_ending<context.effective_from_week_ending','setupClosure.week_ending<nativeContext.effective_from_week_ending');
replace('(context.effective_to_week_ending!==null&&setupClosure.week_ending>context.effective_to_week_ending)',
 '(nativeContext.effective_to_week_ending!==null&&setupClosure.week_ending>nativeContext.effective_to_week_ending)');
replace("const body={schema_version:operationalSchema,phase:'ALL_PRE_SWITCH_GATES_QUALIFIED',release_id:context.release_id,",
 "validateProbeAssignment(assignmentScope,context,setupClosure,nativeContext);\n const body={schema_version:probeSchema,phase:'ALL_OTHER_GATES_QUALIFIED_SECURITY_ACTIVATION_PROBE_ONLY',operational_ready:false,assignment_scope:assignmentScope,release_id:context.release_id,");
replace('return {...body,qualification_sha256:domainHash(operationalSchema,body)};',
 'return {...body,probe_authorization_sha256:domainHash(probeSchema,body)};');
fs.writeFileSync(path.join(out,'activation-probe-evidence.mjs'),prefix+body+'\n');
console.log(JSON.stringify({source_only:true,full_remote_constructor_retained:true,manufactured_activation_positive:false,operational_ready:false}));
