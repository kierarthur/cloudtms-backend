// Copy root's already committed fixed definition observation. No connector is used.
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import assert from 'node:assert/strict';
const here=path.dirname(fileURLToPath(import.meta.url));
const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const commit='f9d6d77d521c027bbab88f12af6f4e5c9f83f16e';
const base='docs/healthroster-build/v6-owner-delivery-20261016/';
const sha=b=>createHash('sha256').update(b).digest('hex');
assert(!fs.existsSync(here+'/MANIFEST.json'));
const names=['S5_NATIVE_SIGNATURE_DEFINITION_READONLY_20261017.json','S5_NATIVE_SIGNATURE_SOURCE_IDENTITY_20261017.json'];
const pins=[];
fs.mkdirSync(here+'/reference/root-native',{recursive:true});
for(const name of names){
  const raw=execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,'show',commit+':'+base+name],{maxBuffer:1e6});
  assert(raw.equals(fs.readFileSync(root+'/'+base+name)));
  fs.writeFileSync(here+'/reference/root-native/'+name,raw);
  pins.push({path:base+name,sha256:sha(raw),bytes:raw.length});
}
const observed=JSON.parse(fs.readFileSync(here+'/reference/root-native/'+names[0]));
const identity=JSON.parse(fs.readFileSync(here+'/reference/root-native/'+names[1]));
assert.equal(observed.observation.matched,1);
assert.equal(observed.nativeFunctionInvoked,false);assert.equal(observed.businessRowsReturned,0);
const definition=observed.observation.definitions[0].definition;
const source=fs.readFileSync(here+'/reference/health/timesheet_lifecycle_guard_signature_v1.sqlinc','utf8');
const inner=s=>{const parts=s.split('$function$');assert.equal(parts.length,3);return parts[1];};
assert.equal(sha(definition),identity.nativeDefinitionSha256);
assert.equal(Buffer.byteLength(definition),identity.nativeDefinitionBytes);
assert.equal(inner(source),inner(definition));
assert.equal(sha(inner(source)),identity.nativeBodySha256);
const binding={version:'S5_ROOT_DEFINITION_OBSERVATION_BINDING_V1',root,commit,pins,
  sourceCompleteFunctionSha256:sha(source),nativeCompleteFunctionSha256:sha(definition),
  completeFunctionHeadersIdentical:source===definition,
  completeInnerBodyRawEqual:true,completeInnerBodySha256:sha(inner(source)),
  newlineOrSqlNormalisationApplied:false,observationScope:'DEFINITION_ONLY_NO_FUNCTION_INVOCATION_OR_BUSINESS_ROWS',
  sourceClosureQualification:'CONDITIONAL_EXACT_OFFICE_MANUAL_FIXTURE_ONLY',
  installedTransitiveDependencies:'NOT_PROVED',fixtureAndActor:'UNBOUND',nativeFalseArmInvocation:'NOT_RUN'};
fs.writeFileSync(here+'/NATIVE_DEFINITION_BINDING.json',JSON.stringify(binding,null,2)+'\n');
console.log(JSON.stringify({definitionOnly:true,innerBodyRawEqual:true,nativeInvocation:'NOT_RUN'}));
