import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import {createHash} from 'node:crypto';import {fileURLToPath,pathToFileURL} from 'node:url';
const out=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(out,'../../../..'),root=path.resolve(own,'../healthroster-v2-backend-13102026'),sha=x=>createHash('sha256').update(x).digest('hex');
const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
const basePath='docs/healthroster-build/checkpoint13102026/FROZEN_PRIVATE_SIGNATURE_UNION_AB26_GIT_PINNED.json';
const baseline=JSON.parse(fs.readFileSync(path.join(root,basePath),'utf8'));assert.equal(baseline.distinctPrivateSignatures,275);
const selected=['OFF_RULE_AUTHORITY.sqlinc','OFF_RULE_COMMAND.sqlinc','OFF_RULE_STATUS.sqlinc','OFF_RULE_TRANSPORT_STATUS.sqlinc',
 '01_operational_rule_projection.sqlinc','02_effective_rule_owner.sqlinc','03_cas_operational_projection.sqlinc','04_final_revision_rule_guard.sqlinc',
 '05_coverage_floor_owner.sqlinc','ACTIVATION_WINDOW_BINDING.sqlinc','FINAL_OPERATIONAL_CERTIFICATE.sqlinc',
 'HISTORICAL_FIXTURE_ASSIGNMENT.sqlinc','HISTORICAL_FIXTURE_SETUP_ASSERT.sqlinc','11_complete_historical_fixture_native_assignment.sqlinc',
 '12_complete_report_nullable_rounding_owner.sqlinc',
 'ACTIVATION_PROBE_AUTHORIZATION_ASSERT.sqlinc','ACTIVATION_PROBE_SCOPE.sqlinc','POST_VERIFIED_PARTIAL_ASSERT.sqlinc',
 'POST_VERIFIED_SETUP_ASSERT.sqlinc','POST_VERIFIED_BULK_SETUP_ASSERT.sqlinc','SETUP_STATIC_SHAPE_ASSERT.sqlinc','NONOPERATIONAL_PREREQUISITE_READER.sqlinc',
 'POST_VERIFIED_EVIDENCE_RECORD.sqlinc','REPORT_FIXTURE_REFERENCE.sqlinc','10_complete_bulk_test_scope.sqlinc',
 '07_automatic_capability_admission.sqlinc','08_cas_admission_owner.sqlinc','09_complete_controlled_setup_transport.sqlinc'];
const exactParams=params=>params.split(',').map(p=>p.trim().replace(/\s+default\s+[\s\S]*$/i,'')
 .replace(/^(?:in\s+|out\s+|inout\s+)?[a-z_][a-z_0-9]*\s+/i,'').replace(/\s+/g,' ').toLowerCase()).join(',');
const definitions=new Map(),inputs=[];
for(const file of selected){const source=fs.readFileSync(path.join(out,file),'utf8');inputs.push({file,bytes:Buffer.byteLength(source),sha256:sha(source)});
 for(const unit of scanManagedSql(source).statements){const body=source.slice(unit.start,unit.end);const match=body.match(/^\s*create\s+or\s+replace\s+function\s+((?:private|public)\.[a-z_0-9]+)\s*\(([\s\S]*?)\)\s*returns/i);if(!match)continue;
  const signature=match[1]+'('+exactParams(match[2])+')';definitions.set(signature,{signature,file,definition_sha256:sha(body),
   security:/security\s+definer/i.test(body)?'DEFINER':'INVOKER',volatility:/\bvolatile\b/i.test(body)?'VOLATILE':'STABLE',
   calls:[...new Set([...body.matchAll(/\b(private|public)\.([a-z_0-9]+)\s*\(/gi)].map(m=>m[1]+'.'+m[2]))].filter(n=>n!==match[1]).sort()});
 }
}
const rootSet=new Set(baseline.signatures.map(r=>r.signature)),all=new Set(rootSet),additions=[],replacements=[];
for(const row of definitions.values())if(row.signature.startsWith('private.')){if(all.has(row.signature))replacements.push(row);else{all.add(row.signature);additions.push(row);}}
const union=[...all].sort();assert.ok([...rootSet].every(s=>union.includes(s)));
const acl=additions.map(row=>`alter function ${row.signature} owner to "postgres";\nrevoke all on function ${row.signature} from PUBLIC,anon,authenticated,service_role,authenticator,supabase_admin;\ngrant execute on function ${row.signature} to "postgres";`).join('\n');
fs.writeFileSync(path.join(out,'OWNER_ONLY_ADDITIONS_ACL.sqlinc'),'-- New private owners only. Root reasserts its complete existing service/private union after composition.\n'+acl+'\n');
fs.writeFileSync(path.join(out,'INTERFACE_LITERAL_SIGNATURE_UNION.json'),JSON.stringify({source_only:true,not_installed_census:true,
 baseline:{file:basePath,sha256:sha(fs.readFileSync(path.join(root,basePath))),signatures:275},inputs,
 composition_note:'Root 275 is a prepared literal closure, not a claim these uninstalled dependencies are physically present. Compose all root-listed original CAP/Q/strict/A0/creator inputs before this successor. Report validator/strategy additions are separately owned and not included here.',
 distinct_private_signatures:union.length,preserved_all_275:true,additions,replacements,complete_source_definitions:[...definitions.values()],signatures:union,
 report_external_dependencies:['private.weekly_source_report_fin_native_comparisons_assert_v1(jsonb,jsonb,text)',
  'private.weekly_source_report_fin_qualifications_assert_v1(jsonb,jsonb,text)'],
 no_broader_service_grant:true,no_operational_acceptance:true},null,2)+'\n');
console.log(JSON.stringify({source_only:true,complete_owner_definitions:definitions.size,new_owner_private:additions.length,union:union.length,preserved_275:true}));
