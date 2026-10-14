// Source-only seal construction from root's actual compiled contract and
// catalogue query outputs. This file executes no SQL and opens no connection.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
import {reportFinNativeOwnerSignatures,nativeEffectRelations,reportNativeMapperQueries} from './forward/scripts/weekly-source-report-fin-native-mapper.mjs';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex'),args=Object.fromEntries(process.argv.slice(2).map(arg=>{const at=arg.indexOf('=');if(at<1)throw Error('SEAL_ARGUMENT_INVALID');return [arg.slice(0,at),arg.slice(at+1)];}));
const required=['--compiled-contract','--native-catalogue','--managed-manifest-sha256','--installed-catalogue-sha256','--tool-source','--output'];
if(required.some(k=>!args[k])||Object.keys(args).some(k=>!required.includes(k)))throw Error('SEAL_ROOT_INPUTS_REQUIRED');
const contractBytes=fs.readFileSync(args['--compiled-contract']),contract=JSON.parse(contractBytes),catalogueBytes=fs.readFileSync(args['--native-catalogue']),catalogue=JSON.parse(catalogueBytes),toolBytes=fs.readFileSync(args['--tool-source']);
if(!Array.isArray(contract.routines)||!Array.isArray(contract.triggers)||catalogue.schema_version!=='WEEKLY_SOURCE_REPORT_FIN_COMPILED_NATIVE_CATALOGUE_V1'
 ||catalogue.contract_sha256!==sha(contractBytes)||!Array.isArray(catalogue.owners)||!Array.isArray(catalogue.hooks))throw Error('SEAL_COMPILED_CATALOGUE_INVALID');
const signature=r=>r.schema+'.'+r.identity.replace(/\(.*\)$/s,()=>{
 const values=r.identity.slice(r.identity.indexOf('(')+1,-1).split(',').map(a=>a.trim().replace(/^(?:p_[A-Za-z0-9_]+|[A-Za-z0-9_]+_in)\s+/,''));return '('+values.join(',')+')';});
const owners=reportFinNativeOwnerSignatures.map(s=>{
 const definitions=contract.routines.filter(r=>signature(r)===s),actual=catalogue.owners.filter(r=>r.signature===s);
 if(definitions.length!==1||actual.length!==1||actual[0].definition_sha256!==definitions[0].definition_sha256
  ||typeof actual[0].anon_execute!=='boolean'||typeof actual[0].authenticated_execute!=='boolean')throw Error('SEAL_LITERAL_OWNER_UNPROVED '+s);
 return {signature:s,definition_sha256:actual[0].definition_sha256,anon_execute:actual[0].anon_execute,authenticated_execute:actual[0].authenticated_execute};
});
if(catalogue.owners.length!==owners.length||catalogue.hooks.length!==contract.triggers.length)throw Error('SEAL_WHOLE_NATIVE_CENSUS_REQUIRED');
const hooks=[...catalogue.hooks].sort((a,b)=>a.schema.localeCompare(b.schema)||a.table.localeCompare(b.table)||a.name.localeCompare(b.name));
for(const hook of hooks){const t=contract.triggers.filter(t=>t.schema===hook.schema&&t.table===hook.table&&t.name===hook.name);
 if(t.length!==1||t[0].enabled!==hook.enabled||sha(t[0].definition)!==hook.definition_sha256||!/^[0-9a-f]{64}$/.test(hook.function_definition_sha256))throw Error('SEAL_TRIGGER_UNPROVED');
 const called=/EXECUTE FUNCTION ([a-z0-9_]+)\.([a-z0-9_]+)\(/.exec(t[0].definition);
 if(!called)throw Error('SEAL_TRIGGER_OWNER_UNRESOLVED');const native=contract.routines.filter(r=>signature(r)===called[1]+'.'+called[2]+'()');
 if(native.length!==1||native[0].definition_sha256!==hook.function_definition_sha256)throw Error('SEAL_TRIGGER_BODY_UNPROVED');}
for(const key of ['--managed-manifest-sha256','--installed-catalogue-sha256'])if(!/^[0-9a-f]{64}$/.test(args[key]))throw Error('SEAL_EXECUTION_HASH_INVALID');
const seal={schema_version:'WEEKLY_SOURCE_REPORT_FIN_NATIVE_SEAL_V1',contract_sha256:sha(contractBytes),managed_manifest_sha256:args['--managed-manifest-sha256'],
 installed_catalogue_jsonb_sha256:args['--installed-catalogue-sha256'],owners,hooks,effect_relations:nativeEffectRelations,
 mapper_query_sha256:sha(reportNativeMapperQueries.toString()),tool_source_sha256:sha(toolBytes)};
const target=path.resolve(args['--output']);if(path.basename(target)!=='weekly-source-report-fin-native-seal.mjs')throw Error('SEAL_OUTPUT_NAME_INVALID');
fs.writeFileSync(target,'// Root seal from actual compiled contract/native catalogue/tool source.\nexport const reportFinNativeSeal = '+JSON.stringify(seal,null,2)+';\n');
console.log(JSON.stringify({contract_sha256:seal.contract_sha256,owners:owners.length,hooks:hooks.length,tool_source_sha256:seal.tool_source_sha256,seal_sha256:sha(fs.readFileSync(target)),no_native_execution:true}));
