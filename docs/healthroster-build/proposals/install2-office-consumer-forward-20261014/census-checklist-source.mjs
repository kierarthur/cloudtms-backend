import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import {execFileSync,spawnSync} from 'node:child_process';
const here=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(here,'../../../..'),office=process.argv[2];if(!office)throw Error('Authorized current Office path required');
const backendPin='b5c1ea1b002b2d6b756707c020b6a170cc9fea80',officePin='f2de35ec8a376a814aefab1db2d7cf099b888784',sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const git=(root,args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,maxBuffer:30000000});
function grep(root,pin,paths){const r=spawnSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','grep','-n','-i','checklist',pin,'--',...paths],{cwd:root,encoding:'utf8',maxBuffer:5000000});if(![0,1].includes(r.status))throw Error(r.stderr);return r.stdout.split('\n').filter(Boolean);}
const backendHits=grep(own,backendPin,['broker/src','supabase/migrations','supabase/repeatable']);
const officeHits=grep(office,officePin,['js','index.html']);
const hitFiles=new Set(backendHits.map(line=>line.slice(backendPin.length+1).split(':')[0]));
const declarations=[];for(const file of hitFiles){if(!/\.sql(?:inc)?$/.test(file))continue;const b=git(own,['show',`${backendPin}:${file}`]);
 for(const m of b.toString().matchAll(/create\s+or\s+replace\s+function\s+(public|private)\.([a-z0-9_]*checklist[a-z0-9_]*)\s*\(/gi))declarations.push({schema:m[1],name:m[2],path:file,sha256:sha(b)});}
const graph=JSON.parse(fs.readFileSync(path.resolve(here,'../install2-a0-runtime-route-wiring-20261014/RUNTIME_IMPORT_CLOSURE.json'))),graphScan=[];
const tree=new Set(git(own,['ls-tree','-r','--name-only',backendPin]).toString().trim().split('\n'));
for(const row of graph.files){if(!tree.has(row.path)){graphScan.push({path:row.path,status:'PROPOSED_NEW_ROUTE_NOT_CURRENT_SOURCE',checklist_occurrences:0});continue;}
 const b=git(own,['show',`${backendPin}:${row.path}`]);graphScan.push({path:row.path,sha256:sha(b),status:'ACTUAL_CURRENT_GIT',checklist_occurrences:[...b.toString().matchAll(/checklist/gi)].length});}
const docs=['WP_U3_READ_OWNER_INVENTORY.md','WP_U3_CONSUMER_CONTRACT.md','WP_U3_INTEGRATION_HUNKS.md'].map(name=>{const file=`docs/healthroster-build/${name}`,b=git(own,['show',`${backendPin}:${file}`]);
 return {path:file,sha256:sha(b),ownership_lines:b.toString().split('\n').filter(line=>/sole|competing|public|Banking.*overlay|uninstalled|root.*owner/i.test(line)).slice(0,12)};});
const noPublic=declarations.every(d=>d.schema==='private')&&!backendHits.some(line=>line.includes('broker/src/index.js:'));
const result={schema_version:'CURRENT_CHECKLIST_OWNERSHIP_SOURCE_CENSUS_V1',source_only:true,backend_pin:backendPin,office_pin:officePin,
 searched_backend:['ALL_COMMITTED_broker/src','ALL_COMMITTED_supabase/migrations','ALL_COMMITTED_supabase/repeatable'],
 searched_office:['ALL_COMMITTED_js','COMMITTED_index.html'],backend_hits:backendHits,office_hits:officeHits,
 checklist_named_declarations:declarations,reviewed_transport_graph_entries:graphScan.length,transport_graph:graphScan,
 controlling_source_documents:docs,no_current_public_checklist_or_index_consumer:noPublic,
 disposition:'ENGINEERING_OWNERSHIP_CONFLICT_UNSEEN_BANKING_OVERLAY_ASSUMPTION_NOT_ACTUAL_CURRENT_PRODUCER',
 no_competing_owner_added:true,private_raw_contexts_exposed:false,native_calls:0};
fs.writeFileSync(path.join(here,'CHECKLIST_SOURCE_CENSUS.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify({backend_hits:backendHits.length,office_hits:officeHits.length,
 declarations:declarations.map(d=>d.schema+'.'+d.name),runtime_graph_entries:graphScan.length,no_current_public_checklist_or_index_consumer:noPublic}));
