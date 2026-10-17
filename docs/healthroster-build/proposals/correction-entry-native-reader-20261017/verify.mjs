// Offline source guard only. No DB, network, process launch, C path or write.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';import {fileURLToPath,pathToFileURL} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),own=path.resolve(here,'../../../..');
const integration=path.resolve(own,'../healthroster-install2-14102026');
const office=path.resolve(own,'../healthroster-v2-office-13102026');
for(const root of [own,integration,office])assert(fs.realpathSync(root).replaceAll('\\','/').startsWith('D:/Development/GitHub/'),'PHYSICAL_D_ROOT_REQUIRED');
const {scanManagedSql}=await import(pathToFileURL(path.join(own,'scripts/cloudtms-managed-release-sql.mjs')));
const sha=s=>createHash('sha256').update(s).digest('hex'),read=f=>fs.readFileSync(f,'utf8').replaceAll('\r\n','\n');
const guards=JSON.parse(read(path.join(here,'SOURCE_GUARDS.json')));
for(const row of guards.files){assert.equal(sha(read(path.join(row.authority==='OFFICE'?office:integration,row.file))),row.canonicalSha256,row.file);}
for(const row of guards.spans){const source=read(path.join(row.authority==='OFFICE'?office:integration,row.file));const start=source.indexOf(row.start),end=source.indexOf(row.end,start+row.start.length);assert(start>=0&&end>start,row.name+' source span');assert.equal(sha(source.slice(start,end)),row.canonicalSha256,row.name);}
const config=read(path.join(integration,'wrangler.toml')),testVars=/\[env\.test\.vars\]\n([\s\S]*?)(?=\n\[|$)/.exec(config);assert(testVars,'actual TEST vars');
const tenant=/^BANKING_PAY_CORRECTION_TENANT_ID_V1\s*=\s*("[^"]+")/m.exec(testVars[1]);assert(tenant,'actual TEST tenant');assert.equal(JSON.parse(tenant[1]),guards.configuredTenant);
const registry=JSON.parse(read(path.join(integration,'docs/bpay-next/final-definer-closures.json')));
for(const row of guards.finalOwners){const current=registry.chains.find(x=>x.signature===row.signature);assert(current,row.signature);assert.equal(current.finalRoot,row.finalRoot);assert.equal(current.finalPhysical,row.finalPhysical);}
const sql=read(path.join(here,'17102026_0109_correction_entry_reader.sql')),parsed=scanManagedSql(sql);
assert.equal(parsed.statements.length,8);assert.equal(parsed.meta.length,0);
assert.equal(scanManagedSql(read(path.join(here,'17102026_0109_correction_entry_one_time_admission.sql'))).statements.length,1);
const definitions=parsed.statements.map(s=>parsed.source.slice(s.start,s.end)).filter(s=>/^create or replace function/i.test(s));assert.equal(definitions.length,3);
for(const source of definitions){assert(/stable security definer/.test(source));assert(/set search_path=pg_catalog,public,private,pg_temp/.test(source));const body=source.replace(/--[^\n]*/g,'');assert(!/\b(insert\s+into|delete\s+from|update\s+public\.|for\s+update|pg_advisory|set_config|execute\s+format)\b/i.test(body),'read-only body');}
assert.equal(scanManagedSql(read(path.join(here,'17102026_0109_correction_entry_reverse.sql'))).statements.length,4);
const contract=JSON.parse(read(path.join(integration,'supabase/release/current-contract.json')));
for(const [name,columns] of Object.entries(guards.requiredColumns)){const relation=contract.relations.find(r=>r.schema==='public'&&r.name===name);assert(relation,name);for(const column of columns)assert(relation.columns.some(c=>c.name===column),name+'.'+column);}
const enumRow=contract.enums.find(e=>e.schema==='public'&&e.name==='timesheet_line_type_enum');assert.deepEqual(enumRow.labels,['HOURS','EXPENSES','MILEAGE']);
const schema=JSON.parse(read(path.join(here,'ENTRY_RESPONSE_PROPOSAL.json')));assert.equal(schema.status,'ROOT_REVIEW_REQUIRED_UNSEALED');assert.deepEqual([...schema.schema.required].sort(),['current_timesheet_id','disabled_reason','may_open_review','placement_revision','purpose','requested_timesheet_id','version']);
console.log('PASS: D paths, exact native sources/final owners/callers, 3 read-only definitions, full additive reverse, referenced schema and unsealed closed response');
