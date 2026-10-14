import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url));
const output=path.join(dir,'FIXTURE_INSERT_COLUMN_CHECK.json');assert.ok(!fs.existsSync(output));
const fixture=fs.readFileSync(path.join(dir,'../root-rekey-alias-native-first-use-20261014/02_NATIVE_HELPER_FIXTURE_OWNER_CONFLICT_FORWARD.sql'),'utf8');
const catalogue=JSON.parse(fs.readFileSync(path.join(dir,'INSTALLED_FIXTURE_CATALOG_PRECHECK.json')));
const checks=[];
for(const match of fixture.matchAll(/\binsert\s+into\s+(public\.[a-z_0-9]+)\s*\(([^)]+)\)/gi)){
 const table=match[1],columns=match[2].split(',').map(s=>s.trim()),actual=catalogue.columns.filter(c=>c.table===table);
 assert.ok(actual.length,table);assert.ok(columns.every(c=>/^[a-z_0-9]+$/.test(c)),table);
 for(const column of columns)assert.ok(actual.some(c=>c.column===column),table+'.'+column);
 const missingRequired=actual.filter(c=>c.notNull&&!c.hasDefault&&!columns.includes(c.column)).map(c=>c.column);
 assert.deepEqual(missingRequired,[],table);
 checks.push({table,columns,allColumnsPresent:true,missingRequired});
}
assert.equal(checks.length,21);
const supplement=JSON.parse(fs.readFileSync(path.join(dir,'INSTALLED_FIXTURE_CATALOG_SUPPLEMENT.json')));
assert.ok(supplement.helperAbsent&&supplement.fixtureCounts.every(c=>c.fixture_matches===0));
assert.ok(supplement.uniqueIndexes.every(c=>c.valid));
const receipt={status:'INSTALLED_COLUMNS_REQUIRED_DEFAULTS_AND_INDEX_VALIDITY_PASS_NOT_NATIVE_EXECUTION',fixtureSha256:crypto.createHash('sha256').update(fixture).digest('hex'),installedCommit:catalogue.release.commit,catalogueObservedAt:catalogue.observedAt,supplementObservedAt:supplement.observedAt,insertDeclarations:checks.length,checks,allUniqueIndexesValid:true,nativeExecuted:false,limits:'Column/default coverage and index validity are proved against the installed catalogue. CHECK/unique/exclusion values and all trigger branches still require independent fixture review and actual native execution. Native CLI/CCR minting triggers replace supplied references; source identity cases use actual UUIDs and do not require those supplied references to persist. Sequence increments may leave harmless gaps.'};
fs.writeFileSync(output,JSON.stringify(receipt,null,2)+'\n',{flag:'wx'});
console.log(JSON.stringify({status:receipt.status,insertDeclarations:checks.length,nativeExecuted:false}));
