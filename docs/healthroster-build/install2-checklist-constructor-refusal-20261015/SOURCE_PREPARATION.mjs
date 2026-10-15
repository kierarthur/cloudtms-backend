import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {mapExecutableSqlSource} from '../scripts/cloudtms-db-release-lib.mjs';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
const source=path.resolve('../healthroster-checklist-native-14102026/docs/healthroster-build/proposals/checklist-current-native-constructor-packet-20261015');
const dest='.codex-tmp/checklist-constructors-native-20261015';
const sha=s=>createHash('sha256').update(s).digest('hex');
const raw=fs.readFileSync(path.join(source,'CONSTRUCTORS_AND_ORIGINAL_ACL.sql'),'utf8');
assert.equal(sha(raw),'5e3dd99c71aa207d4c8996847a207ca439922c4da30c1a60fc1d2b430650c3b2');
const fingerprint=fs.readFileSync(path.join(source,'FRESH_ROUTINE_TRIGGER_FINGERPRINT.sql'),'utf8');
assert.equal(sha(fingerprint),'a5c608c4097d3c9061e99c92e23e2b0d31a4b4906c04923d1dce157d5fb88b83');
let mapped;const previous=process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;
try {process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER='CURRENT_USER';mapped=mapExecutableSqlSource(raw,'checklist-current-native-constructor-packet-20261015');}
finally {if(previous===undefined)delete process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER;else process.env.CLOUDTMS_LOGICAL_POSTGRES_OWNER=previous;}
const sql='set local jit=off;\nset local pg_show_plans.is_enabled=off;\n'+mapped+'\nselect current_database() as physical_database, true as constructor_catalog_pass;\n';
const manifest={source_packet_sha256:sha(raw),sql_bytes:Buffer.byteLength(sql),sql_characters:sql.length,sql_sha256:sha(sql),statements:scanManagedSql(sql).statements.length,fingerprint_sha256:sha(fingerprint),scope:'50 complete function constructors and original ACL tails; catalog-only assertions; not business/role/native-writer/full-release acceptance',logical_owner_mapping:'Only audited postgres logical owner mapped by release library to CURRENT_USER'};
assert(manifest.sql_bytes<=2000000);assert(manifest.statements<=250);assert(!fs.existsSync(dest));
fs.mkdirSync(dest,{recursive:true});fs.writeFileSync(dest+'/TRIAL.sql',sql);fs.writeFileSync(dest+'/FINGERPRINT.sql',fingerprint);fs.writeFileSync(dest+'/MANIFEST.json',JSON.stringify(manifest,null,2)+'\n');
const inputs=JSON.parse(fs.readFileSync(path.join(source,'CONSTRUCTOR_SOURCE_MANIFEST.json'))).inputs;
fs.writeFileSync(dest+'/ROUTINES.json',JSON.stringify(inputs.map(x=>({schema:x.signature.split('.')[0],name:x.signature.slice(x.signature.indexOf('.')+1,x.signature.indexOf('('))})),null,2)+'\n');
console.log(JSON.stringify(manifest));
