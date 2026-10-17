// Public source/Git metadata only; no source(), buildInput(), provider, database or private path.
import fs from 'node:fs';
import path from 'node:path';
import cp from 'node:child_process';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
const packet='D:/Development/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026/.codex-tmp/qt-audit-record-sha256-design-20261017-6ef344744c26453a8aa99f946a85a7d3';
const sha=b=>createHash('sha256').update(b).digest('hex'), read=f=>fs.readFileSync(path.join(packet,f)), json=f=>JSON.parse(read(f));
const F=json('FORWARD.json');
assert.equal(sha(read('MANIFEST.json')),'9313aee7e24378b2c88a1d4a3899b57a9df69888f955cd2655a313d14bd683f7');
assert.equal(sha(read('FORWARD.json')),'4f0770f79858b7b11a6f3f7cf3acc77d0bbb8eb1247538394a5ee946b4ede144');
assert.equal(fs.realpathSync.native(F.root).replaceAll('\\','/'),F.root);
const git=(...args)=>cp.execFileSync('git',['-C',F.root,...args],{windowsHide:true,stdio:['ignore','pipe','pipe'],maxBuffer:16*1024*1024});
const head=git('rev-parse','HEAD').toString().trim();
git('merge-base','--is-ancestor',F.parentCommit,head);
const expected=['qt-assertions.sql','input-builder.mjs','result-validator.mjs','PROGRAMMES.json','OFFLINE_INPUTS.json','MANIFEST.json'];
assert.deepEqual(F.targets.map(x=>path.basename(x.target)),expected);
let checks=0;
for(const t of F.targets){
 const p=read(t.parent), b=read(t.payload), claimed=F.all52ParentImages.filter(x=>F.relative+'/'+x.file===t.target);
 assert.equal(claimed.length,1);assert.equal(claimed[0].bytes,t.beforeBytes);assert.equal(claimed[0].sha256,t.beforeSha256);
 assert.equal(p.length,t.beforeBytes);assert.equal(sha(p),t.beforeSha256);assert.equal(b.length,t.bytes);assert.equal(sha(b),t.sha256);
 for(const actual of [fs.readFileSync(F.root+'/'+t.target),git('show',':'+t.target),git('show',head+':'+t.target),git('show',F.parentCommit+':'+t.target)]){assert.ok(actual.equals(p));checks++;}
 if(!['PROGRAMMES.json','OFFLINE_INPUTS.json','MANIFEST.json'].includes(path.basename(t.target))){
  let inverse=b.toString('utf8');for(const e of F.edits.filter(x=>x.target===path.basename(t.target)).reverse()){assert.equal(inverse.split(e.after).length-1,e.occurrences??1);inverse=inverse.replaceAll(e.after,e.before);}assert.ok(Buffer.from(inverse).equals(p));
 }
}
// Independent source boundary: whole nested row AND actual zone, never a fixed timezone or selected columns.
const sql=read('payload/qt-assertions.sql').toString(), old=read('parent/qt-assertions.sql').toString(), cat=read('encoder-catalogue.sql').toString().trim();
const ct=[...sql.matchAll(/create temporary table hr_contact_qt_audit_(before|after) on commit drop as ([^;]+);/g)];assert.equal(ct.length,2);
for(const x of ct)assert.equal(x[2],"select a.event_sequence,pg_catalog.sha256(pg_catalog.record_send(ROW(a,pg_catalog.current_setting('TimeZone')))) as row_digest from public.audit_events a limit 1500001");
assert.equal(sql.split(cat).length-1,2);
assert.equal(sql.split('octet_length(row_digest)<>32').length-1,3);
assert.equal(sql.split('except all').length-1,2);
const controls=s=>s.match(/do \$qt_controls\$[\s\S]*?end;\$qt_controls\$;/)[0];assert.equal(controls(sql),controls(old));
assert.ok(sql.indexOf('QT_AUDIT_WHOLE_TABLE_ROW_BOUND_EXCEEDED')<sql.indexOf('insert into public.tms_users'));
// Confirm each of the 16 actual descriptor positions, six send/output mappings and three direct calls is named in both guards.
const contract=json('ENCODER_CONTRACT.json');
assert.equal(contract.columns.length,16);assert.equal(contract.types.length,6);assert.equal(contract.functions.length,3);
const proof=json('PG17_SOURCE_PROOF.json');assert.equal(proof.functions.length,25);
const fn=n=>{const x=proof.functions.find(x=>x.name===n);assert.ok(x);const body=read(x.file).toString().slice(x.start,x.end);assert.equal(sha(body),x.sha256);return body;};
assert.ok(fn('record_send').includes('pq_sendint32(&buf, -1)'));assert.ok(fn('record_send').includes('getTypeBinaryOutputInfo(column_type,'));
assert.ok(fn('JsonEncodeDateTime').includes('USE_XSD_DATES'));assert.ok(fn('JsonEncodeDateTime').includes('timestamp2tm(timestamp, tzp ? NULL : &tz'));
assert.ok(fn('jsonb_send').includes('JsonbToCString'));assert.ok(fn('jsonb_out').includes('JsonbToCString'));
assert.equal(git('rev-parse','HEAD').toString().trim(),head);
console.log(JSON.stringify({sourceOnly:true,actualCompleteTargetParents:6,savedIndexHeadHistoricalComparisons:checks,wholeLiteralInverses:3,exactCtas:2,repeatedDescriptorGuards:2,descriptor:[16,6,3],fullControlsRawUnchanged:true,selectedCSourceLinks:6,native:'NOT_RUN',performance:'NOT_PROVED'}));
