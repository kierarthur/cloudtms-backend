import assert from 'node:assert/strict';
import fs from 'node:fs';
import {execFileSync} from 'node:child_process';
import {createHash} from 'node:crypto';

// Reproduces only Health-owned schema root01 from immutable published inputs.
// No database, remote, extracted evidence script or Banking body is executed.
const target='supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql';
const physical='7d1be386431f5a88b7206d24c9af37b504c36f4e';
const invoice='367c60bb198190be261cde153b9ca92abba6a16c';
const source='1ace374d453f1ad46d573ce7405f7c1ef9ab5052';
const physicalBase='docs/healthroster-build/proposals/checklist-v6-contract-correction-20261016/';
const invoiceBase='docs/healthroster-build/proposals/v6-invoice-native-20261016/';
const sourceBase='docs/healthroster-build/proposals/source-pay-decision-v5-foundation-20261015/one-time/';
const sha=v=>createHash('sha256').update(v).digest('hex');
const pins=[];
function input(commit,path,expected) {
  const bytes=execFileSync('git',['cat-file','blob',`${commit}:${path}`],{maxBuffer:32*1024*1024});
  assert.equal(sha(bytes),expected,`published input changed: ${commit}:${path}`);
  assert(!bytes.includes(Buffer.from('\r')),`published SQL/manifest must have LF bytes: ${path}`);
  pins.push({commit,path,sha256:expected,bytes:bytes.length});
  return bytes.toString('utf8');
}
const closure=JSON.parse(input(physical,physicalBase+'CLOSURE.json',
  'd7fbfb0b71017fcfb512af9ad68e7568bc717f90bbc0409af84ffc0f24eb71b6'));
const schemaPin=closure.files.find(f=>f.file==='forward/sql/physical-schema.sqlinc');
assert(schemaPin,'physical schema must be in reviewed closure');
const physicalSql=input(physical,physicalBase+schemaPin.file,schemaPin.sha256);
input(invoice,invoiceBase+'SOURCE_MANIFEST.json','3a8048d53a520fad0e4454cd4a6cf9fffddd14de354f62f01fe415d700c4db54');
const invoiceSql=input(invoice,invoiceBase+'forward/001_billing_schema.sqlinc',
  '7cf768b713f421248e62dbc23178efbf24adf6096eafe63d284eb316a4d81545');
const decisionSql=input(source,sourceBase+'01_complete_health_decision_metadata.sqlinc',
  '63741add62a1d55c5d486abf9c70c622ffbff3c0462bab2dd378d922bac689f9');
const preparationSql=input(source,sourceBase+'02_complete_retained_native_preparations.sqlinc',
  '5b3d65c49cd5bcbaec306bad9ecb91fe57cfcb1c9f648ace0401b283a9a1f9b7');
const securitySql=input(source,sourceBase+'05_complete_owner_only_rls.sqlinc',
  'c38472b6ab2645ef3aa966d2820e7c2b950e011f6af2fc3bcc5086366a22d847');
const tables=sql=>[...sql.matchAll(/create table private\.([a-z0-9_]+)/g)].map(m=>m[1]);
const sourceTables=tables(decisionSql+preparationSql);
assert.equal(sourceTables.length,8);
const allTables=[...tables(physicalSql),...tables(invoiceSql),...sourceTables];
assert.equal(allTables.length,23);assert.equal(new Set(allTables).size,23);
const securityNames=[...securitySql.matchAll(/^alter table private\.([a-z0-9_]+) owner to postgres;/gm)].map(m=>m[1]);
assert.equal(securityNames.length,11);assert(sourceTables.every(n=>securityNames.includes(n)));
// Same reviewed private-owner policy, applied to each creation root rather
// than leaving root01 tables exposed until later root03/root05 exists.
const rootSecurity=sourceTables.map(n=>`alter table private.${n} owner to postgres;`).join('\n')+`
do $source_pay_root01_rls$
declare name text;
begin
 foreach name in array array[${sourceTables.map(n=>`'${n}'`).join(',')}] loop
  execute format('alter table private.%I enable row level security',name);
  execute format('alter table private.%I force row level security',name);
  execute format('revoke all on table private.%I from public,anon,authenticated,service_role',name);
  execute format('create policy cloudtms_source_pay_decision_owner_only on private.%I for all to %I using(true) with check(true)',name,current_user);
 end loop;
end;
$source_pay_root01_rls$;
`;
const body='-- HEALTH_V6_ROOT01_BODY_BEGIN\n'
  +'-- Reviewed final V6 content afaf162e59b2e7901ef0dbc3f484ce51c741f0971557f80fdc877bb8b9ecaa88.\n'
  +'-- Physical, invoice and Source immutable source pins:\n'
  +pins.map(p=>`-- ${p.commit}:${p.path} SHA256 ${p.sha256}`).join('\n')+'\n\n'
  +physicalSql+'\n'+invoiceSql+'\n'+decisionSql+'\n'+preparationSql+'\n'+rootSecurity
  +'-- HEALTH_V6_ROOT01_BODY_END';
assert(!/^\\/m.test(body),'body must be self-contained, without mutable psql includes');
const template=`-- One-time CloudTMS schema/data migration: nhsp_v6_01_physical_source_review_schema
-- State the exact authority, safety boundary, and verification before implementation.

\\set ON_ERROR_STOP on

begin;

-- Implement the narrowly scoped one-time change here.

commit;
`;
const expected=template.replace('-- Implement the narrowly scoped one-time change here.',body);
const saved=fs.readFileSync(target,'utf8');
assert(saved===template||saved===expected,'retain unexpected root01 edits; no overwrite');
assert(process.argv.slice(2).every(v=>v==='--write'),'unknown composition option');
if(process.argv.includes('--write')&&saved!==expected)fs.writeFileSync(target,expected);
console.log(JSON.stringify({target,sha256:sha(expected),bytes:Buffer.byteLength(expected),tables:allTables,
  source_inputs:pins,source_security:'same reviewed policy split across creation roots',
  changed:saved!==expected,written:process.argv.includes('--write'),native:'NOT_RUN',installed:false}));
