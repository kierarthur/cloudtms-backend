// Source-exact prospective prosrc assertions, not observed native seals.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';import assert from 'node:assert/strict';
import {functionEvents} from '../cn-a0-isolation-source-followup/sql-function-source-index.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),stamp='14102026_1030',receipt=JSON.parse(fs.readFileSync(path.join(here,'CURRENT_SOURCE_REVERSE_AND_CENSUS.json'))),wanted=new Set([...receipt.changedCurrentOwners.map(f=>f.id),...receipt.newPrivateSignatures]),expect=[];
for(const file of [stamp+'_a0_transaction_fence_helpers.sqlinc',...receipt.reversals.map(x=>path.basename(x.output))])for(const f of functionEvents(fs.readFileSync(path.join(here,'sql',file),'utf8')).events.filter(f=>f.kind==='CREATE'&&wanted.has(f.id)))expect.push({signature:f.id,bodySha256:crypto.createHash('sha256').update(f.body.replaceAll('\r\n','\n')).digest('hex'),source:'sql/'+file});
assert.equal(expect.length,10);expect.sort((a,b)=>a.signature.localeCompare(b.signature));
fs.writeFileSync(path.join(here,'SOURCE_EXPECTED_BODY_HASHES.json'),JSON.stringify({sourceOnly:true,observedNative:false,normalization:'CRLF to LF only, all other body bytes preserved',expect},null,2)+'\n');
const values=expect.map(e=>"    ('"+e.signature+"','"+e.bodySha256+"')").join(',\n');
const sql=`-- Prospective exact reviewed BODY assertions. These are source expectations;
-- pg_get_functiondef / complete native canonical contract remains a separate gate.
do $a0_transaction_fence_reviewed_bodies$
declare r record;v_body text;
begin
  for r in select * from(values
${values}
  ) expected(signature,sha256)
  loop
    select p.prosrc into v_body from pg_catalog.pg_proc p where p.oid=to_regprocedure(r.signature);
    if not found or pg_catalog.encode(extensions.digest(pg_catalog.convert_to(
      pg_catalog.replace(v_body,E'\\r\\n',E'\\n'),'UTF8'),'sha256'),'hex') is distinct from r.sha256 then
      raise exception 'CORRECTION_A0_REVIEWED_BODY_CHANGED' using errcode='55000';end if;
  end loop;
end;
$a0_transaction_fence_reviewed_bodies$;
`;
fs.writeFileSync(path.join(here,'sql',stamp+'_a0_transaction_fence_reviewed_bodies.sqlinc'),sql);console.log(JSON.stringify({sourceBodyExpectations:expect.length,observedNative:false}));
