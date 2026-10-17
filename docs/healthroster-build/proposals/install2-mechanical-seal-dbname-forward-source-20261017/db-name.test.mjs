import test from 'node:test';import assert from 'node:assert/strict';
import fs from 'node:fs';import path from 'node:path';import {fileURLToPath} from 'node:url';
import {profile,validateTarget} from './payload/scripts/healthroster-install2-local-seal-profile.mjs';
const here=path.dirname(fileURLToPath(import.meta.url));
const read=p=>fs.readFileSync(path.join(here,p),'utf8');
test('two complete owner images differ only in the required database literal; provider unchanged',()=>{
 const proof=JSON.parse(read('FORWARD.json'));
 assert.equal(proof.rows.filter(x=>x.changed).length,2);
 for(const r of proof.rows){const before=read('parents/'+r.target),after=read('payload/'+r.target);
  assert.equal(after,before.split('healthroster_install2_seal').join('banking_modal_v2_test'));
  assert.equal(before,after.split('banking_modal_v2_test').join('healthroster_install2_seal'));
 }
 assert(read('parents/AGENTS.md').includes('fixture database must be named exactly `banking_modal_v2_test`'));
 assert.equal(profile.database,'banking_modal_v2_test');assert.equal(profile.port,'55448');
 assert.equal(profile.task,'hr-i2-seal-20261017');
});
test('old name refuses before execution and the sole fixed corrected target remains accepted',()=>{
 const settings={allowLocal:'1',logicalOwner:'CURRENT_USER',passwordPresent:true};
 const uri=`postgresql://${profile.owner}@127.0.0.1:55448/banking_modal_v2_test`;
 assert.equal(validateTarget(uri,settings).pathname,'/banking_modal_v2_test');
 assert.throws(()=>validateTarget(uri.replace('banking_modal_v2_test','healthroster_install2_seal'),settings));
 const old=read('../install2-mechanical-seal-fresh-source-20261017/COMMANDS.ps1.txt');
 assert.equal(read('COMMANDS.ps1.txt'),old.split('healthroster_install2_seal').join('banking_modal_v2_test'));
});
