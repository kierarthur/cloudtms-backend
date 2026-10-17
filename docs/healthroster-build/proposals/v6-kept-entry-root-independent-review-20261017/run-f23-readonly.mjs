// Execute the genuine finite tests while refusing every other-worktree write.
import fs from 'node:fs';
import assert from 'node:assert/strict';
import {pathToFileURL} from 'node:url';
const root='D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026';
const packet=root+'/docs/healthroster-build/proposals/kept-entry-f23-classification-20261017/';
const allowed=new Set(['ORIGINAL_ALLOWANCE_TRANSFER.json','TEST_RESULTS.json'].map(x=>packet+x));
const original=fs.writeFileSync;
let suppressed=0;
fs.writeFileSync=(file,value)=>{
  const target=String(file).replaceAll('\\','/');
  assert(allowed.has(target),'Unapproved write '+target);
  assert.deepEqual(Buffer.from(value),fs.readFileSync(file),'Changed source evidence '+target);
  suppressed++;
};
try { await import(pathToFileURL(packet+'test.mjs')); }
finally { fs.writeFileSync=original; }
assert.equal(suppressed,2);
console.log('PASS: 13 genuine finite tests; two byte-identical evidence writes suppressed; source writes zero');
