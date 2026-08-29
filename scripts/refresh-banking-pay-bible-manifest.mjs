// Mechanical manifest refresh for the explicitly supplied working Bible only.
// Historical source snapshots and the documents themselves are never rewritten.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
const root=path.resolve(process.argv[2] || '');
const name='CloudTMS_Banking_Pay_Modal_Structure_Bible_20260828';
assert.equal(path.basename(root),name,'An exact working Bible directory is required');
assert.ok(fs.statSync(path.join(root,'01_BANKING_PAY_MODAL_STRUCTURE_BIBLE.md')).isFile());
const files=[];
function walk(directory) {
  for(const entry of fs.readdirSync(directory,{withFileTypes:true})) {
    assert.ok(!entry.isSymbolicLink(),'Bible must not contain symbolic links');
    const absolute=path.join(directory,entry.name);
    if(entry.isDirectory()) walk(absolute);
    else if(entry.isFile() && absolute!==path.join(root,'MANIFEST_SHA256.txt')) files.push(absolute);
  }
}
walk(root);
const lines=files.map(absolute=>{
  const bytes=fs.readFileSync(absolute);
  return {relative:path.relative(root,absolute).split(path.sep).join('/'),
    hash:createHash('sha256').update(bytes).digest('hex').toUpperCase(),size:bytes.length};
}).sort((a,b)=>a.relative<b.relative?-1:a.relative>b.relative?1:0);
fs.writeFileSync(path.join(root,'MANIFEST_SHA256.txt'),[
  '# CloudTMS Banking Pay Modal Structure Bible SHA-256 manifest',
  '# Format: SHA256  BYTES  ARCHIVE_PATH',
  '# This manifest intentionally excludes its own entry.',
  ...lines.map(row=>`${row.hash}  ${row.size}  ${name}/${row.relative}`),''
].join('\n'));
console.log(JSON.stringify({working_bible:name,files:lines.length,manifest_verified:lines.every(row=>{
  const bytes=fs.readFileSync(path.join(root,row.relative));
  return bytes.length===row.size && createHash('sha256').update(bytes).digest('hex').toUpperCase()===row.hash;
})}));
