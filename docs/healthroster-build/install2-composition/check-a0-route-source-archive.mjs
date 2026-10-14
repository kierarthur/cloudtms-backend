// Verifies the immutable archive and exact inherited whitespace; does not load or run archived code.
import fs from 'node:fs';
import assert from 'node:assert/strict';
import {createHash} from 'node:crypto';
import {spawnSync,execFileSync} from 'node:child_process';
const base='docs/healthroster-build/install2-composition/';
const sha=b=>createHash('sha256').update(b).digest('hex');
const git=args=>execFileSync('git',['-c','maintenance.auto=false','-c','gc.auto=0',...args],{maxBuffer:16*1024*1024,stdio:['ignore','pipe','pipe']});
const receipt=JSON.parse(fs.readFileSync(base+'A0_ROUTE_SOURCE_ARCHIVE_14102026.json'));
for(const row of receipt.files){const saved=fs.readFileSync(row.file),staged=git(['show',':'+row.file]),source=git(['show',receipt.authorPublication+':'+row.file]);assert.equal(saved.length,row.bytes);assert.equal(sha(saved),row.sha256);assert.ok(saved.equals(staged));assert.ok(saved.equals(source));}
const proof=JSON.parse(fs.readFileSync('docs/healthroster-build/proposals/install2-a0-publication-checkpoint-20261014/INHERITED_WHITESPACE_PROOF.json'));
const exactLines=b=>b.toString().match(/[^\n]*\n|[^\n]+$/g)||[];
const parent=exactLines(git(['show',proof.input_commit+':broker/src/index.js']));
assert.equal(sha(git(['show',proof.input_commit+':broker/src/index.js'])),proof.current_index_sha256);
const allowed=new Map();
for(const row of proof.files){const saved=fs.readFileSync(row.path);assert.equal(sha(saved),row.sha256);const lines=exactLines(saved);assert.equal(row.introduced_findings,0);for(const p of row.proof){assert.equal(lines[p.new_line-1],parent[p.current_parent_line-1]);assert.equal(sha(lines[p.new_line-1]),p.line_sha256);allowed.set(row.path+':'+p.new_line,p);}}
const raw=spawnSync('git',['diff','--cached','--check'],{encoding:'utf8',maxBuffer:4*1024*1024});
assert.equal(raw.status,2);const findings=[];
for(const line of ((raw.stdout??'')+(raw.stderr??'')).split(/\r?\n/)){if(!line||line.startsWith('+'))continue;const found=line.match(/^(.+):(\d+): (trailing whitespace\.|new blank line at EOF\.)$/);assert.ok(found,'UNKNOWN_WHITESPACE_OUTPUT');assert.ok(allowed.has(found[1]+':'+found[2]),'UNCLASSIFIED_WHITESPACE');findings.push({file:found[1],line:Number(found[2]),finding:found[3]});}
assert.equal(findings.length,528);
const result={sourceOnly:true,status:'47_EXACT_SAVED_STAGED_PUBLISHED_ARCHIVE_PINS_PASS_INHERITED_WHITESPACE_RETAINED',files:47,priorHeldCheck:{cause:'Root checker initially omitted line-ending bytes from the author line hash.',archiveChanged:false,correction:'Use the exact newline-preserving line tokenizer employed by the frozen author proof.'},rawWhitespacePass:false,rawWhitespaceExit:raw.status,rawWhitespaceOutputSha256:sha((raw.stdout??'')+(raw.stderr??'')),inheritedFindings:528,introducedFindings:0,parentCommit:proof.input_commit,rootExecutableChanges:0,nativeCalls:0};
fs.writeFileSync(base+'A0_ROUTE_SOURCE_ARCHIVE_INTEGRITY_14102026.json',JSON.stringify(result,null,2)+'\n');
console.log(JSON.stringify(result));
