// Offline review runner: suppress byte-identical rebuild writes; redirect receipts only.
const fs=require('node:fs'),path=require('node:path');
const source='D:/Development/GitHub/.codex-worktrees/healthroster-v2-money-build-13102026/docs/healthroster-build/proposals/kept-pay-rekey-current-root-successor-20261016';
const out=__dirname,originalWrite=fs.writeFileSync.bind(fs),originalMkdir=fs.mkdirSync.bind(fs);
const receipts=new Set(['TEST_RESULTS.json','PACKAGE_TEST_RESULTS.json']);let suppressed=0;
fs.mkdirSync=(name,options)=>{const p=path.resolve(String(name));if(p===path.resolve(source)||p.startsWith(path.resolve(source)+path.sep)){
 if(!fs.statSync(p).isDirectory())throw Error('OFFLINE_REVIEW_MISSING_SOURCE_DIRECTORY');return;
 }if(p===out||p.startsWith(out+path.sep))return originalMkdir(name,options);throw Error('OFFLINE_REVIEW_UNAUTHORISED_DIRECTORY');};
fs.writeFileSync=(name,data,options)=>{const p=path.resolve(String(name)),relative=path.relative(source,p);
 if(!relative.startsWith('..')&&!path.isAbsolute(relative)){
  if(receipts.has(relative))return originalWrite(path.join(out,relative),data,options);
  const b=Buffer.isBuffer(data)?data:Buffer.from(data,typeof options==='string'?options:options?.encoding||'utf8');
  if(!fs.existsSync(p)||!b.equals(fs.readFileSync(p)))throw Error('OFFLINE_REVIEW_REBUILD_DRIFT:'+relative);
  suppressed++;return;
 }if(p===out||p.startsWith(out+path.sep))return originalWrite(name,data,options);throw Error('OFFLINE_REVIEW_UNAUTHORISED_WRITE');};
process.once('exit',()=>{originalWrite(path.join(out,'SUPPRESSED_WRITES_'+path.basename(process.argv[1])+'.json'),JSON.stringify({suppressedByteIdenticalWrites:suppressed,sourceWrites:0,databaseCalls:0})+'\n');});
