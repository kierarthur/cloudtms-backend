// Internal filesystem primitive; the executable reader supplies its single fixed public base.
import fs from 'node:fs';import path from 'node:path';import {createHash}from'node:crypto';
export const LIMIT=16384,EMPTY_SHA='e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855';
const check=(v,c)=>{if(!v)throw Error(c);},sha=b=>createHash('sha256').update(b).digest('hex');
const slash=s=>s.replaceAll('\\','/');
const id=s=>[s.dev,s.ino,s.birthtimeNs].map(String).join(':');
export function readFixedProgress(base,nonce,previous){
 check(typeof nonce==='string'&&/^[a-f0-9]{32}$/.test(nonce),'COST_BRIDGE_NONCE_REQUIRED');
 check(previous&&Number.isSafeInteger(previous.offset)&&previous.offset>=0&&previous.offset<=LIMIT&&/^[a-f0-9]{64}$/.test(previous.prefixSha256??'')&&typeof previous.fileId==='string','COST_BRIDGE_CURSOR_REQUIRED');
 check(previous.offset!==0||previous.prefixSha256===EMPTY_SHA,'COST_BRIDGE_EMPTY_PREFIX_REQUIRED');
 check(previous.fileId==='-'&&previous.offset===0||/^[0-9]+:[0-9]+:[0-9]+$/.test(previous.fileId),'COST_BRIDGE_FILE_ID_REQUIRED');
 base=slash(path.resolve(base));check(slash(fs.realpathSync.native(base))===base&&!fs.lstatSync(base).isSymbolicLink(),'COST_BRIDGE_PHYSICAL_BASE_REQUIRED');
 const dir=base+'/qt-cost-native-cmd-20261017-'+nonce,file=dir+'/PROGRESS.jsonl';
 const ds=fs.lstatSync(dir);check(ds.isDirectory()&&!ds.isSymbolicLink()&&slash(fs.realpathSync.native(dir))===dir,'COST_BRIDGE_OWN_DIRECTORY_REQUIRED');
 const before=fs.lstatSync(file,{bigint:true});check(before.isFile()&&!before.isSymbolicLink()&&before.nlink===1n&&slash(fs.realpathSync.native(file))===file,'COST_BRIDGE_REGULAR_PROGRESS_REQUIRED');
 check(before.size>=0n&&before.size<=BigInt(LIMIT),'COST_BRIDGE_PROGRESS_BOUND_REQUIRED');
 const fd=fs.openSync(file,'r');let bytes,first,last,after;
 try{
  first=fs.fstatSync(fd,{bigint:true});check(first.isFile()&&first.nlink===1n&&id(first)===id(before)&&first.size<=BigInt(LIMIT),'COST_BRIDGE_FILE_CHANGED');
  check(previous.fileId==='-'||id(first)===previous.fileId,'COST_BRIDGE_FILE_REPLACED');
  bytes=Buffer.alloc(Number(first.size));let n=0;while(n<bytes.length){const got=fs.readSync(fd,bytes,n,bytes.length-n,n);check(got>0,'COST_BRIDGE_PROGRESS_TRUNCATED');n+=got;}
  last=fs.fstatSync(fd,{bigint:true});after=fs.lstatSync(file,{bigint:true});
  check(id(first)===id(last)&&id(last)===id(after)&&after.isFile()&&!after.isSymbolicLink()&&last.nlink===1n&&after.nlink===1n&&last.size>=first.size&&after.size>=first.size&&last.size<=BigInt(LIMIT)&&after.size<=BigInt(LIMIT),'COST_BRIDGE_FILE_CHANGED');
 }finally{fs.closeSync(fd);}
 check(bytes.length>=previous.offset,'COST_BRIDGE_PROGRESS_TRUNCATED');
 check(sha(bytes.subarray(0,previous.offset))===previous.prefixSha256,'COST_BRIDGE_PROGRESS_PREFIX_CHANGED');
 // Fatal decoding preserves BOM and every byte; incomplete UTF-8 is refused, never repaired.
 let delta;try{delta=new TextDecoder('utf-8',{fatal:true,ignoreBOM:true}).decode(bytes.subarray(previous.offset));}catch{throw Error('COST_BRIDGE_UTF8_REQUIRED');}
 return {version:'QT_COST_REDIRECTED_PROGRESS_V1',nonce,offset:bytes.length,prefixSha256:sha(bytes),fileId:id(first),delta};
}
