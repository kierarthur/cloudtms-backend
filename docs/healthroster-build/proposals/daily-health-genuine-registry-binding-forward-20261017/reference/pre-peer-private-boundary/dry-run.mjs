// OFFLINE ONLY: unchanged complete preflight against labelled proposed public images.
// Actual root public files/Git images remain real. Private path metadata is synthetic;
// no state target is opened, hashed, copied or statted. This cannot issue live admission.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {execFileSync} from 'node:child_process';
import {operator,coordinator,continuation,baseCommit,folders,report,ROOT,read,hash} from './recipe.mjs';
const git=(root,...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',root,...args],{windowsHide:true,maxBuffer:128e6,stdio:['ignore','pipe','ignore'],env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
export function runDryPreflight({original=false,corrupt=null,chain=false,omitCascade=false,corruptGit=null,moveHead=false}={}){
 const source=original?read('operator/git-image-batch.mjs'):operator().targets.find(r=>r.file==='git-image-batch.mjs').text;
 const targets=original?[]:[...operator().targets,...(chain&&!omitCascade?[...coordinator(baseCommit).targets,...continuation(baseCommit,baseCommit).targets]:[])],images=new Map(targets.map(r=>[report+'/'+r.target,Buffer.from(r.text)]));
 const privatePath=ROOT+read('operator/contract.mjs').match(/export const PRIVATE_STATE=ROOT\+'([^']+)';/)[1];
 let syntheticMetadata=0,privateReads=0,publicReads=0,gitCalls=0,rootHeadReads=0;
 const safe=file=>{const name=String(file).replaceAll('\\','/');if(name.includes('/.codex-tmp/')||name.includes('/.auth/')||name.includes('PRIVATE_BINDING')){privateReads++;throw Error('OFFLINE_PRIVATE_CONTENT_REFUSED');}return name;};
 const facade={...fs,readFileSync(file,options){const name=safe(file);publicReads++;let b=images.get(name)??fs.readFileSync(file);if(corrupt?.file===name)b=corrupt.change(Buffer.from(b));return typeof options==='string'?b.toString(options):b;},
  lstatSync(file){if(String(file)===privatePath){syntheticMetadata++;return {isFile:()=>true,isSymbolicLink:()=>false};}return fs.lstatSync(file);},
  realpathSync:{native(file){if(String(file)===path.dirname(privatePath)){syntheticMetadata++;return String(file);}return fs.realpathSync.native(file);}}};
 function child(exe,args,options={}){
  if(exe!=='git')throw Error('OFFLINE_ONLY_GIT_CHILD_ALLOWED');gitCalls++;
  const at=args.indexOf('-C'),root=args[at+1],command=args.slice(at+2);
  if(root===ROOT&&command.join(' ')==='rev-parse HEAD'&&++rootHeadReads===2&&moveHead)return Buffer.from('0'.repeat(40)+'\n');
  if(command[0]==='check-ignore')return git(root,...command); // Path/ignore metadata only.
  const output=execFileSync(exe,args,{...options,windowsHide:true,maxBuffer:128e6,stdio:[options.input?'pipe':'ignore','pipe','ignore'],env:{...process.env,GIT_OPTIONAL_LOCKS:'0',GIT_NO_LAZY_FETCH:'1'}});
  if(command.join(' ')!=='cat-file --batch')return output;
  const expressions=options.input.toString().trimEnd().split('\n');let offset=0;const parts=[];
  for(const expression of expressions){
   const end=output.indexOf(10,offset),header=output.subarray(offset,end).toString(),match=/^([a-f0-9]{40}) blob ([0-9]+)$/.exec(header);
   if(!match)throw Error('OFFLINE_GENUINE_GIT_FRAME_REQUIRED');
   const n=Number(match[2]),body=output.subarray(end+1,end+1+n),file=expression.slice(expression.indexOf(':')+1),ref=expression.slice(0,expression.indexOf(':'));let post=root===report?images.get(report+'/'+file):null;
   if(corruptGit?.file===root+'/'+file&&ref===(corruptGit.ref??''))post=Buffer.concat([post??body,Buffer.from(' ')]);
   if(post){const oid=crypto.createHash('sha1').update(Buffer.from('blob '+post.length+'\0')).update(post).digest('hex');parts.push(Buffer.from(oid+' blob '+post.length+'\n'),post,Buffer.from('\n'));}
   else parts.push(output.subarray(offset,end+n+2));
   offset=end+n+2;
  }
  if(offset!==output.length)throw Error('OFFLINE_COMPLETE_GIT_OUTPUT_REQUIRED');return Buffer.concat(parts);
 }
 const executable=source.replace("import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';\n",'').replace(/^export /gm,'');
 const api=new Function('fs','path','execFileSync',executable+'\nreturn {preflight};')(facade,path,child);
 const check=(v,c)=>{if(!v)throw Error(c);};
 let result;
 if(chain){
  function module(text,filename,bindings,names){const code=text.replace(/^import .*;\r?$/gm,'').replace(/\bexport (?=(?:const|function)\b)/g,'').replaceAll('fileURLToPath(import.meta.url)','moduleFilename');return new Function(...Object.keys(bindings),'moduleFilename',code+'\nreturn {'+names.join(',')+'};')(...Object.values(bindings),filename);}
  const helper=module(read('originalBatch/git-image-batch.mjs'),report+'/docs/healthroster-build/proposals/daily-health-genuine-staged-preflight-batch-forward-20261017/git-image-batch.mjs',{fs:facade,path,execFileSync:child},['blobImages','physical','git']);
  const shared={fs:facade,path,sha:hash,check,ROOT,...helper};
  const h=module(read('holder/source.mjs'),report+'/docs/healthroster-build/proposals/daily-health-s0-catalogue-holder-20261017/source.mjs',{...shared,execFileSync:child},['source']);
  const c=module(read('coordinator/source.mjs'),report+'/'+folders.coordinator+'source.mjs',{...shared,originalHolderSource:h.source,report},['source']);
  const s=module(read('continuation/source.mjs'),report+'/'+folders.continuation+'source.mjs',{...shared,originalCoordinatorSource:c.source},['source']);
  result={operatorCommit:s.source(),rootCommit:git(ROOT,'rev-parse','HEAD').toString().trim()};
 }else result=api.preflight({here:report+'/'+folders.operator.slice(0,-1),ROOT,PRIVATE_STATE:privatePath,check,sha:hash});
 if(privateReads!==0||syntheticMetadata!==(chain?0:2))throw Error('OFFLINE_PRIVATE_BOUNDARY_CHANGED');
 return {status:chain?'OFFLINE_COMPLETE_SOURCE_CHAIN_POSTIMAGES_ONLY':'OFFLINE_PUBLIC_POSTIMAGE_PREFLIGHT_ONLY',rootCommit:result.rootCommit,publicReads,gitCalls,syntheticPrivatePathMetadataChecks:syntheticMetadata,privateContentReads:privateReads,liveAdmission:false};
}
