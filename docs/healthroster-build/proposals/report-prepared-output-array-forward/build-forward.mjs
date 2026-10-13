// Adjacent complete forward; frozen Prepared30 is read-only.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath, pathToFileURL } from 'node:url';
export const here=path.dirname(fileURLToPath(import.meta.url));
export const original=path.resolve(here,'../report-prepared-output-retention');
export const sha=x=>crypto.createHash('sha256').update(x).digest('hex');
const CRITIC_SHA='180167ebb3f3979b97056668ef5de46ee96447bc61edd85a4158f23899ef4284';
const OLD_MANIFEST_SHA='80a2577cbf5016f1a76e323717809c5553070e47b0b9a80e11b1ef3a0af34c70';
export const once=(s,from,to)=>{if(s.split(from).length!==2)throw Error('EXACT_FORWARD_ANCHOR_CHANGED');return s.replace(from,()=>to);};
export const oldCodecArray=`'hr_crosscheck_issues',pg_catalog.to_jsonb(p_fin)->'hr_crosscheck_issues'`;
export const newCodecArray=`'hr_crosscheck_issues',p_fin.hr_crosscheck_issues::text`;
export const oldValidation=object=>`  if pg_catalog.jsonb_typeof(${object}->'hr_crosscheck_issues') not in ('null','array') then\n    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;\n  if pg_catalog.jsonb_typeof(${object}->'hr_crosscheck_issues')='array' and exists(select 1 from pg_catalog.jsonb_array_elements(${object}->'hr_crosscheck_issues') a where pg_catalog.jsonb_typeof(a) not in ('null','string')) then\n    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;`;
export const newValidation=object=>`  if pg_catalog.jsonb_typeof(${object}->'hr_crosscheck_issues') not in ('null','string') then\n    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;\n  if pg_catalog.jsonb_typeof(${object}->'hr_crosscheck_issues')='string' then\n    begin\n      -- Native array_out text preserves dimensions, non-1 lower bounds,\n      -- NULL elements versus quoted NULL strings, quotes and backslashes.\n      -- Fixed array_nulls=on is parsing configuration, never authority.\n      if (((${object}->>'hr_crosscheck_issues')::text[])::text)\n        is distinct from ${object}->>'hr_crosscheck_issues' then\n        raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;\n    exception when invalid_text_representation or array_subscript_error or program_limit_exceeded then\n      raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000';\n    end;\n  end if;`;
export const CONFIG="set array_nulls to 'on'\n";
export function forwardCodec(s){let n=once(s,oldCodecArray,newCodecArray);n=once(n,oldValidation('v_output'),newValidation('v_output'));
 const head="create or replace function private.weekly_source_report_prepared_financial_codec_v1(p_fin public.timesheets_financials)\nreturns jsonb language plpgsql stable security definer\nset search_path to 'pg_catalog','pg_temp'\n";
 return once(n,head,head+CONFIG);}
export function reverseCodec(s){let r=once(s,newCodecArray,oldCodecArray);r=once(r,newValidation('v_output'),oldValidation('v_output'));return once(r,CONFIG,'');}
export function forwardAmount(s){let n=once(s,oldValidation('v_fin'),newValidation('v_fin'));return once(n,"set search_path to 'pg_catalog','pg_temp'\n","set search_path to 'pg_catalog','pg_temp'\n"+CONFIG);}
export function reverseAmount(s){let r=once(s,newValidation('v_fin'),oldValidation('v_fin'));return once(r,CONFIG,'');}
export async function build(criticPath){
 const read=p=>fs.readFileSync(path.join(original,p));
 const manifestBytes=read('SOURCE_MANIFEST.json');if(sha(manifestBytes)!==OLD_MANIFEST_SHA)throw Error('FROZEN_PREPARED_MANIFEST_CHANGED');
 const oldManifest=JSON.parse(manifestBytes);
 for(const a of oldManifest.artifacts){const b=read(a.path);if(b.length!==a.bytes||sha(b)!==a.sha256)throw Error('FROZEN_PREPARED_ARTIFACT_CHANGED');}
 const critic=fs.readFileSync(criticPath);if(sha(critic)!==CRITIC_SHA)throw Error('EXACT_COMPLETE_CRITIC_REQUIRED');
 fs.writeFileSync(path.join(here,'SOURCE_CRITIC.md'),critic);
 const oldCodec=read('02_codec.sqlinc').toString(),oldAmount=read('05_retained_amount.sqlinc').toString();
 const codec=forwardCodec(oldCodec),amount=forwardAmount(oldAmount);
 if(reverseCodec(codec)!==oldCodec||reverseAmount(amount)!==oldAmount)throw Error('FULL_OWNER_REVERSE_FAILED');
 fs.writeFileSync(path.join(here,'02_complete_codec_array_forward.sqlinc'),codec);
 fs.writeFileSync(path.join(here,'05_complete_retained_amount_array_forward.sqlinc'),amount);
 // Complete producer successor, guarded against the frozen generator. It is
 // not invoked here and must be composed only in a NEW root-owned packet.
 let generator=read('build-proposal.mjs').toString();
 const oldKind="/^text\\[\\]/.test(t)?'array':'string'";
 generator=once(generator,oldKind,"/^text\\[\\]/.test(t)?'string':'string'");
 const oldGeneratorArray="  if(kind==='array')code+=`\\n  if pg_catalog.jsonb_typeof(${object}->'${f}')='array' and exists(select 1 from pg_catalog.jsonb_array_elements(${object}->'${f}') a where pg_catalog.jsonb_typeof(a) not in ('null','string')) then\\n    raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;`;";
 const newGeneratorArray="  if(/^text\\[\\]/.test(t))code+=`\\n  if pg_catalog.jsonb_typeof(${object}->'${f}')='string' then\\n    begin\\n      -- Native array_out text preserves dimensions, non-1 lower bounds,\\n      -- NULL elements versus quoted NULL strings, quotes and backslashes.\\n      -- Fixed array_nulls=on is parsing configuration, never authority.\\n      if (((${object}->>'${f}')::text[])::text)\\n        is distinct from ${object}->>'${f}' then\\n        raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000'; end if;\\n    exception when invalid_text_representation or array_subscript_error or program_limit_exceeded then\\n      raise exception 'WEEKLY_SOURCE_PREPARED_OUTPUT_CODEC_INVALID' using errcode='55000';\\n    end;\\n  end if;`;";
 generator=once(generator,oldGeneratorArray,newGeneratorArray);
 generator=once(generator,"info.numeric.includes(f)||info.json.includes(f)?", "info.numeric.includes(f)||info.json.includes(f)||/^text\\[\\]/.test(info.types[f])?");
 const oldFn="set search_path to 'pg_catalog','pg_temp'\\nas $function$\\n${body}";
 const newFn="set search_path to 'pg_catalog','pg_temp'\\n${['weekly_source_report_prepared_financial_codec_v1','weekly_source_report_prepared_pay_amount_v1'].includes(name)?\"set array_nulls to 'on'\\n\":''}as $function$\\n${body}";
 generator=once(generator,oldFn,newFn);
 fs.writeFileSync(path.join(here,'build-proposal.complete-array-forward.mjs'),generator);
 const root=path.resolve(here,'../../../../../healthroster-v2-backend-13102026');
 const {scanManagedSql}=await import(pathToFileURL(path.join(root,'scripts/cloudtms-managed-release-sql.mjs')));
 for(const s of [codec,amount])if(scanManagedSql(s).statements.length<1)throw Error('SQL_PACKAGING_FAILED');
 let generatorReverse=once(generator,newKindFix(),oldKind);
 generatorReverse=once(generatorReverse,newGeneratorArray,oldGeneratorArray);
 generatorReverse=once(generatorReverse,"info.numeric.includes(f)||info.json.includes(f)||/^text\\[\\]/.test(info.types[f])?","info.numeric.includes(f)||info.json.includes(f)?");
 generatorReverse=once(generatorReverse,newFn,oldFn);
 if(generatorReverse!==read('build-proposal.mjs').toString())throw Error('COMPLETE_GENERATOR_REVERSE_FAILED');
 const receipt={schema_version:'REPORT_PREPARED_OUTPUT_ARRAY_FORWARD_V1',status:'SOURCE_ONLY_NATIVE_ARRAY_ROUNDTRIP_HELD',
  frozenPreparedCommit:'54e06721ab030e5060272ec6a0a58bf47a17587d',frozenPreparedManifestSha256:OLD_MANIFEST_SHA,
  frozenPreparedPackageSha256:oldManifest.packageSha256,criticSha256:CRITIC_SHA,
  finding:'P2 valid native multidimensional text[] was narrowed to flat JSON array',
  sourceVersionUnchanged:'WEEKLY_SOURCE_REPORT_PREPARED_WRITER_OUTPUT_V1',fieldNames:90,numericStringFields:39,jsonTextFields:9,
  arrayFields:{hr_crosscheck_issues:'Native PostgreSQL canonical text[] array_out string or SQL NULL; not flattened JSON.'},
  wholeNativeRowColumns:95,wholeNativeRowFingerprintDomainUnchanged:true,
  lowerBoundEffect:'The unchanged to_jsonb(whole row) fingerprint cannot distinguish array lower bounds. Closed codec text equality now retains/fences those bounds independently.',
  newFunctions:0,newPermanentColumns:0,oldCombinedACLNotReplaced:true,
  reverseProofs:[{path:'02_complete_codec_array_forward.sqlinc',originalSha256:sha(oldCodec),reverseSha256:sha(reverseCodec(codec))},
   {path:'05_complete_retained_amount_array_forward.sqlinc',originalSha256:sha(oldAmount),reverseSha256:sha(reverseAmount(amount))},
   {path:'build-proposal.complete-array-forward.mjs',originalSha256:sha(read('build-proposal.mjs')),reverseSha256:sha(generatorReverse)}],
  frozenArtifactsVerified:oldManifest.artifactCount,databaseCalls:false,rootWrites:false,bankingSourceReads:false,nativeProof:false,
  remainingGates:['Independent critic review of exact complete forward and fixed array parser configuration.',
   'Root native 2D/non-1 lower bound/quoted NULL/backslash/SQL NULL actual codec cases under SEND_7.',
   'Original capture/receipt/financial/transport/contract/combined ACL/date/API gates remain unchanged and held.']};
 fs.writeFileSync(path.join(here,'FORWARD_RECEIPT.json'),JSON.stringify(receipt,null,2)+'\n');
 return receipt;
}
const newKindFix=()=>"/^text\\[\\]/.test(t)?'string':'string'";
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url)){
 if(!process.argv[2])throw Error('EXACT_READ_ONLY_CRITIC_PATH_REQUIRED');
 const r=await build(path.resolve(process.argv[2]));console.log(JSON.stringify({status:r.status,reverseProofs:r.reverseProofs}));
}
