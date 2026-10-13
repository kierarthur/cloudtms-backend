import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const root='C:/Users/KierArthur/OneDrive - Arthur Rai/Documents/GitHub/.codex-worktrees/healthroster-v2-backend-13102026';
const out=path.dirname(fileURLToPath(import.meta.url)),sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const inputs=[
 ['supabase/repeatable/13102026_1344_weekly_source_self_bill_report/13102026_1344_projection_rows.sqlinc','8218421d0732c10c3b2c2b3f49c2b5a982956d9d70d7920db846d7cdb1cd44e1'],
 ['supabase/repeatable/13102026_1561_weekly_source_magnit_contract_price_v1.sql','d05872065c382d3d4d63330a91dfbf09ce0903ee58bc76c7590b1b3cc1be2c28']
];
// These seven missing suffixes match all corresponding literal predicates in
// the exact installed native owner obtained under the retained SEND_7 turn.
const anchors=[
 "coalesce(v_economic->>'sign','')!~'^[-]?[0-9]+",
 "coalesce(v_economic->>'paid_minutes','')!~'^[0-9]+",
 "coalesce(v_economic->>'break_minutes','')!~'^[0-9]+",
 "coalesce(v_economic#>>array['bucket_minutes',bucket.name],'')!~'^[0-9]+",
 "coalesce(v_economic#>>array['hours',bucket.name],'')!~'^[+-]?[0-9]+([.][0-9]+)?",
 "coalesce(v_economic#>>array['pay_rates',bucket.name],'')!~'^[+]?[0-9]+([.][0-9]+)?",
 "coalesce(v_economic#>>array['charge_rates',bucket.name],'')!~'^[+]?[0-9]+([.][0-9]+)?"
];
const writes=[];
for(const [file,beforeSha256] of inputs){
 const bytes=fs.readFileSync(path.join(root,file));if(sha(bytes)!==beforeSha256)throw Error('PROJECTION_INPUT_CHANGED');
 const before=bytes.toString('utf8');let after=before;
 for(const anchor of anchors){
  if(after.split(anchor).length!==2||after.includes(anchor+"$'"))throw Error('PROJECTION_LITERAL_ANCHOR_CHANGED');
  const position=after.indexOf(anchor)+anchor.length;if(!/^\r?\n/.test(after.slice(position)))throw Error('MISSING_LITERAL_NOT_AT_LINE_END');
  after=after.replace(anchor,()=>anchor+"$'");
 }
 let restored=after;for(const anchor of anchors)restored=restored.replace(anchor+"$'",()=>anchor);
 if(restored!==before)throw Error('UNRELATED_PROJECTION_BYTES_CHANGED');
 const regexLines=after.split(/\r?\n/).filter(line=>line.includes("!~'"));
 if(regexLines.length!==9||regexLines.some(line=>! /!~'\^[^\r\n']+\$'/.test(line)))throw Error('NATIVE_REGEX_CENSUS_INCOMPLETE');
 writes.push({file,beforeSha256,afterSha256:sha(after),suffixInsertions:anchors.length,completeNativeRegexPredicates:9,unrelatedBytesRetained:true,after});
}
for(const w of writes)fs.writeFileSync(path.join(root,w.file),w.after);
const receipt={status:'FORWARD_SOURCE_REPAIR_NATIVE_RETRY_PENDING',baseline:'5d334f47ba5ec788c98e498f1c564fa759c180c5',nativeTrial:'REPORT_M1_PROJECTION_SHORT_TRIAL_1.json',observedFailure:{statement:4,sqlstate:'42601',message:'missing THEN at end of SQL expression'},reason:'Previous total-pay/charge literal repair was incomplete: seven further native economic guards in each complete projection owner still lacked the dollar anchor and closing SQL quote. Exactly fourteen callback-safe suffix insertions restore their native predicates.',changes:writes.map(({after,...w})=>w),signatureChanges:0,privilegeChanges:0,frozenInputsChanged:false,financialAcceptance:false};
const json=JSON.stringify(receipt,null,2)+'\n';
fs.writeFileSync(path.join(out,'REPORT_M1_REMAINING_LITERAL_REPAIR.json'),json);
fs.writeFileSync(path.join(root,'docs/healthroster-build/rehearsals/13102026/REPORT_M1_REMAINING_LITERAL_REPAIR.json'),json);
console.log(JSON.stringify(receipt));
