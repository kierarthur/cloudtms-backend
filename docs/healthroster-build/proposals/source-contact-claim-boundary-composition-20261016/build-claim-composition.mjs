import {execFileSync} from 'node:child_process';
import {readFileSync,writeFileSync,mkdirSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..');
export const FROZEN_COMMIT='90ffcf3f40644280fcdb5b791a62a86613896d6e';
export const FROZEN_PATH='supabase/repeatable/04102026_0558_banking_pay_next_legacy_email_claim_boundary_v1.sql';
export const CONTACT_COMMIT='66c6e5ef2971a22d34264b829877d6546c49a1be';
const sha=value=>createHash('sha256').update(value).digest('hex');
function input(ref,file){return execFileSync('git',['show',`${ref}:${file}`],{cwd:root,encoding:'utf8',maxBuffer:4e6}).replace(/\r\n/g,'\n');}
export function frozenClaim(sql){const start=sql.indexOf('create or replace function public.email_outbox_claim_ready_batch(');
 const end=sql.indexOf('$function$;',sql.indexOf('as $function$',start));
 if(start<0||end<0)throw Error('Frozen claim definition missing');return sql.slice(start,end+12)+'\n';}
export const SELECTOR=`(mo.type<>'TIMESHEET_QUERY' or(p_contact_bridge and exists(
          select 1 from public.hr_issue_email_deliveries d join public.hr_issue_email_deliveries original on original.id=d.original_delivery_id
          where d.mail_outbox_id=mo.id and d.contact_managed and d.legacy_claim_lease_token is null
           and d.status='QUEUED' and d.retired_at_utc is null and original.original_delivery_id=original.id and original.payload_revision=1)))
        and `;
export const STAMP=`    contact_claim as materialized (
      select case when p_contact_bridge then public._email_outbox_contact_claim_stamp_v1(
        coalesce(array_agg(p.id),'{}'::uuid[]),p_attempt_lease_token,v_now,v_now+make_interval(mins=>v_lease),p_agency,p_environment) else null end proof
      from picked p
    ),
`;
function replaceCount(sql,from,to,count){if(sql.split(from).length-1!==count)throw Error('Exact parent anchor count: '+from.slice(0,90));return sql.split(from).join(to);}
export function compose(frozen){let sql=frozen;
 sql=replaceCount(sql,'public.email_outbox_claim_ready_batch(','public._email_outbox_contact_claim_core_v1(',1);
 sql=replaceCount(sql,'  p_lease_minutes integer default 5','  p_lease_minutes integer default 5,p_contact_bridge boolean default false,p_agency uuid default null,p_environment text default null',1);
 sql=replaceCount(sql,"where mo.status='QUEUED'",'where '+SELECTOR+"mo.status='QUEUED'",2);
 sql=replaceCount(sql,'    updated as (',STAMP+'    updated as (',2);
 sql=replaceCount(sql,'from picked p where mo.id=p.id returning mo.*','from picked p cross join contact_claim proof where mo.id=p.id returning mo.*',2);
 sql=replaceCount(sql,'  end if;\n\nend;','  end if;\n  if p_contact_bridge then perform public._email_outbox_contact_claim_cleanup_v1(p_attempt_lease_token);end if;\n\nend;',1);
 return sql;}
export function assertExactComposition(actual,frozen){if(actual!==compose(frozen))throw Error('Claim composition differs from finite reviewed edits');
 for(const label of ['RETAINED LEGACY','INDEXED FACTUAL']){const start=`-- BEGIN ${label} CLAIM QUERY`,end=`-- END ${label} CLAIM QUERY`;
  if(actual.split(start).length!==2||actual.split(end).length!==2)throw Error('Both complete native branches required');}
 if(actual.split('v_legacy:=private.bpay_next_legacy_email_claim_legacy_v1();').length!==2)throw Error('Original owner lock required');}
export function build(){const source=input(FROZEN_COMMIT,FROZEN_PATH),frozen=frozenClaim(source),output=compose(frozen);
 assertExactComposition(output,frozen);mkdirSync(path.join(base,'forward'),{recursive:true});
 writeFileSync(path.join(base,'forward/01_complete_native_claim_core.sqlinc'),output);
 const contactFiles=['supabase/repeatable/includes/weekly_contact_query/082_native_claim_kernel.sqlinc',
  'supabase/repeatable/includes/weekly_contact_query/083_native_claim_bridge.sqlinc'];
 const receipt={version:'CONTACT_CLAIM_BOUNDARY_COMPOSITION_SOURCE_V1',status:'SOURCE_ONLY_NOT_MOUNTED_NATIVE_REQUIRED_NOT_RUN',
  frozen_parent:{commit:FROZEN_COMMIT,path:FROZEN_PATH,sha256_normalized_lf:sha(source),complete_claim_sha256:sha(frozen)},
  current_contact_inputs:contactFiles.map(file=>({commit:CONTACT_COMMIT,path:file,sha256_normalized_lf:sha(input(CONTACT_COMMIT,file))})),
  output:{path:'forward/01_complete_native_claim_core.sqlinc',sha256:sha(output),signature:'public._email_outbox_contact_claim_core_v1(integer,text,integer,boolean,uuid,text)'},
  exact_edit_recipe:['Rename only the existing claim function and append the three existing contact-composition operands.',
   'In BOTH complete native branches add the exact current TIMESHEET_QUERY selector ahead of the unchanged QUEUED predicate.',
   'In BOTH selected-row lease CTEs add the current contact stamp and force it through the same picked rows before the unchanged lease update.',
   'After the complete original LEGACY/factual branch append existing contact cleanup only when bridge=true.'],
  retained:['One actual Banking module FOR SHARE owner assertion before mail locks, unchanged helper and true-LEGACY/failure-factual branch selection.',
   'Separate indexed factual-only NEXT query and all exact Candidate, Source, completed-copy and invoice predicates.',
   'Original INVOKER/RLS, search path, input bounds, SKIP LOCKED/order, leases and returned mail row recipe.'],
  acl:'Retain current 083 wrappers and existing service-only core ACL; no new signature or browser grant. Do not mount historical public parent after the wrapper.',
  tests:'Exact whole-parent transformation and hostile branch/authority/lease mutation tests; no actual SQL execution or provider effect.',
  financial_owner_body_changed:false,database_actions:'NONE'};
 writeFileSync(path.join(base,'SOURCE_RECEIPT.json'),JSON.stringify(receipt,null,2)+'\n');return receipt;}
if(process.argv[1]&&path.resolve(process.argv[1])===fileURLToPath(import.meta.url))console.log(JSON.stringify(build()));
