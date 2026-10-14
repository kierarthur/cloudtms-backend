// One exact native guard extraction. No SQL execution or other-lane mutation.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import assert from 'node:assert/strict';import {fileURLToPath} from 'node:url';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..');
const input='docs/healthroster-build/proposals/checklist-native-invoice-20261014/input/public_weekly_source_invoice_admit_atomic_v1.sqlinc';
const before=fs.readFileSync(path.join(repo,input),'utf8');
const name='private.weekly_source_invoice_unreleased_vat_fit_movements_v1';
const predicate=`select 1 from public.weekly_source_manifest_movements mm join public.weekly_source_billing_movements m on m.id=mm.billing_movement_id
    where mm.client_manifest_id=v_manifest.id and m.source_facts_json->>'invoice_hold'='VAT_FIT'
      and not exists(select 1 from public.weekly_source_invoice_hold_releases receipt where receipt.upload_row_id::text=m.source_facts_json->>'upload_row_id' and receipt.hold_kind='VAT_FIT')`;
const old=predicate.replaceAll('\n',before.includes('\r\n')?'\r\n':'\n');
assert.equal(before.split(old).length,2);
const replacement=`select 1 from ${name}(v_manifest.id)`;
const after=before.replace(old,replacement);
const core=`-- Literal current native admission blocker, factored once without repricing.
-- The native writer still owns all preceding guards, locks and action semantics.
create or replace function ${name}(p_client_manifest_id uuid)
returns setof public.weekly_source_billing_movements language sql stable security definer
set search_path to 'pg_catalog','pg_temp'
as $native_blocker$
${predicate.replace('select 1','select m.*').replace('v_manifest.id','p_client_manifest_id')};
$native_blocker$;
alter function ${name}(uuid) owner to postgres;
revoke all on function ${name}(uuid) from public,anon,authenticated,service_role;
`;
const sql='supabase/repeatable/healthroster_wp_u3_source_proposals/';
const forward=core+'\n'+after,reverse=before+`\ndrop function ${name}(uuid);\n`;
const hash=b=>crypto.createHash('sha256').update(b).digest('hex');
const save=(p,s)=>{const file=path.join(repo,p);if(fs.existsSync(file)&&!fs.readFileSync(file).equals(Buffer.from(s)))throw Error('FROZEN_INVOICE_BLOCKER_CHANGED:'+p);fs.writeFileSync(file,s);};
save(sql+'wp_u3_invoice_block_native.forward.sqlinc',forward);
save(sql+'wp_u3_invoice_block_native.reverse.sqlinc',reverse);
save(path.relative(repo,path.join(here,'NATIVE_BLOCKER_REVERSE.json')),
  JSON.stringify({status:'SOURCE_PROPOSAL_UNMOUNTED_NATIVE_PARITY_AND_PURITY_HELD',source_commit:'fa66183f01913e86651e82816f5e88f97e07123c',
    before_path:input,before_sha256:hash(before),after_sha256:hash(after),core_sha256:hash(core),forward_sha256:hash(forward),reverse_sha256:hash(reverse),
    source_body:'supabase/repeatable/14102026_0541_healthroster_report_current_native_composition/15_complete_native_report_admission.sqlinc',
    hunk:{before:old,after:replacement},native:false,canonical_owner_edited:false,existing_writer_ACL_changed:false},null,2)+'\n');
console.log(JSON.stringify({one_native_predicate:true,native:false,source_only:true}));
