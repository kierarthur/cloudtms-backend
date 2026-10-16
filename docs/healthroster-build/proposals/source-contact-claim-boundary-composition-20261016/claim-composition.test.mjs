import test from 'node:test';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import path from 'node:path';
import {FROZEN_COMMIT,FROZEN_PATH,frozenClaim,compose,assertExactComposition,SELECTOR,STAMP} from './build-claim-composition.mjs';
const base=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(base,'../../../..');
const frozen=frozenClaim(execFileSync('git',['show',`${FROZEN_COMMIT}:${FROZEN_PATH}`],{cwd:root,encoding:'utf8'}).replace(/\r\n/g,'\n'));
const actual=readFileSync(path.join(base,'forward/01_complete_native_claim_core.sqlinc'),'utf8');
test('complete generated core equals only the finite parent transformation',()=>{assert.equal(actual,compose(frozen));assertExactComposition(actual,frozen);});
test('both complete query branches retain their full original bytes after removing only selector/stamp additions',()=>{
 for(const label of ['RETAINED LEGACY','INDEXED FACTUAL']){const start=`-- BEGIN ${label} CLAIM QUERY`,end=`-- END ${label} CLAIM QUERY`;
  const extract=sql=>sql.slice(sql.indexOf(start),sql.indexOf(end)+end.length);
  assert.equal(extract(actual).split(SELECTOR).join('').split(STAMP).join('').replace('cross join contact_claim proof ',''),extract(frozen));}
});
for(const [name,before,after]of[
 ['missing module assertion','v_legacy:=private.bpay_next_legacy_email_claim_legacy_v1();','v_legacy:=true;'],
 ['unconditional financial claim','if v_legacy is true then','if true then'],
 ['missing factual index predicate','and private.bpay_next_legacy_email_is_factual_v1(mo.type,mo.context_kind,mo.reference) is true',''],
 ['weakened invoice authority',"or i.status<>'ISSUED'","or false"],
 ['weakened Candidate authority',"workflow.state='AWAITING_PAPER_RETURN'","true"],
 ['changed lease lifetime','make_interval(mins=>v_lease)','make_interval(mins=>30)'],
 ['changed skip locked selection','for update skip locked limit v_limit','limit v_limit'],
 ['weakened query selector','d.contact_managed','true'],
 ['removed stamp','cross join contact_claim proof ',''],
 ['changed invoker authority','security invoker','security definer']]){
 test('rejects '+name,()=>{assert.notEqual(actual.replace(before,after),actual);assert.throws(()=>assertExactComposition(actual.replace(before,after),frozen));});
}
