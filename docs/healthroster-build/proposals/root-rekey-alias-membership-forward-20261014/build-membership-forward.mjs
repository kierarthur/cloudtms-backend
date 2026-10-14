import fs from 'node:fs';
import path from 'node:path';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import {fileURLToPath} from 'node:url';
const dir=path.dirname(fileURLToPath(import.meta.url));
const beforeFile=path.join(dir,'../root-rekey-alias-native-first-use-20261014/02_NATIVE_HELPER_FIXTURE_OWNER_CONFLICT_FORWARD.sql');
const afterFile=path.join(dir,'03_NATIVE_HELPER_FIXTURE_DATED_MEMBERSHIP_FORWARD.sql');
assert.ok(!fs.existsSync(afterFile));
const before=fs.readFileSync(beforeFile,'utf8'),hash=s=>crypto.createHash('sha256').update(s).digest('hex');
assert.equal(hash(before),'7454f9fc1ac622048b5d15bdd3532aabde10f12fe3e3624bbbf934eb7023c046');
const eol=before.includes('\r\n')?'\r\n':'\n',lines=rows=>rows.join(eol);
const hunks=[
 {name:'Respect existing one-client dated-group GiST exclusion',before:lines([
 "insert into public.weekly_source_group_clients(source_group_id,client_id,valid_from,created_by_user_id)",
 "select id,'f7120000-0000-4000-8000-000000000002','2026-01-01','f7120000-0000-4000-8000-000000000001'",
 "from public.weekly_source_groups where id in('f7120000-0000-4000-8000-000000000005','f7120000-0000-4000-8000-000000000007');"]),after:lines([
 "insert into public.weekly_source_group_clients(source_group_id,client_id,valid_from,valid_to,created_by_user_id)",
 "values('f7120000-0000-4000-8000-000000000005','f7120000-0000-4000-8000-000000000002','2026-01-01','2026-09-15','f7120000-0000-4000-8000-000000000001'),",
 " ('f7120000-0000-4000-8000-000000000007','f7120000-0000-4000-8000-000000000002','2026-09-16',null,'f7120000-0000-4000-8000-000000000001');"])},
 {name:'Temporary seed gets genuine fixture work date with unchanged ordinary default',before:'create function pg_temp.hr_alias_seed_row(p_row uuid,p_upload uuid,p_ordinal integer,p_key text,p_event uuid,p_resolution uuid,p_link uuid)',after:"create function pg_temp.hr_alias_seed_row(p_row uuid,p_upload uuid,p_ordinal integer,p_key text,p_event uuid,p_resolution uuid,p_link uuid,p_work_date date default '2026-09-15')"},
 {name:'Seeded clocks follow the supplied fixture work date',before:lines([
 " values(p_row,p_upload,p_ordinal,p_key,'Synthetic alias candidate','Synthetic alias verifier','2026-09-15',",
 "   '2026-09-15 09:00','2026-09-15 17:00',30,450,'SOURCE_WORKED','Synthetic fixture',decode(repeat('74',32),'hex'));"]),after:lines([
 " values(p_row,p_upload,p_ordinal,p_key,'Synthetic alias candidate','Synthetic alias verifier',p_work_date,",
 "   p_work_date+'09:00'::time,p_work_date+'17:00'::time,30,450,'SOURCE_WORKED','Synthetic fixture',decode(repeat('74',32),'hex'));"])},
 {name:'Wrong-group source row is on the actual next membership date',before:"select pg_temp.hr_alias_seed_row('f7120000-0000-4000-8000-000000000026','f7120000-0000-4000-8000-000000000012',1,'B',null,null,null);",after:"select pg_temp.hr_alias_seed_row('f7120000-0000-4000-8000-000000000026','f7120000-0000-4000-8000-000000000012',1,'B',null,null,null,'2026-09-17');"},
 {name:'Wrong-group test proves real eligibility before the absence assertion',before:" perform pg_temp.hr_alias_assert(private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000026',own_candidate,own_client) is null,'HR_ALIAS_WRONG_GROUP');",after:lines([
 " perform pg_temp.hr_alias_assert(private.weekly_source_upload_client_eligible_v1('f7120000-0000-4000-8000-000000000012',own_client,'2026-09-17'),'HR_ALIAS_OTHER_GROUP_DATED_ELIGIBILITY_REQUIRED');",
 " perform pg_temp.hr_alias_assert(private.weekly_source_known_source_key_v1('f7120000-0000-4000-8000-000000000026',own_candidate,own_client) is null,'HR_ALIAS_WRONG_GROUP');"])}
];
let after=before;for(const h of hunks){assert.equal(after.split(h.before).length,2,h.name);after=after.replace(h.before,h.after);}let reverse=after;for(const h of [...hunks].reverse()){assert.equal(reverse.split(h.after).length,2,h.name);reverse=reverse.replace(h.after,h.before);}assert.equal(reverse,before);
fs.writeFileSync(afterFile,after,{flag:'wx'});
fs.writeFileSync(path.join(dir,'MEMBERSHIP_FORWARD_REVERSE.json'),JSON.stringify({finding:'P1-ALIAS-FIXTURE1',sourceOnly:true,before:{file:beforeFile,sha256:hash(before)},after:{file:path.basename(afterFile),sha256:hash(after)},hunks,uniqueWholeReverseHunks:hunks.length,wholeReverseVerified:true,nativeConstraintChanged:false,fixtureExecuted:false},null,2)+'\n',{flag:'wx'});
console.log(JSON.stringify({finding:'P1-ALIAS-FIXTURE1',afterSha256:hash(after),hunks:hunks.length,wholeReverseVerified:true,nativeExecuted:false}));
