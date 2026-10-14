// Read-only syntax/planning diagnosis; no rehearsal or business write.
import fs from 'node:fs';import {spawnSync} from 'node:child_process';
import {agencyChildEnvironment} from '../scripts/healthroster-rehearsal-turn.mjs';
import {backgroundSnapshotFieldsSql} from '../scripts/healthroster-live-test-restoration.mjs';
import {scanManagedSql} from '../scripts/cloudtms-managed-release-sql.mjs';
const source=fs.readFileSync('supabase/release/export_contract.sql','utf8');
const statement=scanManagedSql(source).statements[1];const query=source.slice(statement.start,statement.end).trim().replace(/;\s*$/,'');
const sql="begin read only; do $check$ begin if current_database()<>'cloudtms_test_clone' or not exists(select 1 from private.cloudtms_database_identity where singleton and environment='TEST' and customer_key is null) then raise exception 'WRONG_TEST'; end if; end $check$;\nexplain with catalogue as ("+query+") select jsonb_build_object("+backgroundSnapshotFieldsSql()+");\nrollback;";
const r=spawnSync('C:/Program Files/PostgreSQL/18/bin/psql.exe',['-X','-q','-A','-t','-v','ON_ERROR_STOP=on','-v','VERBOSITY=verbose','-f','-'],{input:sql,encoding:'utf8',windowsHide:true,env:agencyChildEnvironment('healthroster_install1_readonly_explain'),maxBuffer:2000000});
const line=r.stderr.split(/\r?\n/).find(x=>x.includes('ERROR:'))??'';
const metadata=line.match(/(?:function|operator does not exist:)\s+([a-z0-9_.]+\([a-z0-9_, ]+\)|[a-z0-9_ ]+ [><=+*/-]+ [a-z0-9_ ]+)/i)?.[1]??null;
console.log(JSON.stringify({status:r.status,sqlstate:line.match(/ERROR:\s*([A-Z0-9]{5})/)?.[1]??null,undefinedRoutineOrOperatorMetadata:metadata,sourceOnlyExplain:true}));
