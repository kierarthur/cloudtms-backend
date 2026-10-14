import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import {execFileSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const packet=path.dirname(fileURLToPath(import.meta.url));
const root=path.resolve(process.argv[2]||'');
const pin='74a9c10f7370cd3641ee95d31f6702ecadd900a7';
const git=(...args)=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],{cwd:root,maxBuffer:24*1024*1024});
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
if(git('rev-parse','HEAD').toString().trim()!==pin)throw new Error('UNDO_CURRENT_SOURCE_PIN_CHANGED');
const rel='broker/src/weekly-source/routes.js';
const before=fs.readFileSync(path.join(root,rel));if(!before.equals(git('show',`${pin}:${rel}`)))throw new Error('CURRENT_ROUTES_CHANGED');
const changes=[
  {before:"  NO_SHIFTS_TO_IMPORT: 'weekly_source_no_shifts_attest_atomic_v1',",after:"  NO_SHIFTS_TO_IMPORT: 'weekly_source_no_shifts_attest_atomic_v1',\r\n  UNDO_NO_ONE_WORKED: 'weekly_source_no_shifts_undo_atomic_v1',"},
  {before:'  if (DIRECT_RPC_COMMANDS[action]) {',after:`  if (action === 'UNDO_NO_ONE_WORKED') {
    const keys = ['source_cycle_id', 'source_group_id', 'client_id', 'completion_id', 'expected_cycle_version'];
    if (Object.keys(payload).length !== keys.length || keys.some(key => !Object.hasOwn(payload, key))
        || Object.keys(payload).some(key => !keys.includes(key))) {
      fail('WEEKLY_SOURCE_NO_SHIFTS_UNDO_REQUEST_INVALID', 'Refresh the Checklist and try again.', 400);
    }
    for (const key of keys.filter(key => key.endsWith('_id'))) {
      request[key] = canonicalUuid(payload[key], 'WEEKLY_SOURCE_NO_SHIFTS_UNDO_REQUEST_INVALID', key);
    }
    const version = payload.expected_cycle_version;
    if (!(typeof version === 'string' && /^(0|[1-9][0-9]{0,18})$/.test(version))
        && !(Number.isSafeInteger(version) && version >= 0)) {
      fail('WEEKLY_SOURCE_NO_SHIFTS_UNDO_REQUEST_INVALID', 'Refresh the Checklist and try again.', 400);
    }
    request.expected_cycle_version = String(version);
    if (BigInt(request.expected_cycle_version) >= 9223372036854775807n) {
      fail('WEEKLY_SOURCE_NO_SHIFTS_UNDO_REQUEST_INVALID', 'Refresh the Checklist and try again.', 400);
    }
    const result = await withRefusalRecording(dependencies,
      'broker:weekly-source:weekly_source_no_shifts_undo_atomic_v1', user.id,
      () => callRpc(dependencies, 'weekly_source_no_shifts_undo_atomic_v1', request));
    const receiptKeys = ['ok', 'status', 'idempotent', 'completion_id', 'source_cycle_id', 'source_group_id',
      'client_id', 'cycle_state', 'cycle_version', 'undone_by_user_id', 'undone_at_utc'];
    if (!result || typeof result !== 'object' || Array.isArray(result)
        || Object.keys(result).length !== receiptKeys.length
        || receiptKeys.some(key => !Object.hasOwn(result, key))
        || result.ok !== true || result.status !== 'NO_SHIFTS_UNDONE' || typeof result.idempotent !== 'boolean'
        || !['OPEN', 'FINALISABLE', 'FINALISED'].includes(result.cycle_state)
        || typeof result.cycle_version !== 'string' || !/^(0|[1-9][0-9]{0,18})$/.test(result.cycle_version)
        || typeof result.undone_at_utc !== 'string' || !Number.isFinite(Date.parse(result.undone_at_utc))
        || ['completion_id', 'source_cycle_id', 'source_group_id', 'client_id'].some(key => result[key] !== request[key])
        || result.undone_by_user_id !== user.id) {
      fail('WEEKLY_SOURCE_NO_SHIFTS_UNDO_RECEIPT_UNAVAILABLE', 'The Undo result could not be verified. Refresh the Checklist before trying again.', 503);
    }
    if ((!result.idempotent && BigInt(result.cycle_version) !== BigInt(request.expected_cycle_version) + 1n)
        || BigInt(result.cycle_version) > 9223372036854775807n) {
      fail('WEEKLY_SOURCE_NO_SHIFTS_UNDO_RECEIPT_UNAVAILABLE', 'The Undo result could not be verified. Refresh the Checklist before trying again.', 503);
    }
    return jsonResponse(200, result);
  }
  if (DIRECT_RPC_COMMANDS[action]) {`.replaceAll('\n','\r\n')},
];
let after=before.toString('utf8');
for(const change of changes){if(after.split(change.before).length!==2)throw new Error('UNDO_HUNK_NOT_UNIQUE');after=after.replace(change.before,()=>change.after);}
let reversed=after;for(const change of [...changes].reverse()){if(reversed.split(change.after).length!==2)throw new Error('UNDO_REVERSE_NOT_UNIQUE');reversed=reversed.replace(change.after,()=>change.before);}
if(reversed!==before.toString('utf8'))throw new Error('UNDO_FULL_REVERSE_MISMATCH');
for(const [folder,bytes]of [['before',before],['forward',Buffer.from(after)]]){
  const dest=path.join(packet,folder,rel);fs.mkdirSync(path.dirname(dest),{recursive:true});fs.writeFileSync(dest,bytes);
}
const owners=['supabase/repeatable/15092026_1534_weekly_source_read_projections_v1.sql',
  'supabase/repeatable/14102026_0044_weekly_source_protected_pay_authority_v2.sql',
  'supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql',
  'supabase/migrations/01102026_1201_weekly_source_client_cycle_scope.sql',
  'supabase/baseline/22082026_1500_cloudtms_test_structural_baseline.sql',
  'scripts/cloudtms-managed-release-sql.mjs','package.json','package-lock.json'];
const sourcePins=[];
for(const source of owners){
  const bytes=fs.readFileSync(path.join(root,source));if(!bytes.equals(git('show',`${pin}:${source}`)))throw new Error(`NATIVE_UNDO_PIN_CHANGED:${source}`);
  sourcePins.push({path:source,raw_sha256:sha(bytes),bytes:bytes.length});
}
const draftPath=path.join(packet,'..','install2-checklist-native-owner-20261014','native','03_no_shifts_undo_atomic.sqlinc');
const draft=fs.readFileSync(draftPath,'utf8');
const nativeChanges=[
  {before:'  perform private.weekly_source_query_require_service_v1();',after:`  perform private.weekly_source_query_require_service_v1();
  if current_setting('transaction_isolation') is distinct from 'read committed' then
    raise exception 'WEEKLY_SOURCE_NO_SHIFTS_UNDO_ISOLATION_UNAVAILABLE' using errcode='55000';
  end if;`},
  {before:"    or jsonb_typeof(p_request->'expected_cycle_version') is distinct from 'number'\n    or not pg_input_is_valid(p_request->>'expected_cycle_version','bigint') then",after:"    or jsonb_typeof(p_request->'expected_cycle_version') not in ('number','string')\n    or coalesce(p_request->>'expected_cycle_version','') !~ '^(0|[1-9][0-9]{0,18})$'\n    or not pg_input_is_valid(p_request->>'expected_cycle_version','bigint') then"},
  {before:"or v_completion_id is null or v_expected<0 then",after:"or v_completion_id is null or v_expected<0 or v_expected>=9223372036854775807 then"},
  {before:"'cycle_version',v_cycle.version,'undone_by_user_id'",after:"'cycle_version',v_cycle.version::text,'undone_by_user_id'"},
];
let native=draft.replaceAll('\r\n','\n');
for(const change of nativeChanges){if(native.split(change.before).length!==2)throw new Error(`NATIVE_UNDO_DRAFT_HUNK_NOT_UNIQUE:${change.before}`);native=native.replace(change.before,()=>change.after.replaceAll('\r\n','\n'));}
const nativeDir='supabase/repeatable/14102026_0900_healthroster_no_shifts_undo';
const nativeRel=`${nativeDir}/01_no_shifts_undo_atomic.sqlinc`;
const nativeRoot='supabase/repeatable/14102026_0900_healthroster_no_shifts_undo_v1.sql';
for(const [file,source]of [[nativeRel,native],[nativeRoot,"\\set ON_ERROR_STOP on\nbegin;\n\\ir 14102026_0900_healthroster_no_shifts_undo/01_no_shifts_undo_atomic.sqlinc\nnotify pgrst, 'reload schema';\ncommit;\n"]]){
  try{git('cat-file','-e',`${pin}:${file}`);throw new Error(`UNDO_NEW_DESTINATION_EXISTS:${file}`);}catch(error){if(error.message?.startsWith('UNDO_NEW_DESTINATION_EXISTS'))throw error;}
  const dest=path.join(packet,'forward',file);fs.mkdirSync(path.dirname(dest),{recursive:true});fs.writeFileSync(dest,source);
}
const seen=new Map();
function mount(file){
  if(seen.has(file))return;const bytes=file===rel?Buffer.from(after):fs.readFileSync(path.join(root,file));
  if(file!==rel&&!bytes.equals(git('show',`${pin}:${file}`)))throw new Error(`IMPORT_CHANGED:${file}`);
  const dest=path.join(packet,'test-mount',file);fs.mkdirSync(path.dirname(dest),{recursive:true});fs.writeFileSync(dest,bytes);
  const edges=[];for(const m of bytes.toString('utf8').matchAll(/^[ \t]*(?:import\s+(?:[^;]*?\sfrom\s+)?|export\s+(?:\*|\{[^}]*\})\s+from\s+)['"]([^'"\n]+)['"]/gm)){
    if(!m[1].startsWith('.'))continue;const target=path.posix.normalize(path.posix.join(path.posix.dirname(file),m[1]));
    if(!target.startsWith('broker/')&&!target.startsWith('candidate-broker/'))throw new Error(`IMPORT_ESCAPES_RUNTIME:${target}`);
    edges.push({specifier:m[1],target});
  }
  seen.set(file,{path:file,bytes:bytes.length,sha256:sha(bytes),edges});for(const edge of edges)mount(edge.target);
}
mount(rel);
fs.writeFileSync(path.join(packet,'CURRENT_UNDO_FULL_REVERSE.json'),JSON.stringify({schema:'CURRENT_NO_SHIFTS_UNDO_SOURCE_FORWARD_V1',
  actual_input_commit:pin,changed_owner:{path:rel,before_sha256:sha(before),after_sha256:sha(after),hunks:changes},
  native_source_pins:sourcePins,runtime_import_closure:[...seen.values()],native_seal:null,native_calls:false,database_calls:false},null,2)+'\n');
const entry=JSON.parse(fs.readFileSync(path.join(packet,'CURRENT_UNDO_FULL_REVERSE.json'),'utf8'));
entry.new_native_owners=[{path:nativeRel,before_absent:true,after_sha256:sha(native),signature:'public.weekly_source_no_shifts_undo_atomic_v1(jsonb)',
  logical_owner:'postgres',public_anon_authenticated_execute:false,service_role_execute:true},
  {path:nativeRoot,before_absent:true,after_sha256:sha(fs.readFileSync(path.join(packet,'forward',nativeRoot))),literal_relative_includes:[nativeRel]}];
entry.unreviewed_draft_provenance={path:path.relative(path.resolve(packet,'../../../..'),draftPath).replaceAll('\\','/'),raw_sha256:sha(draft),canonical_draft_sha256:sha(draft.replaceAll('\r\n','\n')),hunks:nativeChanges};
fs.writeFileSync(path.join(packet,'CURRENT_UNDO_FULL_REVERSE.json'),JSON.stringify(entry,null,2)+'\n');
console.log(JSON.stringify({changed_owner:rel,before_sha256:sha(before),after_sha256:sha(after),hunk_count:changes.length,test_import_objects:seen.size}));
