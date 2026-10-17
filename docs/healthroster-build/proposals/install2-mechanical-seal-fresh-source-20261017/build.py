"""Offline packaging only. Never imports or runs a seal/provider/schema helper."""
import pathlib, hashlib, json, subprocess, difflib
HERE=pathlib.Path(__file__).resolve().parent
ROOT=pathlib.Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
def sha(b): return hashlib.sha256(b).hexdigest()
def git(*args):
 return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false',*args],cwd=ROOT)
def save(p,b):
 p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
def js(p): return (ROOT/p).read_bytes().decode('utf-8')
def change(text,before,after):
 assert text.count(before)==1,(before[:100],text.count(before))
 return text.replace(before,after,1)
base=git('rev-parse','HEAD').decode().strip()
parents=[
 'scripts/healthroster-install1-local-provider.mjs','scripts/healthroster-install1-local-schema.mjs',
 'scripts/seal-local-database-contract.mjs','scripts/verify-healthroster-install1-local-seal.mjs',
 'scripts/cloudtms-db-release-lib.mjs','scripts/cloudtms-managed-psql.mjs',
 'scripts/cloudtms-managed-release.mjs','scripts/cloudtms-managed-release-sql.mjs',
 'scripts/bpay-next-final-definers.mjs','scripts/weekly-source-reader-release.mjs',
 'supabase/release/current-release.json','supabase/release/current-contract.json',
 'supabase/release/migration-lock.json','supabase/release/protected-boundary-lock.json',
 'docs/bpay-next/final-definer-closures.json','supabase/release/export_contract.sql',
 'docs/healthroster-build/install1-source/LOCAL_PROVIDER_SEAL_PREREQUISITES.json',
 'docs/healthroster-build/install1-source/LOCAL_SCHEMA_CHECKPOINT.json',
 'docs/healthroster-build/install1-source/LOCAL_SEAL_CLEANUP_DURING_PAUSE.json',
 'docs/healthroster-build/install1-source/LOCAL_PP_CONTRACT_DELTA_REVIEW.json',
 'docs/healthroster-build/install1-source/LOCAL_PP_FIRST_USE_SECURITY.json',
 'docs/healthroster-build/install1-source/OWNER_PAUSE_CHECKPOINT.md',
 'docs/AUTOMATIC_TEST_DEPLOYMENT.md','docs/DATABASE_RELEASE_BIBLE.md',
 'AGENTS.md','.node-version','package.json'
]
release=json.loads((ROOT/'supabase/release/current-release.json').read_text())
for key in ['baselineRepeatableLock','bootstrapFile','controlPlaneMigration']:
 p=release.get(key)
 if isinstance(p,str) and p not in parents and (ROOT/p).is_file(): parents.append(p)
rows=[]
for p in parents:
 b=(ROOT/p).read_bytes();h=git('show',base+':'+p);i=git('show',':'+p)
 assert b==h==i,('CURRENT_PARENT_NOT_GIT_EXACT',p)
 save(HERE/'parents'/p,b);rows.append(dict(path=p,bytes=len(b),sha256=sha(b)))
def output_record(name,value): save(HERE/name,(json.dumps(value,indent=2)+'\n').encode())
output_record('PARENTS.json',dict(version='INSTALL2_MECHANICAL_FRESH_PARENTS_V1',root=str(ROOT),commit=base,rows=rows))
provider=js(parents[0])
first=provider.index("const container=");last=provider.index('function execute(')
provider=provider[:first]+'''import {profile,parseArguments,configureLocalProcess,assertProject,inspectOwnResources} from './healthroster-install2-local-seal-profile.mjs';
const candidate=parseArguments(process.argv.slice(2));
const sourceIdentity=assertProject(repoRoot,candidate);
configureLocalProcess(process.env);
const {container,owner,database}=profile,resources=inspectOwnResources();
const output=path.join(repoRoot,profile.output);
if(fs.existsSync(output))throw Error('I2_SEAL_FRESH_OUTPUT_REQUIRED');
''' +provider[last:]
provider=change(provider,"'psql','-h','127.0.0.1','-U','postgres','-d',target,","'psql','-U','postgres','-d',target,")
provider=change(provider,"if(mode===undefined){",'''if(execute(`select (not exists(select 1 from pg_database where datname='${database}')
 and not exists(select 1 from pg_roles where rolname='${owner}'))::text;`)!=='true')
 throw Error('I2_SEAL_FRESH_DATABASE_AND_OWNER_REQUIRED');
{
 const password="'"+process.env.PGPASSWORD.replaceAll("'","''")+"'";''')
provider=change(provider,'create role ${owner} login nosuperuser nobypassrls nocreatedb nocreaterole;',
 'create role ${owner} login nosuperuser nobypassrls nocreatedb nocreaterole password ${password};')
at=provider.index("fs.writeFileSync(path.join(repoRoot,'docs/healthroster-build/install1-source/LOCAL_PROVIDER_SEAL_PREREQUISITES.json')")
provider=provider[:at]+'''assertProject(repoRoot,candidate);
fs.mkdirSync(output);
fs.mkdirSync(path.join(output,'tmp'));
fs.writeFileSync(path.join(output,'PROVIDER.json'),JSON.stringify({
 status:'LOCAL_MECHANICAL_FRESH_PROVIDER_ONLY',version:profile.version,sourceIdentity,resources,
 sourcePath,sourceSha256:digest(source),identity,applicationRowsCopied:0,hostedConnection:false,
 qualification:'Fresh local provider scaffolding only; no local business test, native/API acceptance or hosted installation.'},null,2)+'\\n',{flag:'wx'});
console.log(JSON.stringify({status:'I2_SEAL_FRESH_PROVIDER_READY',sourceCommit:candidate,identity}));
'''
provider=provider.replace("major:17", "major:17")
schema=js(parents[1])
schema=change(schema,'// B2.23/F-23 schema compilation only. Never a protected release verifier.',
 '// Fresh-only Install2 mechanical schema compilation. No local test programme or protected release verdict.')
start=schema.index('const [mode, ...unexpected]')
end=schema.index('const current = verifyIntegrity();')
schema=schema[:start]+'''import {profile,parseArguments,configureLocalProcess,assertProject,inspectOwnResources} from './healthroster-install2-local-seal-profile.mjs';
const candidate=parseArguments(process.argv.slice(2)),mode='NEW';
const sourceIdentity=assertProject(repoRoot,candidate);
configureLocalProcess(process.env);
const resources=inspectOwnResources(),output=path.join(repoRoot,profile.output);
const provider=JSON.parse(fs.readFileSync(path.join(output,'PROVIDER.json'),'utf8'));
if(provider.status!=='LOCAL_MECHANICAL_FRESH_PROVIDER_ONLY'||provider.version!==profile.version
 ||JSON.stringify(provider.sourceIdentity)!==JSON.stringify(sourceIdentity)
 ||JSON.stringify(provider.resources)!==JSON.stringify(resources)
 ||provider.applicationRowsCopied!==0||provider.hostedConnection!==false)
 throw Error('I2_SEAL_SAME_FRESH_PROVIDER_REQUIRED');
''' +schema[end:]
schema=change(schema,"const checkpointPath = 'docs/healthroster-build/install1-source/LOCAL_SCHEMA_CHECKPOINT.json';\nconst previous = fs.existsSync(path.join(repoRoot, checkpointPath)) ? readJson(checkpointPath) : null;",
 "const checkpointPath = profile.output+'/SCHEMA.json';\nif(fs.existsSync(path.join(repoRoot,checkpointPath)))throw Error('I2_SEAL_FRESH_SCHEMA_OUTPUT_REQUIRED');")
schema=schema.replace('banking_modal_v2_test','healthroster_install2_seal').replace('cloudtms_hr_install1_local_owner','cloudtms_hr_install2_seal_owner')
schema=schema.replace('healthroster-install1-local-schema-','healthroster-install2-mechanical-schema-')
start=schema.index("  if (mode === 'NEW') {")
end=schema.index('  const installed = JSON.parse(await execute(ledgerSql));')
block=schema[start:end]
resume=block.index('  } else {')
block=block[:resume]+'  }\n'
schema=schema[:start]+block+schema[end:]
start=schema.index('  // Deleted repeatable authority cannot silently disappear from a checkpoint.')
end=schema.index('  const selected = new Map(',start)
schema=schema[:start]+schema[end:]
schema=change(schema,"    context = managedReleaseContext", "    context = managedReleaseContext")
schema=change(schema,"      gitCommit: shellGitHead(), expectedHash:","      gitCommit: candidate, expectedHash:")
schema=change(schema,"  if (JSON.stringify(inventory()) !== JSON.stringify(current)) throw Error('BPAY_NEXT_LOCAL_SOURCE_CHANGED_DURING_LOAD');",
 "  if (JSON.stringify(inventory()) !== JSON.stringify(current)) throw Error('BPAY_NEXT_LOCAL_SOURCE_CHANGED_DURING_LOAD');\n  assertProject(repoRoot,candidate);")
schema=change(schema,"    'owner',current_user,'session',session_user,'superuser',r.rolsuper,'bypassrls',r.rolbypassrls)",
 "    'owner',current_user,'session',session_user,'superuser',r.rolsuper,'bypassrls',r.rolbypassrls,\n    'jit',current_setting('jit'),'collector',current_setting('pg_show_plans.is_enabled',true))")
schema=change(schema,"      || identity.superuser || identity.bypassrls) throw Error('BPAY_NEXT_LOCAL_SCHEMA_IDENTITY_INVALID');",
 "      || identity.superuser!==false || identity.bypassrls!==false\n      || identity.jit!=='off'||identity.collector!=='off') throw Error('BPAY_NEXT_LOCAL_SCHEMA_IDENTITY_INVALID');")
schema=change(schema,"    if (present) throw Error('BPAY_NEXT_LOCAL_NEW_REQUIRES_EMPTY_APPLICATION_SCHEMA');",
 "    if (present) throw Error('BPAY_NEXT_LOCAL_NEW_REQUIRES_EMPTY_APPLICATION_SCHEMA');\n    const empty=(await execute(\"select (not exists(select 1 from pg_catalog.pg_class relation_row join pg_catalog.pg_namespace namespace_row on namespace_row.oid=relation_row.relnamespace where namespace_row.nspname in ('public','private') and relation_row.relkind in ('r','p','v','m','S','f')) and not exists(select 1 from auth.users) and not exists(select 1 from vault.secrets))::text;\")).trim();\n    if(empty!=='true')throw Error('I2_SEAL_FRESH_EMPTY_SCHEMAS_REQUIRED');")
schema=change(schema,"    database: identity.database, owner: identity.owner, sourceCommit: shellGitHead(),",
 "    database: identity.database, owner: identity.owner, sourceCommit: candidate, sourceIdentity, resources,\n    applicationRowsCopied:0, localAcceptance:'NOT_RUN', hostedExecution:false,")
# These new files are additive. The old complete files remain byte-for-byte parents.
targets=[('scripts/healthroster-install2-local-provider.mjs',parents[0],provider),
 ('scripts/healthroster-install2-local-schema.mjs',parents[1],schema)]
proof=[]
for target,parent,text in targets:
 b=text.encode();old=(ROOT/parent).read_bytes()
 assert not (ROOT/target).exists(),('ADDITIVE_COLLISION',target)
 save(HERE/'payload'/target,b)
 # Deterministic raw contiguous edits, full old/new files also retained.
 edits=[]
 oldLines=old.decode().splitlines(keepends=True);newLines=text.splitlines(keepends=True)
 for tag,a,b0,c,d in difflib.SequenceMatcher(None,oldLines,newLines,autojunk=False).get_opcodes():
  if tag!='equal': edits.append(dict(start=sum(map(len,oldLines[:a])),end=sum(map(len,oldLines[:b0])),
   before=''.join(oldLines[a:b0]),after=''.join(newLines[c:d])))
 restored=text
 for e in reversed(edits):
  newStart=e['start']+sum(len(x['after'])-len(x['before']) for x in edits if x['start']<e['start'])
  assert restored[newStart:newStart+len(e['after'])]==e['after']
  restored=restored[:newStart]+e['before']+restored[newStart+len(e['after']):]
 assert restored.encode()==old
 proof.append(dict(target=target,parent=parent,parentSha256=sha(old),payloadSha256=sha(text.encode()),hunks=edits))
profile=(HERE/'profile-source.mjs').read_bytes();target='scripts/healthroster-install2-local-seal-profile.mjs'
assert not (ROOT/target).exists();save(HERE/'payload'/target,profile)
output_record('FORWARD.json',dict(version='INSTALL2_MECHANICAL_FRESH_FORWARD_V1',baseCommit=base,additiveOnly=True,rows=proof,
 newProfile=dict(target=target,bytes=len(profile),sha256=sha(profile))))
selection=[dict(target=t,parent=p,sha256=sha((HERE/'payload'/t).read_bytes())) for t,p,_ in targets]
selection.append(dict(target=target,parent=None,sha256=sha(profile)))
output_record('SELECTION.json',dict(version='INSTALL2_MECHANICAL_FRESH_SELECTION_V1',rows=selection))
print(json.dumps(dict(status='OFFLINE_PAYLOADS_PREPARED',base=base,parents=len(rows),targets=len(selection),hunks=sum(len(x['hunks']) for x in proof))))
