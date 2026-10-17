"""Author-only finite local provider/launcher correction. No Docker or SQL."""
import pathlib,json,hashlib,subprocess
P=pathlib.Path(__file__).resolve().parent
R=pathlib.Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
OLD=P.parent/'install2-mechanical-seal-dbname-forward-source-20261017'
def sha(b):return hashlib.sha256(b).hexdigest()
def git(*a):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false',*a],cwd=R)
def put(p,b):p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
def record(p,v):put(P/p,(json.dumps(v,indent=2)+'\n').encode())
base=git('rev-parse','HEAD').decode().strip();rows=[]
names=['scripts/healthroster-install2-local-provider.mjs','scripts/healthroster-install2-local-seal-profile.mjs',
 'scripts/healthroster-install2-local-schema.mjs','tests/fixtures/28082026_1229_banking_modal_local_pg17_prerequisites.sql']
for f in names:
 b=(R/f).read_bytes();assert b==git('show',':'+f)==git('show',base+':'+f)
 put(P/'parents'/f,b);rows.append(dict(path=f,bytes=len(b),sha256=sha(b)))
target=names[0];before=(R/target).read_text();after=before;hunks=[]
for a,b in [
 ("configureLocalProcess(process.env);","configureLocalProcess(process.env);\nif(typeof process.env.POSTGRES_PASSWORD!=='string'||process.env.POSTGRES_PASSWORD.length===0)throw Error('I2_SEAL_LOCAL_ADMIN_PASSWORD_REQUIRED');"),
 ("['exec','-i',container,'psql','-U','postgres','-d',target,","['exec','-i','--env','PGPASSWORD',container,'psql','-h','127.0.0.1','-U','postgres','-d',target,"),
 ("{input:sql,encoding:'utf8',windowsHide:true,maxBuffer:2000000}","{input:sql,encoding:'utf8',windowsHide:true,maxBuffer:2000000,env:{...process.env,PGPASSWORD:process.env.POSTGRES_PASSWORD}}")]:
 assert after.count(a)==1;after=after.replace(a,b,1);hunks.append(dict(before=a,after=b))
restored=after
for h in reversed(hunks):assert restored.count(h['after'])==1;restored=restored.replace(h['after'],h['before'],1)
assert restored==before
put(P/'payload'/target,after.encode());put(P/'payload'/names[1],(R/names[1]).read_bytes())
commands=(OLD/'COMMANDS.ps1.txt').read_bytes();needle=b'--env POSTGRES_PASSWORD postgres:17.11-bookworm -c jit=off';fixed=b'--env POSTGRES_PASSWORD postgres:17.11-bookworm postgres -c jit=off'
assert commands.count(needle)==1
put(P/'parents/COMMANDS.ps1.txt',commands);put(P/'COMMANDS.ps1.txt',commands.replace(needle,fixed))
record('PARENTS.json',dict(root=str(R),commit=base,rows=rows))
record('FORWARD.json',dict(target=target,beforeSha256=sha(before.encode()),afterSha256=sha(after.encode()),hunks=hunks,
 commandForward=dict(before=needle.decode(),after=fixed.decode(),beforeSha256=sha(commands),afterSha256=sha(commands.replace(needle,fixed))),
 unchangedProfile=rows[1],unchangedSchema=rows[2]))
record('SELECTION.json',dict(rows=[dict(target=target,changed=True,beforeSha256=sha(before.encode()),sha256=sha(after.encode()))]))
print(json.dumps(dict(status='SOURCE_ONLY_TCP_FORWARD_PREPARED',commit=base,targets=1,hunks=3,commandHunks=1)))
