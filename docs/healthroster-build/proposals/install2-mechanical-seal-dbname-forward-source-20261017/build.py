"""Author-only value correction. No Docker/provider/schema/auth/source execution."""
import pathlib,json,hashlib,subprocess
HERE=pathlib.Path(__file__).resolve().parent
OLD=HERE.parent/'install2-mechanical-seal-fresh-source-20261017'
ROOT=pathlib.Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')
PREFIX='docs/healthroster-build/proposals/install2-mechanical-seal-fresh-source-20261017/'
before='healthroster_install2_seal';after='banking_modal_v2_test'
def sha(b):return hashlib.sha256(b).hexdigest()
def git(*a):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false',*a],cwd=ROOT)
def write(p,b):p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b)
def record(p,v):write(HERE/p,(json.dumps(v,indent=2)+'\n').encode())
base=git('rev-parse','HEAD').decode().strip()
oldMF=(OLD/'MANIFEST.json').read_bytes();assert sha(oldMF)=='a63d340c08a936c447ea83eaf5bda94751d3a9c2a1afd45809f496447061d87c'
for r in json.loads(oldMF)['rows']:
 b=(OLD/r['path']).read_bytes();assert sha(b)==r['sha256'] and len(b)==r['bytes']
 assert (ROOT/PREFIX/r['path']).read_bytes()==git('show',':'+PREFIX+r['path'])==git('show',base+':'+PREFIX+r['path'])==b
write(HERE/'ORIGINAL45_MANIFEST.json',oldMF)
rows=[];forwards=[]
targets=['scripts/healthroster-install2-local-seal-profile.mjs','scripts/healthroster-install2-local-schema.mjs','scripts/healthroster-install2-local-provider.mjs']
for p in targets+['AGENTS.md']:
 b=(ROOT/p).read_bytes();assert b==git('show',':'+p)==git('show',base+':'+p)
 write(HERE/'parents'/p,b);rows.append(dict(path=p,bytes=len(b),sha256=sha(b)))
 if p in targets:
  text=b.decode();out=text.replace(before,after).encode()
  if p.endswith('provider.mjs'):assert out==b
  else:assert text.count(before)==1
  write(HERE/'payload'/p,out)
  forwards.append(dict(target=p,changed=out!=b,beforeSha256=sha(b),afterSha256=sha(out),
   beforeValue=before if out!=b else None,afterValue=after if out!=b else None,occurrences=text.count(before)))
for p in ['scripts/healthroster-install1-local-provider.mjs','scripts/healthroster-install1-local-schema.mjs']:
 b=(OLD/'parents'/p).read_bytes();write(HERE/'parents'/p,b)
write(HERE/'test.mjs',(OLD/'test.mjs').read_bytes())
write(HERE/'COMMANDS.ps1.txt',(OLD/'COMMANDS.ps1.txt').read_bytes().replace(before.encode(),after.encode()))
record('PARENTS.json',dict(root=str(ROOT),commit=base,rows=rows,originalPrefix=PREFIX,
 originalManifestSha256=sha(oldMF),originalFilesIncludingManifest=45))
record('FORWARD.json',dict(version='INSTALL2_MECHANICAL_DBNAME_VALUE_FORWARD_V1',rows=forwards,rule='AGENTS.md:252'))
record('SELECTION.json',dict(rows=[dict(target=r['target'],changed=r['changed'],beforeSha256=r['beforeSha256'],sha256=r['afterSha256']) for r in forwards]))
print(json.dumps(dict(status='SOURCE_ONLY_VALUE_CORRECTION_PREPARED',base=base,changed=2,unchanged=1)))
