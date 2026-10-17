"""Source/hash/whole inverse checks only. --current reads public Git parents."""
from pathlib import Path
import hashlib,json,subprocess,sys
H=Path('\\\\?\\'+str(Path(__file__).resolve().parent));ROOT=Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026');sha=lambda b:hashlib.sha256(b).hexdigest()
def git(*a):return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-C',str(ROOT),*a],stderr=subprocess.DEVNULL)
parents=json.loads((H/'PARENTS.json').read_text())['rows'];selection=json.loads((H/'SELECTION.json').read_text())['targets']
for r in parents:
 b=(H/'parents'/r['file']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256']
 if '--current' in sys.argv:assert b==(ROOT/r['file']).read_bytes()==git('show',':'+r['file'])==git('show','HEAD:'+r['file'])==git('show',r['commit']+':'+r['file'])
for r in selection:
 b=(H/'payload'/r['file']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'] and b.decode()==r['wholeInverse']['after']
 assert (H/'parents'/r['parentFile']).read_bytes()==r['wholeInverse']['before'].encode()
 if '--current' in sys.argv:assert not (ROOT/r['file']).exists(),'NEW_TARGET_ALREADY_PRESENT'
 if r['file'].endswith('.mjs'):subprocess.run(['node','--check',str(H/'payload'/r['file']).removeprefix('\\\\?\\')],check=True,capture_output=True)
manifest=H/'MANIFEST.json'
if manifest.exists():
 rows=json.loads(manifest.read_text())['files'];assert {r['file'] for r in rows}=={p.relative_to(H).as_posix() for p in H.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p!=manifest}
 for r in rows:
  b=(H/r['file']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256']
print(json.dumps(dict(status='SOURCE_ONLY_PASS',publicParents=len(parents),newTargets=len(selection),wholeRawInverses=3,currentPublicImagesChecked='--current' in sys.argv,native='NOT_RUN',auth='NOT_RUN',privateStateAccess=False)))
