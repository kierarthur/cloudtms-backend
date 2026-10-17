"""Explicit source freeze / read-only frozen saved-index-HEAD verification."""
from pathlib import Path
import json,hashlib,sys,subprocess
HERE=Path('\\\\?\\'+str(Path(__file__).resolve().parent));MONEY=HERE.parents[3]
sha=lambda b:hashlib.sha256(b).hexdigest()
if '--freeze' in sys.argv:
 assert not (HERE/'MANIFEST.json').exists(),'FROZEN_HISTORY'
 rows=[{'file':p.relative_to(HERE).as_posix(),'bytes':p.stat().st_size,'sha256':sha(p.read_bytes())} for p in sorted(HERE.rglob('*')) if p.is_file() and p!=HERE/'MANIFEST.json']
 (HERE/'MANIFEST.json').write_text(json.dumps({'version':'HEALTH_CLOSED9_MANAGED_SOURCE_FREEZE_V1','parent':'e5b113e4a985f08574e558844e3e81579faf49e1','files':rows,'native':'NOT_RUN'},indent=2)+'\n')
m=json.loads((HERE/'MANIFEST.json').read_bytes());rows=m['files']+[{'file':'MANIFEST.json','bytes':(HERE/'MANIFEST.json').stat().st_size,'sha256':sha((HERE/'MANIFEST.json').read_bytes())}]
actual={p.relative_to(HERE).as_posix() for p in HERE.rglob('*') if p.is_file()};assert actual=={x['file'] for x in rows}
for row in rows:
 b=(HERE/row['file']).read_bytes();assert len(b)==row['bytes'] and sha(b)==row['sha256'],row['file']
 if '--committed' in sys.argv:
  relative=(HERE/row['file']).relative_to(MONEY).as_posix()
  for ref in ['HEAD','']:
   raw=subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','-c','core.longpaths=true','-C',str(MONEY).removeprefix('\\\\?\\'),'show',ref+':'+relative]);assert b==raw,(ref,row['file'])
print(json.dumps({'files':len(rows),'manifestSha256':sha((HERE/'MANIFEST.json').read_bytes()),'committed':'--committed' in sys.argv,'native':'NOT_RUN'}))
