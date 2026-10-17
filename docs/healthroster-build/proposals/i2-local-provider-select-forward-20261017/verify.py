"""Non-writing public-source verifier; --current is the strict pre-mount parent check."""
import pathlib,json,hashlib,subprocess,sys
p=pathlib.Path(__file__).resolve().parent
assert sys.argv[1:] in ([],['--current'])
def sha(b):return hashlib.sha256(b).hexdigest()
m=json.loads((p/'MANIFEST.json').read_text())
actual=sorted(str(q.relative_to(p)).replace('\\','/') for q in p.rglob('*') if q.is_file())
assert actual==sorted(['MANIFEST.json']+[r['file'] for r in m['files']])
for r in m['files']:
 b=(p/r['file']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['file']
s=json.loads((p/'SELECTION.json').read_text());f=json.loads((p/'FULL_PARENT_FORWARD.json').read_text())
assert len(s['targets'])==1 and len(s['unchangedParents'])==4 and len(f['hunks'])==3
v=(p/s['targets'][0]['payload']).read_text();assert v==f['wholeAfter']
for h in reversed(f['hunks']):assert v.count(h['after'])==1;v=v.replace(h['after'],h['before'])
assert v==f['wholeBefore'] and v.encode()==(p/s['targets'][0]['before']['capture']).read_bytes()
root=pathlib.Path(s['root']);start=subprocess.check_output(['git','-C',str(root),'rev-parse','HEAD']).decode().strip()
for r in [s['targets'][0]['before'],*s['unchangedParents']]:
 b=(p/r['capture']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256']
 old=subprocess.check_output(['git','-C',str(root),'show',r['historicalCommit']+':'+r['file']]);assert old==b
 if sys.argv[1:]==['--current']:
  assert (root/r['file']).read_bytes()==b
  for ref in [':','HEAD:']:assert subprocess.check_output(['git','-C',str(root),'show',ref+r['file']])==b
assert subprocess.check_output(['git','-C',str(root),'rev-parse','HEAD']).decode().strip()==start
log=(p/f['safeR3Log']['file']).read_bytes();assert len(log)==811 and sha(log)==f['safeR3Log']['sha256']
print(json.dumps({'status':'FROZEN_SOURCE_FORWARD_PASS','files':len(m['files'])+1,'target':1,'hunks':3,'completeHistoricalParents':5,'currentParentsChecked':sys.argv[1:]==['--current'],'observedRootHead':start,'native':False,'Docker':False,'network':False}))
