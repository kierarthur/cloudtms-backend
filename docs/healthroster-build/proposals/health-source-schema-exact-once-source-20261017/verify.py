import hashlib,json,pathlib,subprocess,sys
HERE=pathlib.Path(__file__).resolve().parent;ROOT=pathlib.Path(r'D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026');sha=lambda b:hashlib.sha256(b).hexdigest()
def git(*a):return subprocess.check_output(['git','-C',str(ROOT),*a])
initial=git('rev-parse','HEAD').decode().strip();pins=json.loads((HERE/'PARENTS.json').read_text());forward=json.loads((HERE/'FORWARD.json').read_text())
mf=HERE/'MANIFEST.json'
if mf.exists():
 rows=json.loads(mf.read_text())['files'];expected={r['file'] for r in rows}
 actual={str(p.relative_to(HERE)).replace('\\','/') for p in HERE.rglob('*') if p.is_file() and '__pycache__' not in p.parts and p!=mf}
 assert actual==expected,('inventory',actual^expected)
 for r in rows:
  b=(HERE/r['file']).read_bytes();assert len(b)==r['bytes'] and sha(b)==r['sha256'],r['file']
for pin in pins:
 b=(HERE/pin['capture']).read_bytes();assert sha(b)==pin['sha256'] and len(b)==pin['bytes']
 assert b==git('show',pin['head']+':'+pin['file'])==git('show',pin['historical']+':'+pin['file']),pin['file']
 if '--current' in sys.argv:assert b==(ROOT/pin['file']).read_bytes()==git('show',':'+pin['file'])==git('show','HEAD:'+pin['file']),pin['file']
for t in forward['targets']:
 after=(HERE/'payload'/t['file']).read_bytes();assert len(after)==t['afterBytes'] and sha(after)==t['afterSha256']
 if t['parent']:
  before=(HERE/t['parent']).read_bytes();assert len(before)==t['beforeBytes'] and sha(before)==t['beforeSha256'];s=after.decode()
  for e in reversed(t['edits']):assert s.count(e['after'])==1;s=s.replace(e['after'],e['before'])
  assert s.encode()==before==t['wholeInverse']['before'].encode()
  assert after==t['wholeInverse']['after'].encode()
  p=next(p for p in pins if p['file']==t['file']);assert p['capture']==t['parent'] and p['sha256']==t['beforeSha256']
 else:
  assert t['file']=='scripts/healthroster-source-schema-coverage.mjs'
  if '--current' in sys.argv:
   assert not(ROOT/t['file']).exists();assert subprocess.run(['git','-C',str(ROOT),'cat-file','-e','HEAD:'+t['file']],capture_output=True).returncode!=0
assert git('rev-parse','HEAD').decode().strip()==initial
print(json.dumps(dict(status='PASS_SOURCE_ONLY',parents=len(pins),targets=2,hunks=len(forward['targets'][0]['edits']),inventory=len(rows) if mf.exists() else None,current='--current' in sys.argv,native='NOT_RUN')))
