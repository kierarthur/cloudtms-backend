"""Finite full raw parents, exact manifest delta and all original guard-code transfer."""
import json,pathlib,hashlib,difflib,sys,re
sys.dont_write_bytecode=True
HERE=pathlib.Path(__file__).resolve().parent
sha=lambda b:hashlib.sha256(b).hexdigest()
target=HERE/'FULL_PARENT_FORWARD.json'
if '--freeze' in sys.argv:
    records=[]
    for name in ['source.mjs','MANIFEST.json']:
        before=(HERE/'reference'/name).read_bytes();after=(HERE/'payload'/name).read_bytes();a=before.decode().splitlines(keepends=True);b=after.decode().splitlines(keepends=True);edits=[]
        for kind,i,j,k,l in difflib.SequenceMatcher(None,a,b,autojunk=False).get_opcodes():
            if kind!='equal':edits.append({'beforeOffset':len(''.join(a[:i])),'afterOffset':len(''.join(b[:k])),'before':''.join(a[i:j]),'after':''.join(b[k:l])})
        records.append({'target':'docs/healthroster-build/proposals/daily-health-genuine-staged-operator-20261017/'+name,'beforeFile':'reference/'+name,'afterFile':'payload/'+name,'beforeSha256':sha(before),'afterSha256':sha(after),'edits':edits})
    target.write_bytes((json.dumps({'version':'DAILY_GENUINE_BATCH_FULL_PARENT_FORWARD_V1','records':records},indent=2)+'\n').encode())
records=json.loads(target.read_bytes())['records'];count=0
for r in records:
    a=(HERE/r['beforeFile']).read_bytes();b=(HERE/r['afterFile']).read_bytes();assert sha(a)==r['beforeSha256'] and sha(b)==r['afterSha256'];f=a.decode();v=b.decode()
    for e in reversed(r['edits']):
        i=e['beforeOffset'];assert f[i:i+len(e['before'])]==e['before'];f=f[:i]+e['after']+f[i+len(e['before']):]
        i=e['afterOffset'];assert v[i:i+len(e['after'])]==e['after'];v=v[:i]+e['before']+v[i+len(e['after']):];count+=1
    assert f.encode()==b and v.encode()==a
old=json.loads((HERE/'reference/MANIFEST.json').read_bytes());new=json.loads((HERE/'payload/MANIFEST.json').read_bytes())
for r in old['files']:
    n=next(n for n in new['files'] if n['file']==r['file'])
    if r['file']!='source.mjs':assert n==r
assert len(new['files'])==len(old['files'])+1 and new['files'][-1]['file']=='git-image-batch.mjs'
assert (HERE/'git-image-batch.mjs').read_bytes()==(HERE/'payload/git-image-batch.mjs').read_bytes()
old_codes=set(re.findall(r"'FIXTURE_[A-Z0-9_]+'",(HERE/'reference/source.mjs').read_text()))
new_codes=set(re.findall(r"'FIXTURE_[A-Z0-9_]+'",(HERE/'payload/source.mjs').read_text()+(HERE/'git-image-batch.mjs').read_text()))
assert old_codes<=new_codes
print(json.dumps({'wholeParents':len(records),'finiteEdits':count,'fullRawForwardInverse':'PASS','oldGuardCodesRetained':len(old_codes),'originalOtherManifestImagesUnchanged':len(old['files'])-1,'newHelpers':1,'native':'NOT_RUN'}))
