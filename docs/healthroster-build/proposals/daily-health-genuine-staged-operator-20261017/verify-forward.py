"""Complete raw source parents and finite byte-exact forwards/inverses; public source only."""
import sys,json,pathlib,hashlib,difflib
sys.dont_write_bytecode=True
HERE=pathlib.Path(__file__).resolve().parent
TARGET=HERE/'ADMISSION_FULL_PARENT_FORWARD.json'
def sha(b):return hashlib.sha256(b).hexdigest()
if '--freeze' in sys.argv:
    records=[]
    parents=[(n,'reference/pre-freeze-admission-draft/'+n) for n in ['source.mjs','worker.mjs','native-read.mjs','README.md']]+[('secure-owned.ps1','reference/secure-owned-auth-parent.ps1')]
    for name,before_file in parents:
        before=(HERE/before_file).read_bytes()
        after=(HERE/name).read_bytes()
        a=before.decode('utf-8').splitlines(keepends=True);b=after.decode('utf-8').splitlines(keepends=True)
        changes=[]
        for kind,x0,x1,y0,y1 in difflib.SequenceMatcher(None,a,b,autojunk=False).get_opcodes():
            if kind!='equal':changes.append({'beforeOffset':len(''.join(a[:x0])),'afterOffset':len(''.join(b[:y0])),'before':''.join(a[x0:x1]),'after':''.join(b[y0:y1])})
        records.append({'file':name,'before':{'file':before_file,'bytes':len(before),'sha256':sha(before)},'after':{'bytes':len(after),'sha256':sha(after)},'edits':changes})
    TARGET.write_bytes((json.dumps({'version':'DAILY_GENUINE_ADMISSION_FULL_PARENT_FORWARD_V1','records':records},ensure_ascii=False,indent=2)+'\n').encode())
records=json.loads(TARGET.read_bytes())['records']
count=0
for r in records:
    before=(HERE/r['before']['file']).read_bytes();after=(HERE/r['file']).read_bytes()
    for b,pin in [(before,r['before']),(after,r['after'])]:assert len(b)==pin['bytes'] and sha(b)==pin['sha256']
    forward=before.decode();reverse=after.decode()
    for e in reversed(r['edits']):
        i=e['beforeOffset'];assert forward[i:i+len(e['before'])]==e['before'];forward=forward[:i]+e['after']+forward[i+len(e['before']):]
        i=e['afterOffset'];assert reverse[i:i+len(e['after'])]==e['after'];reverse=reverse[:i]+e['before']+reverse[i+len(e['after']):]
        count+=1
    assert forward.encode()==after and reverse.encode()==before
print(json.dumps({'wholeParents':len(records),'finiteEdits':count,'fullRawForwardAndInverse':'PASS','native':'NOT_RUN'}))
