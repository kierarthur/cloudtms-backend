from pathlib import Path
import os,json,difflib,hashlib
here=Path(__file__).parent
def raw(p): return open(('\\\\?\\'+str(p.absolute())) if os.name=='nt' else p,'rb').read()
edits=json.loads((here/'COUNTER_EDITS.json').read_text())
out=[]
for row in edits['files']:
    before=raw(here/'current'/('04_complete_current_review.sqlinc' if row['input']['definitionOnly'] else row['target'])).decode()
    after=raw(here/'forward'/row['target']).decode()
    assert hashlib.sha256(before.encode()).hexdigest()==row['input']['sha256']
    assert hashlib.sha256(after.encode()).hexdigest()==row['outputSha256']
    forward=before
    for h in row['hunks']:
        assert forward.count(h['before'])==h.get('occurrences',1)
        forward=forward.replace(h['before'],h['after'])
    assert forward==after
    reverse=after
    for h in reversed(row['hunks']):
        assert reverse.count(h['after'])==h.get('occurrences',1)
        reverse=reverse.replace(h['after'],h['before'])
    assert reverse==before
    out.extend(difflib.unified_diff(before.splitlines(True),after.splitlines(True),fromfile='a/'+row['target'],tofile='b/'+row['target']))
(here/'CURRENT_OWNER_PATCH.diff').write_text(''.join(out),encoding='utf8',newline='\n')
print(json.dumps(dict(fullRawOwnerReversals=len(edits['files']),native='NOT_RUN')))
