from pathlib import Path
import json,difflib,hashlib
p=Path(__file__).parent
selection=json.loads((p/'OWNER_SELECTION.json').read_bytes());runtime=json.loads((p/'RUNTIME_COMPOSITION.json').read_bytes());rows=[];patch=[]
sha=lambda x:hashlib.sha256(x.encode()).hexdigest()
read=lambda f:open(chr(92)*2+'?'+chr(92)+str(f.absolute()),'rb').read().decode() if len(str(f.absolute()))>250 else f.read_bytes().decode()
def edits(before,after):
    a=before.splitlines(keepends=True);b=after.splitlines(keepends=True);hunks=[]
    for g in difflib.SequenceMatcher(None,a,b,autojunk=False).get_grouped_opcodes(3):
        i,j=g[0][1],g[-1][2];k,l=g[0][3],g[-1][4]
        hunks.append(dict(oldStartLine=i+1,oldLineCount=j-i,newStartLine=k+1,newLineCount=l-k,before=''.join(a[i:j]),after=''.join(b[k:l])))
    forward=a[:];reverse=b[:]
    for h in reversed(hunks):
        at=h['oldStartLine']-1;bt=h['newStartLine']-1
        assert ''.join(forward[at:at+h['oldLineCount']])==h['before']
        assert ''.join(reverse[bt:bt+h['newLineCount']])==h['after']
        forward[at:at+h['oldLineCount']]=h['after'].splitlines(keepends=True)
        reverse[bt:bt+h['newLineCount']]=h['before'].splitlines(keepends=True)
    assert ''.join(forward)==after and ''.join(reverse)==before
    return hunks
for o in selection['selected']:
    after=read(p/'forward'/o['leaf']).strip()+'\n';before=None if o['current'] is None else read(p/'current'/o['leaf']).strip()+'\n'
    if before is not None:
        assert sha(before.strip())==o['current']['definitionSha256']
        hunks=edits(before,after)
        patch+=difflib.unified_diff(before.splitlines(keepends=True),after.splitlines(keepends=True),fromfile=o['current']['physical']+'#'+o['signature'],tofile='forward/'+o['leaf'])
    else:hunks=None
    rows.append(dict(signature=o['signature'],targetCompleteDefinition='forward/'+o['leaf'],current=o['current'],inputSha256=None if before is None else sha(before.strip()),outputSha256=sha(after.strip()),hunks=hunks,fullDefinitionForwardAndReverse=True if before is not None else 'NEW_DEFINITION_ONLY'))
for o in runtime['files']:
    before=read(p/'current'/o['target']);after=read(p/'forward'/o['target']);assert sha(before)==o['current']['sha256']
    rows.append(dict(target=o['target'],current=o['current'],inputSha256=sha(before),outputSha256=sha(after),hunks=edits(before,after),fullDefinitionForwardAndReverse=True))
    patch+=difflib.unified_diff(before.splitlines(keepends=True),after.splitlines(keepends=True),fromfile=o['target'],tofile=o['target'])
count=sum(e['fullDefinitionForwardAndReverse'] is True for e in rows)
(p/'CURRENT_OWNER_EDITS.json').write_text(json.dumps(dict(checkpoint=selection['checkpoint'],owners=rows,existingOwnerFullReverseCount=count,newDefinitionCount=sum(e['hunks'] is None for e in rows),native='NOT_RUN'),indent=2)+'\n',newline='\n')
(p/'CURRENT_OWNER_PATCH.diff').write_text(''.join(patch),newline='\n')
print(json.dumps(dict(fullReversals=count,newDefinitions=sum(e['hunks'] is None for e in rows),hunks=sum(len(e['hunks'] or []) for e in rows))))
