"""Frozen offline PG17 grammar/actual assignment AST, foreign writes suppressed."""
from pathlib import Path
import subprocess,json,hashlib
here=Path(__file__).resolve().parent
own=here.parents[3]
money=own.parent/'healthroster-v2-money-build-13102026'
prefix='docs/healthroster-build/proposals/same-separate-counter-forward-20261017/'
pin='a7e4522cb87ae6961cb40efcb83e4e69b12fbd6c'
virtual=money/prefix
def blob(p):
    return subprocess.check_output(['git','-c','gc.auto=0','-c','maintenance.auto=false','show',pin+':'+prefix+p],cwd=own)
original_write=Path.write_text
captured={}
def suppress(self,value,*args,**kwargs):
    assert self.parent==virtual and self.name in ('GRAMMAR_RESULTS.json','SOURCE_DATAFLOW_RESULTS.json')
    captured[self.name]=value.encode('utf-8')
    return len(value)
Path.write_text=suppress
scripts=[]
try:
    for leaf in ('grammar.py','source-dataflow.py'):
        source=blob(leaf)
        scripts.append(dict(path=prefix+leaf,sha256=hashlib.sha256(source).hexdigest()))
        exec(compile(source,str(virtual/leaf),'exec'),{'__file__':str(virtual/leaf),'__name__':'__independent_frozen_replay__'})
finally:
    Path.write_text=original_write
for leaf,data in captured.items():
    assert data==blob(leaf),leaf
grammar=json.loads(captured['GRAMMAR_RESULTS.json'])
dataflow=json.loads(captured['SOURCE_DATAFLOW_RESULTS.json'])
assert grammar['statements']==39 and grammar['parsedBodies']==8 and grammar['catalogueDependent']==2
assert len(dataflow['fixtures'])==4 and all(r['passAssignmentOrder'] for r in dataflow['fixtures'])
receipt=dict(correctedCommit=pin,scripts=scripts,grammar=grammar,dataflow=dataflow,byteIdenticalSuppressedWrites=2,foreignWrites=0,databaseConnections=0,native='NOT_RUN',qualification='PG17 grammar and exact bounded assignment AST only; surrogate records do not prove catalogue types, native SQL, origins, permissions, prices or insertion.')
(here/'CORRECTED_GRAMMAR_REPLAY_RESULTS.json').write_bytes((json.dumps(receipt,indent=2)+'\n').encode('utf-8'))
print(json.dumps(dict(statements=39,actualBodiesParsed=8,catalogueDependent=2,assignmentAstVectors=4,foreignWrites=0,native='NOT_RUN')))
