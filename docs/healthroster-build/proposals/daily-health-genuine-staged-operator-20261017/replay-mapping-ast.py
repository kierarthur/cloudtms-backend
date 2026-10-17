"""Run exact frozen predicate proof; suppress its sole byte-equivalent evidence write."""
from pathlib import Path
import sys,json
sys.dont_write_bytecode=True
HERE=Path(__file__).resolve().parent
source=HERE/'reference/mapping/trigger-ast.py'
target=HERE/'reference/mapping/TRIGGER_AST_PROOF.json'
saved=target.read_bytes();expected=target.read_text(encoding='utf8')
write=Path.write_text
observed=[]
def suppress(self,data,*args,**kwargs):
    assert self==target,'UNEXPECTED_FROZEN_WRITE'
    assert data==expected,'FROZEN_AST_RESULT_CHANGED'
    observed.append(str(self.name))
    return len(data)
Path.write_text=suppress
try:exec(compile(source.read_bytes(),str(source),'exec'),{'__file__':str(source),'__name__':'__main__'})
finally:Path.write_text=write
assert observed==['TRIGGER_AST_PROOF.json'] and target.read_bytes()==saved
print(json.dumps({'frozenEquivalentWritesSuppressed':1,'native':'NOT_RUN'}))
