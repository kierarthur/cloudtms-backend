from pathlib import Path
import runpy,sys,subprocess,json
BASE=Path(__file__).resolve().parent
FROZEN=BASE/'frozen/source-status-child-pure-r2-alias-forward-20261017'
original_write=Path.write_bytes;original_run=subprocess.run
suppressed=[];adapters=[]
def guarded(self,data):
    resolved=str(self.resolve()).replace('\\','/')
    if '/frozen/' in resolved:
        assert self.read_bytes()==data,'Frozen output differs: '+str(self)
        suppressed.append({'file':str(self),'bytes':len(data)});return len(data)
    assert '/source-status-child-pure-r2-independent-peer-20261017/' in resolved
    return original_write(self,data)
def run(args,*a,**kw):
    if isinstance(args,list) and len(args)==3 and args[:2]==['node','--check']:
        value=args[2]
        assert value.startswith('\\\\?\\D:\\Development\\GitHub\\.codex-worktrees\\healthroster-v2-money-build-13102026\\docs\\healthroster-build\\proposals\\source-status-child-pure-r2-independent-peer-20261017\\frozen\\')
        fixed=['C:/Program Files/nodejs/node.exe','--check',value[4:]]
        adapters.append({'kind':'Node --check operational path only','before':value,'after':fixed[2]})
        return original_run(fixed,*a,**kw)
    return original_run(args,*a,**kw)
Path.write_bytes=guarded;subprocess.run=run;sys.dont_write_bytecode=True
for name in ['programme/verify-programme.py','catalogue-vectors.py','verify-adapter.py','verify-r2.py','verify-freeze.py']:
    print('RUN '+name)
    sys.argv=[str(FROZEN/name)]
    runpy.run_path(str(FROZEN/name),run_name='__main__')
original_write(BASE/'replay/PYTHON_REPLAY_ADAPTER.json',(json.dumps({'frozenEqualWritesSuppressed':suppressed,'nodeCheckPathAdapters':adapters,'sqlChanges':0,'native':'NOT_RUN'},indent=2)+'\n').encode())
