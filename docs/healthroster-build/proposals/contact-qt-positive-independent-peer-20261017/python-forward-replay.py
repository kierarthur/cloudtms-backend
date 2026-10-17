from pathlib import Path
import runpy,sys,os,json
sys.dont_write_bytecode=True
BASE=Path(__file__).resolve().parent
suppressed=[]
def guard(self,data,*args,**kwargs):
    file=str(self.resolve()).replace('\\','/')
    assert '/frozen/forward/' in file
    before=self.read_bytes()
    encoded=data.encode(kwargs.get('encoding') or 'utf-8')
    native=data.replace('\n',os.linesep).encode(kwargs.get('encoding') or 'utf-8')
    assert before in [encoded,native],'Frozen result differs: '+file
    suppressed.append({'file':file,'bytes':len(before),'unchanged':True,'WindowsMetadataTextNewlineConvention':before==native})
    return len(data)
Path.write_text=guard
for file in ['admission-ast.py','current/grammar-and-bindings.py']:
    runpy.run_path(str(BASE/'frozen/forward'/file),run_name='__main__')
(BASE/'replay/PYTHON_FORWARD_EQUAL_WRITE.json').write_bytes((json.dumps({'suppressed':suppressed,'sqlBytesChanged':0,'native':'NOT_RUN'},indent=2)+'\n').encode())
