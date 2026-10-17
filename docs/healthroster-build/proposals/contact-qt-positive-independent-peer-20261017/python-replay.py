from pathlib import Path
import runpy,sys,os,json
sys.dont_write_bytecode=True
BASE=Path(__file__).resolve().parent
write=Path.write_text;suppressed=[]
def guard(self,data,*args,**kwargs):
    file=str(self.resolve()).replace('\\','/')
    assert '/frozen/original/' in file
    before=self.read_bytes()
    encoded=data.encode(kwargs.get('encoding') or 'utf-8')
    native=data.replace('\n',os.linesep).encode(kwargs.get('encoding') or 'utf-8')
    assert before in [encoded,native],'Frozen grammar result differs'
    suppressed.append({'file':str(self),'bytes':len(before),'unchanged':True,'WindowsTextNewlineConvention':before==native})
    return len(data)
Path.write_text=guard
runpy.run_path(str(BASE/'frozen/original/grammar-and-bindings.py'),run_name='__main__')
(BASE/'replay/PYTHON_EQUAL_WRITE.json').write_bytes((json.dumps({'suppressed':suppressed,'sqlBytesChanged':0,'native':'NOT_RUN'},indent=2)+'\n').encode())
