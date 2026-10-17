"""Validate the finite six-type/three-function mapping against pinned upstream data."""
from pathlib import Path
import re,json
H=Path(__file__).resolve().parent
C=json.loads((H/'ENCODER_CONTRACT.json').read_text())
def records(file):
 s=(H/('reference/pg17/src/include/catalog/'+file)).read_text()
 return[dict(re.findall(r"(\w+)\s*=>\s*'((?:\\.|[^'])*)'",m.group())) for m in re.finditer(r'\{[^}]*\}',s,re.S)]
procs=records('pg_proc.dat');types=records('pg_type.dat');header=(H/'reference/pg17/src/include/catalog/pg_proc.h').read_text()
for x in ['proowner BKI_DEFAULT(POSTGRES)','prolang BKI_DEFAULT(internal)','proisstrict BKI_DEFAULT(t)','proparallel BKI_DEFAULT(s)']:assert x in header
count=0
for _,oid,send,out,sendvol,outvol in C['types']:
 t=[x for x in types if x.get('oid')==str(oid)];assert len(t)==1;t=t[0];assert t['typsend']==send and t['typoutput']==out
 for name,vol,result in[(send,sendvol,'bytea'),(out,outvol,'cstring')]:
  p=[x for x in procs if x.get('proname')==name and x.get('proargtypes')==t['typname']];assert len(p)==1;p=p[0]
  assert p['prosrc']==name and p['prorettype']==result and p.get('provolatile','i')==vol and p.get('proisstrict','t')=='t' and p.get('proparallel','s')=='s'
  count+=1
for identity,symbol,arg_oid,result_oid,vol in C['functions']:
 name=identity.split('(')[0].split('.')[-1];arg={2249:'record',17:'bytea',25:'text'}[arg_oid];result={17:'bytea',25:'text'}[result_oid]
 p=[x for x in procs if x.get('proname')==name and x.get('proargtypes')==arg];assert len(p)==1;p=p[0];assert p['prosrc']==symbol and p['prorettype']==result and p.get('provolatile','i')==vol and p.get('proisstrict','t')=='t' and p.get('proparallel','s')=='s'
 count+=1
record=[t for t in types if t.get('oid')=='2249'];assert len(record)==1 and record[0]['typsend']=='record_send'
print(json.dumps({'sourceOnly':True,'sixBuiltinTypeDeclarations':6,'actualSendOutAndDirectFunctionDeclarations':count,'catalogueHeaderDefaults':4,'outerRecordTypeDeclaration':1,'nativeMetadataForCandidate':'NOT_OBSERVED'}))
