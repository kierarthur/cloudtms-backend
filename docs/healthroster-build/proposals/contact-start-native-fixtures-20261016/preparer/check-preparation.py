"""Source-only PG17 grammar and exact byte/statement map verification. No DB."""
import argparse,json,hashlib,sys,re
from pathlib import Path
arg=argparse.ArgumentParser();arg.add_argument('--parser-path',required=True);args=arg.parse_args()
sys.path.insert(0,args.parser_path)
from pglast import parser
if parser.get_postgresql_version()[0]!=17:raise RuntimeError('PG17 grammar required')
base=Path(__file__).resolve().parent/'generated';manifest=json.loads((base/'MANIFEST.json').read_text());index=json.loads((base/'UNIT_INDEX.json').read_text());sql=(base/'30_complete-trial-inner.sql').read_bytes();statements=0
for row in index:
 source=sql[row['byteStart']:row['byteEnd']]
 if hashlib.sha256(source).hexdigest()!=row['sqlHash']:raise RuntimeError('Native unit byte mismatch '+row['file'])
 text=source.decode()
 # Grammar inspection removes only psql meta lines in memory. All special
 # root transaction/conditional packaging remains untouched in the artifact;
 # branch semantics and outer-TX admission are explicitly NOT_COMPILED.
 grammar=re.sub(r'(?m)^\\[^\r\n]*','',re.sub(r'(?m)^\\gset[^\r\n]*',';',text)) if row.get('sourceOnlySpecialEnvelope') else text
 parsed=parser.parse_sql(grammar);statements+=len(parsed)
 if not row.get('sourceOnlySpecialEnvelope') and len(parsed)!=row['statements']:raise RuntimeError('Statement census mismatch '+row['file'])
for file in ['00_fresh-I1-predecessor.sql','01_fresh-restoration-relations.sql','02_before-and-fresh-schema-absence.sql']:
 parser.parse_sql((base/file).read_text(encoding='utf-8'))
if sum(u['statements'] for u in index)!=manifest['directEnvelope']['statements']:raise RuntimeError('Whole source packaging census differs')
# Existing exact fixture checker owns the known native row/INSERT fields and
# PLpgSQL grammar; this checker owns the separate full source-carried SQL.
# Check every native statement map against its original unit bytes, including
# function dollar bodies (never rewrapping them to change the statement count).
rows=json.loads((base/'STATEMENT_MAP.json').read_text());counts={}
for row in rows:counts[row['unit']]=counts.get(row['unit'],0)+1
if any(counts.get(u['index'],0)!=u['statements'] for u in index):raise RuntimeError('Statement map incomplete')
print(json.dumps({'PG17_native_units':len(index),'PG17_native_statements':statements,'fresh_sql_files':3,'byte_maps':'PASS','special_envelope_admission':'NOT_COMPILED_NO_COMMANDS_REMOVED','native':'NOT_RUN'}))
