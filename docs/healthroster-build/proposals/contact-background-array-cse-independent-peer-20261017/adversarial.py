"""Independent finite NULL/count/collation/lineage AST and bag counterexamples. No SQL run."""
from pathlib import Path
import copy, hashlib, itertools, json, sys
sys.dont_write_bytecode=True
HERE=Path('\\\\?\\'+str(Path(__file__).resolve().parent))
source=(HERE/'reference/ast-proof.py').read_text(encoding='utf-8')
assert source.count('\nproof=[]\n')==1
namespace={'__file__':str(HERE/'reference/ast-proof.py')}
exec(compile(source.split('\nproof=[]\n')[0],str(HERE/'reference/ast-proof.py'),'exec'),namespace)
prove=namespace['prove']; tree=namespace['tree']; clean=namespace['clean']; walk=namespace['walk']
r=json.loads((HERE/'reference/rendered/CONTACT_CONSTRUCTORS_V1.json').read_bytes())
assert prove(r)['allOriginalTwelveRelationRecords']
# Actual complete final-query mutations, not a substitute renderer. Each leaves
# the original actual canonical source unchanged, and must lose its source proof.
mutations={
 'NULL hashes must still count':("select 'public.ts_financials_outbox' relation,count(*) row_count,", "select 'public.ts_financials_outbox' relation,count(bg_entry.value->>'rowHash') row_count,"),
 'empty aggregate must retain empty string':("coalesce(string_agg(bg_entry.value->>'rowHash','|' order by bg_entry.value->>'rowHash'),'')", "string_agg(bg_entry.value->>'rowHash','|' order by bg_entry.value->>'rowHash')"),
 'text default collation cannot be replaced':("order by bg_entry.value->>'rowHash'", "order by (bg_entry.value->>'rowHash') collate \"C\""),
 'text must not be padded to char':("string_agg(bg_entry.value->>'rowHash','|'", "string_agg((bg_entry.value->>'rowHash')::char(64),'|'"),
 'count cannot narrow to int4':("select 'public.ts_financials_outbox' relation,count(*) row_count,", "select 'public.ts_financials_outbox' relation,count(*)::integer row_count,"),
 'row membership cannot be filtered':("as bg_entry(value)", "as bg_entry(value) where false"),
 'relation identity cannot borrow the other array':("value->'public.ts_financials_outbox'", "value->'private.candidate_daily_batch_receipts'"),
 'full map null fields cannot be stripped':(r['parts']['maps'],'jsonb_strip_nulls('+r['parts']['maps']+')'),
}
refused=[]
for label,(before,after) in mutations.items():
 assert before in r['input'],label
 m=copy.deepcopy(r);m['input']=m['input'].replace(before,after,1)
 # These eight counterexamples must remain syntactically complete PG17 SQL;
 # a parser error is not counted as a semantic source-proof refusal.
 tree(namespace['executable'](m['input']))
 try:prove(m)
 except (AssertionError,KeyError):refused.append(label)
 else:raise AssertionError('MUTATION_ADMITTED:'+label)
# Independently derive every original map's all-row membership and native count.
final=tree(namespace['executable'](r['input']))['stmts'][6]['stmt']['SelectStmt']
ctes=namespace['ctes'](final); business=namespace['leaves'](ctes['business']['ctequery']['SelectStmt'])
for relation in business[1:]:
 count=relation['targetList'][1]['ResTarget']['val']['FuncCall']
 assert count['agg_star'] is True and count['funcname']==[{'String':{'sval':'count'}}]
 assert not count.get('agg_distinct') and not count.get('agg_filter')
 assert 'TypeCast' not in relation['targetList'][1]['ResTarget']['val']
# Synthetic value algebra only, not a PostgreSQL execution/collation oracle.
sha=lambda s:hashlib.sha256(s.encode('utf-8')).hexdigest()
def bag(entries):
 vals=[e['rowHash'] for e in entries if e['rowHash'] is not None]
 return len(entries),sha('|'.join(sorted(vals)))
vectors=[]
for values in [[],[None],[None,None],['0'*64,None,'f'*64],['f'*64,'0'*64,'f'*64],['a'*64]*5]:
 entries=[{'keyHash':'same-key','rowHash':v,'nullable':None} for v in values]
 expected=bag(entries)
 for perm in itertools.permutations(entries):assert bag(perm)==expected
 if len(entries)>1:assert bag([entries[-1]])[0]!=expected[0]
 if any(x is None for x in values):assert sum(x is not None for x in values)!=expected[0]
 vectors.append({'entries':len(entries),'nullHashes':sum(x is None for x in values),'multiplicityAndPermutation':True})
assert bag([])[1]==bag([{'rowHash':None}])[1] and bag([])[0]!=bag([{'rowHash':None}])[0]
assert sha('漢字 💠 é')!=sha('漢字 💠 e')
assert bag([{'rowHash':'0'*64},{'rowHash':'f'*64}])[1]!=sha(','.join(['0'*64,'f'*64]))
out={'actualFinalQueryAstRefusals':refused,'originalAllTwelveRecordsRetained':True,'twoNativeCountStarNoCast':True,'syntheticBagVectors':vectors,'nullCountDistinguishedFromEmpty':True,'UTF8AndDelimiterCounterexamples':True,'postgreSQLCollationExecuted':False,'queriesExecuted':0,'native':'NOT_RUN'}
print(json.dumps(out,indent=2))
