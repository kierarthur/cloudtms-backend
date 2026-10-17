import json,pathlib,sys,unittest
sys.path.insert(0,r'D:/Development/Legacy-Work/C-tmp/healthroster-v6-invoice-parser-20261016')
from pglast import parse_sql,parse_plpgsql,ast
from pglast.stream import RawStream
HERE=pathlib.Path(__file__).resolve().parent;M=json.loads((HERE/'MODEL.json').read_text());raw=lambda s:RawStream()(s)
def source(f):return '\n'.join(l for l in (HERE/'parents'/f).read_text().splitlines() if not l.startswith('\\'))
def statements(s):return [x.stmt for x in parse_sql(s) if not isinstance(x.stmt,ast.TransactionStmt)]
def clean(v):
 if isinstance(v,dict):return {k:clean(x) for k,x in v.items() if k not in ['location','stmt_location','stmt_len']}
 if isinstance(v,(list,tuple)):return [clean(x) for x in v] if v else None
 return v
class Proof(unittest.TestCase):
 def test_complete_overlap(self):
  c=[raw(t) for t in statements(source(M['constructor']))]
  for f in M['consumers'][:2]:
   for t in statements(source(f)):self.assertEqual(c.count(raw(t)),1)
  self.assertEqual(len(M['duplicateStatements']),11)
 def test_reference_ast_inverse(self):
  originals=[t for f in M['consumers'][:2] for t in statements(source(f)) if isinstance(t,ast.CreateStmt)]
  refs=statements(M['referenceSql']);self.assertEqual(len(refs),8)
  for o,r in zip(originals,refs):
   self.assertEqual(r.relation.schemaname,'pg_temp');self.assertEqual(r.relation.relpersistence,'t')
   r.relation.schemaname='private';r.relation.relpersistence='p'
   for e in o.tableElts:
    if isinstance(e,ast.ColumnDef):e.constraints=tuple(c for c in e.constraints or () if c.contype.name!='CONSTR_FOREIGN')
   o.tableElts=tuple(e for e in o.tableElts if not(isinstance(e,ast.Constraint) and e.contype.name=='CONSTR_FOREIGN'))
   self.assertEqual(clean(r()),clean(o()))
 def test_reference_shape_counterexamples(self):
  originals=statements(M['referenceSql']);base=clean(originals[0]())
  for mutate in [lambda t:setattr(t.tableElts[0],'constraints',()),lambda t:setattr(t.tableElts[0],'colname','wrong'),lambda t:setattr(t.relation,'relpersistence','p'),lambda t:setattr(t.relation,'schemaname','private')]:
   t=parse_sql(raw(originals[0]))[0].stmt
   mutate(t)
   self.assertNotEqual(clean(t()),base)
 def test_foreign_keys_complete(self):
  f=M['foreignKeys'];self.assertEqual(len(f),31);self.assertEqual(len({(x['table'],x['name']) for x in f}),31)
  self.assertTrue(all(x['noInherit'] and x['validated'] and x['local'] and x['parent']==0 and x['fourEnabledInternalTriggers'] and len(x['triggerFunctions'])==4 for x in f))
  self.assertEqual(sum(x['deferrable'] and x['deferred'] for x in f),3)
  self.assertEqual({x['name'] for x in f if x['deferrable']},{'source_pay_decision_accept_request_fk','source_pay_assembly_review_triangle_fk','source_pay_request_review_fk'})
 def test_security_loop_only_eight_removed(self):
  old=statements(source(M['consumers'][2]));new=statements('\n'.join(l for l in M['security3Source'].splitlines() if not l.startswith('\\')))
  self.assertEqual(len(old),12);self.assertEqual(len(new),12)
  for a,b in zip(old[:-1],new[:-1]):self.assertEqual(clean(a()),clean(b()))
  oldbody=old[-1].args[0].arg.sval;newbody=new[-1].args[0].arg.sval
  self.assertEqual(oldbody.count('foreach name in array'),1);self.assertEqual(newbody.count('foreach name in array'),1)
  self.assertEqual(oldbody[oldbody.index('] loop'):],newbody[newbody.index('] loop'):])
  self.assertEqual(oldbody[:oldbody.index('array[')],newbody[:newbody.index('array[')])
  for n in M['tables']:self.assertIn("'"+n+"'",oldbody);self.assertNotIn("'"+n+"'",newbody)
 def test_generated_full_sql_and_all_do_programmes(self):
  total=0;programmes=0
  for f in M['consumers']:
   s=(HERE/'generated'/pathlib.Path(f).name).read_text();ss=parse_sql(s);total+=len(ss)
   for st in ss:
    if isinstance(st.stmt,ast.DoStmt):parse_plpgsql(raw(st.stmt));programmes+=1
   creates=[x.stmt for x in ss if isinstance(x.stmt,ast.CreateStmt)]
   self.assertTrue(all(t.relation.schemaname=='pg_temp' and t.relation.relpersistence=='t' for t in creates))
   self.assertFalse(any(isinstance(x.stmt,ast.DropStmt) and x.stmt.missing_ok for x in ss))
  self.assertGreater(total,65);self.assertEqual(programmes,21)
if __name__=='__main__':unittest.main(verbosity=2)
