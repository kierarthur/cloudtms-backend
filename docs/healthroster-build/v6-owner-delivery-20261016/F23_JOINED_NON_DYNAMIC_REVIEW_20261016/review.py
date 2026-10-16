"""Finite source/type/cardinality review. Never opens a database or edits a gate."""
import hashlib,json,pathlib,re,subprocess,sys
HERE=pathlib.Path(__file__).resolve().parent
ROOT=pathlib.Path(sys.argv[1])
COMMIT='c1ebb691feb6a11324060e0c28210e2bd735624a'
sys.path.insert(0,r'C:/tmp/healthroster-v6-invoice-parser-20261016')
from pglast import parser
sha=lambda s:hashlib.sha256(s if isinstance(s,bytes)else s.encode()).hexdigest()
frozen=lambda p:subprocess.check_output(['git','show',COMMIT+':'+p],cwd=ROOT)
D=json.loads((HERE/'DISCOVERY.json').read_text(encoding='utf-8'))
S=json.loads(frozen('supabase/release/current-contract.json'))
relations={r['schema']+'.'+r['name']:r for r in S['relations']}
columns={name+'.'+c['name']:c for name,r in relations.items()for c in r['columns']}
ddl_path='supabase/migrations/13102026_1344_weekly_source_self_bill_report_schema.sql'
ddl=frozen(ddl_path).decode('utf-8')
ddl_statement=excerpt_ddl=re.search(r'create table public\.weekly_source_self_bill_reports \(.*?\n\);',ddl,re.I|re.S)[0]
ddl_ast=json.loads(parser.parse_sql_json(ddl_statement))['stmts'][0]['stmt']['CreateStmt']
assert ddl_ast['relation']['schemaname']== 'public' and ddl_ast['relation']['relname']=='weekly_source_self_bill_reports'
ddl_columns=[x['ColumnDef']for x in ddl_ast['tableElts']if 'ColumnDef'in x]
assert any(c['colname']=='id'and any(x['Constraint']['contype']=='CONSTR_PRIMARY'for x in c.get('constraints',[]))for c in ddl_columns)
relations['public.weekly_source_self_bill_reports']={'constraints':[{'type':'p','definition':'PRIMARY KEY (id)','evidence':'Exact additive migration ColumnDef id inline CONSTR_PRIMARY, PG17 AST'}]}
for c in ddl_columns:columns['public.weekly_source_self_bill_reports.'+c['colname']]={'name':c['colname'],'nativeTypeAst':c['typeName'],'nativeDdlPath':ddl_path,'nativeDdlSha256':sha(frozen(ddl_path))}
placement_path='supabase/migrations/16102026_0159_nhsp_v6_01_physical_source_review_schema.sql'
placement_ddl=frozen(placement_path).decode('utf-8')
alias_statement=re.search(r'create table private\.timesheet_contract_family_aliases \(.*?\n\);',placement_ddl,re.I|re.S)[0]
alias_ast=json.loads(parser.parse_sql_json(alias_statement))['stmts'][0]['stmt']['CreateStmt']
alias_columns=[x['ColumnDef']for x in alias_ast['tableElts']if 'ColumnDef'in x]
assert any(c['colname']=='timesheet_id'and c['typeName']['names'][-1]['String']['sval']=='uuid'and any(x['Constraint']['contype']=='CONSTR_PRIMARY'for x in c.get('constraints',[]))for c in alias_columns)
uuid_relations={}
for rel,col in [('public.timesheets','timesheet_id'),('public.nhsp_shifts','id'),('public.candidate_submission_workflows','id'),('public.weekly_source_invoice_presentation_lines','id')]:
 assert columns[rel+'.'+col]['type']=='uuid' and columns[rel+'.'+col]['not_null']
 assert any(c['definition']=='PRIMARY KEY ('+col+')'for c in relations[rel]['constraints'])
 uuid_relations[rel]={'uuidColumn':columns[rel+'.'+col],'primaryKeys':[c for c in relations[rel]['constraints']if c['type']=='p']}
uuid_schema={'currentContractRelations':uuid_relations,'aliases':{'path':placement_path,'sourceSha256':sha(frozen(placement_path)),'createStatement':alias_statement,'statementSha256':sha(alias_statement),'pg17AstProof':'timesheet_id UUID inline PRIMARY KEY; at most one alias/member binding per historical timesheet. The final H103 function is STABLE RETURNS JSONB (pinned in PROVENANCE.json), so repeated joins resolve the same scalar actual UUID.'}}
old_path='docs/healthroster-build/v6-owner-delivery-20261016/F23_AGGREGATE_SOURCE_REVIEW_20261016/EXPRESSION_INDEX.json'
old={r['expression']:r for r in json.loads(frozen(old_path))}
functions=json.loads((HERE/'../../../../.codex-tmp/f23-joined-nondynamic/FUNCTIONS.json').resolve().read_text(encoding='utf-8'))
bodies={r['signature']:(HERE/f'../../../../.codex-tmp/f23-joined-nondynamic/{r["number"]:02}.sqlinc').resolve().read_text(encoding='utf-8')for r in functions}
def excerpt(text,pattern):
 m=re.search(pattern,text,re.I|re.S);assert m,pattern
 return {'text':m.group(0),'sha256':sha(m.group(0))}
def expressions(s):
 ts=parser.scan(s);out=[]
 for i,t in enumerate(ts):
  name=s[t.start:t.end+1]
  if name.lower()not in('min','max')or i+1==len(ts)or s[ts[i+1].start:ts[i+1].end+1]!='(':continue
  start=ts[i+1].end+1;depth=1;j=i+2
  while depth:
   word=s[ts[j].start:ts[j].end+1]
   depth+=1 if word=='('else -1 if word==')'else 0;j+=1
  end=ts[j-1].start;inp=s[start:end]
  out.append({'expression':name.upper()+'('+re.sub(r'\s+',' ',inp)+')','input':inp,'inputSha256':sha(inp),'offset':t.start,'end':ts[j-1].end+1})
 return out
new={
 'MAX(display.week_ending_date)':('date','DISPLAY_WEEK_BOUNDARY','display is source_display; source_display selects timesheet.week_ending_date from actual public.timesheets. MAX is the latest displayed date in the canonical group, not an identity selector.',['public.timesheets.week_ending_date']),
 'MIN(code_order)':('integer','BLOCKER_ORDER','raw_code UNION arms are integer literals1/2 and100+source.ordinality::integer. MIN chooses first native blocker display priority after grouping by code; no business UUID is selected.',[]),
 'MIN(summary.week_ending_date)':('date','INVOICE_DISPLAY_DATE_BOUNDARY','summary is public.v_timesheets_summary_base joined by actual invoice_lines.timesheet_id; the declared view column is date. MIN gives earliest native invoice display week.',['public.v_timesheets_summary_base.week_ending_date']),
 'MAX(summary.week_ending_date)':('date','INVOICE_DISPLAY_DATE_BOUNDARY','Same exact v_timesheets_summary_base date binding; MAX gives latest native invoice display week.',['public.v_timesheets_summary_base.week_ending_date']),
 "MAX(nullif(btrim(s.value->>'ref_num'),''))":('text','SEGMENT_REFERENCE_SINGLETON','s.value is JSONB from native invoice_breakdown_json segments. ->> gives text; BTRIM/NULLIF preserve text. segment_stats also counts exact matching segments; evaluated.ready requires segment_count=1 before a segment reference is eligible. A duplicate segment cannot become Ready via MAX.',['public.timesheets_financials.invoice_breakdown_json']),
}
current_binding={
 5:'Current reference_scope is the current private._invoice_reference_rows_batch(uuid[]) output; row_key/ref_target/day_ymd are declared TEXT. Literal reference label CASEs stay TEXT; native last_row.source_row_ordinal is INTEGER; request_no is ordinality::integer. ranked/candidates priorities are literal integers, ns.ref_num/external_row_key TEXT, ns.id UUID explicitly cast TEXT, with count=1 before UUID result. All corresponding current producer joins/CTE bindings were checked, not inferred from aliases alone.',
 15:'The sole aggregate source is public.weekly_source_billing_movements movement constrained by the admitted v_ids. vat_rate_pct is NUMERIC(5,2), price_check_result TEXT. SUM expressions are existing numeric/JSON-to-numeric projections and unchanged by this finite type classification.',
 24:'economic_target_week is CASE explicit::date/actual natural_week_start date; settings_date is COALESCE of those dates and London effective date::date. canonical_blocker is a text CASE over literal native blockers and the TEXT declared reference-result blocker. The actual source_rows_scoped/canonical_sources bindings, including new current/original eligibility, preserve these scalar types.',
 25:'The current source_display explicitly selects public.timesheets.week_ending_date. The current stale_invoice_timesheets joins public.v_timesheets_summary_base summary; its week_ending_date column is DATE. raw_code is a closed integer UNION as independently recorded below.',
 26:'The complete current SQL-language source reference function derives s.value from native financial invoice_breakdown_json segment array; MAX is NULLIF(BTRIM(JSONB->>text)) and thus TEXT. Exact segment_count=1 readiness/refusal remains in the same current evaluated/result producer.',
 27:'c is public.invoice_operation_chunks reached by exact claim_ids and members. Its payload_json->> labels are TEXT. cl is public.clients joined by tf.client_id; name/primary_invoice_email are TEXT and payment_terms_days INTEGER. No MAX chooses a UUID or raw JSONB.',
 28:'vc/m payload_json comes from actual invoice_operation_chunks and is explicitly::text for MIN. cl is native clients; sd is the sole settings_defaults row id=1; frozen authority #>> is TEXT or explicitly::integer. segment_entries carries source_rows_base tf.* NUMERIC charge buckets and INTEGER timesheet_version, ct TEXT labels and typed native VAT-policy NUMERIC. These are the current complete statement bindings, not a transferred file approval.',
}
schema_columns={};tuple_rows=[];aggregate_rows=[];uuid_rows=[];json_rows=[]
for row in D['rows']:
 row={**row};body=bodies[row['signature']]
 if row['hazard'].startswith('ROWTYPE_SELECT:'):
  _,var,relation=row['hazard'].split(':');relation=re.sub(r'\s+','',relation).lower()
  declaration=re.search(r'\b'+re.escape(var)+r'\s+'+re.escape(relation).replace(r'\.',r'\s*\.\s*')+r'\s*%\s*rowtype\b',body,re.I);assert declaration,(var,relation)
  sql=re.sub(r'\binto\s+(?:strict\s+)?'+re.escape(var)+r'\b','',row['statement'],count=1,flags=re.I)
  tree=json.loads(parser.parse_sql_json(sql))['stmts'];assert len(tree)==1
  select=tree[0]['stmt']['SelectStmt'];targets=select['targetList'];sources=select['fromClause']
  assert len(targets)==1 and targets[0]['ResTarget']['val']['ColumnRef']['fields']==[{'A_Star':{}}]
  assert len(sources)==1 and sources[0]['RangeVar']['schemaname']+'.'+sources[0]['RangeVar']['relname']==relation
  assert not select.get('withClause')and select.get('op','SETOP_NONE')=='SETOP_NONE'
  rel=relations[relation];pks=[c for c in rel['constraints']if c['type']in('p','u')]
  if relation=='public.timesheets_financials':
   assert re.search(r'\blimit\s+1\b',row['statement'],re.I);bound='LIMIT1 returns zero/one complete declared financial tuple; existing native computed/updated/id order retained. This is no proof that multiple current rows are valid.'
  elif relation=='public.weekly_source_row_resolutions':
   assert any(c['definition']=='UNIQUE (upload_row_id, generation)'for c in pks);bound='Exact upload_row_id+generation is native UNIQUE; zero/one whole tuple.'
  else:
   column='timesheet_id'if relation=='public.timesheets'else'id'
   assert any(c['definition']=='PRIMARY KEY ('+column+')'for c in pks),relation
   assert re.search(r'\b'+column+r'\s*=',row['statement'],re.I);bound='Exact '+column+' primary-key point predicate bounds zero/one complete tuple; STRICT variants raise on absence. Other currentness/authority predicates and existing locks are preserved.'
  row['proof']={'verdict':'FINITE_SOURCE_DECLARED_TUPLE_TYPE_PROVED','declaredVariable':var,'declaredRelation':relation,'declaration':declaration[0],'declarationSha256':sha(declaration[0]),'pg17_ast':'One whole A_Star target; one schema-qualified top-level RangeVar exactly matches declaration; no top-level CTE/set operation/join','cardinalityBound':bound,'nativeKeyEvidence':pks,'approvalLimit':'Tuple type and stated point/LIMIT bound only; no permission/native workflow/effect approval'};tuple_rows.append(row)
 elif row['hazard']=='AGGREGATE_TYPE_UNCLASSIFIED':
  es=expressions(row['statement'])
  for a in es:
   prior=old.get(a['expression'])
   if a['expression']in new:typ,role,proof,refs=new[a['expression']]
   else:
    assert prior,a['expression'];typ,role,proof,refs=prior['inputType'],prior['role'],prior['bindingProof'],prior['nativeColumnRefs']
   assert typ in ['text','numeric','integer','bigint','date','timestamp with time zone']
   a.update(inputType=typ,role=role,bindingProof=proof,currentStatementBindingProof=current_binding[row['id']],nativeColumnRefs=refs,priorExpressionEqual=prior is not None,priorExpressionProof=prior)
   for ref in refs:assert ref in columns,ref;schema_columns[ref]={'column':columns[ref],'columnObjectSha256':sha(json.dumps(columns[ref],sort_keys=True,separators=(',',':')))}
  row['proof']={'verdict':'FINITE_SOURCE_SCALAR_AGGREGATE_TYPES_PROVED','aggregateCount':len(es),'aggregates':es,'approvalLimit':'Exact scalar types only; no economic/rounding/authority/group-currentness approval'};aggregate_rows.append(row)
 elif row['hazard']=='UUID_CARDINALITY_UNCLASSIFIED':
  if row['id']==6:
   patterns=[r'case when count\(\*\)=1 then min\(ranked.shift_id::text\)::uuid end shift_id',r'where ranked.match_priority=ranked.winning_priority',r"when coalesce\(hr_match.reference_match_count,0\)>1\s+then 'AMBIGUOUS'",r"when coalesce\(hr_match.reference_match_count,0\)=0\s+then 'MISSING'"]
   proof='One winning native nhsp_shifts row only: CASE count(*)=1 yields its UUID. Zero/multiple gives SQL NULL; reference_match_count yields MISSING/AMBIGUOUS. Every UNION match arm supplies ns.id as shift_id; UUID primary-key type is retained.'
  elif row['id']==16:
   patterns=[r'select pg_catalog.min\(shift_line.id::text\)::uuid.*?having pg_catalog.count\(\*\)=1']
   proof='Optional expense companion scalar subquery over exact manifest/event/non-expense/correction-role rows has HAVING count(*)=1. Zero or multiple excludes the scalar row and returns NULL; never arbitrary representative. Native presentation_line.id is UUID.'
  elif row['id']==17:
   patterns=[r'if private.timesheet_contract_claim_pending_v1.*?return null;end if;',r'count\(distinct p.current_timesheet_id\),min\(p.current_timesheet_id::text\)::uuid',r'if v_count=1 then return v_current;end if;',r'if v_count>1 or exists.*?CANDIDATE_EXPENSE_CURRENT_PLACEMENT_UNBOUND.*?end if;']
   proof='Real pending exits before aggregate; H103/accepted op/member/workflow/component/current alias restrict exact family. COUNT(DISTINCT nonNULL current_timesheet_id) and MIN(text) inspect the same set. Only count1 returns UUID; >1 raises before return; zero has NULL and archived-original refusal or genuine ordinary-current NULL fallback. No synthetic current identity.'
  elif row['id']==29:
   patterns=[r'case when count\(distinct current_week.timesheet_id\)=1\s+then min\(current_week.timesheet_id::text\)::uuid else null::uuid end',r'when workflow_anchor.workflow_count>1 then null::uuid',r'when workflow_anchor.workflow_count=1 and workflow_anchor.timesheet_id is null then null::uuid',r'when base_anchor.anchor_count>1 then null::uuid',r'when additional_anchor.anchor_count>1 then null::uuid',r'where display_timesheet_id is not null and conflict_code is null']
   proof='All four UUID MINs reviewed: current_version_resolution uses CASE distinct-current-count1, else NULL. Workflow anchor is used only when distinct workflow count1; each workflow has one scalar anchor UUID and direct current resolution yields at most one current UUID, so duplicated joined display rows repeat that same value. More than one workflow is rejected even if anchors coincide. Base/additional anchor UUIDs are discarded when their same-source count(*)>1; zero yields NULL. expense_anchor_totals consumes only nonNULL display identity AND conflict_code NULL. Alias branch uses H103 directly, not lexical minimum. Native Timesheet UUID/PK and workflow UUID/PK plus disjoint ordinary-vs-alias resolution preserve this proof.'
  else:raise AssertionError(row['id'])
  evidence=[excerpt(body,p)for p in patterns]
  row['proof']={'verdict':'FINITE_SOURCE_UUID_CARDINALITY_PROVED','proof':proof,'evidence':evidence,'approvalLimit':'Only these exact complete query/body/count/refusal/current-alias shapes; no generic MIN(TEXT)::UUID rule or native state qualification'};uuid_rows.append(row)
 else:
  assert row['hazard']=='JSON_CONCAT'and row['id']==23
  row['proof']={'verdict':'EXPLICIT_GROUPING_CORRECTION_REQUIRED_NO_ALLOWANCE','receipt':'CORRECTION_RECEIPT.json','reason':'Current extraction/concat lacks explicit operand grouping for the generic fence. PG17 AST is identical after grouping; do not claim an observed runtime-value defect.'};json_rows.append(row)
assert(len(tuple_rows),len(aggregate_rows),len(uuid_rows),len(json_rows))==(26,7,4,1)
assert sum(r['proof']['aggregateCount']for r in aggregate_rows)==64
assert sum(not a['priorExpressionEqual']for r in aggregate_rows for a in r['proof']['aggregates'])==5
report={'version':'JOINED_INVOICE_CANDIDATE_NON_DYNAMIC_FINITE_REVIEW_V1','qualification':'SOURCE_ONLY_FINITE_EXTENSION_REVIEW_NOT_NATIVE_OR_GATE_ADMISSION','sourceCommit':COMMIT,'discoverySha256':sha((HERE/'DISCOVERY.json').read_bytes()),'counts':{'diagnostics':38,'tupleStatements':26,'aggregateStatements':7,'minMaxExpressions':64,'newExpressionTypesIndependentlyProved':5,'uuidStatements':4,'uuidCasts':7,'groupingCorrectionsRequired':1,'eligibleFiniteExtensionPoints':37},'schema':{'path':'supabase/release/current-contract.json','sha256':sha(frozen('supabase/release/current-contract.json')),'additiveMigration':{'path':ddl_path,'sha256':sha(frozen(ddl_path)),'createStatement':ddl_statement,'createStatementSha256':sha(ddl_statement),'pg17PrimaryKeyAstProved':True},'columns':schema_columns},'priorExpressionIndex':{'path':old_path,'sha256':sha(frozen(old_path)),'rule':'Exact expression equality is comparison evidence only. Every current statement binding is separately proved; no whole-source transfer.'},'tupleRows':tuple_rows,'aggregateRows':aggregate_rows,'uuidRows':uuid_rows,'jsonRows':json_rows,'limits':['All whole functions/signatures/current source/statement/count pins remain required.','Tuple classification does not approve business eligibility, permissions or effects.','Aggregate classification does not approve money calculations or new authority.','Do not admit the original JSON hazard; apply its exact complete-function correction first.','No generic fence modified; no DB connection or runtime/native claim.'],'native':'NOT_RUN'}
report['uuidNativeSchemaEvidence']=uuid_schema
(HERE/'REVIEW.json').write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8',newline='\n')
print(json.dumps({'counts':report['counts'],'reviewSha256':sha((HERE/'REVIEW.json').read_bytes())},indent=2))
