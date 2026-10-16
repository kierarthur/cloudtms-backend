import hashlib,json,pathlib,sys
sys.path.insert(0,'C:/tmp/healthroster-v6-physical-parser')
import pglast
from pglast import parser
here=pathlib.Path(__file__).resolve().parent
if sys.platform=='win32':here=pathlib.Path('\\\\?\\'+str(here))
relative='payload/supabase/repeatable/16102026_1210_weekly_contact_producer_completion/44_manager_selection_scope_locks.sqlinc'
source=(here/'inputs/assembly'/relative).read_text(encoding='utf-8')
original="facts->'pairs'||p_extra_pairs"
grouped="(facts->'pairs')||p_extra_pairs"
assert source.count(original)==1
assert "jsonb_typeof(p_extra_pairs)is distinct from 'array'" in source
assert "'pairs',pairs" in source
assert "coalesce(jsonb_agg(jsonb_build_object('candidate_id',candidate_id,'client_id',client_id)order by candidate_id,client_id),'[]') into pairs" in source
def strip(value):
 if isinstance(value,list):return [strip(v) for v in value]
 if isinstance(value,dict):return {k:strip(v) for k,v in value.items() if k not in ('location','stmt_location','stmt_len')}
 return value
first=strip(json.loads(parser.parse_sql_json('select jsonb_array_elements('+original+')')))
second=strip(json.loads(parser.parse_sql_json('select jsonb_array_elements('+grouped+')')))
assert first==second
out={'version':'CONTACT_MANAGER_SELECTION_JSON_GROUPING_SOURCE_V1','qualification':'OFFLINE_PG17_AST_AND_EXACT_SOURCE_ONLY','sourceCommit':'bf7a321429969f2c43345ce05ad2679ce3dbe83c','proposalPath':relative,'actualMappedPath':'PENDING_ROOT_MAP','sourceSha256':hashlib.sha256(source.encode()).hexdigest(),'original':original,'explicitGrouping':grouped,'exactOneOccurrence':True,'samePg17AstIgnoringLocations':True,'pglast':pglast.__version__,'postgres':parser.get_postgresql_version(),'argumentTypes':{'facts':"JSONB result of exact scope-facts owner; pairs is coalesced JSONB_AGG array",'p_extra_pairs':'JSONB input explicitly refused unless ARRAY'},'ast':first,'sqlEdits':0,'databaseCalls':0,'native':'NOT_RUN','classification':'WITHHELD_PENDING_ACTUAL_MOUNT_AND_ROOT_TREATMENT'}
(here/'JSON_GROUPING_REVIEW.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps({k:v for k,v in out.items() if k!='ast'}))
