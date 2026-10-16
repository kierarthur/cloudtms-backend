"""Emit SQL from the actual ID set returned by 07. No database/network access.
Usage: python emit-fresh-verification.py receipt.json > fresh.sql
The receipt is the fixture_absence_set JSON value, never a whole tool transcript.
"""
import json,re,sys
from collections import defaultdict
from pathlib import Path
receipt=json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))
if receipt.get('version')!='CONTACT_FIXTURE_ABSENCE_SET_V1' or receipt.get('database') not in ('cloudtms_test_clone','banking_modal_v2_test'):
 raise ValueError('Wrong native absence receipt')
objects=receipt.get('objects')
if not isinstance(objects,list) or not 1<=len(objects)<=10000:raise ValueError('Missing/bounded actual IDs required')
groups=defaultdict(set)
for item in objects:
 if set(item)!={'relation_name','column_name','object_id'}:raise ValueError('Invalid ID entry')
 relation,column,identity=item['relation_name'],item['column_name'],item['object_id']
 if not re.fullmatch(r'(public|private)\.[a-z][a-z0-9_]*',relation):raise ValueError('Invalid native relation')
 if not re.fullmatch(r'[a-z][a-z0-9_]*',column):raise ValueError('Invalid native column')
 if not re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}',identity):raise ValueError('Invalid native UUID')
 groups[(relation,column)].add(identity)
print("DO $fresh$ DECLARE v_found boolean; BEGIN IF current_database()<>'"+receipt['database']+"' THEN RAISE EXCEPTION 'CONTACT_FRESH_WRONG_DATABASE';END IF;")
for (relation,column),identities in sorted(groups.items()):
 schema,table=relation.split('.')
 query='SELECT EXISTS(SELECT 1 FROM "'+schema+'"."'+table+'" WHERE "'+column+'"=ANY(ARRAY['+','.join("'"+i+"'::uuid" for i in sorted(identities))+']))'
 print("IF to_regclass('"+relation+"') IS NOT NULL THEN EXECUTE '"+query.replace("'","''")+"' INTO v_found; IF v_found THEN RAISE EXCEPTION 'CONTACT_FRESH_ID_REMAINS "+relation+'.'+column+"';END IF;END IF;")
print("END;$fresh$;SELECT jsonb_build_object('fixture','CONTACT_START_ACTUAL_ID_ABSENCE','absent',true,'database',current_database());")
