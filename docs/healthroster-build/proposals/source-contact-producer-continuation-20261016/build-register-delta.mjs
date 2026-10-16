import {readFileSync,writeFileSync,readdirSync} from 'node:fs';
import {createHash} from 'node:crypto';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
const base=path.dirname(fileURLToPath(import.meta.url)),file=path.join(base,'REGISTER_DELTA_WIP.json');
const register=JSON.parse(readFileSync(file,'utf8'));const sha=s=>createHash('sha256').update(s).digest('hex');
register.functions=[];
for(const name of readdirSync(path.join(base,'forward')).filter(name=>name.endsWith('.sqlinc')&&!name.endsWith('_acl.sqlinc')).sort()){
 const source='forward/'+name,sql=readFileSync(path.join(base,source),'utf8');
 for(const match of sql.matchAll(/create or replace function ((public|private)\.[a-z0-9_]+)\(([\s\S]*?)\)\s*returns([\s\S]*?)as \$function\$/gi)){
  const args=match[3].trim()?match[3].split(',').map(arg=>arg.trim().split(/\s+/)[1]).join(','):'';
  register.functions.push({signature:match[1]+'('+args+')',source,sha256:sha(sql),schema:match[2],
   owner:'audited logical postgres mapped only by root managed-release owner adapter',
   security_definer:/security definer/i.test(match[4]),fixed_search_path:/set search_path/i.test(match[4]),
   public:false,anon:false,authenticated:false,service_role_execute:match[2]==='public'});
 }
}
register.function_signature_count=register.functions.length;
register.deferred_integrity_triggers=[
 {table:'weekly_contact_source_intent_captures',trigger:'weekly_contact_source_intent_captures_lineage',source:'forward/19_capture_and_rebase_integrity.sqlinc'},
 {table:'weekly_contact_source_intent_capture_members',trigger:'weekly_contact_source_intent_capture_members_lineage',source:'forward/19_capture_and_rebase_integrity.sqlinc'},
 {table:'weekly_contact_source_due_event_rebases',trigger:'weekly_contact_source_due_event_rebases_lineage',source:'forward/19_capture_and_rebase_integrity.sqlinc'}];
register.stateful_row_guards=[{table:'weekly_contact_checking_scope_versions',trigger:'weekly_contact_checking_version_guard'},
 {table:'weekly_contact_source_ordinal_owners',trigger:'weekly_contact_source_ordinal_owner_guard'},
 {table:'weekly_contact_recheck_progress',trigger:'weekly_contact_recheck_progress_guard'}];
writeFileSync(file,JSON.stringify(register,null,2)+'\n');
console.log(JSON.stringify({tables:register.tables.length,function_signatures:register.functions.length,status:register.status}));
