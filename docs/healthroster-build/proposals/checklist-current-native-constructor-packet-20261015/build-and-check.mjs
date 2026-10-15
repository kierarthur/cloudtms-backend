// Constructor source packaging only. This tool never executes the SQL.
import fs from'node:fs';import path from'node:path';import assert from'node:assert/strict';import{execFileSync}from'node:child_process';import{fileURLToPath}from'node:url';import{sha}from'../checklist-current-native-module-recipe-20261015/read-root-source-index.mjs';import{scanManagedSql}from'../../../../scripts/cloudtms-managed-release-sql.mjs';import{functionEvents,sqlTokens}from'../checklist-native-20261014/financial-native-input-20261014/PINNED_A0_CURRENT_SOURCE_INDEX.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),source='c7fcb2519f9d3b93f29e580df6e6cb10c6d30322',base=path.resolve(here,'../checklist-legacy-hold-retention-20261015'),planFile='docs/healthroster-build/proposals/checklist-legacy-hold-retention-20261015/CURRENT_RECIPE_SUCCESSOR.json',raw=f=>execFileSync('git',['cat-file','blob',source+':'+f],{cwd:repo,maxBuffer:24e6}),planRaw=raw(planFile);assert.deepEqual(planRaw,fs.readFileSync(path.join(repo,planFile)));const plan=JSON.parse(planRaw),inputs=[],parts=[],signatures=[],expected=[];
for(const target of plan.targets.filter(t=>t.kind.includes('FUNCTION_OWNER'))){const file=path.relative(repo,path.resolve(base,target.output)).replaceAll('\\','/');assert(!file.startsWith('../'));const b=raw(file);assert.deepEqual(b,fs.readFileSync(path.join(repo,file)));assert.equal(sha(b),target.sha256);const text=b.toString().replaceAll('\r\n','\n'),e=functionEvents(text).events.filter(e=>e.kind==='CREATE');assert.equal(e.length,1);assert(!signatures.includes(e[0].id));signatures.push(e[0].id);const header=e[0].definition.slice(0,e[0].definition.indexOf(e[0].body)),vol=/\bimmutable\b/i.test(header)?'i':/\bstable\b/i.test(header)?'s':'v';expected.push({id:e[0].id,volatility:vol,security_definer:/\bsecurity\s+definer\b/i.test(header)});const scan=scanManagedSql(text);for(const s of scan.statements){const words=sqlTokens(text.slice(s.start,s.end)).map(t=>t.value);assert(['create','alter','revoke','grant'].includes(words[0]),file+' constructor/ACL only');}assert.equal(scan.meta.length,0);inputs.push({commit:source,file,bytes:b.length,sha256:sha(b),canonical_sha256:sha(text),target:target.target,signature:e[0].id});parts.push('-- Exact complete owner: '+file+'\n'+text);}
assert.equal(inputs.length,50);const literal=s=>"'"+s.replaceAll("'","''")+"'",rows=expected.map(r=>'('+[literal(r.id),literal(r.volatility),r.security_definer?'true':'false'].join(',')+')').join(',\n');
const assertion=`do $constructor_assert$
declare r record;p pg_catalog.pg_proc%rowtype;
begin
 for r in select * from(values
${rows}
 ) expected(signature,volatility,security_definer) loop
  select q.* into p from pg_catalog.pg_proc q where q.oid=pg_catalog.to_regprocedure(r.signature);
  if not found or p.provolatile::text is distinct from r.volatility or p.prosecdef is distinct from r.security_definer then
   raise exception 'CHECKLIST_CONSTRUCTOR_CATALOG_MISMATCH: %',r.signature using errcode='55000';end if;
 end loop;
end;
$constructor_assert$;
`;
const sql='-- SOURCE TEMPLATE ONLY. No SQL execution or catalog proof is recorded here.\n-- Root must use its approved rollback-only TEST route and audited logical-owner mapping.\n-- No native business writer is invoked. The final full ACL gate is a separate trial.\n'+parts.join('\n')+'\n'+assertion,scan=scanManagedSql(sql);assert(scan.statements.length<=250);assert(Buffer.byteLength(sql)<=2_000_000);assert.equal(functionEvents(sql).events.filter(e=>e.kind==='CREATE').length,50);assert(!scan.statements.some(s=>['begin','commit','rollback','select'].includes(sqlTokens(sql.slice(s.start,s.end))[0]?.value)));assert.equal(scan.meta.length,0);
const fingerprint=`-- Run fresh and read-only before and after root's rollback trial; compare the entire JSON value.
with wanted(signature) as(values
${signatures.map(s=>'('+literal(s)+')').join(',\n')}
), routines as(
 select w.signature,p.oid::text oid,p.proowner::pg_catalog.regrole::text owner,p.provolatile::text volatility,
  p.prosecdef security_definer,p.proacl::text acl,p.proconfig::text configuration,
  case when p.oid is null then null else pg_catalog.md5(pg_catalog.pg_get_functiondef(p.oid)) end definition_md5
 from wanted w left join pg_catalog.pg_proc p on p.oid=pg_catalog.to_regprocedure(w.signature)
), triggers as(
 select n.nspname schema_name,c.relname table_name,t.tgname trigger_name,
  pg_catalog.md5(pg_catalog.pg_get_triggerdef(t.oid,true)) definition_md5,t.tgenabled enabled
 from pg_catalog.pg_trigger t join pg_catalog.pg_class c on c.oid=t.tgrelid
 join pg_catalog.pg_namespace n on n.oid=c.relnamespace
 where not t.tgisinternal and n.nspname in('private','public')
)
select pg_catalog.jsonb_build_object('contract','CHECKLIST_CONSTRUCTOR_ROUTINE_TRIGGER_FRAME_V1',
 'routines',(select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(r) order by signature) from routines r),
 'triggers',(select pg_catalog.jsonb_agg(pg_catalog.to_jsonb(t) order by schema_name,table_name,trigger_name) from triggers t));
`;
const emit=(file,text)=>{if(process.argv.includes('--seal')||!fs.existsSync(path.join(here,file)))fs.writeFileSync(path.join(here,file),text);else assert.equal(fs.readFileSync(path.join(here,file),'utf8'),text,file+' frozen artifact');};emit('CONSTRUCTORS_AND_ORIGINAL_ACL.sql',sql);emit('FRESH_ROUTINE_TRIGGER_FINGERPRINT.sql',fingerprint);emit('CONSTRUCTOR_SOURCE_MANIFEST.json',JSON.stringify({source_only:true,SQL_executed:false,source_commit:source,plan:{file:planFile,sha256:sha(planRaw)},inputs,constructor_functions:50,constructor_statements:scan.statements.length,constructor_bytes:Buffer.byteLength(sql),constructor_sha256:sha(sql),fingerprint_sha256:sha(fingerprint),logical_owner:'postgres; root must map only audited logical owner through approved release/lab preparation',actual_before_after_fingerprints:null,actual_constructor_results:null,rolled_back:null,fresh_connection_proof:null,final_ACL_role_qualification:false,FALSE_purity:false,TRUE_parity:false,financial_or_business_qualification:false,full_release_rehearsal:false},null,2)+'\n');
for(const p of inputs)assert.deepEqual(raw(p.file),fs.readFileSync(path.join(repo,p.file)));const packet=path.join(here,'PACKET_MANIFEST.json');if(process.argv.includes('--seal')){const files=fs.readdirSync(here).filter(f=>!['PACKET_MANIFEST.json','SOURCE_INTEGRITY_OUTPUT.txt'].includes(f)).sort().map(file=>{const b=fs.readFileSync(path.join(here,file));return{file,bytes:b.length,sha256:sha(b)};});fs.writeFileSync(packet,JSON.stringify({source_only:true,SQL_executed:false,files},null,2)+'\n');}else if(fs.existsSync(packet))for(const p of JSON.parse(fs.readFileSync(packet)).files){const b=fs.readFileSync(path.join(here,p.file));assert.equal(sha(b),p.sha256,p.file);assert.equal(b.length,p.bytes);}console.log(JSON.stringify({source_only:true,raw_saved_Git_inputs:inputs.length,complete_function_constructors:50,statements:scan.statements.length,bytes:Buffer.byteLength(sql),metadata_only_catalog_assertions:50,fresh_fingerprint_query_statements:scanManagedSql(fingerprint).statements.length,SQL_executed:false,native_acceptance:false}));
