// Finite Contact-only compilation. No execution, arbitrary SQL source or provider.
import {createHash} from 'node:crypto';
const check=(v,c)=>{if(!v)throw Error(c);},sha=s=>createHash('sha256').update(s).digest('hex');
export const BASE_KEYS=Object.freeze(['physicalDatabase','identityTest','customerKeyNull','latestRelease','module','applyingCount','markerAbsent','ledgerFingerprint','catalogueFingerprint','businessCountsFingerprint','businessRowsFingerprint','businessRelations','relationCensus','backgroundWriterRoutines','otherActivityCount']);
export const BACKGROUND_KEYS=Object.freeze(['backgroundObservedAt','backgroundSnapshotStartedAt','backgroundSnapshotXmax','backgroundSnapshotEpochZero','backgroundSnapshotXip','backgroundRows']);
// PG17 jsonb_util.c lengthCompareJsonbString/Pair: ASCII byte length, then memcmp.
export const SNAPSHOT_KEYS=Object.freeze([...BASE_KEYS,...BACKGROUND_KEYS].sort((a,b)=>a.length-b.length||(a<b?-1:a>b?1:0)));
export const BAG_NAMES=Object.freeze(['hr_contact_audit_bag','hr_contact_other_bag']);
function between(raw,start,end){check(raw.split(start).length===2,'LOCAL_METADATA_BATCH_SOURCE_REFUSED');const at=raw.indexOf(start)+start.length,last=raw.indexOf(end,at);check(last>=at&&raw.indexOf(end,last+end.length)<0,'LOCAL_METADATA_BATCH_SOURCE_REFUSED');return raw.slice(at,last);}
function replaceOnce(raw,before,after){check(raw.split(before).length===2,'LOCAL_METADATA_BATCH_SOURCE_REFUSED');return raw.replace(before,()=>after);}
export function compileNativeBatch(original,managed,backgroundFields){
 const parsed=managed.scanManagedSql(original.input),s=parsed.statements;check(parsed.meta.length===0&&s.length===6&&s.map(x=>x.tokens[0]).join('|')==='BEGIN|DO|SELECT|SET|WITH|ROLLBACK','LOCAL_METADATA_BATCH_PARENT_REFUSED');
 const pieces=s.map(x=>original.input.slice(x.start,x.end)),canon=managed.scanManagedSql(original.canonicalSql);check(pieces[0]==='begin isolation level repeatable read read only;'&&pieces[3]==='set local search_path = pg_catalog, public;'&&pieces[5]==='rollback;'&&canon.meta.length===0&&canon.statements.length===2&&canon.statements[0].tokens[0]==='SET'&&canon.statements[1].tokens[0]==='WITH','LOCAL_METADATA_BATCH_PARENT_REFUSED');
 const whole=original.canonicalSql.slice(canon.statements[1].start,canon.statements[1].end);check(whole===pieces[4],'LOCAL_METADATA_BATCH_PARENT_REFUSED');
 const business=between(whole,'business as (','),\n ledger as ('),branches=business.split('\nunion all\n');check(branches.length===12,'LOCAL_METADATA_BATCH_SCOPE_REFUSED');
 const audit=branches.filter(x=>x.startsWith("select 'public.audit_events' relation,count(*) row_count,"));check(audit.length===1,'LOCAL_METADATA_BATCH_AUDIT_REFUSED');
 const backgroundRelations=['private.candidate_daily_batch_receipts','public.ts_financials_outbox'];
 const backgroundBranches=backgroundRelations.map(name=>{const found=branches.filter(x=>x.startsWith(`select '${name}' relation,count(*) row_count,`));check(found.length===1,'LOCAL_METADATA_BATCH_SCOPE_REFUSED');return found[0];});
 const other=branches.filter(x=>x!==audit[0]&&!backgroundBranches.includes(x));check(other.length===9,'LOCAL_METADATA_BATCH_SCOPE_REFUSED');
 const snapshot='background_statement_snapshot as materialized (select pg_current_snapshot() snapshot_value)';check(whole.split(snapshot).length===2,'LOCAL_METADATA_BATCH_SNAPSHOT_REFUSED');
 const cut=",\n  'backgroundWriterRoutines',";check(backgroundFields.split(cut).length===2,'LOCAL_METADATA_BATCH_FIELDS_REFUSED');const at=backgroundFields.indexOf(cut),bgFields=backgroundFields.slice(0,at),writer=backgroundFields.slice(at+cut.length-"'backgroundWriterRoutines',".length);
 check(bgFields.startsWith("'backgroundObservedAt',")&&writer.startsWith("'backgroundWriterRoutines',"),'LOCAL_METADATA_BATCH_FIELDS_REFUSED');
 const typedBags="select retained.relation,retained.row_count,retained.row_fingerprint from jsonb_to_recordset(:'hr_contact_audit_bag'::jsonb || :'hr_contact_other_bag'::jsonb) as retained(relation text,row_count bigint,row_fingerprint text)";
 // These are complete arrays, not key-indexed objects. Every entry contributes
 // one native count(*) and its original rowHash to the same sorted aggregate.
 const backgroundBusiness=backgroundRelations.map(name=>`select '${name}' relation,count(*) row_count,
   encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce(string_agg(bg_entry.value->>'rowHash','|' order by bg_entry.value->>'rowHash'),''),'UTF8')),'hex') row_fingerprint
   from jsonb_array_elements((select value->'${name}' from contact_background_maps)) as bg_entry(value)`);
 const typedBusiness=[typedBags,...backgroundBusiness].join('\nunion all\n');
 const inverses=[{before:'business as ('+business+')',after:'business as ('+typedBusiness+')'},{before:', '+snapshot,after:''},{before:backgroundFields,after:writer}];let base=whole;for(const h of inverses)base=replaceOnce(base,h.before,h.after);
 const mapKey="'backgroundRows',";check(bgFields.split(mapKey).length===2,'LOCAL_METADATA_BATCH_MAPS_REFUSED');const maps=bgFields.slice(bgFields.indexOf(mapKey)+mapKey.length);
 check(maps.startsWith('jsonb_build_object(')&&maps.endsWith(')'), 'LOCAL_METADATA_BATCH_MAPS_REFUSED');
 const backgroundFieldsFromMaps=replaceOnce(bgFields,maps,'(select value from contact_background_maps)');
 const selectAnchor='\n select (identity_proof.jsonb_build_object';check(base.startsWith('with ')&&base.split(selectAnchor).length===2&&base.endsWith(';'),'LOCAL_METADATA_BATCH_FINAL_REFUSED');
 const selectAt=base.indexOf(selectAnchor),baseSelect=base.slice(selectAt+2,-1);
 const finalQuery='with '+snapshot+',\n contact_background_maps as materialized (select '+maps+' as value),\n '+base.slice(5,selectAt)+
  '\n select contact_fragments.frame from (\n select 1 as ordinal, contact_base.frame from ('+baseSelect+') as contact_base(frame)\n union all\n select 2 as ordinal, jsonb_build_object('+backgroundFieldsFromMaps+')::text as frame\n ) as contact_fragments order by contact_fragments.ordinal;';
 const bag=(leaves,name)=>`with retained_business as (${leaves.join('\nunion all\n')}), fixed_bag as (select jsonb_agg(jsonb_build_object('relation',relation,'row_count',row_count,'row_fingerprint',row_fingerprint) order by relation)::text value from retained_business)\nselect value as ${name} from fixed_bag where octet_length(value)<=16384`;
 const bags=[bag(audit,BAG_NAMES[0]),bag(other,BAG_NAMES[1])];
 // No delimiter before gset: it sends the pending query exactly once. The only
 // retained values are <=12 fixed relation/bigint/count/hash records (not maps).
 const input=pieces.slice(0,4).join('\n')+'\n'+bags.map(x=>x+'\n\\gset\n').join('')+finalQuery+'\n'+pieces[5]+'\n';
 check((input.match(/^\\gset$/gm)||[]).length===2&&!input.includes(';\n\\gset'),'LOCAL_METADATA_BATCH_GSET_REFUSED');
 return {input,canonicalSql:original.canonicalSql,originalInput:original.input,wholeQuery:whole,baseQuery:base,finalQuery,bagQueries:bags,parts:{business,branches,audit:audit[0],other,backgroundBranches,backgroundRelations,backgroundBusiness,snapshot,backgroundFields,bgFields,maps,backgroundFieldsFromMaps,writer,typedBusiness,typedBags},inverses,descriptor:{version:'CONTACT_SAME_RR_NATIVE_ARRAY_CSE_V1',referenceCanonicalSqlSha256:sha(original.canonicalSql),referenceInputSha256:sha(original.input),executedBatchInputSha256:sha(input),finalQuerySha256:sha(finalQuery),bagQuerySha256:bags.map(sha),bagNames:BAG_NAMES,bagControlBytes:16384,businessRelationRecords:12,bagRelationRecords:[1,9],backgroundBusinessRecords:2,completeBackgroundArraysMaterializedOnce:true,baseKeys:BASE_KEYS,backgroundKeys:BACKGROUND_KEYS,snapshotKeys:SNAPSHOT_KEYS,canonicalStatementExecuted:false,sourceOnly:true,nativeQualification:'NOT_PROVED_BY_BATCH_COMPILER'}};
}

// Retain complete native JSON member substrings. Values are never parsed,
// serialized, hashed, aggregated or numerically interpreted here.
function nativeMembers(text,expected){
 check(typeof text==='string'&&text[0]==='{'&&text.at(-1)==='}','LOCAL_METADATA_BATCH_FRAME_REFUSED');let i=1;const found=new Map(),keys=[];
 while(i<text.length-1){const start=i,match=/^"([A-Za-z][A-Za-z0-9]*)": /.exec(text.slice(i));check(match&&expected.includes(match[1])&&!found.has(match[1]),'LOCAL_METADATA_BATCH_KEY_REFUSED');const key=match[1];i+=match[0].length;const valueStart=i,stack=[];let quoted=false,escape=false;
  for(;i<text.length;i++){const c=text[i];if(quoted){if(escape){escape=false;continue;}if(c==='\\'){escape=true;continue;}if(c==='"')quoted=false;continue;}if(c==='"'){quoted=true;continue;}if(c==='{'||c==='['){stack.push(c);continue;}if(c==='}'||c===']'){if(stack.length===0){check(c==='}','LOCAL_METADATA_BATCH_JSON_REFUSED');break;}check(stack.pop()===(c==='}'?'{':'['),'LOCAL_METADATA_BATCH_JSON_REFUSED');continue;}if(c===','&&stack.length===0)break;}
  check(i>valueStart&&!quoted&&!escape&&stack.length===0&&text[i-1]!==' ','LOCAL_METADATA_BATCH_JSON_REFUSED');found.set(key,text.slice(start,i));keys.push(key);if(text[i]===','){check(text.slice(i,i+2)===', ','LOCAL_METADATA_BATCH_FRAME_REFUSED');i+=2;check(i<text.length-1,'LOCAL_METADATA_BATCH_FRAME_REFUSED');}else{check(text[i]==='}'&&i===text.length-1,'LOCAL_METADATA_BATCH_FRAME_REFUSED');break;}
 }
 const order=SNAPSHOT_KEYS.filter(x=>expected.includes(x));check(keys.length===expected.length&&keys.every((x,n)=>x===order[n]),'LOCAL_METADATA_BATCH_PARTITION_REFUSED');return found;
}
export function joinNativeSnapshotFrames(lines,phase){
 check(['BEFORE','AFTER'].includes(phase)&&lines.length===(phase==='AFTER'?4:3),'LOCAL_METADATA_BATCH_FRAME_COUNT_REFUSED');const base=nativeMembers(lines[1],BASE_KEYS),bg=nativeMembers(lines[2],BACKGROUND_KEYS),members=SNAPSHOT_KEYS.map(k=>base.get(k)??bg.get(k));check(members.length===21&&members.every(x=>typeof x==='string'),'LOCAL_METADATA_BATCH_PARTITION_REFUSED');
 return [lines[0],'{'+members.join(', ')+'}',...phase==='AFTER'?[lines[3]]:[]];
}
