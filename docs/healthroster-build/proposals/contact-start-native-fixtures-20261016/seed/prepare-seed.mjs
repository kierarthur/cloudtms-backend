// Offline TEST artifact preparation; never opens PostgreSQL/R2 or sends a message.
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import zlib from 'node:zlib';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {execFileSync} from 'node:child_process';
const argv=process.argv.slice(2);
const opt=Object.fromEntries(argv.map(x=>{const m=x.match(/^--([^=]+)=(.+)$/);if(!m)throw Error('Use --root=PATH --as-of=YYYY-MM-DD');return [m[1],m[2]];}));
if(!opt.root || !/^\d{4}-\d{2}-\d{2}$/.test(opt['as-of']??'')) throw Error('Explicit root and as-of date required');
const root=path.resolve(opt.root),out=opt.out?path.resolve(opt.out):path.dirname(fileURLToPath(import.meta.url));
fs.mkdirSync(out,{recursive:true});
const day=new Date(opt['as-of']+'T12:00:00Z');if(day.toISOString().slice(0,10)!==opt['as-of'])throw Error('Invalid date');
// Last completed Sunday, including the prior Sunday when run on Sunday itself.
const week=new Date(day);week.setUTCDate(week.getUTCDate()-(week.getUTCDay()||7));
const iso=d=>d.toISOString().slice(0,10),add=(d,n)=>{const x=new Date(d);x.setUTCDate(x.getUTCDate()+n);return x;};
const weekEnd=iso(week),weekStart=iso(add(week,-6)),workDate=iso(add(week,-5));
if(day.getUTCDay()===0)throw Error('Sunday native open-cycle targets current week: regenerate on Monday-Saturday');
const id=n=>'f716c000-0000-4000-8000-'+String(n).padStart(12,'0');
const agency='f7160000-0000-4000-8000-000000000019';
const profile='ROSTER_WEEKLY_SUMMARY_ACTUAL_V1';
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const write=(n,b)=>fs.writeFileSync(path.join(out,n),b);
const load=async rel=>import(pathToFileURL(path.join(root,rel)).href);
const {parseWeeklySourceFile}=await load('broker/src/weekly-source/index.js');
const {adaptWeeklySourceParserOutput}=await load('broker/src/weekly-source/upload-staging-adapter.mjs');
const {buildWeeklySourceProjectionRows}=await load('broker/src/weekly-source/upload-publication-owner.mjs');
const {ROSTER_WEEKLY_SUMMARY_ACTUAL_V1_HEADERS:headers}=await load('broker/src/weekly-source/profiles.js');
const rows=[headers,...['B','C'].map(worker=>{
 const values={'Candidate':'Contact Worker '+worker,'Candidate Id':'CONTACT-TEST-'+worker,
  'Business Unit ID':id(20),'business_unit_name':'Contact Native Fixture Trust','Line ID':'CONTACT-ZERO-'+worker,
  'Booking Start':workDate+' 09:00:00','Booking End':workDate+' 17:00:00','Total Hours':'0'};
 return headers.map(header=>values[header]??'');
})];
const csv=s=>'"'+String(s).replaceAll('"','""')+'"';
const bytes=Buffer.from(rows.map(row=>row.map(csv).join(',')).join('\r\n')+'\r\n');
write('zero-report.csv',bytes);
const parsed=await parseWeeklySourceFile(bytes,{profileId:profile,expectedClient:'Contact Native Fixture Trust'});
if(!parsed.ok)throw Error(JSON.stringify(parsed.fatalErrors));
const produced=adaptWeeklySourceParserOutput(parsed,{actor_user_id:id(3),original_filename:'contact-native-zero.csv',import_use:'CHECKING',
 coverage:{start_local_date:weekStart,end_local_date:weekEnd,confirmed_at_utc:opt['confirmed-at']??new Date().toISOString()}},
 {environment:'TEST',agency_id:agency,source_group_id:id(1),source_cycle_id:null,client_id:id(20),client_name:'Contact Native Fixture Trust'});
if(produced.normalisedRows.length!==2 || produced.moneyEvidence.length || produced.expenseEvidence.length!==2 ||
 produced.normalisedRows.some(r=>r.row_finalisation_state!=='SOURCE_ABSENT_ZERO'||r.actual_net_minutes!==0||r.source_expense_parse_state!=='OMITTED_ZERO'))throw Error('Explicit-zero nonfinancial nonfinancial producer invariant');
// Synthetic EXPECTED context facts, matched against the actual native BUILD_PROJECTION
// return in 12_native-publication.sql before the real native row IDs/policy hashes bind.
// This is not a pretend native context result or mocked RPC. No calculated finance exists.
const expected={profile_id:profile,authority_scope_kind:'CYCLE',rows:produced.normalisedRows.map((r,i)=>({
 source_row_ordinal:r.source_row_ordinal,external_source_key:r.external_source_key,work_date:r.work_date,
 start_at_local:r.start_at_local,end_at_local:r.end_at_local,break_minutes:r.break_minutes,actual_net_minutes:0,row_finalisation_state:r.row_finalisation_state,
 candidate_match_count:1,candidate_id:id(11+i),client_id:id(20),prior_work_event_id:null,
 prior_accepted_contract_id:null,protected_matches:[],contracts:[{contract_id:id(31+i),candidate_id:id(11+i),client_id:id(20),
 valid_from:iso(add(week,-60)),valid_to:iso(add(week,60)),weekly_source_applicable:true,
 schedule_compatible:true,verified_role_band_match:false,effective_policy:{authority_mode:'SOURCE_AUTHORITY',c1_source_mode:'HEALTHROSTER_WEEKLY'}}]
}))};
const projection=buildWeeklySourceProjectionRows(expected);
if(projection.some(r=>r.mapping_state!=='RESOLVED'||r.contract_selection_method!=='AUTO_UNIQUE'||r.link_kind!=='ZERO_SOURCE'||'economic_snapshot' in r))throw Error('Actual projection producer refused fixture facts');
// A valid tiny PNG spelling TEST; synthetic declaration evidence, not a real signature.
const glyphs=['11111010001000100010001000100010','11111010000100001111010000111110','01111010000100000111000001111100','11111000100001000010000100001000'];
const width=24,height=8,raw=Buffer.alloc(height*(1+width*4),255);
for(let y=0;y<height;y++){raw[y*(1+width*4)]=0;for(let x=0;x<width;x++){const g=Math.floor(x/6),col=x%6;const black=col<5&&y<6&&glyphs[g]?.[y*5+col]==='1';const o=y*(1+width*4)+1+x*4;raw[o]=raw[o+1]=raw[o+2]=black?0:255;}}
const crc=b=>{let c=0xffffffff;for(const v of b){c^=v;for(let i=0;i<8;i++)c=(c>>>1)^((c&1)?0xedb88320:0);}return (c^0xffffffff)>>>0;};
const chunk=(t,d)=>{const tb=Buffer.from(t),b=Buffer.alloc(d.length+12);b.writeUInt32BE(d.length);tb.copy(b,4);d.copy(b,8);b.writeUInt32BE(crc(Buffer.concat([tb,d])),8+d.length);return b;};
const ihdr=Buffer.alloc(13);ihdr.writeUInt32BE(width);ihdr.writeUInt32BE(height,4);ihdr[8]=8;ihdr[9]=6;
const png=Buffer.concat([Buffer.from('89504e470d0a1a0a','hex'),chunk('IHDR',ihdr),chunk('IDAT',zlib.deflateSync(raw)),chunk('IEND',Buffer.alloc(0))]);
write('test-declaration.png',png);
const artifact={version:'CONTACT_NATIVE_SOURCE_SEED_ARTIFACT_V1',as_of:opt['as-of'],week_start:weekStart,week_ending:weekEnd,work_date:workDate,
 contract_start:iso(add(week,-60)),contract_end:iso(add(week,60)),root_commit:execFileSync('git',['rev-parse',opt['root-commit']??'HEAD'],{cwd:root,encoding:'utf8'}).trim(),
 synthetic_facts:true,native_execution:'NOT_RUN',report:{sha256:sha(bytes),byte_count:bytes.length,profile_id:profile},
 signature:{sha256:sha(png),byte_count:png.length,width,height,storage_key:'contact-native-fixture/TEST-declaration.png',object_uploaded:false},
 begin_request:produced.beginRequest,stage_request:{physical_rows:produced.physicalRows,normalised_rows:produced.normalisedRows,money_evidence:produced.moneyEvidence,expense_evidence:produced.expenseEvidence},
 expected_context:expected,projection_templates:projection.map((r,i)=>({...r,source_row_ordinal:produced.normalisedRows[i].source_row_ordinal}))};
write('ARTIFACT.json',JSON.stringify(artifact,null,2)+'\n');
const literal=JSON.stringify(artifact).replaceAll("'","''");
write('10_generated-artifact.sql',`-- Generated through actual parser/adapter/projection exports. Synthetic fixture facts only.\nCREATE TEMP TABLE contact_native_seed_artifact(value jsonb NOT NULL) ON COMMIT DROP;\nINSERT INTO pg_temp.contact_native_seed_artifact VALUES ('${literal}'::jsonb);\n`);
console.log(JSON.stringify({artifact_version:artifact.version,as_of:artifact.as_of,report_sha256:sha(bytes),report_rows:2,signature_sha256:sha(png),native:'NOT_RUN'}));
