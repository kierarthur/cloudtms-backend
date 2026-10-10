// F-23 source-only release checks. No database, fixture or runtime execution.
import fs from 'node:fs';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
import { repoRoot, inventory } from './cloudtms-db-release-lib.mjs';
import { scanManagedSql, readManagedClosure } from './cloudtms-managed-release-sql.mjs';
import { changedSourcePaths, routineDefinitions, definerInventory, verifyFinalDefiners } from './bpay-next-final-definers.mjs';

export const FROZEN_BASE='a6f7423eb71e6e7cd173e705c2290e5408a1d077';
const FOUNDATION_RECONCILED='3c91e5e6ce35b5833a5c218b77b741261eeebafa';
export const FROZEN_FILES=Object.freeze([
  'supabase/migrations/03102026_1324_banking_pay_next_core.sql',
  'supabase/repeatable/03102026_1342_banking_pay_next_guards_v1.sql',
  'supabase/repeatable/03102026_1404_banking_pay_next_command_intake_v1.sql',
  'supabase/repeatable/04102026_0555_banking_pay_next_factual_legacy_projection_boundary_v1.sql',
  'supabase/repeatable/04102026_0556_banking_pay_next_source_factual_legacy_boundary_v1.sql',
  'supabase/migrations/04102026_0504_banking_pay_next_factual_email_claim_index.sql',
  'supabase/repeatable/04102026_0558_banking_pay_next_legacy_email_claim_boundary_v1.sql',
]);
const read=file=>fs.readFileSync(path.join(repoRoot,file),'utf8');
const git=args=>{
  const r=spawnSync('git',args,{cwd:repoRoot,encoding:'utf8',maxBuffer:64*1024*1024,windowsHide:true});
  if(r.status!==0)throw Error('BPAY_NEXT_SOURCE_GIT_FAILED');
  return r.stdout;
};

// Keep executable dollar bodies and literals, mask only genuine SQL comments.
// Quoted comment markers never change scanner state; nested comments are bounded
// by the input. This is a source guard, not SQL rewriting or type inference.
export function sqlWithoutComments(source){
  let result='',i=0;
  while(i<source.length){
    const dollar=source[i]==='$'?source.slice(i).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/):null;
    if(dollar){const end=source.indexOf(dollar[0],i+dollar[0].length);if(end<0)throw Error('BPAY_NEXT_SOURCE_UNCLOSED_BODY');result+=source.slice(i,end+dollar[0].length);i=end+dollar[0].length;continue;}
    if(source[i]==="'"||source[i]==='"'){
      const quote=source[i],start=i++,escape=quote==="'"&&/[eE]/.test(source[start-1]??'')&&!/[\w$]/.test(source[start-2]??'');
      let closed=false;
      while(i<source.length){
        if(escape&&source[i]==='\\'){i+=2;continue;}
        if(source[i]===quote){if(source[i+1]===quote){i+=2;continue;}i++;closed=true;break;}i++;
      }
      if(!closed)throw Error('BPAY_NEXT_SOURCE_UNCLOSED_QUOTE');
      const part=source.slice(start,i);
      if(quote==='"'){
        const identifier=part.slice(1,-1).replaceAll('""','"');
        result+=/^[A-Za-z_][A-Za-z0-9_$]*$/.test(identifier)?identifier:`BPAY_NEXT_QUOTED_IDENTIFIER_UNCLASSIFIED ${part}`;
      }else result+=part;
      continue;
    }
    if(source.startsWith('--',i)){while(i<source.length&&source[i]!=='\n')i++;result+=' ';continue;}
    if(source.startsWith('/*',i)){
      i+=2;let depth=1;
      while(i<source.length&&depth){if(source.startsWith('/*',i)){depth++;i+=2;}else if(source.startsWith('*/',i)){depth--;i+=2;}else i++;}
      if(depth)throw Error('BPAY_NEXT_SOURCE_UNCLOSED_COMMENT');result+=' ';continue;
    }
    result+=source[i++];
  }
  return result;
}

function assertProviderDefault(code,file){
  const target='private\\s*\\.\\s*bpay_next_rail_capability\\b';
  if(!new RegExp(target,'i').test(code))return;
  for(const {text} of executableParts(code,file))for(const row of scanManagedSql(text).statements){
    const statement=text.slice(row.start,row.end);
    if(new RegExp(`alter\\s+table\\s+(?:only\\s+)?${target}[\\s\\S]*\\benabled\\s+set\\s+default\\b`,'i').test(statement)){
      const expression=statement.match(/\benabled\s+set\s+default\s+([\s\S]*?)(?:;|$)/i)?.[1].trim();
      if(!/^(?:false|\(\s*false\s*\))$/i.test(expression??''))throw Error(`BPAY_NEXT_PROVIDER_DEFAULT_UNCLASSIFIED: ${file}`);
    }
    const setClause=statement.match(/\bset\s+([\s\S]*?)(?:\bwhere\b|;|$)/i)?.[1]??'';
    const offScalar=/\benabled\s*=\s*false\s*(?=,|$)/i.test(setClause);
    if(new RegExp(`(?:update|merge\\s+into)\\s+(?:only\\s+)?${target}`,'i').test(statement)&&/\brail\s*=/i.test(setClause)&&!offScalar)throw Error(`BPAY_NEXT_PROVIDER_RAIL_MUTATION_UNCLASSIFIED: ${file}`);
    if(new RegExp(`create\\s+table[^;]*${target}`,'i').test(statement)&&/\benabled\b[^,;]*\bdefault\b/i.test(statement)){
      const expression=statement.match(/\benabled\b[^,;]*?\bdefault\s+([^,;]*)/i)?.[1].trim();
      if(!/^false\s*\)?$/i.test(expression??''))throw Error(`BPAY_NEXT_PROVIDER_ENABLE_FORBIDDEN: ${file}`);
    }
    if(new RegExp(`insert\\s+into\\s+(?:only\\s+)?${target}`,'i').test(statement)){
      const insertion=statement.match(/bpay_next_rail_capability\s*\(([^()]*)\)\s*values\s*([\s\S]*?)(?:on\s+conflict|returning|;|$)/i);
      if(!insertion)throw Error(`BPAY_NEXT_PROVIDER_INSERT_UNCLASSIFIED: ${file}`);
      const columns=insertion[1].toLowerCase().split(',').map(v=>v.trim()),rail=columns.indexOf('rail'),enabled=columns.indexOf('enabled');
      if(rail<0||enabled<0)throw Error(`BPAY_NEXT_PROVIDER_INSERT_UNCLASSIFIED: ${file}`);
      const tuples=scanManagedSql(insertion[2]).statements.flatMap(row=>{
        const values=[];let depth=0,tuple=[],field=[];
        for(const token of row.tokens){
          if(token==='('){if(depth++===0){tuple=[];field=[];continue;}}
          if(token===')'&&--depth===0){tuple.push(field.join(' '));values.push(tuple);continue;}
          if(token===','&&depth===1){tuple.push(field.join(' '));field=[];}else if(depth>0)field.push(token);
        }
        return values;
      });
      const labels=[...insertion[2].matchAll(/'((?:[^']|'')*)'/g)].map(m=>m[1]);
      // Rail is the first explicit string column in the named EX-47 seed. Other
      // layouts fail closed rather than infer row values or omitted defaults.
      if(rail!==0||!tuples.length||tuples.length!==labels.length||tuples.some((tuple,i)=>tuple.length!==columns.length
        ||!['CSV','EXTERNAL','SIMULATED','PROVIDER'].includes(labels[i])
        ||(tuple[enabled]!=='FALSE'&&!(['CSV','EXTERNAL'].includes(labels[i])&&tuple[enabled]==='TRUE'))))throw Error(`BPAY_NEXT_PROVIDER_ENABLE_FORBIDDEN: ${file}`);
      if(/on\s+conflict[^;]*\bdo\s+update\b/i.test(statement))throw Error(`BPAY_NEXT_PROVIDER_UPSERT_UNCLASSIFIED: ${file}`);
    }
    if(new RegExp(`(?:update|merge\\s+into)\\s+(?:only\\s+)?${target}`,'i').test(statement)){
      if(offScalar&&!setClause.match(/\benabled\s*=/gi)?.slice(1).length)continue;
      if(/^\s*update\b/i.test(statement)&&/\benabled\s*=\s*true\b/i.test(statement)
        &&/\bwhere\s+rail\s*=\s*'(?:CSV|EXTERNAL)'\s*;?\s*$/i.test(statement))continue;
      throw Error(`BPAY_NEXT_PROVIDER_ENABLE_FORBIDDEN: ${file}`);
    }
  }
}
export function assertSqlWriteBoundaries(source,file){
  source=source.replaceAll('\r\n','\n');
  const parts=executableParts(source,file);
  for(const part of parts){
    const text=part.unclassified??part.text;
    if(/\b(?:update|insert\s+into|delete\s+from|merge\s+into|truncate(?:\s+table)?)\s+(?:only\s+)?private\s*\.\s*bpay_next_module_control\b/i.test(text)){
      const transition=part.signature==='public.bpay_next_owner_transition_v1(jsonb)';
      const bootstrap=file===FROZEN_FILES[0]&&/^insert\s+into\s+private\.bpay_next_module_control\s*\(id\)\s*values\s*\(1\)\s*;$/i.test(text.trim());
      if(!transition&&!bootstrap)throw Error(`BPAY_NEXT_MODULE_WRITER_FORBIDDEN: ${file}`);
    }
    if(/\b(?:insert\s+into|update|delete\s+from|merge\s+into|truncate(?:\s+table)?)\s+(?:only\s+)?private\s*\.\s*bpay_next_test_database_marker\b/i.test(text))throw Error(`BPAY_NEXT_TEST_MARKER_WRITER_FORBIDDEN: ${file}`);
    if(/'SIMULATED_TEST'/i.test(text))throw Error(`BPAY_NEXT_SIMULATION_VOCABULARY_FORBIDDEN: ${file}`);
    assertProviderDefault(text,file);
  }
}

function executableParts(source,file){
  source=source.replaceAll('\r\n','\n');
  return scanManagedSql(source).statements.flatMap(row=>{
    const text=source.slice(row.start,row.end),signature=routineDefinitions(text,file)[0]??`outer:${file}`;
    const isRoutine=signature!==`outer:${file}`,isDo=row.tokens[0]==='DO';
    if(!isRoutine&&!isDo)return [{signature,text:sqlWithoutComments(text)}];
    let i=0,lastWord='';
    while(i<text.length){
      if(text.startsWith('--',i)){while(i<text.length&&text[i]!=='\n')i++;continue;}
      if(text.startsWith('/*',i)){i+=2;let depth=1;while(i<text.length&&depth){if(text.startsWith('/*',i)){depth++;i+=2;}else if(text.startsWith('*/',i)){depth--;i+=2;}else i++;}continue;}
      const tag=text[i]==='$'?text.slice(i).match(/^\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$/)?.[0]:null;
      if(tag){const start=i+tag.length,end=text.indexOf(tag,start);if(end<0)throw Error('BPAY_NEXT_SOURCE_UNCLOSED_BODY');if(lastWord==='AS'||isDo)return [{signature,text:sqlWithoutComments(text.slice(start,end))}];i=end+tag.length;continue;}
      if(text[i]==="'"||text[i]==='"'){
        const quote=text[i],start=i++,escape=quote==="'"&&lastWord==='E';let closed=false;
        while(i<text.length){if(escape&&text[i]==='\\'){i+=2;continue;}if(text[i]===quote){if(text[i+1]===quote){i+=2;continue;}i++;closed=true;break;}i++;}
        if(!closed)throw Error('BPAY_NEXT_SOURCE_UNCLOSED_QUOTE');
        if(quote==="'"&&(lastWord==='AS'||isDo))return [{signature,text:'',unclassified:text.slice(start,i)}];
        continue;
      }
      const word=text.slice(i).match(/^[A-Za-z_][A-Za-z0-9_$]*/)?.[0];
      if(word){lastWord=word.toUpperCase();i+=word.length;}else i++;
    }
    return [{signature,text:'',unclassified:text}];
  });
}
export function assertExecutableSourceBoundaries(source,file){
  // Only permanent Worker/config/release-script inputs are admitted here.
  // Examples, source tests and this gate's own forbidden tokens are excluded.
  const code=source.replace(/\/\*[\s\S]*?\*\//g,'').replace(/^\s*\/\/.*$/gm,'');
  if(/['"]SIMULATED_TEST['"]/.test(code))throw Error(`BPAY_NEXT_SIMULATION_VOCABULARY_FORBIDDEN: ${file}`);
  for(const name of ['bpay_next_module_control','bpay_next_test_database_marker']){
    if(new RegExp(`(?:update|insert\\s+into|delete\\s+from|merge\\s+into|truncate(?:\\s+table)?)\\s+(?:private\\s*\\.\\s*)?${name}\\b`,'i').test(code)
      ||new RegExp(`\\.from\\(\\s*['"](?:private\\.)?${name}['"]\\s*\\)[\\s\\S]{0,160}?\\.(?:insert|update|upsert|delete)\\s*\\(`,'i').test(code))throw Error(`BPAY_NEXT_EXECUTABLE_CONTROL_WRITER_FORBIDDEN: ${file}`);
  }
  if(/bpay_next_rail_capability/.test(code)&&/(?:enabled\s*[:=]\s*true|\.\s*(?:update|upsert|insert)\s*\()/i.test(code))throw Error(`BPAY_NEXT_EXECUTABLE_PROVIDER_DEFAULT_UNCLASSIFIED: ${file}`);
}
function operatorTokens(tokens){
  const out=[];
  for(let i=0;i<tokens.length;i++){
    const c=tokens[i],n=tokens[i+1];
    if((c==='-'||c==='#')&&n==='>'){let op=c+'>';i++;if(tokens[i+1]==='>'){op+='>';i++;}out.push(op);}
    else if((c==='|'&&n==='|')||(c===':'&&n===':')){out.push(c+n);i++;}
    else out.push(c);
  }
  return out;
}
function fixedQueryLiterals(source,file){
  const result=new Map(),assignments=new Map();
  for(const m of source.matchAll(/\b([a-z_][a-z0-9_]*)\s*:=\s*(\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$)([\s\S]*?)\2\s*;/gi)){
    const name=m[1].toLowerCase(); assignments.set(name,assignments.has(name)||!/^\s*select\b/i.test(m[3])?null:[m[3]]);
  }
  for(const [name,literals] of assignments){
    if(!literals)continue;
    const rows=scanManagedSql(source).statements;
    let valid=true;
    for(const row of rows){
      const raw=source.slice(row.start,row.end),at=raw.search(new RegExp(`\\b${name}\\s*:=`,'i'));
      if(at<0)continue;
      const assignment=raw.slice(at);
      if(new RegExp(`^${name}\\s*:=\\s*\\$(?:[A-Za-z_][A-Za-z0-9_]*)?\\$`,'i').test(assignment))continue;
      const suffix=assignment.match(new RegExp(`^${name}\\s*:=\\s*${name}\\s*\\|\\|\\s*'((?:[^']|'')*)'\\s*;\\s*$`,'i'));
      if(!suffix){valid=false;break;}literals.push(suffix[1].replaceAll("''","'"));
    }
    // Reject other assignment mechanisms; query parameters stay bound in USING.
    if(new RegExp(`\\b(?:into|fetch[^;]*into)\\s+(?:strict\\s+)?${name}\\b|\\b${name}\\s*=(?!=)`,'i').test(source))valid=false;
    if(valid){assertSpecialConstructs(literals.join(''),file);result.set(name,literals);}
  }
  return result;
}
function closedHistoryTemplates(part,file){
  if(part.signature!=='public.bpay_next_payment_history_page_v1(jsonb)')return false;
  const literal=part.text.match(/v_sections\s+constant\s+jsonb\s*:=\s*(\$\w*\$)([\s\S]*?)\1\s*(?:::jsonb\s*)?;/i);
  if(!literal)return false;
  let sections;try{sections=JSON.parse(literal[2]);}catch{return false;}
  if(!sections||Array.isArray(sections)||!Object.keys(sections).length)return false;
  for(const definition of Object.values(sections)){
    if(!/^[a-z_][a-z0-9_]*$/.test(definition.table)||!/^[a-z_][a-z0-9_]*$/.test(definition.fk)
      ||!/^([a-z_][a-z0-9_]*)(,[a-z_][a-z0-9_]*)*$/.test(definition.fields)
      ||!Array.isArray(definition.keys)||!definition.keys.length||definition.keys.some(key=>!Array.isArray(key)||key.length!==2
        ||!/^[a-z_][a-z0-9_]*$/.test(key[0])||!['uuid','bigint','integer','date','timestamptz','text'].includes(key[1])))return false;
  }
  if([...part.text.matchAll(/\bv_sections\s*:=/gi)].length||[...part.text.matchAll(/\bv_def\s*:=/gi)].length!==1
    ||!/v_def\s*:=\s*v_sections\s*->\s*v_section\s*;/i.test(part.text))return false;
  const strings=[...part.text.matchAll(/\bformat\(\s*'((?:[^']|'')*)'/gi)].map(m=>m[1].replaceAll("''","'"));
  if(!strings.length)return false;
  for(const template of strings)assertSpecialConstructs(template.replaceAll('%I','safe_identifier').replaceAll('%L',"'safe_literal'").replaceAll('%s','0'),file);
  // The established history owner has finite SQL-fragment producers. Compare
  // every producer (including the order text) with those exact reviewed shapes;
  // a new producer requires classification, never a mere helper-presence waiver.
  const sqlVariables=new Set(['V_QUERY','V_WHERE','V_FIELDS','V_ORDER','V_TUPLE','V_KEY_SQL','V_AFTER_SQL','V_UPPER_SQL']);
  const producerRows=text=>scanManagedSql(text).statements.filter(row=>{
    const tokens=operatorTokens(row.tokens);
    return tokens.some((token,i)=>(sqlVariables.has(token)||token==='V_TEXT')&&((tokens[i+1]===':'&&tokens[i+2]==='=')||tokens[i+1]==='='))
      ||tokens.some((token,i)=>token==='INTO'&&(sqlVariables.has(tokens[i+1+(tokens[i+1]==='STRICT'?1:0)])||tokens[i+1+(tokens[i+1]==='STRICT'?1:0)]==='V_TEXT'));
  }).map(row=>sqlWithoutComments(text.slice(row.start,row.end)).trim());
  const original=executableParts(git(['show',`${FOUNDATION_RECONCILED}:supabase/repeatable/06102026_1206_banking_pay_payment_history_page_v1.sql`]),file).find(row=>row.signature===part.signature);
  if(!original||JSON.stringify(producerRows(part.text))!==JSON.stringify(producerRows(original.text)))return false;
  const allowed=new Set([...sqlVariables,'V_DEF','V_KEY','V_COLUMN','V_I','FORMAT','QUOTE_LITERAL','<QUOTED>']);
  for(const row of scanManagedSql(part.text).statements){
    const tokens=operatorTokens(row.tokens),at=tokens.findIndex((token,i)=>sqlVariables.has(token)&&tokens[i+1]===':'&&tokens[i+2]==='=');
    if(at<0)continue;
    if(tokens.slice(at+3).some(token=>/^[A-Z_][A-Z0-9_]*$/.test(token)&&!allowed.has(token)))return false;
  }
  return /select\s+string_agg\(format\('t\.%I desc',value->>0\),',' order by ord\)\s+into v_text\s+from jsonb_array_elements\(v_def->'keys'\) with ordinality as k\(value,ord\);/i.test(part.text)
    &&/for v_key in select value from jsonb_array_elements\(v_def->'keys'\) loop/i.test(part.text);
}
// Return exact same-signature/declared-type/statement occurrences. Baseline
// allowances are consumed, never borrowed from another routine or whole file.
export function specialConstructViolations(source,file){
  const result=[];
  for(const part of executableParts(source,file)){
    if(part.unclassified){result.push({hazard:'BODY_UNCLASSIFIED',key:JSON.stringify([part.signature,'BODY_UNCLASSIFIED',part.unclassified])});continue;}
    const rows=scanManagedSql(part.text).statements;
    const fixedQueries=fixedQueryLiterals(part.text,file);
    const historyTemplates=closedHistoryTemplates(part,file);
    const all=rows.flatMap(row=>operatorTokens(row.tokens)),declarations=new Map(),types=new Map();
    const joined=all.join(' ');
    for(const match of joined.matchAll(/\b([A-Z_][A-Z0-9_]*)\s+((?:[A-Z_][A-Z0-9_]*\s*\.\s*)?[A-Z_][A-Z0-9_]*)\s*%\s*ROWTYPE\b/g))declarations.set(match[1],match[2]);
    const declareAt=all.indexOf('DECLARE'),beginAt=all.indexOf('BEGIN',declareAt+1);
    const localDeclarations=declareAt>=0&&beginAt>declareAt?all.slice(declareAt+1,beginAt).join(' '):'';
    for(const match of localDeclarations.matchAll(/\b([A-Z_][A-Z0-9_]*)\s+(UUID|TEXT|NUMERIC|BIGINT|INTEGER|SMALLINT|DATE|TIMESTAMPTZ)\b/g))types.set(match[1],match[2]);
    for(const row of rows){
      const t=operatorTokens(row.tokens),text=t.join(' '),hazards=new Set();
      if(t.includes('BPAY_NEXT_QUOTED_IDENTIFIER_UNCLASSIFIED'))hazards.add('QUOTED_IDENTIFIER_UNCLASSIFIED');
      const statement=part.text.slice(row.start,row.end);
      // The current migrations' owner-only policy uses one escaped identifier
      // from CURRENT_USER, a fixed private relation and fixed true predicates.
      // It cannot contain a SELECT, financial/control write or SQL special form.
      const ownerPolicy=/^\s*(?:begin\s+)?execute\s+(?:pg_catalog\.)?format\(\s*'create policy [a-z_][a-z0-9_]* on private\.[a-z_][a-z0-9_]* for all to %I using \(true\) with check \(true\)'\s*,\s*current_user\s*\)\s*;\s*$/i.test(statement);
      const dynamic=t.some((token,at)=>token==='EXECUTE'&&!['FUNCTION','PROCEDURE','ON'].includes(t[at+1]));
      const queryExecution=t.some((token,at)=>token==='EXECUTE'&&fixedQueries.has(t[at+1]?.toLowerCase())&&t[at+2]==='USING');
      const queryLiteral=t[1]===':'&&t[2]==='='&&fixedQueries.has(t[0]?.toLowerCase())&&t.includes('<BODY>');
      const schemaFormat=/execute\s+pg_catalog\.format\(\s*'(?:alter table private\.%I (?:owner to postgres|enable row level security)|revoke all on private\.%I from public,anon,authenticated,service_role|create trigger %I before (?:update or delete|truncate) on private\.%I for each (?:row|statement) execute function private\.[a-z_][a-z0-9_]*\(\))'\s*,\s*v_table(?:\s*\|\|\s*'_(?:immutable|no_truncate)'\s*,\s*v_table)?\s*\)\s*;\s*$/i.test(statement)
        &&[...part.text.matchAll(/foreach\s+v_table\s+in\s+array\s+array\[([^\]]*)\]\s+loop/gi)].length>0
        &&[...part.text.matchAll(/foreach\s+v_table\s+in\s+array\s+array\[([^\]]*)\]\s+loop/gi)].every(m=>/^\s*'[a-z_][a-z0-9_]*'(?:\s*,\s*'[a-z_][a-z0-9_]*')*\s*$/i.test(m[1]))
        &&[...part.text.matchAll(/foreach\s+v_table\s+in\s+array\b/gi)].length===[...part.text.matchAll(/foreach\s+v_table\s+in\s+array\s+array\[([^\]]*)\]\s+loop/gi)].length
        &&!/\bv_table\s*(?::=|=)|\binto\s+(?:strict\s+)?v_table\b/i.test(part.text);
      const historyExecution=historyTemplates&&(/execute\s+format\(\s*'select jsonb_build_array\(%s\) from private\.%I t where %s order by %s limit 1'\s*,\s*v_key_sql\s*,\s*v_def->>'table'\s*,\s*v_where\s*,\s*v_text\s*\)\s+into\s+v_upper\s+using/i.test(statement)
        ||/for\s+v_item\s+in\s+execute\s+v_query\s+using/i.test(statement));
      const historyConstant=historyTemplates&&/^\s*declare\s+v_sections\s+constant\s+jsonb\s*:=/i.test(statement);
      if((dynamic&&!ownerPolicy&&!schemaFormat&&!queryExecution&&!historyExecution)||(t.includes('<BODY>')&&!queryLiteral&&!historyConstant))hazards.add('DYNAMIC_SQL_UNCLASSIFIED');
      if(/\bPG_CATALOG \. (?:COALESCE|NULLIF|LEAST|GREATEST) \(/.test(text))hazards.add('SPECIAL_QUALIFIER');
      const intoAt=t.indexOf('INTO'),selectAt=intoAt>=0?t.lastIndexOf('SELECT',intoAt):t.indexOf('SELECT');
      if(selectAt>=0&&intoAt>selectAt){
        const targetAt=intoAt+(t[intoAt+1]==='STRICT'?2:1),target=t[targetAt];
        if(declarations.has(target)&&t[targetAt+1]!=='.'){
          const projection=t.slice(selectAt+1,intoAt);
          const expected=declarations.get(target).split(/\s+/);
          const binding=t.findIndex((token,at)=>at>intoAt&&['FROM','JOIN'].includes(token)
            &&t.slice(at+1,at+1+expected.length).join(' ')===expected.join(' ')
            &&t[at+1+expected.length+(t[at+1+expected.length]==='AS'?1:0)]===projection[0]);
          if(projection.length!==3||!/^\w+$/.test(projection[0])||projection[1]!=='.'||projection[2]!=='*'
             ||binding<0)hazards.add(`ROWTYPE_SELECT:${target}:${declarations.get(target)}`);
        }
      }
      const frames=[{concat:false,extract:false}];
      for(let i=0;i<t.length;i++){
        const token=t[i];
        if(token==='('){frames.push({concat:false,extract:false});continue;}
        if(token===')'){if(frames.length>1)frames.pop();continue;}
        const frame=frames.at(-1);
        if([',','=','AND','OR','THEN','ELSE','WHEN','FROM','WHERE','SET','RETURN','IF','LOOP','IS','IN'].includes(token)){frame.concat=false;frame.extract=false;continue;}
        if(token==='||')frame.concat=true;
        if(['->','->>','#>','#>>'].includes(token))frame.extract=true;
        if(frame.concat&&frame.extract)hazards.add('JSON_CONCAT');
        if(['MIN','MAX'].includes(token)&&t[i+1]==='('){
          let end=i+2,depth=1;
          for(;end<t.length;end++){if(t[end]==='(')depth++;if(t[end]===')'&&--depth===0)break;}
          const argument=t.slice(i+2,end);
          // Explicit scalar casts are finite typed evidence. An unresolved new
          // aggregate is refused for source review instead of guessing *_id.
          const cast=argument.indexOf('::'),type=cast>=0?argument.slice(cast+1).join(' ')
            :argument.length===1?(types.get(argument[0])??(/^\d+$/.test(argument[0])?'NUMERIC':null)):null;
          if(type==='UUID')hazards.add('UUID_AGGREGATE');
          else if(!['TEXT','NUMERIC','BIGINT','INTEGER','SMALLINT','DATE','TIMESTAMPTZ'].includes(type))hazards.add('AGGREGATE_TYPE_UNCLASSIFIED');
          else if(t[end+1]==='::'&&t[end+2]==='UUID')hazards.add('UUID_CARDINALITY_UNCLASSIFIED');
        }
      }
      const raw=part.text.slice(row.start,row.end),at=selectAt>=0?raw.search(/\bselect\b/i):-1;
      for(const hazard of hazards)result.push({hazard,key:JSON.stringify([part.signature,hazard,(hazard.startsWith('ROWTYPE_SELECT:')&&at>=0?raw.slice(at):raw).replaceAll('\r\n','\n').trim()])});
    }
  }
  return result;
}
export function assertSpecialConstructs(source,file,baseline=new Map()){
  const remaining=new Map(baseline),failures=[];
  for(const row of specialConstructViolations(source,file)){
    const count=remaining.get(row.key)??0;
    if(!count){failures.push(`BPAY_NEXT_SQL_${row.hazard}: ${file} (${JSON.parse(row.key)[0]})`);continue;}
    remaining.set(row.key,count-1);
  }
  if(failures.length)throw Error(failures.join('\n'));
}
export function assertRpcReload(source,file){
  source=source.replaceAll('\r\n','\n');
  const rows=scanManagedSql(source).statements;
  let lastAuthority=-1,lastReload=-1;
  for(let i=0;i<rows.length;i++){
    const row=rows[i],text=source.slice(row.start,row.end);
    if(routineDefinitions(text,file).some(identity=>identity.startsWith('public.'))||['GRANT','REVOKE'].includes(row.tokens[0]))lastAuthority=i;
    if(row.tokens[0]==='NOTIFY'&&/^notify\s+pgrst\s*,\s*'reload schema'\s*;$/i.test(sqlWithoutComments(text).trim()))lastReload=i;
  }
  if(lastAuthority<0||lastReload<=lastAuthority||rows.slice(lastReload+1).some(row=>row.tokens.join(' ')!=='COMMIT'))throw Error(`BPAY_NEXT_RPC_RELOAD_MISSING: ${file}`);
}
function frozenAuthorityPolicy(sources){
  const triggers=new Map(),tables=new Set(),callbacks=new Set(),routines=new Set();
  for(const [file,source] of sources){
    for(const identity of routineDefinitions(source,file))routines.add(identity.split('(')[0]);
    const code=sqlWithoutComments(source);
    for(const m of code.matchAll(/create\s+(?:constraint\s+)?trigger\s+(\w+)\s+[\s\S]*?\bon\s+((?:private|public)\s*\.\s*\w+)\b[\s\S]*?execute\s+(?:function|procedure)\s+((?:private|public)\s*\.\s*\w+)\s*\(/gi)){
      const relation=m[2].replace(/\s+/g,'').toLowerCase(),name=m[1].toLowerCase(),callback=m[3].replace(/\s+/g,'').toLowerCase();
      triggers.set(`${relation}:${name}`,callback);callbacks.add(callback);
    }
    for(const m of code.matchAll(/create\s+table\s+(?:if\s+not\s+exists\s+)?((?:private|public)\s*\.\s*\w+)\b/gi))tables.add(m[1].replace(/\s+/g,'').toLowerCase());
  }
  if(triggers.size!==18||tables.size!==45)throw Error('BPAY_NEXT_FROZEN_POLICY_INVENTORY_CHANGED');
  return {triggers,tables,callbacks,routines};
}
export function assertFrozenAuthority(source,file,policy,baseline=''){
  source=source.replaceAll('\r\n','\n');baseline=baseline.replaceAll('\r\n','\n');
  for(const part of executableParts(source,file))for(const row of scanManagedSql(part.text).statements){
    const statement=part.text.slice(row.start,row.end);
    const authorityAt=row.tokens.findIndex(token=>['CREATE','DROP','ALTER','GRANT','REVOKE'].includes(token));
    const original=sqlWithoutComments(statement).toLowerCase();
    const code=authorityAt>=0?row.tokens.slice(authorityAt).join(' ').toLowerCase():original,flat=code.replace(/\s*\.\s*/g,'.');
    let competing=false;
    if(/\bexecute\s+(?:pg_catalog\.)?format\(/i.test(original)){
      const arrays=[...part.text.matchAll(/foreach\s+v_table\s+in\s+array\s+array\[([^\]]*)\]\s+loop/gi)].map(m=>m[1]);
      const names=arrays.flatMap(array=>[...array.matchAll(/'([a-z_][a-z0-9_]*)'/gi)].map(m=>`private.${m[1].toLowerCase()}`));
      if(names.some(name=>policy.tables.has(name))||[...policy.callbacks].some(callback=>flat.includes(callback)))competing=true;
      const fixedPolicy=flat.match(/create policy \w+ on ((?:private|public)\.\w+)/)?.[1];
      if(policy.tables.has(fixedPolicy))competing=true;
    }
    if(/^(?:create|drop|alter)\s+(?:(?:or\s+replace|constraint)\s+)?trigger\b/.test(code)){
      const relation=flat.match(/\bon\s+((?:private|public)\.\w+)/)?.[1],name=code.match(/\btrigger\s+(?:if\s+exists\s+)?(\w+)/)?.[1];
      competing=policy.triggers.has(`${relation}:${name}`)||[...policy.callbacks].some(callback=>flat.includes(callback));
      // F05/B1.4 explicitly require these four immutable capture attachments,
      // reusing the unchanged generic immutable callback. They replace none of
      // the original eighteen guard bindings; exact shape is required here.
      const capture=flat.match(/^create trigger (bpay_next_run_(?:work|line)_frozen_(?:immutable|no_truncate)_v1) before (update or delete|truncate) on (private\.bpay_next_run_(?:work|line)) for each (row|statement) execute function private\.bpay_next_effect_immutable_v1\s*\(\s*\)\s*;?\s*$/);
      if(capture){const [,,events,relation,level]=capture,kind=relation.split('_').at(-1),tail=events==='truncate'?'no_truncate':'immutable';
        if(capture[1]===`bpay_next_run_${kind}_frozen_${tail}_v1`&&level===(events==='truncate'?'statement':'row'))competing=false;
      }
    }
    // Rule-B snapshot's exact two additive guards reuse the unchanged callback.
    if(/^create trigger bpay_next_run_work_query_snapshot_immutable before update or delete on private\.bpay_next_run_work_query_snapshot for each row execute function private\.bpay_next_effect_immutable_v1\s*\(\s*\)\s*;?\s*$/.test(flat)
      ||/^create trigger bpay_next_run_work_query_snapshot_no_truncate before truncate on private\.bpay_next_run_work_query_snapshot for each statement execute function private\.bpay_next_effect_immutable_v1\s*\(\s*\)\s*;?\s*$/.test(flat))competing=false;
    if(/^alter\s+table\b/.test(code)){
      const relation=flat.match(/^alter\s+table\s+(?:if\s+exists\s+)?(?:only\s+)?((?:private|public)\.\w+)/)?.[1];
      const hasTriggers=[...policy.triggers.keys()].some(key=>key.startsWith(`${relation}:`));
      const triggerChanges=[...code.matchAll(/\b(?:disable|enable(?:\s+always|\s+replica)?)\s+trigger\s+(\w+)/g)];
      competing=hasTriggers&&triggerChanges.some(m=>['all','user'].includes(m[1])||policy.triggers.has(`${relation}:${m[1]}`));
      if((policy.tables.has(relation)||hasTriggers)&&/\b(?:rename\s+to|set\s+schema)\b/.test(code))competing=true;
      if(policy.tables.has(relation)&&/\b(?:owner\s+to|disable\s+row\s+level\s+security|no\s+force\s+row\s+level\s+security)\b/.test(code))competing=true;
    }
    const protectedTables=new Set([...policy.tables,...[...policy.triggers.keys()].map(key=>key.split(':')[0])]);
    if(/^create\s+table\b/.test(code)){
      const relation=flat.match(/^create\s+table\s+(?:if\s+not\s+exists\s+)?((?:private|public)\.\w+)\b/)?.[1];
      if(protectedTables.has(relation))competing=true;
    }
    if(/^drop\s+table\b/.test(code)&&[...flat.matchAll(/\b(?:private|public)\.\w+\b/g)].some(m=>protectedTables.has(m[0])))competing=true;
    if(/^(?:alter|drop|grant|revoke)\b/.test(code)&&/\b(?:function|procedure|routine)\b/.test(code)&&[...policy.routines].some(name=>flat.includes(name+'(')||flat.includes(name+' (')))competing=true;
    if(/^drop\s+schema\s+(?:if\s+exists\s+)?(?:private|public)\b/.test(code)||/^alter\s+schema\s+(?:private|public)\s+rename\b/.test(code))competing=true;
    if(competing&&!baseline.includes(statement))throw Error(`BPAY_NEXT_FROZEN_COMPETING_AUTHORITY: ${file}`);
  }
}

export function verifyBpayNextSourceGates(){
  verifyFinalDefiners();
  git(['merge-base','--is-ancestor',FROZEN_BASE,'HEAD']);
  const current=inventory(),changed=changedSourcePaths();
  const {routines,migrationRoots}=definerInventory(current),protectedSignatures=new Map(),frozenSources=new Map();
  for(const file of FROZEN_FILES){
    const base=spawnSync('git',['show',`${FROZEN_BASE}:${file}`],{cwd:repoRoot,maxBuffer:64*1024*1024,windowsHide:true});
    if(base.status!==0||!fs.readFileSync(path.join(repoRoot,file)).equals(base.stdout))throw Error(`BPAY_NEXT_FROZEN_BYTES_CHANGED: ${file}`);
    frozenSources.set(file,base.stdout.toString('utf8'));
    for(const signature of routineDefinitions(base.stdout.toString('utf8'),file))protectedSignatures.set(signature,file);
  }
  for(const [signature,file] of protectedSignatures){
    const definitions=[...(routines.get(signature)??[]),...migrationRoots.flatMap(root=>root.definitions.filter(row=>row.signature===signature))];
    if(!definitions.length)throw Error(`BPAY_NEXT_FROZEN_AUTHORITY_MISSING: ${signature}`);
    for(const definition of definitions.filter(row=>row.physical!==file)){
      const previous=spawnSync('git',['show',`${FROZEN_BASE}:${definition.physical}`],{cwd:repoRoot,encoding:'utf8',maxBuffer:64*1024*1024,windowsHide:true});
      const currentSource=read(definition.physical).replaceAll('\r\n','\n');
      const exact=scanManagedSql(currentSource).statements.filter(row=>routineDefinitions(currentSource.slice(row.start,row.end),definition.physical).includes(signature)).map(row=>currentSource.slice(row.start,row.end).replaceAll('\r\n','\n'));
      if(previous.status!==0||exact.some(text=>!previous.stdout.replaceAll('\r\n','\n').includes(text)))throw Error(`BPAY_NEXT_FROZEN_COMPETING_DEFINER: ${signature}`);
    }
  }
  const sources=new Set([...current.migrations,...current.repeatables].flatMap(row=>row.paths??[row.path]));
  const productFiles=git(['ls-files','--cached','--others','--exclude-standard']).split(/\r?\n/).filter(Boolean);
  const baselineHazards=new Map();
  const authorityPolicy=frozenAuthorityPolicy(frozenSources),baseSources=new Map();
  const baselineFiles=git(['ls-tree','-r','--name-only',FROZEN_BASE,'--','supabase/migrations','supabase/repeatable','supabase/verification']).split(/\r?\n/).filter(file=>/\.sql$/i.test(file));
  const batch=spawnSync('git',['cat-file','--batch'],{cwd:repoRoot,input:baselineFiles.map(file=>`${FROZEN_BASE}:${file}\n`).join(''),maxBuffer:128*1024*1024,windowsHide:true});
  if(batch.status!==0)throw Error('BPAY_NEXT_SOURCE_BASE_BATCH_FAILED');
  let offset=0;
  for(const file of baselineFiles){
    const end=batch.stdout.indexOf(10,offset),header=batch.stdout.subarray(offset,end).toString('ascii'),match=header.match(/^[0-9a-f]{40} blob (\d+)$/);
    if(end<0||!match)throw Error('BPAY_NEXT_SOURCE_BASE_BLOB_INVALID');
    const size=Number(match[1]),start=end+1;
    if(!Number.isSafeInteger(size)||start+size>=batch.stdout.length||batch.stdout[start+size]!==10)throw Error('BPAY_NEXT_SOURCE_BASE_BLOB_TRUNCATED');
    const base=batch.stdout.subarray(start,start+size).toString('utf8');offset=start+size+1;baseSources.set(file,base);
    const counts=new Map();
    for(const row of specialConstructViolations(base,file))counts.set(row.key,(counts.get(row.key)??0)+1);
    for(const [key,count]of counts)baselineHazards.set(key,Math.max(baselineHazards.get(key)??0,count));
  }
  if(offset!==batch.stdout.length)throw Error('BPAY_NEXT_SOURCE_BASE_BATCH_TRAILING');
  for(const file of productFiles)if(/\.sql$/i.test(file)&&/^supabase\/(?:seed|seeds|verification)\//.test(file))sources.add(file);
  // Installed historical F05 definitions stay immutable. These four exact
  // committed row reads are retained only after their final successor passes.
  const projectionFile='supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql';
  const correctionSignatures=new Set(["private.bpay_next_bind_simple_csv_destination_v1(uuid)","private.bpay_next_issue_simple_csv_v1(uuid,uuid,uuid)","private.bpay_next_receive_simple_reissue_v1(uuid,uuid,uuid)","private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid)"]);
  git(['merge-base','--is-ancestor',FOUNDATION_RECONCILED,'HEAD']);
  const retainedProjection=git(['show',FOUNDATION_RECONCILED+':'+projectionFile]);
  if(read(projectionFile).replaceAll('\r\n','\n')!==retainedProjection.replaceAll('\r\n','\n'))throw Error('BPAY_NEXT_RETAINED_PROJECTION_CHANGED');
  const projectionHazards=new Map(baselineHazards);
  for(const signature of correctionSignatures){
    const final=routines.get(signature)?.at(-1);
    if(!final||final.physical===projectionFile)throw Error('BPAY_NEXT_ROW_PROJECTION_SUCCESSOR_MISSING');
    const currentSource=read(final.physical).replaceAll('\r\n','\n');
    const finalStatement=scanManagedSql(currentSource).statements.find(row=>routineDefinitions(currentSource.slice(row.start,row.end),final.physical).includes(signature));
    if(!finalStatement)throw Error('BPAY_NEXT_ROW_PROJECTION_SUCCESSOR_MISSING');
    assertSpecialConstructs(currentSource.slice(finalStatement.start,finalStatement.end),final.physical,baselineHazards);
  }
  for(const hazard of specialConstructViolations(retainedProjection,projectionFile))if(correctionSignatures.has(JSON.parse(hazard.key)[0])&&hazard.hazard.startsWith('ROWTYPE_SELECT:'))projectionHazards.set(hazard.key,1);
  const sourceFailures=[];
  for(const file of sources){
    const source=read(file);assertSqlWriteBoundaries(source,file);
    if(changed.has(file)){
      try{assertSpecialConstructs(source,file,file===projectionFile?projectionHazards:baselineHazards);assertFrozenAuthority(source,file,authorityPolicy,baseSources.get(file)??'');}
      catch(error){sourceFailures.push(error.message);}
    }
  }
  if(sourceFailures.length)throw Error(sourceFailures.join('\n'));
  for(const file of productFiles.filter(file=>/^(?:broker\/|candidate-(?:broker|private-api|synthetic-private-api)\/|weekly-source-delivery-worker\/|invoice-(?:queue-dispatcher|document-processor)\/|scripts\/|wrangler\.)/.test(file)
    &&/\.(?:[cm]?js|ts|jsonc?|toml)$/.test(file)&&file!=='scripts/bpay-next-source-gates.mjs'))assertExecutableSourceBoundaries(read(file),file);
  const rpcRoots=new Set();
  for(const [signature,definitions] of routines){
    if(!signature.startsWith('public.')||!definitions.some(row=>changed.has(row.physical)))continue;
    rpcRoots.add(definitions.at(-1).root);
  }
  for(const root of migrationRoots.filter(root=>root.includes.some(file=>changed.has(file))))for(const definition of root.definitions){
    if(!definition.signature.startsWith('public.'))continue;
    const final=routines.get(definition.signature)?.at(-1);
    if(!final)throw Error(`BPAY_NEXT_RPC_FINAL_REPEATABLE_MISSING: ${definition.signature}`);
    rpcRoots.add(final.root);
  }
  for(const file of rpcRoots)assertRpcReload(readManagedClosure(file,repoRoot).expanded,file);
  return {frozenFiles:FROZEN_FILES.length,frozenTriggers:authorityPolicy.triggers.size,frozenTables:authorityPolicy.tables.size,protectedSignatures:protectedSignatures.size,sqlSources:sources.size,rpcRoots:rpcRoots.size};
}
