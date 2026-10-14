// Source evidence only. No SQL evaluation, DB calls or inferred dynamic DDL.
import assert from 'node:assert/strict';

const wordStart=c=>/[A-Za-z_]/.test(c||''),wordPart=c=>/[A-Za-z_0-9$]/.test(c||'');
function quotedEnd(text,start,quote){let i=start+1;const escaped=quote==="'"&&/[eE]/.test(text[start-1]||'')&&!wordPart(text[start-2]);
 while(i<text.length){if(escaped&&text[i]==='\\'){i+=2;continue;}if(text[i]===quote){if(text[i+1]===quote){i+=2;continue;}return i+1;}i++;}
 throw new Error('Unclosed quoted SQL at '+start);}
function dollarEnd(text,start){const tag=text.slice(start).match(/^\$(?:[A-Za-z_]\w*)?\$/)?.[0];if(!tag)return null;
 const end=text.indexOf(tag,start+tag.length);assert.ok(end>=0,'Unclosed dollar quote at '+start);return {end:end+tag.length,tag};}
function commentEnd(text,start){if(text.startsWith('--',start)){const n=text.indexOf('\n',start);return n<0?text.length:n;}
 let depth=1,i=start+2;while(i<text.length){if(text.startsWith('/*',i)){depth++;i+=2;}else if(text.startsWith('*/',i)){depth--;i+=2;if(!depth)return i;}else i++;}
 throw new Error('Unclosed SQL block comment at '+start);}

export function stripSqlComments(text){let out='',i=0;
 while(i<text.length){if(text.startsWith('--',i)||text.startsWith('/*',i)){const e=commentEnd(text,i);out+=text.slice(i,e).replace(/[^\r\n]/g,' ');i=e;continue;}
  if(text[i]==="'"||text[i]==='"'){const e=quotedEnd(text,i,text[i]);out+=text.slice(i,e);i=e;continue;}
  if(text[i]==='$'){const d=dollarEnd(text,i);if(d){out+=text.slice(i,d.end);i=d.end;continue;}}
  out+=text[i++];}return out;}

// Semicolons inside comments, strings, quoted identifiers and dollar bodies
// never terminate a statement. psql control lines are recorded separately.
export function splitSqlStatements(text){const statements=[],meta=[];let start=0,i=0,lineStart=0;
 while(i<text.length){if(text[i]==='\\'&&/^\s*$/.test(text.slice(lineStart,i))){const e=text.indexOf('\n',i),end=e<0?text.length:e;
   meta.push({offset:i,text:text.slice(i,end)});if(text.slice(start,i).trim()==='')start=end;i=end;continue;}
  if(text.startsWith('--',i)||text.startsWith('/*',i)){i=commentEnd(text,i);continue;}
  if(text[i]==="'"||text[i]==='"'){i=quotedEnd(text,i,text[i]);continue;}
  if(text[i]==='$'){const d=dollarEnd(text,i);if(d){i=d.end;continue;}}
  if(text[i]===';'){statements.push({offset:start,text:text.slice(start,i+1)});start=i+1;}
  if(text[i]==='\n')lineStart=i+1;i++;}
 if(text.slice(start).trim())statements.push({offset:start,text:text.slice(start)});return {statements,meta};}

export function sqlTokens(text){const tokens=[];let i=0;
 while(i<text.length){if(/\s/.test(text[i])){i++;continue;}
  if(text[i]==='\\'&&/^\s*$/.test(text.slice(text.lastIndexOf('\n',i-1)+1,i))){const end=text.indexOf('\n',i);i=end<0?text.length:end;continue;}
  if(text.startsWith('--',i)||text.startsWith('/*',i)){i=commentEnd(text,i);continue;}
  const start=i;
  if(text[i]==="'"||text[i]==='"'){const quote=text[i],e=quotedEnd(text,i,quote);tokens.push({kind:quote==='"'?'identifier':'string',value:text.slice(i+1,e-1).replaceAll(quote+quote,quote),raw:text.slice(i,e),start,end:e});i=e;continue;}
  if(text[i]==='$'){const d=dollarEnd(text,i);if(d){tokens.push({kind:'dollar',value:text.slice(i+d.tag.length,d.end-d.tag.length),raw:text.slice(i,d.end),start,end:d.end});i=d.end;continue;}}
  if(wordStart(text[i])){i++;while(wordPart(text[i]))i++;tokens.push({kind:'word',value:text.slice(start,i).toLowerCase(),raw:text.slice(start,i),start,end:i});continue;}
  tokens.push({kind:'punctuation',value:text[i],raw:text[i],start,end:++i});}return tokens;}

const builtinAliases=new Map(Object.entries({int:'integer',int4:'integer',integer:'integer',int2:'smallint',smallint:'smallint',int8:'bigint',bigint:'bigint',
 bool:'boolean',boolean:'boolean',timestamptz:'timestamp with time zone',timestamp:'timestamp without time zone','timestamp with time zone':'timestamp with time zone',
 'timestamp without time zone':'timestamp without time zone',timetz:'time with time zone',time:'time without time zone','time with time zone':'time with time zone',
 'time without time zone':'time without time zone',float8:'double precision','double precision':'double precision',float4:'real',real:'real',decimal:'numeric',numeric:'numeric',
 varchar:'character varying','character varying':'character varying',char:'character',character:'character',varbit:'bit varying','bit varying':'bit varying',
 uuid:'uuid',text:'text',json:'json',jsonb:'jsonb',date:'date',bytea:'bytea',record:'record',void:'void',interval:'interval',inet:'inet',name:'name',oid:'oid',regclass:'regclass',
 anyelement:'anyelement',anyarray:'anyarray',anycompatible:'anycompatible',anycompatiblearray:'anycompatiblearray',cstring:'cstring',trigger:'trigger',internal:'internal',bit:'bit'}));
function removeTypmod(tokens){const out=[];let depth=0;for(const t of tokens){if(t.value==='('){depth++;continue;}if(t.value===')'){depth--;continue;}if(!depth)out.push(t);}return out;}
function identityToken(t){return t.kind==='identifier'&&t.value===t.value.toLowerCase()&&/^[a-z_]\w*$/.test(t.value)?t.value:t.kind==='word'?t.value:t.raw;}
function typeText(tokens){let s='';for(const t of tokens){if(t.value==='.'||t.value==='['||t.value===']'||t.value==='%')s=s.trimEnd()+t.raw;
 else if(s.endsWith('.')||s.endsWith('[')||s.endsWith('%'))s+=identityToken(t);else s+=(s?' ':'')+identityToken(t);}return s.trim();}
export function canonicalType(tokens,context={}){const core=removeTypmod(tokens),isArray=core.some(t=>t.value==='['),base=core.filter(t=>t.value!=='['&&t.value!==']');
 let value=typeText(base);const lower=value.toLowerCase();if(!value.includes('"')){const simple=lower.replace(/^pg_catalog\./,'');
  if(builtinAliases.has(simple))value=builtinAliases.get(simple);
  else if(context.types?.has(lower))value=context.types.get(lower);
  else value=lower;}
 return value+(isArray?'[]':'');}
function readName(tokens,index,defaultSchema='public'){const a=tokens[index];if(!a||!['word','identifier'].includes(a.kind))return null;
 let parts=[identityToken(a)],next=index+1;
 while(tokens[next]?.value==='.'&&['word','identifier'].includes(tokens[next+1]?.kind)){const t=tokens[next+1];parts.push(identityToken(t));next+=2;}
 return {name:parts.length===1?defaultSchema+'.'+parts[0]:parts.join('.'),parts,next};}
function argumentList(tokens,index){assert.equal(tokens[index]?.value,'(');let round=1,square=0,begin=index+1,args=[];
 for(let i=index+1;i<tokens.length;i++){const v=tokens[i].value;if(v==='(')round++;else if(v===')'){round--;if(!round){args.push(tokens.slice(begin,i));return {args,next:i+1};}}
  else if(v==='[')square++;else if(v===']')square--;else if(v===','&&round===1&&!square){args.push(tokens.slice(begin,i));begin=i+1;}}
 throw new Error('Unclosed function argument list');}
function isWholeUnnamedType(tokens,context){const value=typeText(removeTypmod(tokens)).replace(/\[\]/g,'').toLowerCase().replace(/^pg_catalog\./,'');
 return builtinAliases.has(value)||context.types?.has(value)||tokens.length===1||tokens[1]?.value==='.';}
function argumentIdentity(args,context){return args.filter(a=>a.length).flatMap(raw=>{
 let a=[...raw];if(a[0]?.kind==='word'&&a[0]?.value==='out')return [];if(a[0]?.kind==='word'&&['in','inout','variadic'].includes(a[0]?.value))a.shift();
 const d=a.findIndex(t=>(t.kind==='word'&&t.value==='default')||(t.kind==='punctuation'&&t.value==='='));if(d>=0)a=a.slice(0,d);assert.ok(a.length,'Empty input argument');
 if(!isWholeUnnamedType(a,context)){assert.ok(['word','identifier'].includes(a[0].kind),'Invalid parameter name');a=a.slice(1);}
 return [canonicalType(a,context)];}).join(',');}
export function inputIdentity(declaration,context={}){const t=sqlTokens(declaration),i=t.findIndex(v=>v.value==='(');assert.ok(i>=0,'Arguments required');return argumentIdentity(argumentList(t,i).args,context);}
function signature(tokens,index,context,{optional=false}={}){const n=readName(tokens,index,context.defaultSchema);if(!n)return null;
 if(tokens[n.next]?.value!=='('){assert.ok(optional,'Function arguments required for '+n.name);return {...n,inputTypes:null,next:n.next};}
 const a=argumentList(tokens,n.next);return {...n,inputTypes:argumentIdentity(a.args,context),next:a.next};}
function bodyFromTokens(tokens,index){const a=tokens.findIndex((t,i)=>i>=index&&t.kind==='word'&&t.value==='as');if(a<0)return null;
 const q=tokens[a+1];return q&&['dollar','string'].includes(q.kind)?q.value:null;}

export function functionEvents(text,context={}){const result=[],unresolved=[];const split=splitSqlStatements(text);
 for(const statement of split.statements){const t=sqlTokens(statement.text);if(!t.length)continue;let i=0;
  if(t[0].value==='create'){i=1;if(t[i]?.value==='or'&&t[i+1]?.value==='replace')i+=2;if(t[i]?.value!=='function')continue;
   const s=signature(t,i+1,context);const body=bodyFromTokens(t,s.next);if(body===null){unresolved.push({offset:statement.offset,kind:'UNRESOLVED_CREATE_BODY',name:s.name});continue;}
   result.push({kind:'CREATE',name:s.name,inputTypes:s.inputTypes,id:s.name+'('+s.inputTypes+')',offset:statement.offset+t[0].start,
    definition:statement.text.slice(t[0].start),body,header:stripSqlComments(statement.text.slice(t[i+1].start,t.find(v=>v.kind==='word'&&v.value==='as').start)).replace(/\s+/g,' ').trim()});
  }else if(t[0].value==='drop'&&t[1]?.value==='function'){i=2;let ifExists=false;if(t[i]?.value==='if'&&t[i+1]?.value==='exists'){ifExists=true;i+=2;}
   while(i<t.length){const s=signature(t,i,context,{optional:true});if(!s)break;result.push({kind:'DROP',name:s.name,inputTypes:s.inputTypes,
     id:s.inputTypes===null?null:s.name+'('+s.inputTypes+')',ifExists,offset:statement.offset+t[0].start});i=s.next;if(t[i]?.value===',')i++;else break;}
  }else if(t[0].value==='alter'&&t[1]?.value==='function'){const s=signature(t,2,context,{optional:true});if(!s)continue;
   if(t[s.next]?.value==='rename'&&t[s.next+1]?.value==='to'){const n=readName(t,s.next+2,s.name.slice(0,s.name.lastIndexOf('.')));assert.ok(n);result.push({kind:'RENAME',name:s.name,inputTypes:s.inputTypes,id:s.inputTypes===null?null:s.name+'('+s.inputTypes+')',newName:n.name,offset:statement.offset+t[0].start});}
   else if(t[s.next]?.value==='set'&&t[s.next+1]?.value==='schema'){const schema=t[s.next+2];assert.ok(['word','identifier'].includes(schema?.kind));result.push({kind:'SET_SCHEMA',name:s.name,inputTypes:s.inputTypes,
     id:s.inputTypes===null?null:s.name+'('+s.inputTypes+')',newName:(schema.kind==='word'?schema.value:schema.raw)+'.'+s.name.slice(s.name.lastIndexOf('.')+1),offset:statement.offset+t[0].start});}
  }else if(t[0].value==='do'){const body=t.find(v=>['dollar','string'].includes(v.kind))?.value;
   if(body&&/\b(?:create\s+(?:or\s+replace\s+)?function|drop\s+function|alter\s+function)\b/i.test(stripSqlComments(body)))unresolved.push({offset:statement.offset,kind:'DYNAMIC_OR_CONDITIONAL_FUNCTION_DDL_IN_DO',statementShaInput:statement.text});}
 }
 return {events:result,unresolved,meta:split.meta};}

export function applyFunctionEvents(state,events,describe=e=>e){for(const e of events){const receipt=describe(e);
 if(e.kind==='CREATE'){const previous=state.live.get(e.id);state.live.set(e.id,{...receipt,id:e.id,name:e.name,inputTypes:e.inputTypes,definition:e.definition,body:e.body});state.history.push({...receipt,replacedId:previous?.id??null});continue;}
 const keys=e.id?[e.id]:[...state.live.keys()].filter(k=>state.live.get(k).name===e.name);
 if(e.id===null&&keys.length!==1){state.unresolved.push({...receipt,kind:'AMBIGUOUS_OR_ABSENT_UNQUALIFIED_FUNCTION_EVENT',matches:keys});continue;}
 for(const key of keys){const previous=state.live.get(key);if(e.kind==='DROP'){state.live.delete(key);state.history.push({...receipt,removed:!!previous,removedId:key});}
  else if(previous){const id=e.newName+'('+previous.inputTypes+')';assert.ok(!state.live.has(id),'Rename collision '+id);state.live.delete(key);state.live.set(id,{...previous,id,name:e.newName,renameEvents:[...(previous.renameEvents||[]),receipt]});state.history.push({...receipt,movedFrom:key,movedTo:id});}
  else state.history.push({...receipt,absentSourceOwner:true});}}
 return state;}
export function emptyFunctionIndex(){return {live:new Map(),history:[],unresolved:[]};}

// Trigger identity is separate from its routine owner. PostgreSQL orders
// same-event BEFORE row triggers by their actual trigger names.
export function parseTriggerIdentity(trigger){const t=sqlTokens(trigger.definition),i=t.findIndex(v=>v.value==='trigger');assert.ok(i>=0&&t[i+1]);
 const triggerName=t[i+1].kind==='identifier'?t[i+1].value:t[i+1].value;assert.equal(triggerName,trigger.name,'Catalogue/CREATE trigger identity differs');
 const execute=t.findIndex(v=>v.value==='execute'),routine=readName(t,execute+2,'public');assert.ok(routine);
 return {triggerName,routineName:routine.name};}
export function triggerOrderChecks(triggers){const proposed=[{schema:'public',table:'timesheets',triggerName:'aaa_healthroster_a0_booking_before_v1',events:['insert','update'],direction:'BEFORE_EXISTING'},
 {schema:'public',table:'invoice_lines',triggerName:'aaa_healthroster_a0_reference_before_v1',events:['insert','update'],direction:'BEFORE_EXISTING'},
 {schema:'public',table:'candidates',triggerName:'zzzz_healthroster_a0_candidate_before_v1',events:['insert','update','delete'],direction:'AFTER_EXISTING'},
 {schema:'public',table:'timesheet_validations',triggerName:'zzzz_healthroster_a0_reference_before_v1',events:['insert','update'],direction:'AFTER_EXISTING'}];
 const checks=[];for(const p of proposed)for(const t of triggers.filter(t=>t.schema===p.schema&&t.table===p.table&&/\bBEFORE\b/i.test(t.definition)&&/\bFOR EACH ROW\b/i.test(t.definition))){
  const id=parseTriggerIdentity(t),eventText=t.definition.split(/\bBEFORE\b/i)[1].split(/\bON\b/i)[0];for(const event of p.events)if(new RegExp('\\b'+event+'\\b','i').test(eventText)){
   const pass=p.direction==='BEFORE_EXISTING'?p.triggerName<id.triggerName:p.triggerName>id.triggerName;
   checks.push({schema:p.schema,table:p.table,event,proposedTriggerName:p.triggerName,existingTriggerName:id.triggerName,routineName:id.routineName,
    enabled:t.enabled,direction:p.direction,pass});}}
 return checks;}
