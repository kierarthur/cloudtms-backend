// CM-04 source gate only: no database, money fixtures or Worker execution.
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import assert from 'node:assert/strict';
import { repoRoot } from '../scripts/cloudtms-db-release-lib.mjs';
import { executableParts } from '../scripts/bpay-next-source-gates.mjs';

const HELPERS = new Set(['public._pay_pct_to_frac(numeric)','public._pay_pct_to_mult(numeric)',
  'public._pay_convert_paye_to_umbrella(numeric,numeric,numeric,boolean)',
  'public._pay_convert_umbrella_to_paye_ex(numeric,numeric)',
  'public._pay_umbrella_vat_calc(numeric,numeric,boolean)']);
// The auditable pattern list belongs here, as the CM-04 card requires.
const PATTERNS = Object.freeze({
  rate: /\b(?:[a-z_][a-z0-9_]*_)?(?:erni(?:_[a-z0-9_]+)?|vat_(?:rate|pct|frac|mult)[a-z0-9_]*|umbrella_margin[a-z0-9_]*|margin_(?:rate|pct|frac|mult)[a-z0-9_]*)\b/i,
  arithmetic: /[\w)]\s*[*/]\s*[\w(]|\b1\s*\+|\+\s*1\b|\b1\s*-/,
  amount: /\b\w*(?:source_ex_vat|paye_ex|umbrella_ex|target_ex_vat|gross_ex_vat)\w*\b/i,
  copiedFactor: /[*/]\s*1\.\d+\b/,
  vatTarget: /\b(?!\w*(?:_ex_vat|_inc_vat)\b)(?:vat|[a-z_]\w*_vat)\s*:=\s*[^;]*[\w)]\s*[*/]\s*[\w(]/i,
  wrongBasis: /\b(?:\w*_)?net(?:_\w*)?\b|\b\w*(?:inc_vat|incvat)\w*\b/i
});

// Preserve the length of literals for balanced call scanning. Only JSON value
// selectors carry field-name evidence; prose/diagnostic strings are not code.
function valueCode(text) {
  let out='',i=0;
  const literal=value=>/(?:->>?|#>>?)\s*$/.test(out)&&/^(?:[a-z_][a-z0-9_]*|\{[a-z0-9_, ]+\})$/i.test(value)
    ?value.replace(/[{},]/g,' '):' '.repeat(value.length);
  while(i<text.length){
    const tag=text[i]==='$'?text.slice(i).match(/^\$(?:[a-z_][a-z0-9_]*)?\$/i)?.[0]:null;
    if(tag){const end=text.indexOf(tag,i+tag.length);if(end<0)throw Error('CM04_UNCLOSED_LITERAL');out+=literal(text.slice(i+tag.length,end))+' '.repeat(tag.length*2);i=end+tag.length;continue;}
    if(text[i]==="'"){
      const start=i++,escaped=/[eE]/.test(text[start-1]??'')&&!/[\w$]/.test(text[start-2]??'');let closed=false;
      while(i<text.length){if(escaped&&text[i]==='\\'){i+=2;continue;}if(text[i]==="'"){if(text[i+1]==="'"){i+=2;continue;}i++;closed=true;break;}i++;}
      if(!closed)throw Error('CM04_UNCLOSED_LITERAL');out+=literal(text.slice(start+1,i-1))+'  ';continue;
    }
    out+=text[i++];
  }
  return out.toLowerCase();
}
function callEnd(text,open,firstOnly=false){
  let depth=1;
  for(let i=open+1;i<text.length;i++){
    if(text[i]==='(')depth++;
    else if(text[i]===')'&&--depth===0)return i;
    if(firstOnly&&text[i]===','&&depth===1)return i;
  }
  throw Error('CM04_UNCLOSED_CALL');
}
function hideCalculatorCalls(text){
  const re=/\b(?:(?:public\s*\.\s*)?_pay_(?:convert_paye_to_umbrella|convert_umbrella_to_paye_ex|umbrella_vat_calc)|(?:private\s*\.\s*)?pay_finance_selected_quote_v1)\s*\(/g;
  const spans=[];let match;
  while((match=re.exec(text))){const open=re.lastIndex-1,end=callEnd(text,open);spans.push([match.index,end+1]);re.lastIndex=end+1;}
  for(const [start,end]of spans.reverse())text=text.slice(0,start)+' '.repeat(end-start)+text.slice(end);
  return text;
}
function calculatorArguments(text){
  const result=[],re=/\b(?:(?:public\s*\.\s*)?_pay_(?:convert_paye_to_umbrella|convert_umbrella_to_paye_ex|umbrella_vat_calc)|(?:private\s*\.\s*)?pay_finance_selected_quote_v1)\s*\(/g;let match;
  while((match=re.exec(text))){
    const open=re.lastIndex-1,end=callEnd(text,open);let depth=0,start=open+1;
    for(let i=start;i<=end;i++){
      if(i===end||text[i]===','&&depth===0){const arg=text.slice(start,i);result.push(arg,...calculatorArguments(arg));start=i+1;}
      else if(text[i]==='(')depth++;else if(text[i]===')')depth--;
    }
    re.lastIndex=end+1;
  }
  return result;
}
// A pure JSON transport groups independent values, not one rate formula.
// Keep scalar consumers intact; keys and shared-calculator arguments are code too.
function literalTransportKey(raw){
  const text=raw.trim().replace(/\s*::\s*(?:pg_catalog\s*\.\s*)?text\s*$/i,'');
  const ordinary=text.match(/^(?:e)?'([a-z_][a-z0-9_]*)'$/i);
  if(ordinary)return ordinary[1].toLowerCase();
  const dollar=text.match(/^\$([a-z_][a-z0-9_]*)?\$([a-z_][a-z0-9_]*)\$([a-z_][a-z0-9_]*)?\$$/i);
  return dollar&&(dollar[1]??'')===(dollar[3]??'')?dollar[2].toLowerCase():null;
}
function transportExpressions(code,raw,depth=0){
  if(depth>16)return [code];
  const match=code.match(/^\s*(?:(?:return\s+)|(?:[a-z_][a-z0-9_]*\s*:=\s*))?(?:pg_catalog\s*\.\s*)?jsonb_build_object\s*\(/i);
  if(!match)return [code];
  const open=match[0].length-1,end=callEnd(code,open);
  if(code.slice(end+1).trim())return [code];
  const spans=[];let start=open+1,nesting=0,squares=0;
  for(let i=start;i<=end;i++){
    if(i===end||code[i]===','&&nesting===0&&squares===0){spans.push([start,i]);start=i+1;}
    else if(code[i]==='(')nesting++;else if(code[i]===')')nesting--;
    else if(code[i]==='[')squares++;else if(code[i]===']')squares--;
  }
  if(nesting||squares||spans.length%2||/\bvariadic\b/.test(code))return [code];
  const keys=spans.filter((_,i)=>i%2===0).map(([start,end])=>literalTransportKey(raw.slice(start,end)));
  if(keys.some(key=>key===null))return [code]; // Unknown/dynamic keys keep the original conservative scan.
  const result=[];
  for(let i=0;i<spans.length;i+=2){
    const [ks,ke]=spans[i],[vs,ve]=spans[i+1],key=code.slice(ks,ke),value=code.slice(vs,ve);
    result.push(key,...transportExpressions(value,raw.slice(vs,ve),depth+1));
    // Preserve the existing VAT-target evidence when the target is a JSON key.
    if(/^(?:vat|[a-z_][a-z0-9_]*_vat)$/.test(keys[i/2]))result.push('vat := '+value);
  }
  return result;
}
export function assertSharedCalculatorSource(source,file){
  for(const part of executableParts(source,file)){
    if(HELPERS.has(part.signature))continue;
    if(part.unclassified)throw Error('CM04_BODY_UNCLASSIFIED: '+file);
    const code=valueCode(part.text);
    const calls=/\b(?:private\s*\.\s*)?bpay_next_value_source_for_target_v1\s*\(/g;let call;
    // Only executable calls, not CREATE/ALTER/grant routine declarations.
    if(!/^\s*(?:alter|grant|revoke|comment|drop)\b/i.test(code))while((call=calls.exec(code))){
      const first=code.slice(calls.lastIndex,callEnd(code,calls.lastIndex-1,true));
      if(PATTERNS.wrongBasis.test(first))throw Error('CM04_SOURCE_BASIS_REQUIRED: '+file);
    }
    const arithmetic=hideCalculatorCalls(code),rateNames=new Set();
    const names=code.match(/\b[a-z_][a-z0-9_]*\b/g)??[];
    for(const name of names)if(PATTERNS.rate.test(name))rateNames.add(name);
    // Follow ordinary local rate/fraction/multiplier aliases; never infer an
    // amount from a table/ledger or reject generic manual-rate/unit arithmetic.
    const assignments=[...arithmetic.matchAll(/\b([a-z_][a-z0-9_]*)\s*(?:numeric(?:\s*\([^)]*\))?\s*)?:=\s*([^;]+);/g)];
    let added;
    do{added=false;for(const [,name,rhs]of assignments)if(!rateNames.has(name)
      &&!/\b(?:is|and|or|not|exists|jsonb_build_object|jsonb_build_array|to_jsonb)\b|[<>=]/.test(rhs.replace(/->>?|#>>?/g,''))
      &&(rhs.match(/\b[a-z_][a-z0-9_]*\b/g)??[]).some(n=>rateNames.has(n))){rateNames.add(name);added=true;}}while(added);
    const expressions=[];let offset=0;
    for(const statement of arithmetic.split(';')){
      expressions.push(...transportExpressions(statement,part.text.slice(offset,offset+statement.length)));
      offset+=statement.length+1;
    }
    for(const expression of [...expressions,...calculatorArguments(code).map(hideCalculatorCalls)]){
      const rate=(expression.match(/\b[a-z_][a-z0-9_]*\b/g)??[]).some(name=>rateNames.has(name));
      const duplicate=rate&&PATTERNS.arithmetic.test(expression)
        ||PATTERNS.amount.test(expression)&&PATTERNS.copiedFactor.test(expression)
        ||PATTERNS.amount.test(expression)&&PATTERNS.vatTarget.test(expression);
      if(duplicate)throw Error('CM04_SHARED_CALCULATOR_REQUIRED: '+file+' ('+part.signature+') '+JSON.stringify({rateNames:[...rateNames].filter(name=>new RegExp('\\b'+name+'\\b').test(expression)),factor:expression.match(PATTERNS.copiedFactor)?.[0],vatTarget:expression.match(PATTERNS.vatTarget)?.[0]})+' '+expression.trim().slice(0,200));
    }
  }
}
export function verifySharedCalculatorFinalSources(){
  const registry=JSON.parse(fs.readFileSync(path.join(repoRoot,'docs/bpay-next/final-definer-closures.json'),'utf8'));
  const files=new Set(registry.chains.filter(row=>/^(?:private|public)\.bpay_next_|comparison/i.test(row.signature)
    ||row.signature.startsWith('private.pay_finance_selected_quote_v1(')).map(row=>row.finalPhysical));
  if(!files.size)throw Error('CM04_FINAL_SOURCE_SCOPE_EMPTY');
  for(const file of files){
    if(typeof file!=='string'||!/^supabase\/repeatable\/.+\.sql(?:inc)?$/.test(file)||file.split('/').includes('..'))throw Error('CM04_FINAL_SOURCE_PATH_INVALID');
    assertSharedCalculatorSource(fs.readFileSync(path.join(repoRoot,file),'utf8'),file);
  }
  return files.size;
}

test('CM04 current F04 physical final sources use the shared calculators',()=>{
  assert.ok(verifySharedCalculatorFinalSources()>0);
});
test('CM04 trusted case quote transports separate ratios and metadata without hiding scalar formulas',()=>{
  const source=fs.readFileSync(path.join(repoRoot,'supabase/repeatable/12102026_1646_banking_pay_next_case_channel_resolution_v1.sql'),'utf8');
  const start=source.indexOf('create or replace function private.bpay_next_case_channel_quote_v1(');
  const end=source.indexOf('create or replace function private.bpay_next_case_channel_quotes_v1(',start);
  assert.ok(start>=0&&end>start);
  const quote=source.slice(start,end);
  assert.match(quote,/q:=private\.pay_finance_selected_quote_v1/);
  assert.doesNotThrow(()=>assertSharedCalculatorSource(quote,'case-quote.sql'));
  const copied=quote.replace("round((q->>'ex')::numeric/basis,10)",'round(basis*(1+w.erni_pct/100),10)');
  assert.notEqual(copied,quote);
  assert.throws(()=>assertSharedCalculatorSource(copied,'case-quote.sql'),/CM04_SHARED_CALCULATOR_REQUIRED/);
  const wrap=body=>`create function private.bpay_next_cm04_json() returns jsonb language plpgsql as $body$begin null; ${body} end$body$;`;
  assert.doesNotThrow(()=>assertSharedCalculatorSource(wrap("return jsonb_build_object('quote_operands',jsonb_build_object('target_vat_per_source',round((q->>'vat')::numeric/basis,10),'erni_pct',w.erni_pct::text));"),'json-ratios.sql'));
  for(const body of [
    "return jsonb_build_object('vat',p_source_ex_vat*0.2,'erni_pct',w.erni_pct);",
    "return jsonb_build_object(E'vat',p_source_ex_vat*0.2,'erni_pct',w.erni_pct);",
    "return jsonb_build_object($key$vat$key$,p_source_ex_vat*0.2,'erni_pct',w.erni_pct);",
    "return jsonb_build_object('vat'::text,p_source_ex_vat*0.2,'erni_pct',w.erni_pct);",
    "return jsonb_build_object('quote',jsonb_build_object('ex',p_source_ex_vat/1.15));",
    "return jsonb_build_object('ex',(jsonb_build_object('r',v_erni_pct)->>'r')::numeric*p_source_ex_vat);",
    "return jsonb_build_object((v_erni_pct/100)::text,p_source_ex_vat);",
    "return jsonb_build_object('rows',ARRAY[1,round(p_source_ex_vat*(1+v_erni_pct/100),2)]);",
    "return jsonb_build_object('quote',private.pay_finance_selected_quote_v1(mode,p_source_ex_vat/(1+v_erni_pct/100),a,b,c,d,e,v_erni_pct,v_vat_rate_pct,true));"
  ])assert.throws(()=>assertSharedCalculatorSource(wrap(body),'json-formula.sql'),/CM04_SHARED_CALCULATOR_REQUIRED/,body);
});
test('CM04 source controls reject copied formulas/wrong value bases, retain caller classification',()=>{
  const wrap=body=>`create function private.bpay_next_cm04_example() returns void language plpgsql as $body$begin ${body} end$body$;`;
  for(const body of [
    'v_ex:=round(p_source_ex_vat*(1+v_erni_pct/100),2);',
    'v_frac:=public._pay_pct_to_frac(v_vat_rate_pct);v_vat:=round(p_source_ex_vat*v_frac,2);',
    'v_mult:=public._pay_pct_to_mult(v_erni_pct);v_ex:=round(p_source_ex_vat/v_mult,2);',
    'v_ex:=round(p_source_ex_vat/1.15,2);',
    'v_vat:=round(p_source_ex_vat*0.2,2);',
    'v_ex:=p_source_ex_vat*(1-v_umbrella_margin_pct/100);',
    "v_ex:=p_source_ex_vat*(1+(policy->>'erni_pct')::numeric);"
    ,"v_frac:=(policy->>'erni_pct')::numeric;v_ex:=p_source_ex_vat/(1+v_frac);"
    ,'v_ex:=public._pay_convert_umbrella_to_paye_ex(p_source_ex_vat*(1+v_erni_pct/100),v_erni_pct);'
    ,'v_quote:=private.pay_finance_selected_quote_v1(mode,p_source_ex_vat/(1+v_erni_pct/100),a,b,c,d,e,v_erni_pct,v_vat_rate_pct,true);'
  ])assert.throws(()=>assertSharedCalculatorSource(wrap(body),'final.sql'),/CM04_SHARED_CALCULATOR_REQUIRED/,body);
  for(const input of ['v_net','round(v_target_inc_vat,2)',"coalesce(p->>'net_amount','0')::numeric","coalesce(p#>>'{net_amount}','0')::numeric", "p#>>'{captured,amount_inc_vat}'", 'p->>$key$net_amount$key$', 'p_source_ex_vat => v_cash_net'])
    assert.throws(()=>assertSharedCalculatorSource(wrap(`perform private.bpay_next_value_source_for_target_v1(${input},a,b,c,d,e);`),'final.sql'),/CM04_SOURCE_BASIS_REQUIRED/);
  for(const body of [
    'v_amount:=round(p_target_units*p_manual_replacement_rate,2);',
    'v_vat_per_source:=round(v_target_vat/v_basis_amount_ex_vat,10);',
    'v_vat:=v_vat+v_frozen_vat;v_inc:=v_ex+v_vat;',
    'if p_erni_pct>1 then null;end if;v_erni_pct:=round(p_erni_pct,6);',
    'v_pages:=(v_count+99)/100;v_money:=v_pence/100;',
    'v_changed:=old.vat_rate_pct is distinct from new.vat_rate_pct;v_rows:=(v_count+99)/100;',
    'v_overnight:=false;v_pay:=round(v_hours*v_rate,2);',
    'perform private.bpay_next_value_source_for_target_v1(round(v_included_source,2),net,a,b,c,d);',
    'perform public._pay_convert_paye_to_umbrella(p_source_ex_vat,v_erni_pct,v_vat_rate_pct,true);',
    'v_quote:=private.pay_finance_selected_quote_v1(mode,p_source_ex_vat,a,b,c,d,e,v_erni_pct,v_vat_rate_pct,true);v_selected:=round((v_quote->>\'ex\')::numeric,2);v_per_source:=v_selected/p_source_ex_vat;',
    "-- v_vat:=p_source_ex_vat*v_vat_rate_pct;\nraise exception using message='p_source_ex_vat / v_erni_pct';"
  ])assert.doesNotThrow(()=>assertSharedCalculatorSource(wrap(body),'final.sql'));
  // The exemption applies to a helper body, never to its neighbouring function.
  const helper=wrap('v_ex:=p_source_ex_vat*v_erni_pct;').replace('private.bpay_next_cm04_example()','public._pay_convert_paye_to_umbrella(a numeric,b numeric,c numeric,d boolean)');
  assert.doesNotThrow(()=>assertSharedCalculatorSource(helper,'final.sql'));
  assert.throws(()=>assertSharedCalculatorSource(helper.replace('a numeric','a text'),'final.sql'),/CM04_SHARED_CALCULATOR_REQUIRED/);
  assert.throws(()=>assertSharedCalculatorSource(helper+wrap('v_vat:=p_source_ex_vat*v_vat_rate_pct;'),'final.sql'),/CM04_SHARED_CALCULATOR_REQUIRED/);
  assert.throws(()=>assertSharedCalculatorSource('do $body$begin perform private.bpay_next_value_source_for_target_v1(v_net,a,b,c,d,e);end$body$;','final.sql'),/CM04_SOURCE_BASIS_REQUIRED/);
  const current=fs.readFileSync(path.join(repoRoot,'supabase/repeatable/06102026_1157_banking_pay_shared_selected_quote_v1.sql'),'utf8');
  const mutation=current.replace('v_ex:=round(p_manual_amount_ex_vat,2);','v_vat:=round(p_source_ex_vat*p_vat_rate_pct/100,2);');
  assert.notEqual(mutation,current);
  assert.throws(()=>assertSharedCalculatorSource(mutation,'supabase/repeatable/06102026_1157_banking_pay_shared_selected_quote_v1.sql'),/CM04_SHARED_CALCULATOR_REQUIRED/);
});
