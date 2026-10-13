// EX-08: exact existing GET/POST contracts; SQL alone owns the future change.
const enc=new TextEncoder(),UUID=/^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const object=v=>v!==null&&typeof v==='object'&&!Array.isArray(v);
const equal=(a,b)=>object(a)&&object(b)?Object.keys(a).length===Object.keys(b).length&&Object.keys(a).every(k=>Object.hasOwn(b,k)&&equal(a[k],b[k])):Array.isArray(a)&&Array.isArray(b)?a.length===b.length&&a.every((v,i)=>equal(v,b[i])):a===b;
const whole=v=>typeof v==='string'&&/^(?:0|[1-9]\d{0,18})$/.test(v)&&BigInt(v)<=9223372036854775807n;
function field(v,t){if(t.endsWith('?'))return v===null||field(v,t.slice(0,-1));
 if(t==='uuid')return typeof v==='string'&&UUID.test(v);
 if(t==='integer')return whole(v);if(t==='positive')return whole(v)&&v!=='0';
 if(t==='channel')return ['PAYE','UMBRELLA'].includes(v);if(t==='boolean')return typeof v==='boolean';
 if(t==='money')return typeof v==='string'&&/^-?(?:0|[1-9]\d*)(?:\.\d{1,2})?$/.test(v)&&enc.encode(v).length<=1024;
 if(t==='date')return typeof v==='string'&&/^(?!0000)\d{4}-\d{2}-\d{2}$/.test(v)&&Number.isFinite(Date.parse(v+'T00:00:00Z'))&&new Date(v+'T00:00:00Z').toISOString().slice(0,10)===v;
 if(t==='key'||t==='text')return typeof v==='string'&&!v.includes('\0')&&enc.encode(v).length<=(t==='key'?1024:1024)&&(t!=='key'||v.length>0);return false;}
const shape=(v,s)=>object(v)&&Object.keys(v).length===Object.keys(s).length&&Object.entries(s).every(([k,t])=>Object.hasOwn(v,k)&&field(v[k],t));
const CONTEXT={agency_id:'uuid',actor_user_id:'uuid',owner_epoch:'positive',candidate_id:'uuid',source_method:'channel',source_umbrella_id:'uuid?',target_method:'channel',target_umbrella_id:'uuid?',target_vat_chargeable:'boolean?',target_umbrella_enabled:'boolean?',financial_view_revision:'integer',metadata_version:'integer',workbench_revision:'integer',adoption_revision:'integer',last_command_sequence:'integer',valuation_policy_id:'uuid',official_pay_date:'date'};
function context(v){return shape(v,CONTEXT)&&enc.encode(JSON.stringify(v)).length<=4096
 &&(v.source_method!=='PAYE'||v.source_umbrella_id===null)
 &&(v.target_method==='PAYE'?[v.target_umbrella_id,v.target_vat_chargeable,v.target_umbrella_enabled].every(x=>x===null):v.target_umbrella_id!==null&&typeof v.target_vat_chargeable==='boolean'&&v.target_umbrella_enabled===true);}
function cursor(v){return object(v)&&Object.keys(v).sort().join()==='context,last_key'&&context(v.context)&&field(v.last_key,'key')&&enc.encode(JSON.stringify(v)).length<=8192;}
function request(v){return object(v)&&Object.keys(v).sort().join()==='destination_patch,expected_old_method,new_method,operation_id,preview_context,reason'
 &&field(v.operation_id,'uuid')&&field(v.new_method,'channel')&&field(v.expected_old_method,'channel')&&object(v.destination_patch)
 &&(v.reason===null||typeof v.reason==='string'&&!v.reason.includes('\0')&&enc.encode(v.reason).length<=2048)
 &&context(v.preview_context)&&v.new_method===v.preview_context.target_method&&v.expected_old_method===v.preview_context.source_method
 &&(v.new_method==='PAYE'?Object.keys(v.destination_patch).length===0:Object.keys(v.destination_patch).join()==='umbrella_id'&&v.destination_patch.umbrella_id===v.preview_context.target_umbrella_id)
 &&enc.encode(JSON.stringify(v)).length<=8192;}
const ROW={key:'key',member_kind:'text',work_id:'uuid?',revision_id:'uuid?',approved_line_id:'uuid?',approved_segment_id:'uuid?',case_id:'uuid?',case_component_id:'uuid?',case_component_revision:'positive?',transfer_id:'uuid?',run_id:'uuid?',source_channel:'channel?',source_umbrella_id:'uuid?',classification:'text',source_amount:'money?',label:'text',detail:'text',original_section:'text'};
const QUOTE={policy_id:'uuid',policy_window_id:'uuid',source_channel:'channel',target_channel:'channel',source_ex_vat:'money',target_ex_vat:'money',target_vat:'money',target_inc_vat:'money',treatment:'text?'};
function row(v,c){if(!object(v)||Object.keys(v).length!==Object.keys(ROW).length+1||!Object.hasOwn(v,'quote')||!shape(Object.fromEntries(Object.keys(ROW).map(k=>[k,v[k]])),ROW)
 ||!['WORK','CASE','PROTECTED_WORK','PROTECTED_CASE','TRANSFER'].includes(v.member_kind)||!['COMPATIBLE','NEEDS_DECISION','UNCHANGED','PROTECTED'].includes(v.classification)
 ||!['READY','ACTION','BLOCKED','WAITING'].includes(v.original_section))return false;
 if(v.member_kind.includes('WORK')&&(v.work_id===null||v.revision_id===null||v.case_id!==null||v.transfer_id!==null)
  ||v.member_kind.includes('CASE')&&(v.case_id===null||v.case_component_id===null||v.case_component_revision===null||v.work_id!==null||v.transfer_id!==null)
  ||v.member_kind==='TRANSFER'&&(v.transfer_id===null||v.work_id!==null||v.case_id!==null)
  ||['PROTECTED_WORK','PROTECTED_CASE','TRANSFER'].includes(v.member_kind)!==(v.classification==='PROTECTED'))return false;
 return v.quote===null?v.classification!=='COMPATIBLE':shape(v.quote,QUOTE)&&v.classification==='COMPATIBLE'&&v.quote.source_channel===v.source_channel&&v.quote.target_channel===c.target_method&&v.quote.source_ex_vat===v.source_amount;}
function preview(v,{actor,candidate,target,umbrella,after=null,limit=20}){if(!object(v)||Object.keys(v).sort().join()!=='candidate_name,candidate_reference,category_counts,complete,context,next_cursor,rows,total_member_count,umbrella_name'
 ||!context(v.context)||v.context.actor_user_id!==actor||v.context.candidate_id!==candidate||v.context.target_method!==target||v.context.target_umbrella_id!==umbrella
 ||after!==null&&!equal(after.context,v.context)||!field(v.candidate_name,'text')||!field(v.candidate_reference,'text?')||!field(v.umbrella_name,'text?')
 ||!Array.isArray(v.rows)||v.rows.length>limit||typeof v.complete!=='boolean'||!whole(v.total_member_count)||BigInt(v.total_member_count)>1000n
 ||!shape(v.category_counts,{stays_payable:'integer',needs_decision:'integer',not_changed:'integer'})
 ||Object.values(v.category_counts).reduce((n,x)=>n+BigInt(x),0n)!==BigInt(v.total_member_count)||enc.encode(JSON.stringify(v)).length>65536)return false;
 let last=after?.last_key??null;for(const r of v.rows){if(!row(r,v.context)||last!==null&&r.key<=last)return false;last=r.key;}
 return v.complete?v.next_cursor===null:cursor(v.next_cursor)&&equal(v.next_cursor.context,v.context)&&v.rows.length>0&&v.next_cursor.last_key===last;}
function receipt(v,r,candidate){return shape(v,{accepted:'boolean',operation_id:'uuid',candidate_id:'uuid',pay_method:'channel',umbrella_id:'uuid?',metadata_version:'positive',task_id:'uuid',task_generation:'positive',affected_member_count:'integer',projection_pending:'boolean'})
 &&v.accepted===true&&v.projection_pending===true&&v.operation_id===r.operation_id&&v.candidate_id===candidate&&v.pay_method===r.new_method
 &&v.umbrella_id===r.preview_context.target_umbrella_id&&BigInt(v.metadata_version)>BigInt(r.preview_context.metadata_version)&&BigInt(v.affected_member_count)<=1000n;}
export {equal,context,cursor,request,preview,receipt};

export function createBpayNextPayMethodChangeRpc({baseUrl,headers,fetchImpl=fetch}){
 const url=new URL(baseUrl);if(url.username||url.password||url.search||url.hash||(url.protocol!=='https:'&&!(url.protocol==='http:'&&['127.0.0.1','localhost','[::1]'].includes(url.hostname))))throw TypeError('BPAY_NEXT_PAY_METHOD_TRANSPORT_INVALID');
 const h=new Headers(headers);if(!/^Bearer [A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(h.get('authorization')??'')||typeof fetchImpl!=='function')throw TypeError('BPAY_NEXT_PAY_METHOD_TRANSPORT_INVALID');
 return async(name,args,mutation)=>{
  if(name!==(mutation?'bpay_next_pay_method_change_apply_v1':'bpay_next_pay_method_change_preview_v1')||enc.encode(JSON.stringify(args)).length>12288)throw fail('BPAY_NEXT_PAY_METHOD_INPUT_INVALID');
  const controller=new AbortController();let timer,reader;
  const cancel=()=>{controller.abort();try{if(reader)void reader.cancel().catch(()=>{});}catch{}};
  const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{cancel();reject(fail('BPAY_NEXT_PAY_METHOD_UNAVAILABLE'));},mutation?8000:4000);});
  try{return await Promise.race([deadline,(async()=>{
   const callHeaders=new Headers(h);callHeaders.set('content-type','application/json');callHeaders.set('accept','application/json');callHeaders.set('X-CloudTMS-Banking-Pay-Route-Class',mutation?'OPERATION_NUDGE':'DISPLAY');
   const response=await fetchImpl(url.href.replace(/\/+$/,'')+'/rest/v1/rpc/'+name,{method:'POST',headers:callHeaders,body:JSON.stringify(args),redirect:'error',signal:controller.signal});
   if(controller.signal.aborted){void response.body?.cancel().catch(()=>{});throw fail('BPAY_NEXT_PAY_METHOD_UNAVAILABLE');}
   if(!/^application\/json(?:\s*;|$)/i.test(response.headers.get('content-type')??'')||!response.body)throw fail('BPAY_NEXT_PAY_METHOD_UNAVAILABLE');
   reader=response.body.getReader();const decoder=new TextDecoder('utf-8',{fatal:true});let bytes=0,text='';
   for(;;){const part=await reader.read();if(controller.signal.aborted)throw fail('BPAY_NEXT_PAY_METHOD_UNAVAILABLE');if(part.done)break;bytes+=part.value.length;if(bytes>65536)throw fail('BPAY_NEXT_PAY_METHOD_RESPONSE_INVALID');text+=decoder.decode(part.value,{stream:true});}
   text+=decoder.decode();const value=JSON.parse(text);if(!response.ok)throw Object.assign(new Error('BPAY_NEXT_PAY_METHOD_RPC_REFUSED'),{json:value,status:response.status});return value;
  })()]);}finally{clearTimeout(timer);cancel();}
 };
}

const REFUSALS=new Set(['BPAY_NEXT_PAY_METHOD_FORBIDDEN','BPAY_NEXT_MODULE_NOT_ACTIVE','BPAY_NEXT_PAY_METHOD_TARGET_INVALID','BPAY_NEXT_PAY_METHOD_CONTEXT_UNAVAILABLE','BPAY_NEXT_PAY_METHOD_SCOPE_TOO_LARGE','BPAY_NEXT_PAY_METHOD_PAGE_INVALID','BPAY_NEXT_PAY_METHOD_PREVIEW_CHANGED','BPAY_NEXT_PAY_METHOD_RESPONSE_TOO_LARGE','BPAY_NEXT_PAY_METHOD_INPUT_INVALID','BPAY_NEXT_PAY_METHOD_PROTECTED_REWRITE_FORBIDDEN','BPAY_NEXT_PAY_METHOD_CANDIDATE_MISSING','BPAY_NEXT_PAY_METHOD_OPERATION_CONFLICT','BPAY_NEXT_PAY_METHOD_NO_CHANGE','BPAY_NEXT_PAY_METHOD_PROJECTION_MISSING']);
const fail=code=>Object.assign(new Error(code),{code});
async function jsonBody(req){if(!req.body)throw fail('BPAY_NEXT_PAY_METHOD_INPUT_INVALID');const reader=req.body.getReader();let bytes=0,parts=[],timer;const deadline=new Promise((_,reject)=>{timer=setTimeout(()=>{void reader.cancel().catch(()=>{});reject(fail('BPAY_NEXT_PAY_METHOD_INPUT_INVALID'));},4000);});try{return await Promise.race([deadline,(async()=>{for(;;){const {done,value}=await reader.read();if(done)break;bytes+=value.length;if(bytes>8192)throw fail('BPAY_NEXT_PAY_METHOD_INPUT_INVALID');parts.push(value);}const all=new Uint8Array(bytes);let n=0;for(const p of parts){all.set(p,n);n+=p.length;}return JSON.parse(new TextDecoder('utf-8',{fatal:true}).decode(all));})()]);}finally{clearTimeout(timer);try{void reader.cancel().catch(()=>{});}catch{}}}
export async function handleBpayNextPayMethodChange(req,{actor,candidate,rpc,wake}){
 const send=(status,value)=>new Response(JSON.stringify(value),{status,headers:{'content-type':'application/json','cache-control':'no-store'}});
 let r=null,attempted=false;
 try{
  if(!field(actor,'uuid')||!field(candidate,'uuid')||typeof rpc!=='function')throw fail('BPAY_NEXT_PAY_METHOD_FORBIDDEN');
  if(req.method==='GET'){
   const q=new URL(req.url).searchParams;if([...q.keys()].some(k=>!['new_method','umbrella_id','after','limit'].includes(k))||[...q.keys()].some(k=>q.getAll(k).length!==1))throw fail('BPAY_NEXT_PAY_METHOD_PAGE_INVALID');
   const target=q.get('new_method'),umbrella=q.get('umbrella_id')||null,raw=q.get('after');
   const after=raw===null?null:raw.length<=8192?JSON.parse(raw):false,limit=q.has('limit')?Number(q.get('limit')):20;
   if(!field(target,'channel')||!field(umbrella,'uuid?')||target==='PAYE'&&umbrella!==null||target==='UMBRELLA'&&umbrella===null||after!==null&&!cursor(after)||!Number.isInteger(limit)||limit<1||limit>20)throw fail('BPAY_NEXT_PAY_METHOD_PAGE_INVALID');
   const result=await rpc('bpay_next_pay_method_change_preview_v1',{p_actor_user_id:actor,p_candidate_id:candidate,p_new_method:target,p_umbrella_id:umbrella,p_after:after,p_limit:limit},false);
   if(!preview(result,{actor,candidate,target,umbrella,after,limit}))throw fail('BPAY_NEXT_PAY_METHOD_RESPONSE_INVALID');return send(200,result);
  }
  if(req.method!=='POST')throw fail('BPAY_NEXT_PAY_METHOD_INPUT_INVALID');
  r=await jsonBody(req);if(!request(r)||r.preview_context.actor_user_id!==actor||r.preview_context.candidate_id!==candidate)throw fail('BPAY_NEXT_PAY_METHOD_INPUT_INVALID');
  attempted=true;
  const result=await rpc('bpay_next_pay_method_change_apply_v1',{p_actor_user_id:actor,p_candidate_id:candidate,p_request:r},true);
  if(!receipt(result,r,candidate))throw fail('BPAY_NEXT_PAY_METHOD_RESPONSE_INVALID');
  // Metadata was committed with its durable task. A missed wake is recovered
  // by the existing bounded task scanner, not reported as a failed mutation.
  try{if(typeof wake==='function')await wake({id:result.task_id,kind:'WORKBENCH'});}catch{/* Existing durable recovery owns a missed wake. */}
  return send(200,result);
 }catch(e){const text=[e?.code,e?.json?.message,e?.json?.details,e?.body,e?.message].filter(Boolean).join(' '),code=[...REFUSALS].find(c=>text.includes(c))??null;
  const unknown=attempted&&!code;
  return send(code==='BPAY_NEXT_PAY_METHOD_FORBIDDEN'?403:code?409:503,{ok:false,error_code:code??(unknown?'BPAY_NEXT_PAY_METHOD_OUTCOME_UNKNOWN':'BPAY_NEXT_PAY_METHOD_UNAVAILABLE'),operation_id:r?.operation_id??null,unknown_outcome:unknown});}
}
