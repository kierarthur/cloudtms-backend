// Finite TEST-only read-only observation; stops only the actual rollback child.
// Resource loss is always a failed rehearsal, never a waived check or a retry.
const SERVICE='01a02f5a-2bee-7db2-910d-a7e71f11ba0a',POD='postgres-service-p0qoe-1',NAMESPACE='migetuq4';
const METRICS=['miget_instance_memory_used_bytes','miget_instance_resource_limit','miget_instance_restarts_total','miget_instance_oom_events_total','miget_volume_used_bytes','miget_volume_size_bytes'];
const error=code=>Object.assign(new Error(code),{code});
const closed=(value,keys)=>value&&typeof value==='object'&&!Array.isArray(value)&&Object.keys(value).sort().join('|')===[...keys].sort().join('|');
export function assertRehearsalResourceSample(sample,{now=Date.now}={}){
 const keys=['service','pod','namespace','observedAt','metricTimes','memoryUsed','memoryLimit','volumeUsed','volumeLimit','restarts','oomEvents'];
 if(!sample||Object.keys(sample).sort().join('|')!==keys.sort().join('|')||sample.service!==SERVICE||sample.pod!==POD||sample.namespace!==NAMESPACE
  ||typeof sample.observedAt!=='string'||!Number.isFinite(Date.parse(sample.observedAt))||['memoryUsed','memoryLimit','volumeUsed','volumeLimit','restarts','oomEvents'].some(key=>!Number.isSafeInteger(sample[key])||sample[key]<0)
  ||sample.memoryLimit!==4294967296||sample.volumeLimit<sample.volumeUsed)throw error('REHEARSAL_RESOURCE_SAMPLE_REFUSED');
 if(!closed(sample.metricTimes,METRICS))throw error('REHEARSAL_RESOURCE_TIME_INVALID');
 const seconds=now()/1000;
 for(const [name,value] of Object.entries(sample.metricTimes)){
  // Memory gauges scrape frequently; independent event/volume exporters
  // have a slower observed cadence. Neither accepts Prometheus lookback as fresh.
  const maximumSampleAge=METRICS.slice(0,2).includes(name)?30:90;
  if(!closed(value,['evaluatedAt','sampledAt'])||![value.evaluatedAt,value.sampledAt].every(time=>typeof time==='number'&&Number.isFinite(time)&&time>0))throw error('REHEARSAL_RESOURCE_TIME_INVALID');
  if(seconds-value.evaluatedAt>15||value.evaluatedAt-seconds>5||seconds-value.sampledAt>maximumSampleAge||value.sampledAt-seconds>5||value.sampledAt-value.evaluatedAt>5)throw error('REHEARSAL_RESOURCE_SAMPLE_STALE');
 }
 if(sample.observedAt!==new Date(Math.min(...Object.values(sample.metricTimes).map(value=>value.evaluatedAt))*1000).toISOString())throw error('REHEARSAL_RESOURCE_TIME_INVALID');
 return sample;
}
export function createRehearsalResourceGuard({readSample,onSample=()=>{},intervalMs=5000,memoryStopBytes=3221225472,minimumFreeBytes=4294967296,schedule=setInterval,unschedule=clearInterval,now=Date.now}){
 if(typeof readSample!=='function'||typeof onSample!=='function'||intervalMs!==5000||memoryStopBytes!==3221225472||minimumFreeBytes!==4294967296)throw error('REHEARSAL_RESOURCE_GUARD_REFUSED');
 let baseline=null,stopped=false,timer=null,pending=null,cancelChild=null,failure=null;
 const stopFor=code=>{failure??=error(code);cancelChild?.(failure);};
 async function check(){
  if(stopped)return;
  const sample=assertRehearsalResourceSample(await readSample(),{now});onSample({...sample});
  if(baseline&&(sample.restarts!==baseline.restarts||sample.oomEvents!==baseline.oomEvents))throw error('REHEARSAL_TEST_RESTART_OR_OOM_OBSERVED');
  if(sample.memoryUsed>=memoryStopBytes)throw error('REHEARSAL_MEMORY_HEADROOM_STOP');
  if(sample.volumeLimit-sample.volumeUsed<minimumFreeBytes)throw error('REHEARSAL_DISK_HEADROOM_STOP');
  baseline??=sample;
 }
 async function prepare(){await check();return{memoryStopBytes,minimumFreeBytes,intervalMs};}
 function start(cancel){
  if(!baseline||typeof cancel!=='function'||cancelChild||stopped)throw error('REHEARSAL_RESOURCE_GUARD_NOT_PREPARED');
  cancelChild=cancel;
  timer=schedule(()=>{if(pending||stopped)return;pending=check().catch(cause=>stopFor(/^[A-Z][A-Z0-9_]+$/.test(cause.code??'')?cause.code:'REHEARSAL_RESOURCE_OBSERVATION_FAILED')).finally(()=>{pending=null;});},intervalMs);
  timer.unref?.();
 }
 async function stop(){stopped=true;unschedule(timer);await pending;return{failed:!!failure,code:failure?.code??null,baseline};}
 return Object.freeze({prepare,start,stop});
}
export function createMigetRehearsalResourceReader({fetcher=fetch,headers,now=Date.now}){
 if(!headers||typeof headers.authorization!=='string'||!headers.authorization.startsWith('Bearer '))throw error('REHEARSAL_RESOURCE_AUTH_REQUIRED');
 const names=METRICS;
 // Prometheus set-union normally ignores __name__, which would silently
 // collapse memory/restart/OOM rows with identical labels. Retain that identity.
 const selector=name=>name+'{addon="postgres-service-p0qoe"'+(name===names[1]?',resource="memory"':'')+'}';
 const terms=[...names.map(selector),...names.map(name=>`label_replace(timestamp(${selector(name)}),"source_metric","${name}","addon",".*")`)];
 const query=terms.join(' or on(__name__,source_metric,instance,volume,resource,namespace) ');
 return async()=>{
  const response=await fetcher('https://metrics.miget.com/prometheus/api/v1/query?'+new URLSearchParams({query}),{headers,signal:AbortSignal.timeout(10000)});
  if(!response.ok)throw error('REHEARSAL_RESOURCE_READ_HTTP_'+response.status);
  const body=await response.json();if(body.status!=='success'||body.data?.resultType!=='vector')throw error('REHEARSAL_RESOURCE_READ_REFUSED');
  const rows=body.data.result;if(!Array.isArray(rows))throw error('REHEARSAL_RESOURCE_READ_REFUSED');
  const numeric=(value,integer)=>{if(typeof value!=='string'||! /^(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(value))throw error('REHEARSAL_RESOURCE_VALUE_INVALID');const number=Number(value);
   if(!Number.isFinite(number)||number<0||(integer&&!Number.isSafeInteger(number)))throw error('REHEARSAL_RESOURCE_VALUE_INVALID');return number;};
  const take=(name,resource,time=false)=>{const matches=rows.filter(row=>row.metric?.[time?'source_metric':'__name__']===name&&row.metric.addon==='postgres-service-p0qoe'&&row.metric.namespace===NAMESPACE
   &&(row.metric.instance===POD||row.metric.volume===POD)&&(!resource||row.metric.resource===resource));
   if(matches.length!==1||!Array.isArray(matches[0].value)||matches[0].value.length!==2)throw error('REHEARSAL_RESOURCE_READ_AMBIGUOUS');
   const point=matches[0].value;if(typeof point[0]!=='number'||!Number.isFinite(point[0])||point[0]<=0)throw error('REHEARSAL_RESOURCE_TIME_INVALID');return{evaluatedAt:point[0],value:numeric(point[1],!time)};};
  const values=Object.fromEntries(names.map(name=>[name,take(name,name===names[1]?'memory':null)]));
  const metricTimes=Object.fromEntries(names.map(name=>{const stamp=take(name,name===names[1]?'memory':null,true);if(stamp.evaluatedAt!==values[name].evaluatedAt)throw error('REHEARSAL_RESOURCE_TIME_INVALID');return[name,{evaluatedAt:values[name].evaluatedAt,sampledAt:stamp.value}];}));
  return assertRehearsalResourceSample({service:SERVICE,pod:POD,namespace:NAMESPACE,observedAt:new Date(Math.min(...Object.values(metricTimes).map(value=>value.evaluatedAt))*1000).toISOString(),metricTimes,
   memoryUsed:values[names[0]].value,memoryLimit:values[names[1]].value,volumeUsed:values[names[4]].value,volumeLimit:values[names[5]].value,restarts:values[names[2]].value,oomEvents:values[names[3]].value},{now});
 };
}
