// Finite actual-observation timing metadata; never a native authority substitute.
import {requireThat,ROUTE} from './source.mjs';
export const WORK_DEADLINE_CODE='PURE_SEQUENCE_WORK_BUDGET_HELD';
export function observedInstant(value){
 requireThat(typeof value==='string'&&value.length<=64&&Number.isFinite(Date.parse(value)),'PURE_OBSERVED_CLOCK_REFUSED');
 return Date.parse(value);
}
export function rootCallTiming(value,{sqlSha256,readyAt,resultReadAt}){
 requireThat(value&&JSON.stringify(Object.keys(value).sort())===JSON.stringify(['callReturnedAt','callStartedAt','route','sqlSha256','version'])
  &&value.version==='ROOT_OBSERVED_MCP_CALL_TIMING_V1'&&value.route===ROUTE&&value.sqlSha256===sqlSha256,'PURE_ROOT_CALL_TIMING_BINDING_REFUSED');
 for(const field of ['callStartedAt','callReturnedAt'])requireThat(new Date(observedInstant(value[field])).toISOString()===value[field],'PURE_ROOT_CALL_CLOCK_CANONICAL_REQUIRED');
 const start=observedInstant(value.callStartedAt),end=observedInstant(value.callReturnedAt);
 requireThat(start>=observedInstant(readyAt)&&end>=start&&end<=observedInstant(resultReadAt),'PURE_ROOT_CALL_CLOCK_ORDER_REFUSED');
 return {...value,rootObservedCallMilliseconds:end-start,authority:'ACTUAL_ROOT_ORCHESTRATION_CLOCK_OBSERVATIONS_NOT_SERVER_OR_TRANSPORT_LATENCY'};
}
