// Explicitly synthetic boundary values; never a native result or acceptance receipt.
import {buildCostInput} from './factory.mjs';
const abi=JSON.parse((await import('node:fs')).default.readFileSync(new URL('./reference/NATIVE_ABI_REFERENCE.json',import.meta.url),'utf8'));
export const now=Date.parse('2026-10-17T18:00:00.000Z');
export const identity={physicalDatabase:'cloudtms_test_clone',databaseOid:12345,backendPid:81234,turnId:'hr_turn_'+'a'.repeat(32),key:[20261013,1],takenAt:'2026-10-17T17:59:40.000Z',task:'QT_AUDIT_COST_1000_DIAGNOSTIC'};
export const frame=(statement,command,field,value)=>({statement,command,rowCount:field?1:null,fields:field?[field]:[],rows:field?[{[field]:value}]:[],rowsTruncated:false});
export function synthetic(mode='JSON_MD5',n='1000'){
 const built=buildCostInput(identity,mode),clock=(phase,offset)=>({version:'QT_AUDIT_COST_CLOCK_V1',mode,phase,sampleLimit:1000,database:'cloudtms_test_clone',databaseOid:12345,backendPid:82345,holderPid:81234,holderGranted:true,readOnly:true,noRoleSwitch:true,observedAt:new Date(now+offset).toISOString(),closureAdmitted:true});
 const count={version:'QT_AUDIT_COST_CONSUMED_V1',mode,sampleLimit:1000,sampleCount:n,evaluatedCount:n,bytesTotal:String(BigInt(n)*32n),database:'cloudtms_test_clone',backendPid:82345,holderPid:81234,holderGranted:true,readOnly:true,noRoleSwitch:true};
 return{built,result:{database:'cloudtms_test_clone',databaseTarget:'agency_test',label:built.receipt.label,sqlSha256:built.receipt.sqlSha256,statementCount:4,accepted:true,rolledBack:true,warnings:abi.warnings,fatalError:null,statements:[frame(1,'SET'),frame(2,'SELECT','cost_clock',clock('START',-2000)),frame(3,'SELECT','cost_counters',count),frame(4,'SELECT','cost_clock',clock('END',-1000))],rollbackVerification:{connectedFresh:true,database:'cloudtms_test_clone',statements:[frame(1,'SELECT','cost_clock',clock('FRESH',0))]}}};
}
