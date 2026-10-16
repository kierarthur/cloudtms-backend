// Exact prebody census for one immutable migration. No database access or waiver.
import fs from 'node:fs';
import path from 'node:path';
import {canonical,digest,exactAtomicBody,scanManagedSql} from './cloudtms-managed-release-sql.mjs';

export const MONEY_PREDECESSOR_MIGRATION=Object.freeze({
 version:'HEALTHROSTER_MONEY_CHECK_PREBODY_COMPANION_V1',
 path:'supabase/migrations/13102026_1430_weekly_source_invoice_money_origins.sql',
 contentSha256:'931765daf31387fd5e5de7c9a9f6944ab1bf1c030a6ea7a4b9f2c3f0cc79a40c',
 closureSha256:'ab41e992a869943bff423239b9fe53f0e08b43a6c693e517cf489790a87d6154',
 rawBodySha256:'faca7a0392015a812bf7e1aea9089387ee38030f7e2994fe5b4cc46da01695b5',
 mappedSourceSha256:'95f0967a011df18b2cbe5b4d554387c226b3a24d2d2ed6e9b051f30641924ea6',
 mappedBodySha256:'a30e64be80a1381daab403c1bd2c52b0f7d4bc4cd076bee66a1949b8385f6a20',
 companionPath:'supabase/release/16102026_0943_healthroster_money_predecessor_assertion.sqlinc',
 companionSha256:'96f5f4e37f330ca510eed591eb36eb86e78e982b4bf93176e4f126231e04fa50',
 constructorPath:'supabase/migrations/15092026_1534_weekly_source_plan6_schema.sql',
 constructorSha256:'1fc7833c7054cb56da725d15280ff757f3b78e1046b80f12c337020695374c0d',
 evidencePath:'docs/healthroster-build/v6-owner-delivery-20261016/F23_PHYSICAL_PREDECESSOR_PEER_REVIEW_20261016/REVIEW.json',
 evidenceSha256:'10573f03cf4d6667627efe08d9e614a8a71d18d77dab9b70f259bf006a688799',
 expectedChargeChecks:11,expectedBillingChecks:12,expectedPriceChecks:1
});
const fail=code=>{throw Error('HEALTHROSTER_MONEY_PREDECESSOR_'+code);};
function assertClosure(closure,kind){
 const pin=MONEY_PREDECESSOR_MIGRATION;
 if(kind!=='migration'||closure.path!==pin.path||closure.files?.size!==1||closure.ordered?.length!==1
  ||closure.ordered[0].path!==pin.path||closure.files.get(pin.path)!==pin.contentSha256
  ||digest(closure.ordered[0].source)!==pin.contentSha256||closure.closureHash!==pin.closureSha256
  ||closure.source!==closure.expanded)fail('IMMUTABLE_SOURCE_CHANGED');
 const sourceHash=digest(closure.expanded);
 const expectedBody=sourceHash===pin.contentSha256?pin.rawBodySha256:
  sourceHash===pin.mappedSourceSha256?pin.mappedBodySha256:null;
 if(!expectedBody)fail('MAPPED_BODY_CHANGED');
 if(digest(exactAtomicBody(closure.expanded))!==expectedBody)fail('MAPPED_BODY_CHANGED');
}

export function moneyPredecessorCompanionFor(closure,kind,{root,mapSource=value=>value}={}){
 const pin=MONEY_PREDECESSOR_MIGRATION;
 if(closure.path!==pin.path)return null;
 assertClosure(closure,kind);
 const read=file=>canonical(fs.readFileSync(path.join(root,file),'utf8'));
 const program=read(pin.companionPath),constructor=read(pin.constructorPath),evidence=read(pin.evidencePath);
 if(digest(program)!==pin.companionSha256||digest(constructor)!==pin.constructorSha256
   ||digest(evidence)!==pin.evidenceSha256)fail('COMPANION_SOURCE_CHANGED');
 const parsed=scanManagedSql(program);
 if(parsed.meta.length||parsed.statements.length!==2||parsed.statements[0].tokens[0]!=='LOCK'
   ||parsed.statements[1].tokens[0]!=='DO')fail('COMPANION_PACKET_CHANGED');
 const sql=mapSource(program,pin.companionPath);
 if(sql!==program)fail('COMPANION_MAPPING_CHANGED');
 return {version:pin.version,sourceRoot:root,migrationPath:pin.path,migrationSha256:pin.contentSha256,
  closureSha256:pin.closureSha256,bodySha256:digest(exactAtomicBody(closure.expanded)),
  companionPath:pin.companionPath,companionSha256:pin.companionSha256,
  statementSha256:parsed.statements.map(row=>digest(program.slice(row.start,row.end))),
  placement:'BEFORE_IMMUTABLE_BODY_SAME_TRANSACTION',sql,
  sourceFiles:[{file:pin.companionPath,sha256:pin.companionSha256},
   {file:pin.constructorPath,sha256:pin.constructorSha256}],
  evidenceFiles:[{file:pin.evidencePath,sha256:pin.evidenceSha256}]};
}

export function moneyPredecessorMigrationSql(unit,mapSource=value=>value){
 const pin=MONEY_PREDECESSOR_MIGRATION;
 if(unit.kind!=='migration'||unit.item?.path!==pin.path||unit.item.sha256!==pin.contentSha256
  ||unit.exception||unit.ownerRlsEnvelope||unit.immutableGuardDependency||unit.contactClaimCompanion
  ||!unit.moneyPredecessorCompanion||unit.body!==exactAtomicBody(unit.closure.expanded))fail('ATOMIC_SCOPE_INVALID');
 assertClosure(unit.closure,unit.kind);
 const expected=moneyPredecessorCompanionFor(unit.closure,unit.kind,
  {root:unit.moneyPredecessorCompanion.sourceRoot,mapSource});
 if(JSON.stringify(unit.moneyPredecessorCompanion)!==JSON.stringify(expected))fail('ATOMIC_COMPANION_CHANGED');
 return expected.sql;
}
