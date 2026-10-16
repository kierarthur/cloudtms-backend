// Exact0400 prebody boundary. Source only; no native execution or generic waiver.
import fs from'node:fs';import path from'node:path';
import{canonical,digest,exactAtomicBody,scanManagedSql}from'./cloudtms-managed-release-sql.mjs';
const deepFreeze=value=>{if(value&&typeof value==='object'){for(const v of Object.values(value))deepFreeze(v);Object.freeze(value);}return value;};
export const CAPABILITY_PREDECESSOR_MIGRATION=deepFreeze({
  "version": "HEALTHROSTER_CAPABILITY_CHECK_PREBODY_COMPANION_V1",
  "path": "supabase/migrations/14102026_0400_healthroster_protected_source_capability.sql",
  "contentSha256": "5f0a2fd5535897286601dc4d2c84d07f0a14cda9c0222158ac89168758a70a16",
  "closureSha256": "601493c1cda73cc21a177dfc834c6c68db36bc1f0b6cd185e5d7b96e241023c4",
  "rawBodySha256": "f2383f31b668a683282d70491e1cd2a93a1e0053f3fd9fae70da970147fdf7e4",
  "mappedSourceSha256": "5f0a2fd5535897286601dc4d2c84d07f0a14cda9c0222158ac89168758a70a16",
  "mappedBodySha256": "f2383f31b668a683282d70491e1cd2a93a1e0053f3fd9fae70da970147fdf7e4",
  "companionPath": "supabase/release/16102026_1120_healthroster_capability_predecessor_assertion.sqlinc",
  "companionSha256": "72cc28893189715a73bf39258d4c1f5835033e381ea31670e842136b32e9a68c",
  "statementSha256": [
    "36c0abeb1bc4b31d6741eed04e7806f0168d754445432a2e08609c21f281799f",
    "b97b8204efef03757c0138a212634810dad3983f9b19d2277adc1b04bc4e8758"
  ],
  "constructorPath": "supabase/migrations/13102026_1133_weekly_source_rule_sets_schema.sql",
  "constructorSha256": "8fc11ed0be84c7c53f4bf41a9d92775e2b4f071c49c3cb69c07458cded30e7bf",
  "constructorRawBodySha256": "03866ee59284377a0021defc77fb40d984e8e8127e5f3aced7bf223f99c90791",
  "constructorMappedSourceSha256": "c64fec2cb109b79fc6a82bb6caf991af1770fd2c21d6aaa850f1d0354a3f8dd1",
  "constructorMappedBodySha256": "0884899495bea783cad34abeb82610b954e3fc6c0940f1e2fd19994b89455af0",
  "evidencePath": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/PROPOSAL.json",
  "expressionPath": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/EXPRESSION_REVIEW.json",
  "capturePath": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/NATIVE_PREDECESSOR_CENSUS.json",
  "evidenceFiles": [
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/16102026_1120_healthroster_capability_predecessor_assertion.sqlinc",
      "sha256": "72cc28893189715a73bf39258d4c1f5835033e381ea31670e842136b32e9a68c",
      "preservedRawSha256": "72cc28893189715a73bf39258d4c1f5835033e381ea31670e842136b32e9a68c"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/build-proposal.mjs",
      "sha256": "d5ae4c0a6485c0075d15e003fb5475b5f31d08376cae0210bc2fba0738d94019",
      "preservedRawSha256": "d5ae4c0a6485c0075d15e003fb5475b5f31d08376cae0210bc2fba0738d94019"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/check_expressions.py",
      "sha256": "3caed0f8ac684841db69d308d827ab3afe6b0665c075db1993fc0a685fe51bb8",
      "preservedRawSha256": "3caed0f8ac684841db69d308d827ab3afe6b0665c075db1993fc0a685fe51bb8"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/EXPRESSION_REVIEW.json",
      "sha256": "730f8126b447e64bf0a7d39c59fc9ffbb04ca63accc025ac7f8897bd6322ba48",
      "preservedRawSha256": "640ad6a503cf889e7b337eaac2269d61c2c0de5777d3de78ff32e89ca06245c4"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/CONSTRUCTOR_1133.sql",
      "sha256": "8fc11ed0be84c7c53f4bf41a9d92775e2b4f071c49c3cb69c07458cded30e7bf",
      "preservedRawSha256": "8fc11ed0be84c7c53f4bf41a9d92775e2b4f071c49c3cb69c07458cded30e7bf"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/IMMUTABLE_CONSUMER_0400.sql",
      "sha256": "5f0a2fd5535897286601dc4d2c84d07f0a14cda9c0222158ac89168758a70a16",
      "preservedRawSha256": "5f0a2fd5535897286601dc4d2c84d07f0a14cda9c0222158ac89168758a70a16"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/INPUT_RECEIPT_R1.json",
      "sha256": "e0c99e59c94c70f49e3db388cd493fcb1684738558741dd189bc839c1116ee2a",
      "preservedRawSha256": "e0c99e59c94c70f49e3db388cd493fcb1684738558741dd189bc839c1116ee2a"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/NATIVE_PREDECESSOR_CENSUS.json",
      "sha256": "a482d9cb8fb9814ca2a33967bebc5e18138fea61d4c8611012c5a665d772bea6",
      "preservedRawSha256": "a482d9cb8fb9814ca2a33967bebc5e18138fea61d4c8611012c5a665d772bea6"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/NATIVE_R2.json",
      "sha256": "0a8997f662d045996832a384866d1fa26c08f5f9db188f6f855d1fe4eb255086",
      "preservedRawSha256": "0a8997f662d045996832a384866d1fa26c08f5f9db188f6f855d1fe4eb255086"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/PLAN.json",
      "sha256": "e75b276f6e766f4d98d10729982352e4b9801bb2ec16bb8cc28f231e492399eb",
      "preservedRawSha256": "e75b276f6e766f4d98d10729982352e4b9801bb2ec16bb8cc28f231e492399eb"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/PREFIX_01_13102026_1112_healthroster_invoice_support_members_v1.sql",
      "sha256": "f0282ddc39e64c8522ee3f0d5a4916235436dbca1478a5e7f65d651ff20c7b5b",
      "preservedRawSha256": "f0282ddc39e64c8522ee3f0d5a4916235436dbca1478a5e7f65d651ff20c7b5b"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/PREFIX_02_13102026_1127_healthroster_timesheet_correction_requests_v1.sql",
      "sha256": "49e7110033679e40e9290ad1d6f043fdeb507aa4c0129a6acf20a9f7fce180a4",
      "preservedRawSha256": "49e7110033679e40e9290ad1d6f043fdeb507aa4c0129a6acf20a9f7fce180a4"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/PREFIX_03_13102026_1133_weekly_source_rule_sets_schema.sql",
      "sha256": "8fc11ed0be84c7c53f4bf41a9d92775e2b4f071c49c3cb69c07458cded30e7bf",
      "preservedRawSha256": "8fc11ed0be84c7c53f4bf41a9d92775e2b4f071c49c3cb69c07458cded30e7bf"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/PREFIX_04_13102026_1210_weekly_source_final_value_snapshot_authority.sql",
      "sha256": "0d0564eea33cff211efe8452d3c7d4cf0817aeb21854a967a4f7ad402966a7d6",
      "preservedRawSha256": "0d0564eea33cff211efe8452d3c7d4cf0817aeb21854a967a4f7ad402966a7d6"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/PREFIX_05_13102026_1344_weekly_source_self_bill_report_schema.sql",
      "sha256": "34b22ce64481534a73f0e2f32c97e426a7b57d8f11341ab94d709839b734a6ae",
      "preservedRawSha256": "34b22ce64481534a73f0e2f32c97e426a7b57d8f11341ab94d709839b734a6ae"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/PREFIX_06_13102026_1430_weekly_source_invoice_money_origins.sql",
      "sha256": "931765daf31387fd5e5de7c9a9f6944ab1bf1c030a6ea7a4b9f2c3f0cc79a40c",
      "preservedRawSha256": "931765daf31387fd5e5de7c9a9f6944ab1bf1c030a6ea7a4b9f2c3f0cc79a40c"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/PREFIX_07_13102026_1456_weekly_source_protection_query_support_v1.sql",
      "sha256": "5869f480951c18794715807e5f4168b4f9154d29449757024e053f80c07aae17",
      "preservedRawSha256": "5869f480951c18794715807e5f4168b4f9154d29449757024e053f80c07aae17"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/PREFIX_08_13102026_1519_healthroster_correction_reason_bound_v1.sql",
      "sha256": "5012c6e0e79c28a53b7aa6d40003370f2bc088e673a3b7ebaed36aedd908a6a2",
      "preservedRawSha256": "5012c6e0e79c28a53b7aa6d40003370f2bc088e673a3b7ebaed36aedd908a6a2"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/PREFIX_09_13102026_1680_weekly_source_invoice_window_receipt_schema.sql",
      "sha256": "b19dbf220c5661df5b8edc4851f8b22a9505429658b0a82bf18b18f2a2df81d0",
      "preservedRawSha256": "b19dbf220c5661df5b8edc4851f8b22a9505429658b0a82bf18b18f2a2df81d0"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/RESTORATION.json",
      "sha256": "582ba3016d5fcc9673036032e78ad786c5a3b88abeb472f40f3b1f73c896e1fc",
      "preservedRawSha256": "582ba3016d5fcc9673036032e78ad786c5a3b88abeb472f40f3b1f73c896e1fc"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/inputs/SUMMARY.json",
      "sha256": "2617a9e826cf22eb8d52701771f889653bca47198536ca147638341c10b52627",
      "preservedRawSha256": "2617a9e826cf22eb8d52701771f889653bca47198536ca147638341c10b52627"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/MOUNT_PLAN.json",
      "sha256": "006f98080472d362ec48317917c1927ae07c0b22d700084fdd0c8aa6b0aecde3",
      "preservedRawSha256": "006f98080472d362ec48317917c1927ae07c0b22d700084fdd0c8aa6b0aecde3"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/PROPOSAL.json",
      "sha256": "ccdf7881bb4d2d728a00d66959cdb8846ead1027b9378c9f322d142c62946b9e",
      "preservedRawSha256": "ccdf7881bb4d2d728a00d66959cdb8846ead1027b9378c9f322d142c62946b9e"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/README.md",
      "sha256": "7fd291b8dc98fb86feb06933931d90e7fbea2e1f3ac60e67716df27b990e9f91",
      "preservedRawSha256": "7fd291b8dc98fb86feb06933931d90e7fbea2e1f3ac60e67716df27b990e9f91"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/seal-review.mjs",
      "sha256": "1dfa95956c5bbdc01e28876f8d702f08ee48a679ddf8c0bfa88efb8630ef03e5",
      "preservedRawSha256": "1dfa95956c5bbdc01e28876f8d702f08ee48a679ddf8c0bfa88efb8630ef03e5"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/test-proposal.mjs",
      "sha256": "e97b9bcb0e8f3cae3b5a04c2f94f267af82b2766d7dd9e448996f443eaff0055",
      "preservedRawSha256": "e97b9bcb0e8f3cae3b5a04c2f94f267af82b2766d7dd9e448996f443eaff0055"
    },
    {
      "file": "docs/healthroster-build/v6-owner-delivery-20261016/F23_CAPABILITY_PREDECESSOR_REVIEW_20261016/TEST_RESULTS.json",
      "sha256": "337a8fe8e6d6b4681db20d775b8693ff00d9c85fa9fe695b1e58052560ab8588",
      "preservedRawSha256": "337a8fe8e6d6b4681db20d775b8693ff00d9c85fa9fe695b1e58052560ab8588"
    }
  ],
  "evidenceNormalization": "Canonical LF approval hashes plus exact preserved raw artifact hashes; historical evidence bytes unchanged",
  "expectedSelectedChecks": 2,
  "expectedAllChecks": 10,
  "expectedRetainedChecks": 8
});
const fail=code=>{throw Error('HEALTHROSTER_CAPABILITY_PREDECESSOR_'+code);};
function assertClosure(closure,kind){
 const p=CAPABILITY_PREDECESSOR_MIGRATION;
 if(kind!=='migration'||closure.path!==p.path||closure.files?.size!==1||closure.ordered?.length!==1
  ||closure.ordered[0].path!==p.path||closure.files.get(p.path)!==p.contentSha256
  ||digest(closure.ordered[0].source)!==p.contentSha256||closure.closureHash!==p.closureSha256
  ||closure.source!==closure.expanded)fail('IMMUTABLE_SOURCE_CHANGED');
 const source=digest(closure.expanded),expected=source===p.contentSha256?p.rawBodySha256:
  source===p.mappedSourceSha256?p.mappedBodySha256:null;
 if(!expected||digest(exactAtomicBody(closure.expanded))!==expected)fail('MAPPED_BODY_CHANGED');
}
export function capabilityPredecessorCompanionFor(closure,kind,{root,mapSource=value=>value}={}){
 const p=CAPABILITY_PREDECESSOR_MIGRATION;if(closure.path!==p.path)return null;
 assertClosure(closure,kind);const read=file=>canonical(fs.readFileSync(path.join(root,file),'utf8'));
 const program=read(p.companionPath),constructor=read(p.constructorPath);
 if(digest(program)!==p.companionSha256||digest(constructor)!==p.constructorSha256)fail('COMPANION_SOURCE_CHANGED');
 for(const input of p.evidenceFiles)if(digest(read(input.file))!==input.sha256)fail('EVIDENCE_SOURCE_CHANGED');
 const mappedConstructor=mapSource(constructor,p.constructorPath),ctorHash=digest(mappedConstructor);
 const ctorBody=ctorHash===p.constructorSha256?p.constructorRawBodySha256:
  ctorHash===p.constructorMappedSourceSha256?p.constructorMappedBodySha256:null;
 if(!ctorBody||digest(exactAtomicBody(mappedConstructor))!==ctorBody)fail('CONSTRUCTOR_MAPPING_CHANGED');
 const proof=JSON.parse(read(p.evidencePath)),expression=JSON.parse(read(p.expressionPath)),capture=JSON.parse(read(p.capturePath));
 if(proof.version!=='HEALTHROSTER_CAPABILITY0400_PREBODY_SOURCE_DESIGN_V1'
  ||proof.consumer.kind!=='migration'||proof.consumer.path!==p.path||proof.consumer.sourceSha256!==p.contentSha256
  ||proof.program.sha256!==p.companionSha256||proof.constructor.sha256!==p.constructorSha256
  ||proof.successor.expressionReview.sha256!==p.evidenceFiles.find(x=>x.file===p.expressionPath)?.preservedRawSha256
  ||proof.successor.sourceDesignApproved!==true||proof.successor.nativeCompanionApproved!==false
  ||proof.successor.nativeCompanion!=='NOT_RUN'||expression.sourceDesignExpressionApproved!==true
  ||expression.nativeCompanionApproved!==false||expression.nativeCompanion!=='NOT_RUN'
  ||capture.expression_approved!==false||capture.native_companion_approved!==false
  ||capture.all_check_count!==10||capture.selected_count!==2||capture.retained_check_count!==8)fail('EVIDENCE_SCOPE_CHANGED');
 const parsed=scanManagedSql(program),statementSha256=parsed.statements.map(s=>digest(program.slice(s.start,s.end)));
 if(parsed.meta.length||parsed.statements.length!==2||parsed.statements[0].tokens[0]!=='LOCK'
  ||parsed.statements[1].tokens[0]!=='DO'||JSON.stringify(statementSha256)!==JSON.stringify(p.statementSha256))fail('COMPANION_PACKET_CHANGED');
 const sql=mapSource(program,p.companionPath);if(sql!==program)fail('COMPANION_MAPPING_CHANGED');
 return{version:p.version,sourceRoot:root,migrationPath:p.path,migrationSha256:p.contentSha256,
  closureSha256:p.closureSha256,bodySha256:digest(exactAtomicBody(closure.expanded)),
  companionPath:p.companionPath,companionSha256:p.companionSha256,statementSha256,
  placement:'BEFORE_IMMUTABLE_BODY_SAME_TRANSACTION',sql,
  sourceFiles:[{file:p.companionPath,sha256:p.companionSha256},{file:p.constructorPath,sha256:p.constructorSha256}],
  evidenceFiles:p.evidenceFiles.map(row=>({...row}))};
}
export function capabilityPredecessorMigrationSql(unit,mapSource=value=>value){
 const p=CAPABILITY_PREDECESSOR_MIGRATION;
 if(unit.kind!=='migration'||unit.item?.path!==p.path||unit.item.sha256!==p.contentSha256
  ||unit.exception||unit.ownerRlsEnvelope||unit.immutableGuardDependency||unit.contactClaimCompanion||unit.moneyPredecessorCompanion
  ||!unit.capabilityPredecessorCompanion||unit.body!==exactAtomicBody(unit.closure.expanded))fail('ATOMIC_SCOPE_INVALID');
 assertClosure(unit.closure,unit.kind);
 const expected=capabilityPredecessorCompanionFor(unit.closure,unit.kind,{root:unit.capabilityPredecessorCompanion.sourceRoot,mapSource});
 if(JSON.stringify(unit.capabilityPredecessorCompanion)!==JSON.stringify(expected))fail('ATOMIC_COMPANION_CHANGED');
 return expected.sql;
}
