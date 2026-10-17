import pathlib,json,hashlib
HERE=pathlib.Path(__file__).resolve().parent
sha=lambda b:hashlib.sha256(b).hexdigest()
model=json.loads((HERE/'MODEL.json').read_text())
helper=(HERE/'runtime-template.mjs').read_text().replace('__MODEL__',json.dumps(model,separators=(',',':')))
name='scripts/healthroster-source-schema-coverage.mjs'
p=HERE/'payload'/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_text(helper,encoding='utf8',newline='\n')
f='scripts/cloudtms-managed-release.mjs';before=(HERE/'parents'/f).read_bytes();s=before.decode();edits=[]
def edit(old,new):
 global s
 assert s.count(old)==1,(old,s.count(old));s=s.replace(old,new);edits.append(dict(before=old,after=new))
edit("import { CAPABILITY_PREDECESSOR_MIGRATION,capabilityPredecessorCompanionFor,capabilityPredecessorMigrationSql } from './healthroster-capability-predecessor-boundary.mjs';", "import { CAPABILITY_PREDECESSOR_MIGRATION,capabilityPredecessorCompanionFor,capabilityPredecessorMigrationSql } from './healthroster-capability-predecessor-boundary.mjs';\nimport { HEALTH_SOURCE_COVERAGE,healthSourceCoverageFor,healthSourceCoverageSql } from './healthroster-source-schema-coverage.mjs';")
edit("    capability_check_prebody_companion_sha256: CAPABILITY_PREDECESSOR_MIGRATION.companionSha256,", "    capability_check_prebody_companion_sha256: CAPABILITY_PREDECESSOR_MIGRATION.companionSha256,\n    health_source_constructor_coverage_version: HEALTH_SOURCE_COVERAGE.version,\n    health_source_constructor_coverage_sha256: HEALTH_SOURCE_COVERAGE.authoritySha256,")
edit("  const capabilityPredecessorCompanion = capabilityPredecessorCompanionFor(closure,kind,{root,mapSource});", "  const capabilityPredecessorCompanion = capabilityPredecessorCompanionFor(closure,kind,{root,mapSource});\n  const healthSourceCoverage = healthSourceCoverageFor(closure,kind,{root,mapSource});")
edit("immutableGuardDependency,contactClaimCompanion,moneyPredecessorCompanion,capabilityPredecessorCompanion };", "immutableGuardDependency,contactClaimCompanion,moneyPredecessorCompanion,capabilityPredecessorCompanion,healthSourceCoverage };")
edit("  if(unit.capabilityPredecessorCompanion||unit.item.path===CAPABILITY_PREDECESSOR_MIGRATION.path){", "  if(unit.healthSourceCoverage||HEALTH_SOURCE_COVERAGE.consumers.includes(unit.item.path)){\n    const coverage=healthSourceCoverageSql(unit,context);\n    return `begin;\\n${assertWriterSql}\\n${coverage}\\n${assertWriterSql}\\n${ledger}\\ncommit;`;\n  }\n  if(unit.capabilityPredecessorCompanion||unit.item.path===CAPABILITY_PREDECESSOR_MIGRATION.path){")
edit("      capabilityCheckPrebodyCompanion: CAPABILITY_PREDECESSOR_MIGRATION,", "      capabilityCheckPrebodyCompanion: CAPABILITY_PREDECESSOR_MIGRATION,\n      healthSourceConstructorCoverage: HEALTH_SOURCE_COVERAGE,")
post=s.encode();p=HERE/'payload'/f;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(post)
rev=s
for e in reversed(edits):assert rev.count(e['after'])==1;rev=rev.replace(e['after'],e['before'])
assert rev.encode()==before
forward=dict(version='HEALTH_SOURCE_EXACT_ONCE_COMPILER_FORWARD_V1',targets=[dict(file=f,parent='parents/'+f,beforeSha256=sha(before),beforeBytes=len(before),afterSha256=sha(post),afterBytes=len(post),edits=edits,wholeInverse=dict(before=before.decode(),after=s)),dict(file=name,parent=None,beforeSha256=None,afterSha256=sha(helper.encode()),afterBytes=len(helper.encode()))])
(HERE/'FORWARD.json').write_text(json.dumps(forward,indent=2)+'\n',encoding='utf8')
print(json.dumps(dict(targets=2,hunks=len(edits),helperBytes=len(helper.encode()),compilerBytes=len(post))))
