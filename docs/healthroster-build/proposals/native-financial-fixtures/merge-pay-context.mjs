import fs from 'node:fs';
// Pure file bridge. Neither file is a committed artifact; both contain only
// the root-owned transaction's bounded fixture context/calculation result.
const context=JSON.parse(fs.readFileSync(process.argv[2],'utf8'));
const result=JSON.parse(fs.readFileSync(process.argv[3],'utf8'));
if(context.contract!=='NFX_NATIVE_CONTEXT_V1'||context.phase!=='PAY_SNAPSHOT'
 ||result.producer!=='weeklySourceScenarioJourneyInternals.buildWeeklyCorrectFinalServiceSnapshot'
 ||result.seeded_outcomes!==false)throw Error('NFX_PAY_CONTEXT_PRODUCER_MISMATCH');
fs.writeFileSync(process.argv[4],JSON.stringify({...context,phase:'PAY_APPLY',calculator_result:result})+'\n');
