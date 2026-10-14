import assert from 'node:assert/strict';
import test from 'node:test';
import {sourceCheckEnvironment} from '../scripts/healthroster-source-check-environment.mjs';
const context={gitDir:'C:/fixture/own.git',workTree:'C:/fixture/own-archive',finalDefinerBase:'a'.repeat(40)};
test('archive children cannot inherit native database probes, service credentials or an alternate Git index',()=>{
 const keys=['PGHOST','PGPASSWORD','CLOUDTMS_DATABASE_URL','CLOUDTMS_DATABASE_URL_TEST','CLOUDTMS_DATABASE_URL_LIVE',
  'CLOUDTMS_RELEASE_PORTABILITY_PG17_URL','CLOUDTMS_RELEASE_PORTABILITY_PG18_URL','MIGET_API_TOKEN',
  'SUPABASE_SERVICE_ROLE_KEY','DATABASE_URL','MYTMS_CONTROL_URL','BPAY_NEXT_DATABASE_URL',
  'SERVICE_ROLE_KEY','SERVICE_PASSWORD','OTHER_CONNECTION_STRING','GIT_INDEX_FILE','GIT_NAMESPACE','GIT_CONFIG_PARAMETERS',
  'GIT_DIR','GIT_WORK_TREE','NODE_OPTIONS','NODE_PATH','HTTP_AUTHORIZATION','UNKNOWN_CREDENTIAL_LABEL'];
 const inherited={PATH:'approved-system-path',SystemRoot:'approved-system-root',...Object.fromEntries(keys.map(key=>[key,'synthetic-nondisclosing-value']))};
 const result=sourceCheckEnvironment(inherited,context);
 for(const key of keys.filter(key=>!['GIT_DIR','GIT_WORK_TREE'].includes(key)))assert.equal(Object.hasOwn(result,key),false,key);
 assert.equal(result.GIT_DIR,context.gitDir);assert.equal(result.GIT_WORK_TREE,context.workTree);
 assert.equal(result.CLOUDTMS_FINAL_DEFINER_BASE,context.finalDefinerBase);
 assert.equal(result.PATH,inherited.PATH);assert.equal(result.SystemRoot,inherited.SystemRoot);
 assert.equal(inherited.GIT_INDEX_FILE,'synthetic-nondisclosing-value');
});
test('only an explicit exact final-definer context can be introduced',()=>{
 for(const bad of [{...context,finalDefinerBase:'main'},{...context,gitDir:null},{...context,workTree:null}])
  assert.throws(()=>sourceCheckEnvironment({},bad),/SOURCE_CHECK_CONTEXT_REFUSED/);
});
