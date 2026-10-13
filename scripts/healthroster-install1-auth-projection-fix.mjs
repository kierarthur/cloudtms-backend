import fs from 'node:fs';import path from 'node:path';
import {repoRoot} from './cloudtms-db-release-lib.mjs';
import {digest} from './cloudtms-managed-release-sql.mjs';
const file='broker/src/index.js',full=path.join(repoRoot,file),before=fs.readFileSync(full,'utf8');
const start=before.indexOf('async function sbGetUserById(env, id) {'),end=before.indexOf('\n}',start)+2;
if(start<0||end<start)throw Error('CURRENT_USER_OWNER_MISSING');
const body=before.slice(start,end),old='&select=id,email,role,is_active,password_hash,session_version';
if(body.indexOf(old)<0||body.indexOf(old)!==body.lastIndexOf(old))throw Error('CURRENT_USER_PROJECTION_DRIFT');
const replacement=body.replace(old,old+',payment_authoriser,payment_golden_key');
const after=before.slice(0,start)+replacement+before.slice(end);
fs.writeFileSync(full,after);
fs.writeFileSync(path.join(repoRoot,'docs/healthroster-build/install1-source/PP_CURRENT_USER_PROJECTION_FIX.json'),JSON.stringify({
 status:'FORWARD_SOURCE_FIX_NATIVE_HTTP_PENDING',file,beforeSha256:digest(before),afterSha256:digest(after),
 beforeOwnerSha256:digest(body),afterOwnerSha256:digest(replacement),
 fix:'Read both existing payment flags on the actual requireUser current-row transport. Preserve all original fields, session/tenant/token and password-update behavior; no permission inferred from token/body.'},null,2)+'\n');
