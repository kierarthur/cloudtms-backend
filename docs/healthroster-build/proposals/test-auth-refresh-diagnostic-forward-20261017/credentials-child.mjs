// Import only in the standalone Playwright child, after its browser is launched.
import {ENV_FILE,check} from './policy.mjs';
export function credentials(){
 check(typeof process.send==='function'&&process.argv.includes('--owned-auth-worker'),'AUTH_WORKER_ONLY');
 const before={...process.env};let email,password;
 try{
  process.loadEnvFile(ENV_FILE);
  email=process.env.E2E_USER_EMAIL;password=process.env.E2E_USER_PASSWORD;
 }finally{
  for(const key of Object.keys(process.env))if(!Object.hasOwn(before,key))delete process.env[key];
  for(const[key,value]of Object.entries(before))process.env[key]=value;
 }
 check(typeof email==='string'&&email.length>0&&typeof password==='string'&&password.length>0,'AUTH_APPROVED_ENV_UNAVAILABLE');
 return {email,password};
}
