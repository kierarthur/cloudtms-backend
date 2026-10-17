// Test-only relocation of one public module specifier. No process or private-state access.
import fs from 'node:fs';
const specifier="'../daily-health-genuine-staged-operator-20261017/contract.mjs'",resolved=JSON.stringify(new URL('../daily-health-genuine-staged-operator-20261017/contract.mjs',import.meta.url).href);
async function load(file){const raw=fs.readFileSync(new URL(file,import.meta.url),'utf8');if(raw.split(specifier).length!==2)throw Error('S0_PID_SINGLE_TEST_LOCATOR_REQUIRED');return import('data:text/javascript;base64,'+Buffer.from(raw.replace(specifier,resolved)).toString('base64'));}
export const ownedWorker=(await load('./payload/owned-worker.mjs')).ownedWorker;
export const originalOwnedWorker=(await load('./reference/owned-worker.mjs')).ownedWorker;
