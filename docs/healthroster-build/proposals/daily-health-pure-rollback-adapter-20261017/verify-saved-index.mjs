// Read-only owned-file comparison. Does not stage, commit, repair or prune.
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';
import {here,sha,requireThat} from './source.mjs';
const git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false','-C',here,...args],{windowsHide:true,maxBuffer:2*1024*1024});
const top=git(['rev-parse','--show-toplevel']).toString('utf8').trim(),relative=path.relative(top,here).replaceAll('\\','/');
const saved=fs.readdirSync(here).filter(file=>fs.statSync(path.join(here,file)).isFile()).sort();
const indexed=git(['ls-files','--full-name','--','.']).toString('utf8').trim().split(/\r?\n/).filter(Boolean).map(f=>path.posix.basename(f)).sort();
requireThat(JSON.stringify(saved)===JSON.stringify(indexed),'OWNED_SAVED_INDEX_FILE_SET_MISMATCH');
for(const file of saved)requireThat(fs.readFileSync(path.join(here,file)).equals(git(['show',':'+relative+'/'+file])),'OWNED_SAVED_INDEX_BYTES_MISMATCH');
console.log(JSON.stringify({status:'OWNED_SAVED_INDEX_EQUAL',files:saved.length,
 executableManifestSha256:sha(fs.readFileSync(path.join(here,'SOURCE_MANIFEST.json'))),sourcePinsSha256:sha(fs.readFileSync(path.join(here,'SOURCE_PINS.json'))),
 gitMaintenance:{gcAuto:0,maintenanceAuto:false},native:'NOT_RUN'}));
