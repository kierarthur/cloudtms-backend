// Fixed mechanical-only profile. Importing this module opens no connection.
import fs from 'node:fs';
import path from 'node:path';
import {execFileSync} from 'node:child_process';

export const profile = Object.freeze({
 version:'HEALTHROSTER_INSTALL2_MECHANICAL_SEAL_V1',
 root:'D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026',
 branch:'codex/healthroster-install2-14102026',
 task:'hr-i2-seal-20261017', container:'hr-i2-seal-pg17-20261017',
 volume:'hr-i2-seal-data-20261017', image:'postgres:17.11-bookworm',
 port:'55448', database:'banking_modal_v2_test', owner:'cloudtms_hr_install2_seal_owner',
 output:'.codex-tmp/healthroster-install2-mechanical-seal-20261017'
});
const requireThat=(ok,code)=>{if(!ok)throw Error(code);};
const sha=/^[a-f0-9]{40}$/;
export function parseArguments(args){
 requireThat(Array.isArray(args)&&args.length===2&&args[0]==='NEW'
  &&typeof args[1]==='string'&&/^--candidate=[a-f0-9]{40}$/.test(args[1]),'I2_SEAL_FRESH_ARGUMENTS_REQUIRED');
 return args[1].slice('--candidate='.length);
}
export function validateTarget(raw,{allowLocal,logicalOwner,passwordPresent}={}){
 let value;try{value=new URL(raw);}catch{throw Error('I2_SEAL_LOCAL_TARGET_REFUSED');}
 requireThat(['postgres:','postgresql:'].includes(value.protocol)&&value.hostname==='127.0.0.1'
  &&value.port===profile.port&&value.pathname==='/'+profile.database
  &&value.username===profile.owner&&value.password===''&&value.search===''&&value.hash===''
  &&allowLocal==='1'&&logicalOwner==='CURRENT_USER'&&passwordPresent===true,'I2_SEAL_LOCAL_TARGET_REFUSED');
 return value;
}
export function validateVolume(value){
 requireThat(value?.Name===profile.volume&&value.Driver==='local'&&value.Scope==='local'
  &&value.Labels?.['codex.task']===profile.task
  &&(value.Options==null||Object.keys(value.Options).length===0),'I2_SEAL_OWN_VOLUME_REFUSED');
 return {name:profile.volume,driver:'local',task:profile.task};
}
export function validateContainer(value,imageId){
 requireThat(/^sha256:[a-f0-9]{64}$/.test(imageId??'')&&value?.Image===imageId
  &&/^[a-f0-9]{64}$/.test(value.Id??'')&&value.Name==='/'+profile.container
  &&value.Config?.Image===profile.image&&value.Config.Labels?.['codex.task']===profile.task
  &&JSON.stringify(value.Config.Cmd)===JSON.stringify(['postgres','-c','jit=off','-c','pg_show_plans.is_enabled=off'])
  &&value.HostConfig?.Privileged===false&&value.HostConfig.NetworkMode==='bridge'
  &&JSON.stringify(value.HostConfig.PortBindings)===JSON.stringify({'5432/tcp':[{HostIp:'127.0.0.1',HostPort:profile.port}]})
  &&Array.isArray(value.Mounts)&&value.Mounts.length===1
  &&value.Mounts[0].Type==='volume'&&value.Mounts[0].Name===profile.volume
  &&value.Mounts[0].Destination==='/var/lib/postgresql/data'&&value.Mounts[0].RW===true,
  'I2_SEAL_OWN_CONTAINER_REFUSED');
 return {container:profile.container,containerId:value.Id,image:profile.image,imageId,
  volume:profile.volume,port:Number(profile.port),task:profile.task};
}
// Only these metadata projections are requested: Docker Env is never read.
export const dockerImageArgs=Object.freeze(['image','inspect','--format','{{.Id}}',profile.image]);
export const dockerVolumeArgs=Object.freeze(['volume','inspect','--format',
 '{"Name":{{json .Name}},"Driver":{{json .Driver}},"Scope":{{json .Scope}},"Labels":{{json .Labels}},"Options":{{json .Options}}}',profile.volume]);
export const dockerContainerArgs=Object.freeze(['inspect','--format',
 '{"Id":{{json .Id}},"Image":{{json .Image}},"Name":{{json .Name}},"Config":{"Image":{{json .Config.Image}},"Labels":{{json .Config.Labels}},"Cmd":{{json .Config.Cmd}}},"HostConfig":{"Privileged":{{json .HostConfig.Privileged}},"NetworkMode":{{json .HostConfig.NetworkMode}},"PortBindings":{{json .HostConfig.PortBindings}}},"Mounts":{{json .Mounts}}}',profile.container]);
export function inspectOwnResources(){
 const read=args=>{try{return execFileSync('docker',args,{encoding:'utf8',windowsHide:true,maxBuffer:2000000}).trim();}
  catch{throw Error('I2_SEAL_OWN_DOCKER_METADATA_REQUIRED');}};
 const imageId=read(dockerImageArgs);
 let volume,container;try{volume=JSON.parse(read(dockerVolumeArgs));container=JSON.parse(read(dockerContainerArgs));}
 catch{throw Error('I2_SEAL_OWN_DOCKER_METADATA_REFUSED');}
 validateVolume(volume);return validateContainer(container,imageId);
}
export function assertProject(root,candidate){
 requireThat(sha.test(candidate??''),'I2_SEAL_CANDIDATE_REFUSED');
 const physical=fs.realpathSync.native(root).replaceAll('\\','/');
 requireThat(physical.toLowerCase()===profile.root.toLowerCase(),'I2_SEAL_PHYSICAL_ROOT_REFUSED');
 const git=args=>execFileSync('git',['-c','gc.auto=0','-c','maintenance.auto=false',...args],
  {cwd:root,encoding:'utf8',windowsHide:true,maxBuffer:8*1024*1024});
 requireThat(git(['rev-parse','--show-toplevel']).trim().replaceAll('\\','/').toLowerCase()===physical.toLowerCase()
  &&git(['rev-parse','HEAD']).trim()===candidate&&git(['branch','--show-current']).trim()===profile.branch,
  'I2_SEAL_CANDIDATE_CHANGED');
 for(const option of ['--absolute-git-dir','--git-common-dir']){
  const metadata=fs.realpathSync.native(path.resolve(root,git(['rev-parse',option]).trim())).replaceAll('\\','/');
  requireThat(metadata.toLowerCase().startsWith('d:/development/github/'),'I2_SEAL_GIT_STORAGE_REFUSED');
 }
 requireThat(git(['status','--porcelain=v1','--untracked-files=no']).trim()==='','I2_SEAL_TRACKED_SOURCE_DIRTY');
 requireThat(git(['ls-files','--others','--exclude-standard','--','scripts','supabase']).trim()==='',
  'I2_SEAL_UNTRACKED_EXECUTABLE_SOURCE_REFUSED');
 return {candidate,branch:profile.branch,physicalRoot:physical};
}
export function configureLocalProcess(env){
 validateTarget(env.CLOUDTMS_DATABASE_URL,{allowLocal:env.CLOUDTMS_ALLOW_LOCAL,
  logicalOwner:env.CLOUDTMS_LOGICAL_POSTGRES_OWNER,passwordPresent:typeof env.PGPASSWORD==='string'&&env.PGPASSWORD.length>0});
 delete env.PGSERVICE;delete env.PGSERVICEFILE;
 env.PGHOSTADDR='127.0.0.1';env.PGHOST='127.0.0.1';env.PGPORT=profile.port;
 env.PGDATABASE=profile.database;env.PGUSER=profile.owner;
 env.PGOPTIONS='-c jit=off -c pg_show_plans.is_enabled=off';
 // Preserve the canonical library's temp-tree mechanism, with task-owned D: storage.
 env.TEMP=path.join(profile.root,profile.output,'tmp');env.TMP=env.TEMP;
}
