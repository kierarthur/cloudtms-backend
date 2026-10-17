"""Fixed two-repository, public-source-only actual-value reader. No write command."""
from pathlib import Path
import os,re,subprocess
from reseal import must,P
ROOTS={'report':Path('D:/Development/GitHub/.codex-worktrees/healthroster-v2-report-build-13102026'),'root':Path('D:/Development/GitHub/.codex-worktrees/healthroster-install2-14102026')}
class GitValues:
 def __init__(self):
  must(Path.cwd().resolve().as_posix().startswith('D:/Development/GitHub/'),'ACTUAL_CONFIGURED_D_CWD_REQUIRED')
  for owner,root in ROOTS.items():
   must(root.resolve()==root,'PHYSICAL_D_PROJECT_REQUIRED')
   must(Path(self._git(owner,'rev-parse','--show-toplevel').decode().strip()).resolve()==root,'PHYSICAL_REPOSITORY_REQUIRED')
   common=Path(self._git(owner,'rev-parse','--git-common-dir').decode().strip());common=common if common.is_absolute() else root/common
   must(common.resolve()==Path('D:/Development/GitHub/cloudtms-backend/.git'),'PHYSICAL_COMMON_GIT_REQUIRED')
   actual=Path(self._git(owner,'rev-parse','--absolute-git-dir').decode().strip()).resolve()
   must(actual==Path('D:/Development/GitHub/cloudtms-backend/.git/worktrees')/root.name,'PHYSICAL_WORKTREE_GIT_REQUIRED')
 def _git(self,owner,*args):
  must(owner in ROOTS,'FIXED_OWNER_REQUIRED')
  r=subprocess.run(['git','-c','gc.auto=0','-c','maintenance.auto=false','-c','core.autocrlf=false','-c','core.longpaths=true','-C',str(ROOTS[owner]),*args],stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,env={**os.environ,'GIT_OPTIONAL_LOCKS':'0','GIT_NO_LAZY_FETCH':'1'},timeout=15)
  must(r.returncode==0 and len(r.stdout)<=32_000_000,'BOUNDED_REAL_GIT_OBJECT_REQUIRED');return r.stdout
 def file(self,file):
  must(type(file) is str and '\\' not in file and not file.startswith('/') and '..' not in file.split('/') and (file.startswith(P) or file.startswith('scripts/healthroster-')) and not any(x.lower() in ['.auth','private_binding.json','qualified.json','user.json','playwright-test.env'] for x in file.split('/')),'PUBLIC_SOURCE_ONLY_FILE_REQUIRED');return file
 def head(self,owner):
  h=self._git(owner,'rev-parse','HEAD').decode().strip();must(re.fullmatch('[a-f0-9]{40}',h),'REAL_CURRENT_HEAD_REQUIRED');return h
 def commit(self,owner,ref,head):
  must(type(ref) is str and re.fullmatch('[a-f0-9]{40}',ref),'EXACT_REAL_COMMIT_REQUIRED')
  must(self._git(owner,'cat-file','-t',ref)==b'commit\n','EXISTING_COMMIT_OBJECT_REQUIRED');self._git(owner,'merge-base','--is-ancestor',ref,head)
 def blob(self,owner,ref,file):
  must(type(ref) is str and re.fullmatch('[a-f0-9]{40}',ref),'EXACT_REAL_HISTORY_REQUIRED');return self._git(owner,'show',ref+':'+self.file(file))
 def index(self,owner,file):return self._git(owner,'show',':'+self.file(file))
 def saved(self,owner,file):
  f=ROOTS[owner]/self.file(file);must(f.is_file() and not f.is_symlink() and f.resolve()==f and 0<f.stat().st_size<=2_000_000,'BOUNDED_PHYSICAL_PUBLIC_SOURCE_REQUIRED');return f.read_bytes()
