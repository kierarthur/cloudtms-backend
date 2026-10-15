// Exact native read extraction, no SQL execution or public/Bank owner edit.
import fs from 'node:fs';import path from 'node:path';import assert from 'node:assert/strict';import{fileURLToPath}from'node:url';
import{readCurrentNativeIndex,sha}from'../checklist-native-invoice-20261014/read-current-native-index.mjs';
const here=path.dirname(fileURLToPath(import.meta.url)),repo=path.resolve(here,'../../../..'),n=readCurrentNativeIndex(repo);
const native=n.index.live.get('public.timesheet_pay_state(uuid,uuid)');assert(native);const source=n.cache.get(native.file);assert.equal(sha(source),native.source_file_sha256);
fs.mkdirSync(path.join(here,'input'),{recursive:true});fs.mkdirSync(path.join(here,'forward'),{recursive:true});
fs.writeFileSync(path.join(here,'input/complete_native_timesheet_pay_state.sqlinc'),native.definition);
const audit=`  BEGIN
    IF p_actor_user_id IS NOT NULL THEN
      PERFORM public._imp_debug_audit(
        p_actor_user_id,
        'TIMESHEET_PAY_STATE',
        jsonb_build_object(
          'timesheet_id', p_timesheet_id::text,
          'has_tsfin', v_has_tsfin,
          'result', v_out
        ),
        'timesheets',
        p_timesheet_id::text
      );
    END IF;
  EXCEPTION WHEN OTHERS THEN
    NULL;
  END;
`;
const hunks=[{before:'CREATE OR REPLACE FUNCTION public.timesheet_pay_state(p_timesheet_id uuid, p_actor_user_id uuid DEFAULT NULL::uuid)',after:'CREATE OR REPLACE FUNCTION private.weekly_source_checklist_legacy_pay_state_core_v1(p_timesheet_id uuid)'},{before:' LANGUAGE plpgsql\n SECURITY DEFINER',after:' LANGUAGE plpgsql STABLE\n SECURITY DEFINER'},{before:audit,after:'  -- Native diagnostic audit belongs to the unchanged public owner only.\n'}];
let s=native.definition;for(const h of hunks){assert.equal(s.split(h.before).length,2);s=s.replace(h.before,()=>h.after);}let reversed=s;for(const h of [...hunks].reverse()){assert.equal(reversed.split(h.after).length,2);reversed=reversed.replace(h.after,()=>h.before);}assert.equal(reversed,native.definition);assert(!s.includes('p_actor_user_id'));
s+='\nalter function private.weekly_source_checklist_legacy_pay_state_core_v1(uuid) owner to postgres;\nrevoke all on function private.weekly_source_checklist_legacy_pay_state_core_v1(uuid) from public,anon,authenticated,service_role;\n';
fs.writeFileSync(path.join(here,'forward/wp_u3_legacy_pay_state_core.sqlinc'),s);
fs.writeFileSync(path.join(here,'COMPLETE_NATIVE_EXTRACTION.json'),JSON.stringify({source_only:true,native_SQL:false,source_commit:n.sourceCommit,source_file:native.file,source_file_sha256:native.source_file_sha256,id:native.id,native_definition_sha256:sha(native.definition),saved_native:'input/complete_native_timesheet_pay_state.sqlinc',output:'forward/wp_u3_legacy_pay_state_core.sqlinc',output_sha256:sha(s),only_native_body_change:'remove optional diagnostic audit; all native classifier and quantities byte-preserved',full_reverse_equal:true,hunks,added_private_ACL:true,public_native_owner_unchanged:true},null,2)+'\n');
console.log(JSON.stringify({source_only:true,whole_native_owner:native.id,finite_hunks:3,public_native_owner_unchanged:true}));
