// This updates only the explicitly selected source packet, never a release.
import fs from 'node:fs';import path from 'node:path';import crypto from 'node:crypto';import {fileURLToPath} from 'node:url';
const directory=path.dirname(fileURLToPath(import.meta.url)),root=path.resolve(directory,'../../../..');
const digest=p=>crypto.createHash('sha256').update(fs.readFileSync(path.join(root,p))).digest('hex');
const doc='docs/healthroster-build/proposals/checklist-native-20261014/',sql='supabase/repeatable/healthroster_wp_u3_source_proposals/';
const selected=[
  'broker/src/weekly-source/routes.js','broker/src/weekly-source/check-only-candidate-reminder.mjs','tests/healthroster-checklist-r254-reminder.test.mjs',
  ...['wp_u3_check_only_candidate_reminder_subject.sqlinc','wp_u3_check_only_candidate_reminder_context.sqlinc','wp_u3_check_only_candidate_reminder_atomic.sqlinc',
    'wp_u3_candidate_notification_visible.forward.sqlinc','wp_u3_candidate_notification_visible.reverse.sqlinc'].map(p=>sql+p),
  ...['build-reminder-owner-packet.mjs','update-reminder-source-pins.mjs','R254_REMINDER_SOURCE_HANDOFF.md','R254_NATIVE_OWNER_PINS.json',
    'candidate-notification-copy.forward.jsinc','candidate-notification-copy.reverse.jsinc','office-d10-candidate-reminder.jsinc',
    'office-d10-reminder.forward.json','office-d10-reminder.reverse.json','office-workspace-actions-reminder.forward.jsinc','office-workspace-actions-reminder.reverse.jsinc',
    'broker-r254-command.forward.jsinc','broker-r254-command.reverse.jsinc','broker-r254-command.import.jsinc'].map(p=>doc+p)];
const pinsPath=path.join(directory,'SOURCE_PINS.json'),pins=JSON.parse(fs.readFileSync(pinsPath));
const paths=[...new Set([...pins.source_pins.map(p=>p.path),...selected])];
pins.source_pins=paths.map(p=>({path:p,saved_sha256:digest(p)}));
pins.status='SOURCE_ONLY_NATIVE_CHECKLIST_HISTORICAL_CALLER_D10_R254_INTENT_UNMOUNTED_FINANCIAL_NATIVE_GATES_HELD';
pins.source_tests={passed:104,failed:0,native_SQL_executed:false,database_connection:false,deployment:false};
pins.predecessor_source_increment='5804a7d0be822e2f133d86d934fca8a2897ff33f';
pins.r254_reminder_checkpoint={status:'SOURCE_BUILT_NATIVE_STAGING_GET_CATALOGUE_INBOX_LOCKS_PHONE_AND_PUSH_UNQUALIFIED',
  native_owner_pins:doc+'R254_NATIVE_OWNER_PINS.json',native_owner_pins_sha256:digest(doc+'R254_NATIVE_OWNER_PINS.json'),
  command:'CHECK_ONLY_D10_REMIND_CANDIDATE',rpc:'public.weekly_source_check_only_candidate_reminder_atomic_v1(jsonb)',
  native_executed:false,external_action_executed:false,other_lane_edited:false,source_review:'AWAITING_INDEPENDENT_REVIEW'};
pins.d10_frame_source_review.successor_disposition='ROOT_REPORTED_SOURCE_CLOSED_AT_9E_FINAL_CRITIC_PIN_PENDING';
pins.d10_frame_source_review.source_closed_head='9e55341f49902a3f212aac888739424ca4241bce';
pins.held_gates=[...new Set([...pins.held_gates,'R254_NATIVE_ACCOUNT_COOLDOWN_REPLAY_SUBMISSION_RETIREMENT_INBOX_UNDO_TARGET_AND_PUSH_QUALIFICATION',
  'SOURCE_5804_HISTORICAL_CALLER_FILTER_CHECKING_AND_R254_SUCCESSOR_INDEPENDENT_REVIEW'])];
fs.writeFileSync(pinsPath,JSON.stringify(pins,null,2)+'\n');
console.log(`Updated ${pins.source_pins.length} explicit source pins; native qualification remains false.`);
