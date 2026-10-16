/** Canonical positive decimal string <= SQL BIGINT maximum; runtime validated. */
export type SettingsRevision = string;
export type IntervalUnit = 'HOURS' | 'CALENDAR_DAYS' | 'CLIENT_WORKING_DAYS';
export interface ManagerFollowup {
 sequence: number; interval_value: number; interval_unit: IntervalUnit; mode: 'MANUAL' | 'AUTOMATIC';
}
export interface GlobalContactSettings {
 candidate_reminder_minutes: number;
 candidate_response_deadline_minutes: number;
 manager_digest_minutes: number;
 manager_manual_resend_cooldown_minutes: number;
 candidate_manual_reminder_cooldown_minutes: number;
 manager_secure_link_days: number;
 automatic_reconcile_actor_user_id: string | null;
 manager_first_delay_value: number;
 manager_first_delay_unit: IntervalUnit;
 manager_followups: ManagerFollowup[];
 manager_recent_completed_weeks: number;
 manager_max_work_age_days: number;
}
export interface GlobalContactSettingsSave {
 request_id: string;
 expected_settings_version: SettingsRevision;
 settings: Omit<GlobalContactSettings,'automatic_reconcile_actor_user_id'> & {automatic_reconcile_actor_user_id?: string | null};
}
export interface GlobalContactSettingsState {
 ok: true; settings_version: SettingsRevision; policy_revision: SettingsRevision;
 settings: GlobalContactSettings;
 cutoff: {kind:'CLIENT_CUTOFF_PRECEDENCE'; editable:false; explanation:string};
 actions: [{action:'SAVE_GLOBAL_CONTACT_TIMING'; enabled:true; reason:null}];
}
/** Additive generated-row shape for the five columns of the existing singleton. */
export interface GlobalContactTimingColumns {
 manager_first_delay_value: number; manager_first_delay_unit: IntervalUnit;
 manager_followups: ManagerFollowup[]; manager_recent_completed_weeks: number; manager_max_work_age_days: number;
}
export interface GlobalSettingsReceiptRow {
 request_id:string; agency_id:string; environment:'TEST'|'LIVE'; actor_user_id:string;
 request_digest:unknown; expected_settings_version:SettingsRevision; accepted_settings_version:SettingsRevision;
 response_json:GlobalContactSettingsState; created_at_utc:string; completed_at_utc:string;
}
