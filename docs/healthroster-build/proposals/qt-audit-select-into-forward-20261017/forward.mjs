import {createHash} from 'node:crypto';
export const sha=b=>createHash('sha256').update(b).digest('hex');
export const base='docs/healthroster-build/proposals/contact-qt-positive-native-proposal-20261017';
export const digest="md5(coalesce(string_agg(md5(to_jsonb(a)::text),'|' order by md5(to_jsonb(a)::text)),''))";
export const before=` audits_before:=(select ${digest} from public.audit_events a);`;
export const after=` select ${digest} into audits_before from public.audit_events a;`;
export function replaceOnce(text,oldValue,newValue){if(text.split(oldValue).length!==2)throw Error('QT_EXACT_SINGLE_BINDING_REQUIRED');return text.replace(oldValue,()=>newValue);}
export function assertionForward(raw){const text=Buffer.isBuffer(raw)?raw.toString('utf8'):raw;const out=replaceOnce(text,before,after);if(replaceOnce(out,after,before)!==text)throw Error('QT_WHOLE_ASSERTION_INVERSE_REQUIRED');return Buffer.from(out);}
export const pretty=o=>JSON.stringify(o,null,2)+'\n';
