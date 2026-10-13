# EX-08 exact existing route contract

GET `/api/candidates/:id/pay-method-change-preview` and POST `/api/candidates/:id/pay-method-change` retain their existing path/method. Fresh owner dispatch is LEGACY → original handler, NEXT → this closed contract, DISABLED/unavailable → plain typed refusal. Existing module admission/active-admin authority remains. No new command kind or queue protocol.

| Boundary | Exact contract |
| --- | --- |
| GET input | Only new_method(PAYE/UMBRELLA), umbrella_id for Umbrella, after{context,last_key}, limit1..20. Duplicate/unknown keys refuse. |
| Preview RPC | `public.bpay_next_pay_method_change_preview_v1(uuid,uuid,text,uuid,jsonb,integer)`; service-only. |
| Context | Closed native installation/actor/owner epoch/Candidate, original/target method/company/VAT/enabled, financial-view/metadata/workbench/adoption/last-command revisions, original policy/internal official-date key.4KiB. No browser-visible date. |
| Preview result | Exactly context,candidate_name,candidate_reference,umbrella_name,rows,complete,next_cursor,total_member_count,category_counts. Native three counts cover the entire scope. Each typed native row carries original Work/revision/line/segment or Case/component/version or transfer/run, source channel/company, classification, exact decimal source amount, optional closed shared quote, label/detail/original section.20rows/64KiB per page;1,000members/1MiB complete-scope overflow refuses, never truncates. |
| POST input | Exactly operation_id,new_method,expected_old_method,destination_patch,reason,preview_context.8KiB. PAYE patch empty; Umbrella patch exactly umbrella_id. No bank/money/Contract/protected rewrite operands. |
| Apply RPC | `public.bpay_next_pay_method_change_apply_v1(uuid,uuid,jsonb)`; service-only. Exact original actor/Candidate/epoch/request replay precedes mutable-currentness checks. First acceptance writes only Candidate method/company, immutable receipt/members and existing metadata task, in one transaction. |
| Apply result | Exactly accepted,operation_id,candidate_id,pay_method,umbrella_id,metadata_version,task_id,task_generation,affected_member_count,projection_pending. Closed equality/identity checked against original request. It claims accepted/pending, never synchronous projection completion. |
| Refusal | Exactly ok=false,error_code,operation_id(nullable),unknown_outcome. Office presents one sentence; no visible technical code. Foreign/missing POST operation or malformed result remains uncertain. |
| RPC bounds | One fixed RPC request; GET4s/POST8s, streamed64KiB maximum. No automatic auth refresh or mutation retry. Browser boundary6s/10s. |
| Wake | Accepted POST sends only original `{id:task_id,kind:'WORKBENCH'}`. Existing durable task/recovery owns a missed wake. Preview never wakes. |
| Office recovery | Persist endpoint/actor/Candidate namespace and exact serialized request before dispatch, then uncertainty before fetch. Reopen retains original context/epoch/target; chooser hidden, saved native display shown. Explicit Try again sends same bytes. A refusal after prior uncertainty never erases the earlier identity. Accepted native result returns to its original parent only; frame/actor/owner fences prevent late writes. |

Compatible quote/binding consumption stays with later named WB29/WB45/CM11. Existing1429 metadata trigger,0426Work core,1438refresh,0559Caseclassifier,1157pure quote, original queue/protected money owners and LEGACY history handler remain unchanged. NEXT Candidate own-bank edits stay separately on the existing ordinary save path; automatic company display cannot become own-bank intent.

Evidence: implementation boundary/design review, core/Office source reviews, current-owner catalogue, local-isolation catalogue, successor provenance and SCR74 A4 receipt. Normal local compile/catalogue and fixed-local presentation evidence only. All joined protected-Draft/case/returned-transfer/preparation/queue/app checks are AUTHORED_NOT_RUN; no business data or hosted actions were performed.
