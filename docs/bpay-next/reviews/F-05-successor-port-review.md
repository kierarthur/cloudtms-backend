# F-05 successor port — independent source review

**PASS** — reviewed saved source on9 October2026. No defects found.

No product edits, SQL execution, tests, fixtures, application runtime or hosted calls. Root-reported local changed-schema compilation is acknowledged as builder evidence, not rerun or claimed as independent installed/app proof.

Reviewed `supabase/repeatable/09102026_2018_banking_pay_next_foundation_pending_successors_v1.sql` (4346 lines), raw SHA256 `7433AC2A1CCFE64613666EED4FF672E81C67A36BB8C9ABBC6585833B9DCBE09B`, plus F05 provenance, current F04 registry, exact saved RESET18 sources, preserved18 BUILD-BE roots and F06 dependency source.

## Declaration/body/config and scope

Independently extracted exactly22 declarations, with no extra or missing signatures. All22 full CREATE statements (including argument declarations, RETURNS, language, security mode, search_path/config and complete body) match the saved RESET declarations byte-for-byte after only LF normalization. All22 separately computed dollar-body SHA256 values also match. All18 RESET raw source hashes match provenance; all18 old BUILD-BE raw roots remain exactly the recorded upstream source. Therefore no unrelated function in those roots was overwritten.

The successor adds no table/index/ALTER-table migration or release-control DDL. Outside function bodies it contains22 owner statements,22 complete revokes, service-only EXECUTE grants for the5 public routines, one schema reload notification and transaction controls. Every owner is logical postgres, subsequently handled by the existing audited managed owner mapping; no browser role retains an added grant. Private routines remain unavailable to public/anon/authenticated/service_role directly. Public invocation admission remains the identical reviewed body.

Only the18 reviewed F05 rows contribute bodies.1500 remains relocation-only, with no CREATE or DROP;1157 is the target-valuer final physical/root authority. The unreviewed1620 reservation correction is not selected; the copied RESET1620 declaration/hash is the ordinary F05 source. No pack comparison schema/body is imported. The unchanged Source lock-inventory stays final at0557; only the changed first-authorise signature moves to2018, preserving Source query admission, outer stage/publication and delayed metadata-only initial binding.

All22 canonical signatures have one registry entry with finalPhysical/finalRoot/finalClosures at2018 and allowedPhysical admitting that root. Canonical timestamptz identity is timestamp with time zone.2018 sorts after its historical03–07 October roots and the first-authorise predecessor reassertion; it is real new scaffolded source. Earlier historical bodies remain registered predecessors and the actual final closure can be selected after them. No body was replaced by a changed-path metadata-only reassertion.

F06 canonical source dependencies are present: freshly scaffolded2010–2013 migrations, source-binding1246, receipt-binding1301, initial/Daily0018, channel0236 and shared1157. They provide the schema and late-bound helper inputs for the exact reviewed binding/Daily/channel deltas; F05/F06 must remain one reviewed release unit. This static review does not assert runtime first-use or hosted catalogue equality.

## Exact independently matched routines

| Deliverable / exact signature | RESET line → successor line | Declaration SHA256 | Function body SHA256 |
| --- | --- | --- | --- |
| F-05-D02 `private.bpay_next_publish_ordinary_from_financial_v1(uuid)` | 8 → 9 | `1C98823563232936D0F2D04A6816977A44DC3DADAF1CC1351A537814B4DA95C2` | `90FA9AF33F256B94D999B1F2E0862A45B0934E37371ECE21D1C79031F635ED4F` |
| F-05-D01 `private.bpay_next_claim_prepare_job_v1(uuid,integer)` | 8 → 633 | `5143036036BD632E6C396C9351CEA38ABD0F2141FBF00738FE5F8AB79E167C23` | `F4C4F746A8C39C7F0B34DDCD7E1233A1AB5E370E141F31974CEE569659C67381` |
| F-05-D06 `private.bpay_next_capture_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | 9 → 739 | `0EFAF0E53DDF84B5FFFCD9564C8528AE9EB2B785BA6E2BF3174A2334BA138B6E` | `9B47CCAFC1A5BE15A8632254DB2DD8C3C5CC5AFD2F4F597D6BC7EE6E9691F4B4` |
| F-05-D05 `private.bpay_next_hold_fresh_position_page_v1(uuid,uuid,bigint,uuid,text,integer)` | 10 → 1161 | `58F36E5E46ABA156268B483361AD9F746FA264C5C5C892C45EA7711DD439AA9B` | `C978996A9A30A11041D1A4FFA001593E98107FCEEACC98CA4B2FF60891171981` |
| F-05-D04 `private.bpay_next_confirm_simple_run_v1(uuid,bigint)` | 89 → 1445 | `B36DDB1640122B3FAE1530968B80A5933575294864F0EAB4AE8BEAAF2FEFE54F` | `9D748E82E5FA7B884BD4971D6DFE3F707380DCCBACE4FE74EE140C26458BCB61` |
| F-05-D07 `private.bpay_next_begin_simple_transfer_v1(uuid,uuid)` | 7 → 1524 | `A51FE68C3928BA68DCACC425FF1FA0B798146CA35F4A9A5121DF58D2CC140C4F` | `D6CEA579BD2BCBD6F7FD7479FD35765AC7298C3DE1EE9E4AD89C9ECDF7CE68F7` |
| F-05-D16 `private.bpay_next_build_simple_transfer_page_v1(uuid,uuid,bigint,bigint,integer)` | 67 → 1655 | `4105701BBF963146DDB36C1C82B1298E0319D403E0B43FA72A8488F6D595DF55` | `3ED257D724AE31D77A4F07FA7D9A58370843578CE5C77F3A341A0DEACD8AC40F` |
| F-05-D15 `private.bpay_next_bind_simple_csv_destination_v1(uuid)` | 9 → 1859 | `6CD95C74C82CAFE69E4F4532B741C92AF2B4F1451CB622912BB0807EFE4879B1` | `A9CA70E729C29B3A1BC2A0D687A41C3297CFDC856BA5A45CAB8BD759BBB7C13E` |
| F-05-D14 `private.bpay_next_issue_simple_csv_v1(uuid,uuid,uuid)` | 9 → 1990 | `C8DDB38EED1AF7360D341D928A0D19E2358F2C4CE166AA8D9792D89C7E6DF6DA` | `A17A7767A9C30FF89C79E404552E6462A5C556939B9FF5D050C9A23B50DAA401` |
| F-05-D13 `private.bpay_next_receive_simple_csv_settlement_v1(uuid,uuid,text,numeric,timestamp with time zone,uuid)` | 6 → 2191 | `AE2D353CCF0244095A614D000F4A25CE7F8641C212ACA1CB07334F1BC4DC3B40` | `0A7061B754751969188E8A72B455917C837179395C46B7D753824FC4E6386141` |
| F-05-D13 `private.bpay_next_post_simple_outcome_page_v1(uuid,uuid,bigint,bigint,integer)` | 154 → 2298 | `9E9C81A0476B4462E9F675A14E167F2884AFB294D48FD65EF870302708011E88` | `387A9360DD3AAC71BD25F6BA9561C9E95F1FE9BF87C07EBF0E2826AC95FE780C` |
| F-05-D12 `private.bpay_next_receive_simple_reissue_v1(uuid,uuid,uuid)` | 5 → 2536 | `A7947B2F01A5DD411F387C22F0568652FFBA8F602CF592A14FB2A4293E8EBABB` | `C2FD2890F8CA84DF17900EC82A7A20953FA8C1F258E212649BE81EC0A8F1A692` |
| F-05-D11 `public.bpay_next_original_csv_advice_page_v1(uuid,uuid,text,uuid,uuid,uuid,uuid,bigint,integer,integer,text,text,text,integer)` | 24 → 2610 | `E5ECBAF10E461F6D227CA1EADC148663A5DDD2E221A9C5CA2744F605CD71D1AE` | `C1AEE07EC6B48CE1AF8A47396EBF6FD9902EC437F5734658EFC9E476F054F828` |
| F-05-D10 `private.bpay_next_simple_cancel_block_v1(uuid,boolean)` | 13 → 2869 | `6FAED06FD1EB50647D53B486E7581EDA060B3E24FF24F1195EBB087565F34368` | `9EABA6D0E566FB234B1938000F3CBD45CB31E9826D906DE5D866BC58959817D4` |
| F-05-D09 `public.bpay_next_financial_intake_v1(uuid,text,jsonb)` | 9 → 3044 | `120311D07CDACC85C4CF778D46BD397C2F6559F10F05064C4EE422E1C27B1B9F` | `9D13050C95C56E8CAE6D7FE1DE6BB3476D338999FEE4D2C6DCBC771CCE512F2A` |
| F-05-D08 `private.bpay_next_apply_week_outcome_v1(uuid,uuid)` | 8 → 3331 | `5C289FAB6A826902E0EC2AC52174690269B9CAA96DD100CEAD23B63D895AC19F` | `390CC16A576358A203C7DCE0405266FBA751E10628A204162999E283A003519B` |
| F-05-D19 `public.weekly_source_first_authorise_v1(uuid,uuid,text,uuid)` | 93 → 3528 | `31C66A3DE3C4A9053E0A4A74FB63CEED6269C3DAC6A11ABC259A9D443F881B51` | `EDEC88A6BC4199DF3210405693B30A61BAA0086B8ADC4A14CCE4B3E407E74209` |
| F-05-D18 `private.bpay_next_preparation_args_v1(jsonb,text[])` | 25 → 3621 | `38CFA6EB072D361ECC434D86D214ACFD5FB1A641B9C3EFAFB66CD17FF84979DB` | `661BB31693D4C12D3D1543DF5599BB7C29DE3AD2C14A99845EF0B26304D7EED5` |
| F-05-D18 `public.bpay_next_preparation_intake_v1(uuid,text,jsonb)` | 78 → 3678 | `13D5351E6880219E4A0878C55EE7170387CB831907CAAF037F92D92E393D9912` | `10B2A036F38D24C36FA5826BB484089B62AEF6C3E30FD2FB6F8889203F0B9FD9` |
| F-05-D18 `public.bpay_next_preparation_page_v1(uuid,text,jsonb)` | 171 → 3776 | `D056EE51FACE1A12BFB7D0D2B29DD77AA5960941CE87CD241FB37CBF53AC05F9` | `2BAAC0636E5F41B6598C60709259D7CAB9AA48527B87E4CC69A40160A385EBFA` |
| F-05-D17 `private.bpay_next_reconcile_work_collection_v1(uuid,uuid,text,uuid)` | 325 → 3955 | `5D5E0CFAE5250C60C199176C60A083576DE764E0730FB3FE7B167CE0F5BDD02B` | `CAE40567EFDB2FEC9DE1348BFD79433CDD34FACFE6BE7B24053D14074BD554D5` |
| F-05-D17 `private.bpay_next_apply_work_collection_recovery_v1(uuid,uuid)` | 632 → 4263 | `861ECE5C6276399517563AF9FB1880BB1CE90D45717939B88C75A92CD488F410` | `732D3953FE57FD9CA17D8DBAA13AF5BA31E828BF795445E5594E0DF8B0ABA47F` |

## Verification limits

Source identity and implementation scope reviewed independently; no tests or database runtime executed. Current raw hashes are a point-in-time saved-source proof. Re-read if builder edits the successor or registry after this review. INSTALLED and TESTED in app remain unclaimed. Full raw-path/ACL/registry findings are in the normal-backend BPAY_NEXT_F05_INDEPENDENT_SOURCE_REVIEW_20261009.json companion.
