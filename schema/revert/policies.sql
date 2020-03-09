-- Deploy seattleflu/id3c-customizations:policies to pg
-- requires: seattleflu/schema:warehouse/presence_absence
-- requires: seattleflu/schema:roles/reporter
-- requires: seattleflu/schema:roles/fhir-processor/create
-- requires: seattleflu/schema:roles/presence-absence-processor
-- requires: seattleflu/schema:shipping/views@2020-02-25
-- requires: roles/reportable-condition-notifier/create
-- requires: roles/hcov19-visibility/create
-- requires: roles/view-owner/create

begin;

/* This change is intended to comprehensively describe our row-level security
 * policies and be easily reworkable for future changes.  Statements should be
 * idempotent.
 */

alter table warehouse.presence_absence
    enable row level security;

drop policy if exists "public: visible if not HCoV-19"
    on warehouse.presence_absence;

create policy "public: visible if not HCoV-19"
    on warehouse.presence_absence
    as permissive
    for all to public using (
        target_id not in (
            select
                target_id
            from
                warehouse.target
                join warehouse.organism using (organism_id)
            where
                lineage <@ 'Human_coronavirus.2019'
        )
    )
;

drop policy if exists "hcov19-visibility: visible unconditionally"
    on warehouse.presence_absence;

create policy "hcov19-visibility: visible unconditionally"
    on warehouse.presence_absence
    as permissive
    for all to "hcov19-visibility" using (true)
;

/* This grant seems more closely coupled to our policies than the roles
 * themselves, so it lives here rather than adjacent to either the granted or
 * grantee roles.
 */
grant "hcov19-visibility" to
    "fhir-processor",
    "presence-absence-processor",
    "reportable-condition-notifier";

/* Normal p/a results don't get HCoV-19 visibility.  We'll make
 * separate views as necessary.
 *
 * XXX TODO: Statements are here since these are core views.  There's a bad
 * interplay where the owner will revert back to postgres if the core views are
 * dropped and re-created.  This is similar to ACLs interplay with
 * shipping.reportable_condition_v1 I wrote about in
 * schema/deploy/shipping/views.sql.
 *   -trs, 7 March 2020
 */
alter view shipping.presence_absence_result_v1
    owner to "view-owner";

alter view shipping.presence_absence_result_v2
    owner to "view-owner";

/* Adjust ACLs on a core table.
 *
 * XXX TODO: Suffers a bad interplay with core ID3C if the roles/reporter
 * change gets reworked, similar to the same situation with views referred to
 * above.
 *   -trs, 7 March 2020
 */
revoke all
    on receiving.presence_absence
    from reporter;

grant select (presence_absence_id, received, processing_log)
    on receiving.presence_absence
    to reporter;

revoke all
    on receiving.presence_absence
    from "hcov19-visibility";

commit;
