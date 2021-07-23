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
    disable row level security;

drop policy if exists "public: visible if not HCoV-19"
    on warehouse.presence_absence;

drop policy if exists "hcov19-visibility: visible unconditionally"
    on warehouse.presence_absence;

/* This grant seems more closely coupled to our policies than the roles
 * themselves, so it lives here rather than adjacent to either the granted or
 * grantee roles.
 */
revoke "hcov19-visibility" from
    "fhir-processor",
    "presence-absence-processor",
    "reportable-condition-notifier";

/* XXX TODO: Statements are here since these are core views.  There's a bad
 * interplay where the owner will revert back to postgres if the core views are
 * dropped and re-created.  This is similar to ACLs interplay with
 * shipping.reportable_condition_v1 I wrote about in
 * schema/deploy/shipping/views.sql.
 *   -trs, 7 March 2020
 */
alter view shipping.presence_absence_result_v1
    owner to current_user;

alter view shipping.presence_absence_result_v2
    owner to current_user;

/* Adjust ACLs on a core table.
 *
 * XXX TODO: Suffers a bad interplay with core ID3C if the roles/reporter
 * change gets reworked, similar to the same situation with views referred to
 * above.
 *   -trs, 7 March 2020
 */
grant select
    on receiving.presence_absence
    to reporter;

revoke select (presence_absence_id, received, processing_log)
    on receiving.presence_absence
  from reporter;

revoke select
    on receiving.presence_absence
  from "hcov19-visibility";

commit;
