-- Revert seattleflu/id3c-customizations:policies from pg
-- requires: seattleflu/schema:warehouse/presence_absence
-- requires: seattleflu/schema:roles/reporter
-- requires: seattleflu/schema:roles/fhir-processor/create
-- requires: seattleflu/schema:roles/presence-absence-processor
-- requires: roles/reportable-condition-notifier/create
-- requires: roles/hcov19-visibility/create

begin;

alter table warehouse.presence_absence
    disable row level security;

drop policy if exists "public: visible if not HCoV-19"
    on warehouse.presence_absence;

drop policy if exists "hcov19-visibility: visible unconditionally"
    on warehouse.presence_absence;

revoke "hcov19-visibility" from
    "fhir-processor",
    "presence-absence-processor",
    "reportable-condition-notifier";

alter view shipping.presence_absence_result_v1
    owner to current_user;

alter view shipping.presence_absence_result_v2
    owner to current_user;

revoke all
    on receiving.presence_absence
    from reporter;

grant select
    on receiving.presence_absence
    to reporter;

commit;
