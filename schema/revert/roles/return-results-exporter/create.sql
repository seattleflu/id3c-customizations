-- Revert seattleflu/id3c-customizations:roles/return-results-exporter/create from pg
-- requires: shipping/views

begin;

drop role "return-results-exporter";

commit;
