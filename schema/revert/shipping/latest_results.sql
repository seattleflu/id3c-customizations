-- Revert seattleflu/id3c-customizations:shipping/latest_results from pg

begin;

drop table if exists shipping.latest_results;

commit;
