-- Revert seattleflu/id3c-customizations:operations/deliverables_log from pg

begin;

drop table if exists operations.deliverables_log;

commit;
