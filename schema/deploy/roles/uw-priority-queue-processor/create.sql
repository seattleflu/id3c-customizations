-- Deploy seattleflu/id3c-customizations:roles/uw-priority-queue-processor/create to pg

begin;

create role "uw-priority-queue-processor";

comment on role "uw-priority-queue-processor" is
    'Used to prcoess the testing priority queue for the UW Reopening project';

commit;
