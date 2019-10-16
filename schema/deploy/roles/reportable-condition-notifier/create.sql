-- Deploy seattleflu/id3c-customizations:roles/reportable-condition-notifier/create to pg

begin;

create role "reportable-condition-notifier";

comment on role "reportable-condition-notifier" is 'For reportable condition notification routines';


commit;
