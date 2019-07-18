-- Deploy seattleflu/id3c-customizations:roles/reportable-condition-processor/create to pg

begin;

create role "reportable-condition-processor";

comment on role "reportable-condition-processor" is 'For reportable condition notification routines';


commit;
