-- Verify seattleflu/id3c-customizations:roles/view-owner/create on pg

begin;

set local role "view-owner";

rollback;
