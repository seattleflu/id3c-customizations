-- Revert seattleflu/id3c-customizations:roles/view-owner/grants to pg
-- requires: roles/view-owner/create seattleflu/schema:roles/reporter

begin;

revoke reporter from "view-owner";

commit;
