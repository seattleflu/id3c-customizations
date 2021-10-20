-- Revert seattleflu/id3c-customizations:roles/latest-results-viewer/grants from pg

begin;

revoke select
    on shipping.latest_results
  from "latest-results-viewer";

revoke usage
    on schema shipping
  from "latest-results-viewer";

revoke connect on database :"DBNAME" from "latest-results-viewer";


commit;
