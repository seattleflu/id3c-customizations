-- Deploy seattleflu/id3c-customizations:roles/latest-results-viewer/grants to pg

begin;

revoke all on database :"DBNAME" from "latest-results-viewer";
revoke all on schema receiving, warehouse, shipping, operations from "latest-results-viewer";
revoke all on all tables in schema receiving, warehouse, shipping, operations from "latest-results-viewer";

grant connect on database :"DBNAME" to "latest-results-viewer";

grant usage
    on schema shipping
    to "latest-results-viewer";

grant select
    on shipping.latest_results
    to "latest-results-viewer";

commit;
