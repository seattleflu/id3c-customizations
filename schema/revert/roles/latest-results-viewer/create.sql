-- Revert seattleflu/id3c-customizations:roles/latest_results_viewer/create from pg

begin;

drop role "latest-results-viewer";

commit;
