-- Verify seattleflu/id3c-customizations:roles/latest-results-viewer/create on pg

begin;

-- No real need to test that the user was created; the database would have
-- thrown an error if it wasn't.

rollback;
