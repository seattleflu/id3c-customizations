-- Verify seattleflu/id3c-customizations:roles/return-results-exporter/create on pg
-- requires: shipping/views

begin;

-- No real need to test that the user was created; the database would have
-- thrown an error if it wasn't.

rollback;
