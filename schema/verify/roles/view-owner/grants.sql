-- Verify seattleflu/id3c-customizations:roles/view-owner/grants to pg
-- requires: roles/view-owner/create seattleflu/schema:roles/reporter

begin;

do $$
begin
    assert pg_has_role('view-owner', 'reporter', 'usage');
end
$$;

rollback;
