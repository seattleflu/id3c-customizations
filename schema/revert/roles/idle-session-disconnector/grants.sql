-- Deploy seattleflu/id3c-customizations:roles/idle-session-disconnector/grants to pg

begin;

revoke all on database :"DBNAME" from "idle-session-disconnector";
revoke all on schema receiving, warehouse, shipping from "idle-session-disconnector";
revoke all on all tables in schema receiving, warehouse, shipping from "idle-session-disconnector";

grant connect on database :"DBNAME" to "idle-session-disconnector";

-- Regular users cannot see the state of sessions that they don't own. Create a
-- new view + function using security definer with limited columns that this
-- role can access to see a session's state.
drop view public.pg_stat_activity_nonsuperuser;
drop function public.pg_stat_get_activity_nonsuperuser();

create function public.pg_stat_get_activity_nonsuperuser() returns table(
    pid integer, usename name, application_name text, client_addr inet,
    state_change timestamp with time zone, state text) as
    $$
    select
        pid, usename, application_name, client_addr, state_change, state
        from pg_catalog.pg_stat_activity;
    $$
    language sql
    volatile
    security definer
    set search_path = pg_catalog;

revoke execute
    on function public.pg_stat_get_activity_nonsuperuser
  from public;

grant execute
    on function public.pg_stat_get_activity_nonsuperuser
    to "idle-session-disconnector";

create view public.pg_stat_activity_nonsuperuser as
    select * from public.pg_stat_get_activity_nonsuperuser();

grant select
    on public.pg_stat_activity_nonsuperuser
    to "idle-session-disconnector";

grant pg_signal_backend to "idle-session-disconnector";

commit;
