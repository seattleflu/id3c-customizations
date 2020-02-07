-- Revert seattleflu/id3c-customizations:roles/idle-session-disconnector/grants from pg

begin;

revoke pg_signal_backend from "idle-session-disconnector";

revoke select
    on public.pg_stat_activity_nonsuperuser
  from "idle-session-disconnector";

drop view public.pg_stat_activity_nonsuperuser;

revoke execute
    on function public.pg_stat_get_activity_nonsuperuser
  from "idle-session-disconnector";

drop function public.pg_stat_get_activity_nonsuperuser;

revoke all on database :"DBNAME" from "idle-session-disconnector";
revoke all on all tables in schema public from "idle-session-disconnector";
revoke all on schema public from "idle-session-disconnector";

commit;
