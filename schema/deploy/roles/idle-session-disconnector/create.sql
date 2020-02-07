-- Deploy seattleflu/id3c-customizations:roles/idle-session-disconnector/create to pg

begin;

create role "idle-session-disconnector";

comment on role "idle-session-disconnector" is
    'For disconnecting idle database sessions';

commit;
