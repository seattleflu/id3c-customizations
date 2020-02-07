-- Revert seattleflu/id3c-customizations:roles/idle-session-disconnector/create from pg

begin;

drop role "idle-session-disconnector";

commit;
