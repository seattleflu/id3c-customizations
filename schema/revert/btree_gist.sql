-- Revert seattleflu/id3c-customizations:btree_gist from pg

begin;

drop extension btree_gist;

commit;
