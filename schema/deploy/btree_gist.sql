-- Deploy seattleflu/id3c-customizations:btree_gist to pg

begin;

create extension btree_gist with schema public;

commit;
