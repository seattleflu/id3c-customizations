-- Deploy seattleflu/id3c-customizations:operations/schema to pg

begin;

create schema operations;

comment on schema operations is 'Data supporting operations management processes';

commit;
