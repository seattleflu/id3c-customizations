-- Deploy seattleflu/id3c-customizations:operations/test-quota to pg

begin;

set local search_path to operations;

create table operations.test_quota (
    name text not null,
    timespan tstzrange not null,

    /* Using a reserved word is slightly annoying because we'll have to quote
     * it everywhere… but I dunno, the other words I can think of aren't quite
     * as descriptive.  Going with it for now!
     *   -trs, 17 Sept 2020
     */
    "limit" integer check ("limit" >= 0) not null,
    used integer check (used >= 0) not null default 0,

    primary key (name, timespan),

    constraint test_quota_timespan_non_overlapping_within_name
        exclude using gist (timespan with &&, name with =)
);

create index test_quota_timespan on test_quota using gist (timespan);
create index test_quota_limit on test_quota ("limit");
create index test_quota_used on test_quota (used);

comment on table test_quota is
    'XXX FIXME';
comment on column test_quota.name is
    'XXX FIXME';
comment on column test_quota.timespan is
    'XXX FIXME';
comment on column test_quota."limit" is
    'XXX FIXME';
comment on column test_quota.used is
    'XXX FIXME';

commit;
