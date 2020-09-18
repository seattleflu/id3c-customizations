-- Deploy seattleflu/id3c-customizations:operations/test-quota to pg

begin;

set local search_path to operations;

create table operations.test_quota (
    name text not null,
    timespan tstzrange not null,

    max integer check (max >= 0) not null,
    used integer check (used >= 0) not null default 0,

    primary key (name, timespan),

    constraint test_quota_timespan_non_overlapping_within_name
        exclude using gist (timespan with &&, name with =)
);

create index test_quota_timespan on test_quota using gist (timespan);
create index test_quota_max on test_quota (max);
create index test_quota_used on test_quota (used);

comment on table test_quota is
    'A maximum limit on tests offered (i.e. specimens collected) during a specific timespan';
comment on column test_quota.name is
    'Name identifying a logical quota schedule across timespans';
comment on column test_quota.timespan is
    'Specific period of time over which the quota applies';
comment on column test_quota.max is
    'Maximum number of tests to offer during the timespan';
comment on column test_quota.used is
    'Number of tests offered during the timespan';

commit;
