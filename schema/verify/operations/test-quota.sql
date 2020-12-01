-- Verify seattleflu/id3c-customizations:operations/test-quota on pg

begin;

set local search_path to operations;

do $$
begin
    insert into test_quota (name, timespan, max) values
        ('a', tstzrange('2020-09-17 12:00', '2020-09-17 13:00', '(]'), 10),
        ('a', tstzrange('2020-09-17 13:00', '2020-09-17 14:00', '(]'), 20),
        ('b', tstzrange('2020-09-17 12:00', '2020-09-17 13:00', '(]'), 10);

    begin
        insert into test_quota (name, timespan, max) values
            ('a', tstzrange('2020-09-17 00:00', '2020-09-17 01:00', '(]'), 10),
            ('a', tstzrange('2020-09-17 00:30', '2020-09-17 01:30', '(]'), 10);
        assert false, 'insert succeeded';
    exception
        when exclusion_violation then
            null; -- expected
    end;
end
$$;

rollback;
