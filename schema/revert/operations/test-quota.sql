-- Revert seattleflu/id3c-customizations:operations/test-quota from pg

begin;

drop table operations.test_quota;

commit;
