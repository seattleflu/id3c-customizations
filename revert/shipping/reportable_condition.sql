-- Revert seattleflu/id3c-customizations:shipping/reportable_condition from pg

begin;

drop table shipping.reportable_condition;

commit;
