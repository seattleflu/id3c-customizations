-- Deploy seattleflu/id3c-customizations:warehouse/site/data to pg
-- requires: seattleflu/schema:warehouse/site

begin;

delete from warehouse.site
  where identifier in ('ChildcareCenter70thAndSandPoint', 'ChildcareSwabNSend', 'UWChildrensCenterRadfordCourt')
;

commit;
