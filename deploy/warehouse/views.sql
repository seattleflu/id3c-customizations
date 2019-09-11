-- Deploy seattleflu/id3c-customizations:views to pg
-- requires: seattleflu/schema:warehouse/location

-- Hello!  All custom views are defined here.  Rework this change with Sqitch
-- to change a view definition or add new views.  This workflow helps keep
-- inter-view dependencies manageable.

begin;


create or replace view warehouse.address as
    select * from warehouse.location where scale = 'address';

comment on view warehouse.address is
    'View of all address-scale locations; for convenience when joining';


create or replace view warehouse.tract as
    select * from warehouse.location where scale = 'tract';

comment on view warehouse.tract is
    'View of all tract-scale locations; for convenience when joining';


commit;
