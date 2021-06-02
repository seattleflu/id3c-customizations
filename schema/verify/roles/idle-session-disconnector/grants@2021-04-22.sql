-- Verify seattleflu/id3c-customizations:roles/idle-session-disconnector/grants on pg

begin;

select 1/pg_catalog.has_database_privilege('idle-session-disconnector', :'DBNAME', 'connect')::int;
select 1/pg_catalog.has_schema_privilege('idle-session-disconnector', 'public', 'usage')::int;
select 1/pg_catalog.has_table_privilege('idle-session-disconnector', 'public.pg_stat_activity_nonsuperuser', 'select')::int;
select 1/pg_catalog.has_function_privilege('idle-session-disconnector', 'public.pg_stat_get_activity_nonsuperuser()', 'execute')::int;

select 1/(not pg_catalog.has_table_privilege('idle-session-disconnector', 'pg_catalog.pg_stat_activity', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_table_privilege('idle-session-disconnector', 'public.pg_stat_activity_nonsuperuser', 'insert,update,delete'))::int;
select 1/(not pg_catalog.has_function_privilege('public', 'public.pg_stat_get_activity_nonsuperuser()', 'execute'))::int;

rollback;
