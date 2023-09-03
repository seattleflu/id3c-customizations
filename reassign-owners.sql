/*
Reassign all database objects (including the database itself) from the current
owner (postgres) to the newly created id3c role.  This gives us a known-good
common role to own things and to which attach admin privileges and members.

Unfortunately, we can't use REASSIGN OWNED statements because we have objects
owned by postgres.

This approach is adapted from Viroverse where I ran into this same issue years ago:
  <https://github.com/MullinsLab/viroverse/blob/ee949dc2/sql/sqitch-initial-setup.sql>

which in turn was adapted from <http://stackoverflow.com/questions/1348126>.

To bad I didn't manage to avoid it this time! :-)

This change can't be run from within Sqitch itself, because the sqitch schema
is locked during sqitch deploy.  When this script tries to change the owner of
the sqitch schema, the deploy deadlocks.

    -trs, 12 July 2019
*/

begin;

alter database :"DBNAME" owner to id3c;

create function eval(text) returns text language plpgsql volatile as $$
    begin
        execute $1;
        return $1;
    end;
$$;

-- Tables (relkind = r), views (v), and sequences (S)
select eval(format('alter table %I.%I owner to id3c', schema_name, relation_name))
  from (select nspname as schema_name, relname as relation_name
          from pg_class
          join pg_namespace on (pg_class.relnamespace = pg_namespace.oid)
         where nspname not like E'pg\\_%'
           and nspname != 'information_schema'
           and relkind in ('r','v','S')
         order by relkind = 'S')
    as s;

-- Schemas
select eval(format('alter schema %I owner to id3c', schema_name))
  from (select nspname as schema_name
          from pg_namespace
         where nspname not like e'pg\\_%'
           and nspname != 'information_schema'
         order by nspname)
    as s;

-- Fix default privileges
update pg_default_acl
   set defaclrole = (select oid from pg_authid where rolname = 'id3c'),
       defaclacl = replace(defaclacl::text, 'postgres', 'id3c')::aclitem[]
 where defaclrole = (select oid from pg_authid where rolname = 'postgres');

drop function eval(text);

commit;
