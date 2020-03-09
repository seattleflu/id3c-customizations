-- Deploy seattleflu/id3c-customizations:roles/view-owner/grants to pg
-- requires: roles/view-owner/create seattleflu/schema:roles/reporter

begin;

grant reporter to "view-owner";

/* This is stupidly necessary for RDS since postgres isn't a true superuser and
 * only gets superuser-like permissions via its CREATE ROLE attribute and broad
 * ownership of schemas, tables, views, etc.
 *
 * Without it, for example, postgres wouldn't be able to select the views owned
 * by view-owner, which is crazy.
 */
grant "view-owner" to postgres;

commit;
