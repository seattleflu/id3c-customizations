"""
Triage the UW Husky Coronavirus Testing queue.

XXX FIXME

Look up quota for current time, locking "for update".
Select (limit - used) from the priority queue.
Update REDCap.
    XXX How does this work with dry run mode?
Set used = (limit - used).
Commit.

This is idempotent and thus lets us re-run the command whenever we want, e.g.
to pickup a missed cronjob or troubleshoot or fix bugs.  Running the command
more than once won't release extra tests, as it might if we had a fixed rate of
release.

# XXX TODO: As a future improvement, automatically pick up any remaining
# quota left from _past_ timespans in the current day.
#   -trs, 17 Sept 2020

Alternatively, we could use a log/ledger (like we keep in receiving.* tables)
which has credits (tests capacity scheduled for release at a certain time) and
debits (tests offered at a certain time).  While this requires recalculating
the balance every run, we would be able to query when tests were released and
keep more metadata about that.  Alternatively, we could track that same
information by turning our normal logging output into structured event logs.  I
think that's preferrable, so decided not to implement as a ledger right now.

XXX FIXME: lots of FIXME comments below.  Plus, the following views need
creating:

shipping.uw_priority_queue_v1
    purpose is to deduplicate by individual.identifier in priority order

    select distinct on (individual.identifier)
        individual.identifier
        redcap_record_id
        redcap_event_name
        redcap_repeat_instance_id
        *
    from shipping.__uw_priority_queue_v1
    order by priority desc, individual.identifier

shipping.__uw_priority_queue_v1
    purpose is to identify all event instances which indicate testing by study
    criteria, such as:
        - symptoms attested, if within past X days
        - exposure attested 2-3 days ago (so that testing is scheduled for day
          3-4 after exposure; confirm timeline here)
        - marked for surge testing within past X days
        - marked for baseline testing (within past X days?)
        - marked for surveillance testing within past 7 days

    pulls primarily from REDCap instrument fields in warehouse.encounter.details, has columns like:
        individual.identifier

        redcap_record_id
        redcap_event_name
        redcap_repeat_instance_id

        testing_determination_internal fields?

        numeric priority, calculated from event instance details
            priority calculation may be defined as a case statement or a PL/PgSQL function

            priority can likely be calculated by assigning each testing
            indication (see above) a numeric value and adding adjustments for
            timeliness (prioritize older or newer indications?)

        human-readable reason we want to test based on this event instance, for
        internal bookkeeping/troubleshooting/viz/communicating to participant
"""
import click
import logging
from id3c.cli import cli
from id3c.cli.command import with_database_session
from id3c.db.session import DatabaseSession
from ...utils import unwrap


LOG = logging.getLogger(__name__)



@cli.command("triage-uw-priority-queue", help = __doc__)
@with_database_session

@click.option("--at",
    metavar = "<timestamp>",
    default = "now",
    help    = unwrap("""
        Use quota for the given <timestamp>.

        With --dry-run, can be used to test a quota schedule.  Otherwise, can
        be used to apply unused quota from past timespans.  (Though you should
        do that with caution, especially across day boundaries.)

        Defaults to the current time."""))

def triage_uw_test_queue(*, at: str, db: DatabaseSession):
    LOG.debug(f"Triaging the UW Husky Coronavirus Testing queue with quota @ {at}")

    # Lookup the quota for the current time, locking it for update at the end
    # after we make offers.
    quota = db.fetch_row("""
        select
            name,
            timespan,
            "limit",
            used,
            "limit" - used as remaining
        from
            operations.test_quota
        where
            name = 'uw' and timespan @> timestamp with time zone ?
        for update
        """, (at,))

    if not quota:
        LOG.warning(f"No quota row found, aborting")
        return

    if not quota.remaining > 0:
        LOG.warning(f"No quota remaining for {quota.name} during {quota.timespan}, aborting")
        return

    LOG.info(
        f"Quota remaining for {quota.name} during {quota.timespan} "
        f"is {quota.remaining}/{quota.limit} (remaining/limit)")

    # Offer testing to the top entries in our priority queue by updating REDCap.
    offers = db.fetch_rows("""
        select
            redcap_record_id,
            redcap_event_name,
            redcap_repeat_instance_id,
            reason,
            priority
        from
            shipping.uw_priority_queue_v1
        limit
            %s
        """, (quota.remaining,))

    if not offers:
        LOG.info(f"Nothing in the queue")
        return

    offered_count = 0

    # XXX FIXME
    LOG.info(f"Offering testing to ...")

    # Don't actually update REDCap if we're running under --dry-run mode.
    if db.command_action != "rollback":
        # XXX FIXME: update the internal flag in REDCap for each event instance
        # identified in offers
        ...

        # XXX FIXME: this value should probably be based on the REDCap API's
        # return value, in case not all records are succesfully updated.
        offered_count = len(offers)

    # XXX FIXME: maybe also update an internal testing_offered flag to avoid
    # delay of roundtrip thru REDCap?  if we don't do this, then worst case we
    # try to offer testing to the same records more than once?  this is
    # probably more complicated than we want to deal with on the first
    # iteration.
    ...

    updated_quota = db.fetch_row("""
        update operations.test_quota
           set used = used + %s
         where (name, timespan) = (%s, %s)
        returning name, timespan, "limit", used
        """, (offered_count, quota.name, quota.timespan))

    LOG.info(
        f"Quota used for {updated_quota.name} during {updated_quota.timespan} "
        f"is now {updated_quota.used}/{updated_quota.limit} (used/limit).")
