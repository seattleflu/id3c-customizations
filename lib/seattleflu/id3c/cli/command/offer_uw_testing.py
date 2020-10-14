"""
Offer UW Husky Coronavirus Testing.

Updates REDCap to trigger offers of testing to the individuals in the priority
queue based on the testing capacity quotas for the given time period.

This command is idempotent and can be safely re-run to e.g. pickup a missed
cronjob or troubleshoot or fix bugs.  Running the command more than once won't
release extra tests above the quota.
"""
import click
import logging
import os
import id3c.cli.redcap as redcap
from datetime import date
from id3c.cli import cli
from id3c.cli.command import with_database_session
from id3c.db.session import DatabaseSession
from ...utils import unwrap


LOG = logging.getLogger(__name__)

TODAY = date.today().isoformat()


@cli.command("offer-uw-testing", help = __doc__)
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

def offer_uw_testing(*, at: str, db: DatabaseSession):
    LOG.debug(f"Offering UW Husky Coronavirus Testing @ {at}")

    # This uses a mutable quota to track available vs. used testing capacity
    # for given time periods.  An alternate approach would be to use a
    # log/ledger (like we keep in receiving.* tables) which records credits
    # (tests capacity scheduled for release at a certain time) and debits
    # (tests offered at a certain time).  While this requires recalculating the
    # balance every run, we would be able to query when tests were released and
    # keep more metadata about that.  These same benefits could be realized by
    # turning our normal logging output into structured event logs.  I think
    # that's preferrable, so decided not to implement as a ledger right now.
    #   -trs, 17 Sept & 13 Oct 2020

    # Lookup the quota for the current time, locking it for update at the end
    # after we make offers.
    #
    # XXX TODO: As a future improvement, automatically pick up any remaining
    # quota left from _past_ timespans in the current day.
    #   -trs, 17 Sept 2020
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
    queued = db.fetch_rows("""
        select
            redcap_url,
            redcap_project_id,
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

    if not queued:
        LOG.info(f"Nothing in the queue")
        return

    LOG.info(f"Fetched {len(queued):,} entries from the head of the queue")

    offers = [ offer(q) for q in queued ]
    offer_count = len(offers)

    # XXX FIXME
    # Don't actually update REDCap if we're running under --dry-run mode.
    if db.command_action != "rollback":
        # XXX FIXME: assert on url and project id
        LOG.info(f"Offering testing to {offer_count:,} REDCap records")

        # XXX: use CachedProject instead with values from each row?
        redcap_project = redcap.CachedProject(
            urljoin(..., "api/"),
            os.environ["REDCAP_API_TOKEN"],
            ...)

        offer_count = redcap_project.update_records(offers)

        LOG.info(f"Updated {offer_count:,} REDCap records")

    # XXX FIXME: How to deal with lack of DET from REDCap import?
    #   1. Update internal flag (as below) to be eventually consistent with REDCap
    #   2. Push a synthetic DET to trigger an import

    # XXX TODO: Maybe also update an internal testing_offered flag (in
    # encounter.details?) to avoid delay of roundtrip thru REDCap?  If we don't
    # do this, then worst case we try to offer testing to the same records more
    # than once?  This is probably more complicated than we want to deal with
    # on the first iteration and involves cooperation between this command and
    # the priority queue definition.  I think timing will work out most of the
    # time and the worst case is we offer less testing than we can handle
    # (better than offering more!).  If it happens commonly, we can address
    # later.
    #   -trs, 17 Sept & 13 Oct 2020

    updated_quota = db.fetch_row("""
        update operations.test_quota
           set used = used + %s
         where (name, timespan) = (%s, %s)
        returning name, timespan, "limit", used
        """, (offer_count, quota.name, quota.timespan))

    LOG.info(
        f"Quota used for {updated_quota.name} during {updated_quota.timespan} "
        f"is now {updated_quota.used}/{updated_quota.limit} (used/limit).")


def offer(queued) -> dict:
    """
    Given a *queued* row from the priority queue, returns a :py:class:`dict`
    suitable for updating the associated REDCap record with an offer of
    testing.
    """
    return {
        "record_id": offer["redcap_record_id"],
        "redcap_event_name": offer["redcap_event_name"],
        "redcap_repeat_instance": offer["redcap_repeat_instance_id"],
        "testing_trigger": "1",
        "testing_type": "...",
        "testing_date": TODAY,
        "testing_determination_internal_complete": redcap.InstrumentStatus.Complete.value,
        # XXX FIXME: reason, priority?
    }
