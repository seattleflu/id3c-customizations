"""
Offer UW Husky Coronavirus Testing.

Updates REDCap to trigger offers of testing to the individuals in the priority
queue based on the testing capacity quotas for the given time period.

This command is idempotent and can be safely re-run to e.g. pickup a missed
cronjob or troubleshoot or fix bugs.  Running the command more than once won't
release extra tests above the quota.
"""
import click
import enum
import json
import logging
import os
from datetime import date, datetime
from more_itertools import bucket, chunked
from psycopg2.extras import execute_values
from typing import List
from id3c.cli import cli
from id3c.cli.command import with_database_session, DatabaseSessionAction
from id3c.cli.redcap import Project, InstrumentStatus, det
from id3c.db.datatypes import Json
from id3c.db.session import DatabaseSession
from id3c.json import dump_ndjson
from ...utils import unwrap


LOG = logging.getLogger(__name__)

STUDY_START_DATE = date(2021, 9, 9)

TESTING_INSTRUMENT = "testing_determination_internal"

REDCAP_BATCH_SIZE = 250


@cli.command("offer-uw-testing", help = __doc__)
@with_database_session(pass_action = True)

@click.option("--at",
    metavar = "<timestamp>",
    default = "now",
    help    = unwrap("""
        Use quota for the given <timestamp>.

        With --dry-run, can be used to test a quota schedule.  Otherwise, can
        be used to apply unused quota from past timespans.  (Though you should
        do that with caution, especially across day boundaries.)

        Defaults to the current time."""))

@click.option("--log-offers/--no-log-offers",
    help    = "Write REDCap offers of testing to stdout as an NDJSON stream.",
    default = False)

def offer_uw_testing(*, at: str, log_offers: bool, db: DatabaseSession, action: DatabaseSessionAction):
    LOG.debug(f"Offering UW Husky Coronavirus Testing @ {at}")

    dry_run = action is DatabaseSessionAction.DRY_RUN

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
            max,
            used,
            max - used as remaining
        from
            operations.test_quota
        where
            name = 'uw' and timespan @> timestamp with time zone %s
        for update
        """, (at,))

    if not quota:
        LOG.info(f"No quota row found, aborting")
        return

    if not quota.remaining > 0:
        LOG.info(f"No quota remaining for {quota.name} during {quota.timespan}, aborting")
        return

    LOG.info(
        f"Quota for {quota.name} during {quota.timespan} "
        f"is now {quota.remaining:,} = {quota.max:,} - {quota.used:,} (remaining = max - used)")

    # Offer testing to the top entries in our priority queue.
    next_in_queue = db.fetch_all("""
        select
            redcap_url,
            redcap_project_id,
            redcap_record_id,
            redcap_event_name,
            redcap_repeat_instance,
            priority,
            priority_reason
        from
            shipping.uw_priority_queue_v1
        limit
            %s
        """, (quota.remaining,))

    if not next_in_queue:
        LOG.info(f"Nothing in the queue")
        return

    LOG.info(f"Fetched {len(next_in_queue):,} entries from the head of the queue")

    # Use the REDCap URL and project id from the queue rather than hardcoding.
    buckets = bucket(next_in_queue, lambda q: (q.redcap_url, q.redcap_project_id))
    queued_by_project = {
        key: list(buckets[key])
            for key in buckets }

    offer_count = 0

    for (url, project_id), queued in queued_by_project.items():
        offers = [ offer(q) for q in queued ]

        LOG.info(f"Making {len(offers):,} offers for {url} project {project_id} {'(dry run)' if dry_run else ''}")

        if log_offers:
            dump_ndjson(offers)

        # Token will automatically come from the environment.  If we're doing a
        # dry run, then Project will make sure we update_records() doesn't
        # actually update records.
        project = Project(url, project_id, dry_run = dry_run)

        batches = list(chunked(offers, REDCAP_BATCH_SIZE))

        for i, batch in enumerate(batches, 1):
            LOG.info(f"Updating REDCap record batch {i:,}/{len(batches):,} of size {len(batch):,}")
            offer_count += project.update_records(batch)

        # Insert synthetic DETs into our receiving table to trigger a new
        # import.  This helps complete the roundtrip data update for the REDCap
        # records we just updated since API imports don't trigger natural DETs.
        insert_dets(db, project, offers)

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
        update
            operations.test_quota
        set
            used = used + %s
        where
            (name, timespan) = (%s, %s)
        returning
            name,
            timespan,
            max,
            used,
            max - used as remaining
        """, (offer_count, quota.name, quota.timespan))

    LOG.info(
        f"Quota for {updated_quota.name} during {updated_quota.timespan} "
        f"is now {updated_quota.remaining:,} = {updated_quota.max:,} - {updated_quota.used:,} (remaining = max - used)")



def offer(queued) -> dict:
    """
    Given a *queued* row from the priority queue, returns a :py:class:`dict`
    suitable for updating the associated REDCap record with an offer of
    testing.
    """
    event = Event(queued.redcap_event_name)

    # Mark the qualifying encounter instance for testing if the qualifying
    # event is an encounter (e.g. daily check-in).
    if event is Event.Encounter:
        instance = queued.redcap_repeat_instance

    # Mark today's encounter instance for testing if the qualifying event is
    # enrollment (e.g. baseline/surveillance)
    elif event is Event.Enrollment:
        instance = repeat_instance(date.today())

    else:
        assert False, "logic error"

    return {
        "record_id": queued.redcap_record_id,
        "redcap_event_name": Event.Encounter.value,
        "redcap_repeat_instance": instance,
        "testing_trigger": TestingTrigger.Yes.value,
        "testing_type": testing_type(queued.priority_reason).value,
        "testing_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        f"{TESTING_INSTRUMENT}_complete": InstrumentStatus.Complete.value,
    }


def insert_dets(db: DatabaseSession, project: Project, offers: List[dict]):
    """
    Inserts synthethic DETs into ``receiving.redcap_det`` for the REDCap record
    *offers* made for *project*.
    """
    dets = [
        (Json(det(project, offer, TESTING_INSTRUMENT)),)
            for offer in offers ]

    LOG.info(f"Inserting {len(dets):,} synthetic REDCap DETs for {project}")

    with db.cursor() as cursor:
        execute_values(cursor, """
            insert into receiving.redcap_det (document) values %s
            """, dets)


@enum.unique
class Event(enum.Enum):
    """
    Event names used by REDCap.
    """
    Enrollment = "enrollment_arm_1"
    Encounter  = "encounter_arm_1"


def repeat_instance(date: date) -> int:
    """
    REDCap encounter event's repeat instance number for *date*.

    Represents the number of days since study start.

    >>> repeat_instance(STUDY_START_DATE)
    1
    >>> repeat_instance(date(2021, 9, 9))
    1
    >>> repeat_instance(date(2021, 11, 13))
    66
    """
    return 1 + (date - STUDY_START_DATE).days


@enum.unique
class TestingTrigger(enum.Enum):
    """
    Numeric codes used by the ``testing_trigger`` field in REDCap.
    """
    No  = "0"
    Yes = "1"


@enum.unique
class TestingType(enum.Enum):
    """
    Numeric codes used by the ``testing_type`` field in REDCap.
    """
    Baseline            = "0"
    Surveillance        = "1"
    SymptomsOrExposure  = "2"
    ContactTracing      = "3"
    KioskWalkIn         = "4"
    UwHousingResident   = "5"


def testing_type(priority_reason: str) -> TestingType:
    """
    Map a ``priority_reason`` from our priority queue to a ``testing_type``
    code used in REDCap.
    """
    testing_type = {
        'symptomatic':                  TestingType.SymptomsOrExposure,
        'exposure_to_known_positive':   TestingType.SymptomsOrExposure,
        'gathering_over_10':            TestingType.SymptomsOrExposure,
        'travel':                       TestingType.SymptomsOrExposure,
        'baseline':                     TestingType.Baseline,
        'surveillance':                 TestingType.Surveillance,
        'surge_testing':                TestingType.ContactTracing,
    }
    return testing_type[priority_reason]
