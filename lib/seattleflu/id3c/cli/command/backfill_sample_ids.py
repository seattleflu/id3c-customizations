"""
Backfill sample ids

Mint new sample identifiers and assign them to warehouse.sample records that are missing sample identifiers. This is useful for tiny swabs samples, which do not have room on the tube for a sample identifier, only a collection identifier. This process fills in the sample record in our warehouse table but notably does not notify the lab of any identifier assignments. We can use this backfilled sample ids as an identifier when sharing data with third parties.
"""

import logging

from id3c.cli import cli
from id3c.cli.command import DatabaseSessionAction, with_database_session
from id3c.db import mint_identifiers, upsert_sample
from id3c.db.session import DatabaseSession

LOG = logging.getLogger(__name__)


@cli.command("backfill-sample-ids", help = __doc__)
@with_database_session(pass_action = True)
def backfill_sample_ids(*, db: DatabaseSession, action: DatabaseSessionAction):
    LOG.debug(f"Backfilling sample ids")

    sample_records = db.fetch_all("""
            select sample.sample_id
            from warehouse.sample
            join warehouse.identifier on sample.collection_identifier = identifier.uuid::text
            join warehouse.identifier_set using (identifier_set_id)
            where identifier is null and
            identifier_set.name in ('collections-uw-tiny-swabs-home', 'collections-uw-tiny-swabs-observed', 
                'collections-scan-tiny-swabs', 'collections-adult-family-home-outbreak-tiny-swabs', 'collections-workplace-outbreak-tiny-swabs')
            order by sample.sample_id
            """)

    if not sample_records:
        LOG.info(f"No sample records that need sample identifier backfill")
        return

    LOG.info(f"Fetched {len(sample_records):,} sample records that need sample identifier backfill")

    minted_sample_identifiers = mint_identifiers(db, 'samples', len(sample_records))
    assert len(minted_sample_identifiers) == len(sample_records), "Didn't generate expected number of identifiers"

    for minted_sample_identifier, sample_record in zip(minted_sample_identifiers, sample_records):
        LOG.info(f"Updating sample {sample_record.sample_id} with identifier {minted_sample_identifier.uuid} in warehouse.samples")

        sample = db.fetch_row("""
            update warehouse.sample
            set identifier = %s
            where sample_id = %s
            returning sample_id as id, identifier
            """, (minted_sample_identifier.uuid, sample_record.sample_id))

        assert sample.id, "Update sample identifier affected no rows!"
        LOG.info(f"Updated sample {sample.id} with identifier «{sample.identifier}»")



