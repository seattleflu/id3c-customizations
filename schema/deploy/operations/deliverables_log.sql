-- Deploy seattleflu/id3c-customizations:operations/deliverables_log to pg
-- requires: citext

begin;

create table operations.deliverables_log (
    deliverables_log_id integer primary key generated by default as identity,
    sample_barcode citext,
    collection_barcode citext,
    details jsonb,
    process_name text,
    sent timestamp with time zone
);

comment on table operations.deliverables_log is 'A log of deliverables sent, used for tracking purposes';
comment on column operations.deliverables_log.deliverables_log_id is 'Internal id of this log entry';
comment on column operations.deliverables_log.sample_barcode is 'Sample barcode that was included in the deliverable';
comment on column operations.deliverables_log.collection_barcode is 'Collection barcode that was included in the deliverable';
comment on column operations.deliverables_log.details is 'Details about the deliverable sent';
comment on column operations.deliverables_log.process_name is 'Process name for the deliverable (e.g. return-of-results)';
comment on column operations.deliverables_log.sent is 'When the deliverable was sent';

create index deliverables_log_sample_barcode_idx on operations.deliverables_log (sample_barcode);
create index deliverables_log_collection_barcode_idx on operations.deliverables_log (collection_barcode);
create index deliverables_log_sent_idx on operations.deliverables_log (sent);

commit;