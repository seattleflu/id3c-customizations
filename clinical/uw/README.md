# Workflow for prepping UW clinical datasets

This Snakefile captures what I was doing by hand to get the latest clinical
data from UW (_5.08.19,11.17.19.deduplicate.csv_) ingested.

This workflow should either:

  a. be replaced by direct usage of geocoding and location lookup
     functions in `id3c clinical parse-uw`, or

  b. grow and take over more parts of `id3c clinical parse-uw`,
     eventually replacing it with a pipeline of `geocode`, `location
     lookup`, `de-identify` and other (not yet existing) core commands.

I think (a) is the more sensible option in the short and middle term.  Option
(b) might be called the "augur strategy", which might be better longer term.

## Running

The workflow is written to be run on a single dataset at a time, which is
automatically pulled from <s3://fh-pi-bedford-t/seattleflu/uw/>.  The dataset
filename must be specified as a configuration value when running `snakemake`,
for example:

    snakemake -C dataset=5.08.19,11.17.19.deduplicate.csv

Output files are also stored on S3 and named after the input dataset.

Various steps of the workflow require different environment variables.  To run
the entire thing against the production database, you'll need the following
defined:

  * PostgreSQL connection variables, e.g. `PGSERVICE=seattleflu-production`

  * `PARTICIPANT_DEIDENTIFICATION_SECRET` that we've previously used
    (eventually to be replaced by `ID3C_DEIDENTIFY_SECRET`).

  * Fred Hutch AWS credentials either in the environment or in local config
    files.  If you use a separate config profile for the Hutch, you can define
    `AWS_PROFILE`.

  * SmartyStreets API credentials in `SMARTYSTREETS_AUTH_ID` and
    `SMARTYSTREETS_AUTH_TOKEN`.  These are technically only required the first
    time before the local geocoding cache is populated.

Additionally, `snakemake` must be able to run the `id3c` command.  This is
enabled by running it within an appropriate `pipenv shell` or using `pipenv
run`.  Make sure to use the deployed _id3c-production_ environment from the
_backoffice_ repo unless you're doing development.
