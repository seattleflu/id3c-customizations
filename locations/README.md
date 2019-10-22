This directory contains location data we use in ID3C for the Seattle Flu Study
and code for importing it.


## Commands

Snakemake is used to fetch and import files.  There are two primary commands:

* __download__: Fetches geospatial data from the US Census website.  This
  should be run as few times as possible, as the Census temporarily bans repeat
  downloaders.  Similar, it should not be run in parallel.

* __import__: Imports locations from geospatial data into ID3C.  This expects
  that a clone of the ID3C source code is a sibling to the clone of this
  repository.  If it's not, set the `ID3C` environment variable appropriately.


## Data

Files stored in the `data/` directory are:

* `states.txt` contains a mapping of ANSI FIPS codes and USPS abbreviations for
  all states, as described at
  <https://www.census.gov/geo/reference/ansi_statetables.html>.  This file was
  downloaded unmodified from
  <http://www2.census.gov/geo/docs/reference/state.txt>.

* `omitted-states.txt` lists the _names_ of states we want to omit from
  downloads of census tracts, with reasons provided in comments.

* `tract/tl_2016_${state_fips_code}_tract.zip` contain the 2016-vintage Census
  tract Shapefiles, as described at
  <https://www.census.gov/geo/maps-data/data/tiger-line.html>.

  These are not checked into version control and must be downloaded locally by
  running `snakemake download`.  Do not download the files in parallel or
  repeatedly or the Census will likely ban your IP address!

* `tract/cb_2016_${state_fips_code}_tract_500k.zip` contain the 2016-vintage Census
  tract cartographic boundary Shapefiles, as described at
  <https://www.census.gov/programs-surveys/geography/technical-documentation/naming-convention/cartographic-boundary-file.html>.

  These are also not checked into version control and must be downloaded
  locally by running `snakemake download`.  Do not download the files in
  parallel or repeatedly or the Census will likely ban your IP address!