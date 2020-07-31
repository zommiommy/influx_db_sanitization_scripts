# influx_db_sanitization_scripts
Collection of scripts to sanitize an InfluxDB

To install just use pip:
```bash
pip install --user .
```

Then you can just use the executables in the root of the repository or import the module and call it functions.

### Db settings
By default all scripts read the configs from the file `db_settings.json` but another path can be passed with the `-dsp` argument. The file must be a json like:
```json
{
    "database": "NOAA_water_database",
    "host": "localhost",
    "port": 8086,
    "username": "root",
    "password": "root",
    "ssl": false,
    "verify_ssl": false,
    "timeout": 60,
    "retries": 3,
    "use_udp": false,
    "udp_port": 4444,
    "proxies": {},
    "path": ""
}
```

## Peak Remover
This scripts calculate the mean in 2 hours windows of data and remove peaks that are 3 times bigger than the mean of its window.
The values are configurable.

```bash
$ ./peaks_remover -h 
usage: -c [-h] [-v VERBOSITY] [-dr] [-f] [-dsp DB_SETTINGS_PATH] [-c COEFF] [-w WINDOW] [-r RANGE] [-fi FIELD] measurement

Example:
./drop_dead_measurements -w 2h -r 20w -c 3 -fi value -v 1

This scripts calculate the mean if the values in the field `value` in 2 hours windows
of the data of the lasts 20 weeks and remove peaks that are 3 times bigger than 
the mean of its window. The values are configurable.

positional arguments:
  measurement           The measurement to use

optional arguments:
  -h, --help            show this help message and exit
  -v VERBOSITY, --verbosity VERBOSITY
                        Verbosity of the program. 0 - Critical, 1 - Info, 2 - Debug
  -dr, --dryrun         Test run, access the DB in read only mode.
  -f, --force           No not show the are you sure prompt.
  -dsp DB_SETTINGS_PATH, --db-settings-path DB_SETTINGS_PATH
                        Path to the json with the settings for the DB connections.
  -c COEFF, --coeff COEFF
                        How many time the point has to be over the mean to be considered a peak and removed.
  -w WINDOW, --window WINDOW
                        How big are the chunks with which the means are computed.
  -r RANGE, --range RANGE
                        How back the scripts goes to clean the data.
  -fi FIELD, --field FIELD
                        The name of the column to use for peak deletion
```
This script is bottlenecked by the limitation of InfluxDB which does not allows (to the best of my knowledge) to delete multiple points from a measurements given their timestamp and tags).
Currently it takes 0.04 seconds per point which means that it takes 4 seconds per 100 points.


## Drop Dead measurements
If a measurement had no new data in the last 2 years, drop the measurements.
The values are configurable.

```bash
 $ ./drop_dead_measurements -h
usage: -c [-h] [-v VERBOSITY] [-dr] [-f] [-dsp DB_SETTINGS_PATH] [-m MAX_TIME]

Example:
./drop_dead_measurements -m 104w -v 1

If a measurement had no new data in the last 2 years (104 weeks), drop the measurements.
The values are configurable.

optional arguments:
  -h, --help            show this help message and exit
  -v VERBOSITY, --verbosity VERBOSITY
                        Verbosity of the program. 0 - Critical, 1 - Info, 2 - Debug
  -dr, --dryrun         Test run, access the DB in read only mode.
  -f, --force           No not show the are you sure prompt.
  -dsp DB_SETTINGS_PATH, --db-settings-path DB_SETTINGS_PATH
                        Path to the json with the settings for the DB connections.
  -m MAX_TIME, --max-time MAX_TIME
                        Threshold time, if a measurement has no points newer than

```

## Drop dead tags
```
./drop_dead_tags -h
usage: -c [-h] [-v VERBOSITY] [-dr] [-f] [-dsp DB_SETTINGS_PATH] [-m MAX_TIME]

Example:
./drop_dead_tags -m 104w -v 1

If a tag had no new data in the last 2 years (104 weeks), drop the tag from all measurements.
The values are configurable.

optional arguments:
  -h, --help            show this help message and exit
  -v VERBOSITY, --verbosity VERBOSITY
                        Verbosity of the program. 0 - Critical, 1 - Warnings, 2 - Info, 3 - Debug
  -dr, --dryrun         Test run, access the DB in read only mode.
  -f, --force           No not show the are you sure prompt.
  -dsp DB_SETTINGS_PATH, --db-settings-path DB_SETTINGS_PATH
                        Path to the json with the settings for the DB connections.
  -m MAX_TIME, --max-time MAX_TIME
                        Threshold time in seconds, if a measurement has no points newer than
```

## Drop dead values
```
./drop_dead_values -h
usage: -c [-h] [-v VERBOSITY] [-dr] [-f] [-dsp DB_SETTINGS_PATH] [-t MAX_TIME] [-H HOSTNAME [HOSTNAME ...]]
          [-s SERVICE [SERVICE ...]] [-m METRIC] [-M MEASUREMENT] [-w WORKERS] [-p] [-snn]

Example:
./drop_dead_values -t 15w -v 1 -w 5 -M ping -snn

If had no new data in the last 15 weeks on the measurement "ping"
drop the values (grouped by hostname, service, and metric).
Set verbosity to 1 (WARNING) and set 5 workers for parallel analysis.
-ssn since in this case the service tag can't be null.

optional arguments:
  -h, --help            show this help message and exit
  -v VERBOSITY, --verbosity VERBOSITY
                        Verbosity of the program. 0 - Critical, 1 - Warnings, 2 - Info, 3 - Debug
  -dr, --dryrun         Test run, access the DB in read only mode.
  -f, --force           No not show the are you sure prompt.
  -dsp DB_SETTINGS_PATH, --db-settings-path DB_SETTINGS_PATH
                        Path to the json with the settings for the DB connections.
  -t MAX_TIME, --max-time MAX_TIME
                        Threshold time, if a measurement has no points newer than the threshold
  -H HOSTNAME [HOSTNAME ...], --hostname HOSTNAME [HOSTNAME ...]
                        The hostname to select
  -s SERVICE [SERVICE ...], --service SERVICE [SERVICE ...]
                        The service to select
  -m METRIC, --metric METRIC
                        The metric to select
  -M MEASUREMENT, --measurement MEASUREMENT
                        The measurement to select
  -w WORKERS, --workers WORKERS
                        How many query to execute in parallel
  -p, --use-processes   If the parallelization should use threads or processes
  -snn, --service-not-nullable
                        if the service can be null or not
```

## Data Downsampling
First of all we need to set a retention policy on the measurements.
A retention policy impose an upperlimits on for how much time the data will be kept in the DB.
```
CREATE RETENTION POLICY "<name>" ON "<measurement>" DURATION <time> REPLICATION 1 DEFAULT
```
a 3 year retention policy approximately corrisponds to 156 weeks.

To reduce the DB size we will futher downsample the data **on the same measurement**.

The standard way to do this is to have multiple measurements with different retention policies and use continous queries to
add the aggregated values to each measuremnt. This should have been done when designing the database architecture and doing it now would mean modify every script that access the measurements which is impracticable.
This will be done by aggregating the data in windows (of e.g. 15 minutes), delete the data and write the new downsampled ones.

```bash
./data_downsampler -h
usage: -c [-h] [-v VERBOSITY] [-dr] [-f] [-dsp DB_SETTINGS_PATH] [-m MEASUREMENT] [-H HOSTNAME [HOSTNAME ...]]
          [-S SERVICE [SERVICE ...]] [-e END] [-s START] [-w WINDOW] [-b] [-i INTERVAL]

Example
./data_downsampler -v 2 -w 15m -s 26w -e 52w

This scripts take the values between 26 weeks and 52 weeks from now
and downsample them by aggregating values from windows of 15 minutes
and set the verbosity to 2 (INFO)

optional arguments:
  -h, --help            show this help message and exit
  -v VERBOSITY, --verbosity VERBOSITY
                        Verbosity of the program. 0 - Critical, 1 - Warnings, 2 - Info, 3 - Debug
  -dr, --dryrun         Test run, access the DB in read only mode.
  -f, --force           No not show the are you sure prompt.
  -dsp DB_SETTINGS_PATH, --db-settings-path DB_SETTINGS_PATH
                        Path to the json with the settings for the DB connections.
  -m MEASUREMENT, --measurement MEASUREMENT
                        The measurement to use
  -H HOSTNAME [HOSTNAME ...], --hostname HOSTNAME [HOSTNAME ...]
                        The hostname to select
  -S SERVICE [SERVICE ...], --service SERVICE [SERVICE ...]
                        The service to select
  -e END, --end END     Inclusive Upper-bound of the time to be parsed
  -s START, --start START
                        Inclusive Lower-bound of the time to be parsed
  -w WINDOW, --window WINDOW
                        How big are the chunks with which the means are computed.
  -b, --backup          If setted, the script will save as csv all the value on which the analysis will work
  -i INTERVAL, --interval INTERVAL
                        The analysis will be divided in intervals to bypass the timeout error
```