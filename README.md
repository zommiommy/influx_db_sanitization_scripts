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
$ ./peaks_remover -h                                                                                                                                                                                                    ─╯
usage: -c [-h] [-v VERBOSITY] [-dr] [-f] [-dsp DB_SETTINGS_PATH] [-c COEFF] [-w WINDOW] [-r RANGE] [-fi FIELD] measurement

This scripts calculate the mean in 2 hours windows of data and remove peaks that are 3 times bigger than the mean of its window. The values are configurable.

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

## Drop Dead measurements
If a measurement had no new data in the last 2 years, drop the measurements.
The values are configurable.

```bash
 $ ./drop_dead_measurements -h                                                                                                                                                                                           ─╯
usage: -c [-h] [-v VERBOSITY] [-dr] [-f] [-dsp DB_SETTINGS_PATH] [-m MAX_TIME]

If a measurement had no new data in the last 2 years, drop the measurements. The values are configurable.

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
