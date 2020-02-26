# influx_db_sanitization_scripts
Collection of scripts to sanitize an InfluxDB

## Peak Remover
This scripts calculate the mean in 2 hours windows of data and remove peaks that are 3 times bigger than the mean of its window.
The values are configurable.

## Drop Dead measurements
If a measurement had no new data in the last 2 years, drop the measurements.
The values are configurable.
