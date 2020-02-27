import pandas as pd
from time import time
from ..core import DataGetter
from humanize import naturaldelta
from ..core import logger

MOST_RECENT_QUERY = """SELECT * FROM "{measurement}" ORDER BY time DESC LIMIT 1"""

# Add blacklist and or whitelist

def drop_dead_measurements(data_getter: DataGetter, dryrun: bool = True, max_time: int = 3 * 365 * 24 * 60 * 60):
    measurements = data_getter.get_measurements()
    logger.info("The available measurements are %s", measurements)

    deleted_measurements = set()
    for measurement in measurements:
        most_recent_time = data_getter.exec_query(MOST_RECENT_QUERY.format(**locals()))[0]["time"]
        seconds_since_last_write = time() - most_recent_time
        logger.info("%s hasn't been written to in %s", measurement, naturaldelta(seconds_since_last_write))
        if seconds_since_last_write > max_time:
            logger.info("Going to delete %s", measurement)
            if not dryrun: 
                deleted_measurements.add(measurement)
                data_getter.drop_measurement(measurement)


    if deleted_measurements:
        logger.info("Deleted %s", deleted_measurements)
    else:
        logger.info("No measurement was deleted.")
        