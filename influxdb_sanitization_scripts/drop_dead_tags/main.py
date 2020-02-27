import pandas as pd
from time import time
from ..core import DataGetter
from humanize import naturaldelta
from ..core import logger



MOST_RECENT_QUERY = """SELECT time, hostname, service FROM (SELECT * FROM {measurement} WHERE time > now() - 2w) GROUP BY service, hostname ORDER BY time DESC LIMIT 1"""


# Add blacklist and or whitelist

def drop_dead_tags(data_getter: DataGetter, dryrun: bool = True, max_time: int = 3 * 365 * 24 * 60 * 60):
    max_time = int(max_time)


    #values = {"time":, "hostname":, "service":}

    measurements = data_getter.get_measurements()
    logger.info("The available measurements are %s", measurements)

    for measurement in measurements:
        values = data_getter.exec_query(MOST_RECENT_QUERY.format(**locals()))
        logger.info("Got %d combinations of hostname and service for the measurements %s", len(values), measurement)
        logger.debug("Got combinations %s", values)
        for combination in values:
            _drop_dead_tags(data_getter, measurement, combination, dryrun, max_time)


def _drop_dead_tags(data_getter, measurement, combination, dryrun, max_time):
    hostname, service, time = combination["hostname"], combination["service"], combination["time"]

    most_recent_time = data_getter.exec_query(MOST_RECENT_QUERY.format(**locals()))

    if not len(most_recent_time):
        logger.info("%s %s %s has no data", measurement, hostname, service)
        return

    most_recent_time = most_recent_time[0]["time"]

    seconds_since_last_write = time() - most_recent_time

    logger.info("%s %s %s hasn't been written to in %s", measurement, hostname, service, naturaldelta(seconds_since_last_write))
    if seconds_since_last_write > max_time:
        logger.info("Going to delete %s %s %s", measurement, hostname, service)
        if not dryrun: 
            #data_getter.drop_measurement(measurement)
            pass
        