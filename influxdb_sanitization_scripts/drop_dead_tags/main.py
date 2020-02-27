import pandas as pd
from time import time
from ..core import DataGetter
from humanize import naturaldelta
from ..core import logger



MOST_RECENT_QUERY = """SELECT time, service, hostname FROM "{measurement}" WHERE service = '{service}' AND hostname = '{hostname}' AND time > now() - {max_time} ORDER BY time DESC LIMIT 1"""


# Add blacklist and or whitelist

def drop_dead_tags(data_getter: DataGetter, dryrun: bool = True, max_time: int = 3 * 365 * 24 * 60 * 60):


    services = data_getter.get_tag_values("service")

    logger.info("Got %d services", len(services))
    logger.debug("Got services %s", services)

    hostnames = data_getter.get_tag_values("hostname")
    logger.info("Got %d hostnames", len(hostnames))
    logger.debug("Got hostnames %s", hostnames)


    measurements = data_getter.get_measurements()
    logger.info("The available measurements are %s", measurements)

    for measurement in measurements:
        for hostname in hostnames:
            for service in services:
                _drop_dead_tags(data_getter, measurement, hostname, service, dryrun, max_time)


def _drop_dead_tags(data_getter, measurement, hostname, service, dryrun, max_time):

    most_recent_time = data_getter.exec_query(MOST_RECENT_QUERY.format(**locals()))

    if not len(most_recent_time):
        logger.info("%s %s %s has no data", measurement, hostname, service)
        return
    most_recent_time = most_recent_time

    seconds_since_last_write = time() - most_recent_time

    logger.info("%s %s %s hasn't been written to in %s", measurement, hostname, service, naturaldelta(seconds_since_last_write))
    if seconds_since_last_write > max_time:
        logger.info("Going to delete %s %s %s", measurement, hostname, service)
        if not dryrun: 
            #data_getter.drop_measurement(measurement)
            pass
        