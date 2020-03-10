import pandas as pd
from time import time
from ..core import DataGetter
from humanize import naturaldelta
from itertools import product
from ..core import logger

EXAMINE_TIME_INTERVAL = """SELECT * FROM "{measurement}" WHERE hostname = '{hostname}' AND service = '{service}' AND metric = '{metric}' AND time > now() - {time_delta} LIMIT 1"""
DELETE_VALUES = """DELETE FROM "{measurement}" WHERE hostname = '{hostname}' AND service = '{service}' AND metric = '{metric}' """

def time_sample_scheduler(max_time, min_time=3600):
    t = min_time
    while t < max_time:
        yield "%ss"%int(t)
        t *= 1.7
    
    yield "%ss"%int(max_time)


def drop_dead_values_dispatcher(data_getter, dryrun, max_time, measurement, hostname, service, metric):
    if measurement and measurement != "None":
        drop_dead_values_per_measurement(data_getter, dryrun, max_time, measurement, hostname, service, metric)
    else:
        drop_dead_values(data_getter, dryrun, max_time)


# Add blacklist and or whitelist

def drop_dead_values(data_getter: DataGetter, dryrun: bool = True, max_time: int = 3 * 365 * 24 * 60 * 60):
    measurements = data_getter.get_measurements()
    logger.info("The available measurements are %s", measurements)

    for measurement in measurements:
        drop_dead_values_per_measurement(data_getter, dryrun, max_time, measurement)

def get_tag_set(data_getter, measurement, tag, value):
    if value and value != "None":
        return [value]
    return data_getter.get_tag_values(tag, measurement) or [""]

def drop_dead_values_per_measurement(data_getter, dryrun, max_time, measurement, hostname=None, service=None, metric=None):
        
        hostnames = get_tag_set(data_getter, measurement, "hostname", hostname)
        logger.info("Found hostnames %s", hostnames)
        services = get_tag_set(data_getter, measurement, "service", service)
        logger.info("Found services %s", services)
        metrics = get_tag_set(data_getter, measurement, "metric", metric)
        logger.info("Found metrics %s", metrics)

        for hostname, service, metric in product(hostnames, services, metrics):
            logger.info("Analyzing %s %s %s %s", measurement, hostnames, service, metric)
            drop_dead_values_specific(data_getter, dryrun, max_time, measurement, hostname, service, metric)

def drop_dead_values_specific(data_getter, dryrun, max_time, measurement, hostname, service, metric):
    for time_delta in time_sample_scheduler(max_time):
        data = data_getter.exec_query(EXAMINE_TIME_INTERVAL.format(**locals()))
        if len(data) == 1:
            logger.info("Found values for measurement %s hostname %s service %s metric %s in the last %s", measurement, hostname, service, metric, naturaldelta(int(time_delta[:-1])))
            break
    else:   
        logger.info("Not found values for measurement %s hostname %s service %s metric %s",  measurement, hostname, service, metric)
        logger.info("Going to delete the values")
        if not dryrun:
            data_getter.exec_query(DELETE_VALUES.format(**locals()))