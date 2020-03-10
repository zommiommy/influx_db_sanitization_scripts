import pandas as pd
from time import time
from ..core import logger
from itertools import product
from ..core import DataGetter
from humanize import naturaldelta
from concurrent.futures import ThreadPoolExecutor

EXAMINE_TIME_INTERVAL = """SELECT * FROM "{measurement}" WHERE hostname = '{hostname}' AND service = '{service}' AND metric = '{metric}' AND time > now() - {time_delta_new}  AND time < now() - {time_delta_old} LIMIT 1"""
DELETE_VALUES = """DELETE FROM "{measurement}" WHERE hostname = '{hostname}' AND service = '{service}' AND metric = '{metric}' """

def time_sample_scheduler(max_time, min_time):
    t = min_time
    while t < max_time:
        yield "%ss"%int(t)
        t *= 2
    
    yield "%ss"%int(max_time)

def pair_times_scheduler(max_time, min_time=15*60):
    old = "0s"
    for new in time_sample_scheduler(max_time, min_time):
        yield old, new
        old = new

# In future we might want to add a blacklist or whitelist
class DropDeadValues:

    def __init__(self, data_getter : DataGetter, dryrun : bool, max_time : int, workers : int):
        self.dryrun = dryrun
        self.workers = workers
        self.max_time = max_time
        self.data_getter = data_getter

    def drop_dead_values_dispatcher(self, measurement, hostname, service, metric):
        if measurement and measurement != "None":
            self.drop_dead_values_per_measurement(measurement, hostname, service, metric)
        else:
            self.drop_dead_values()

    def drop_dead_values(self):
        measurements = self.data_getter.get_measurements()
        logger.info("The available measurements are %s", measurements)

        for measurement in measurements:
            self.drop_dead_values_per_measurement(measurement)

    def get_tag_set(self, measurement, tag, value):
        if value and value != "None":
            return [value]
        return self.data_getter.get_tag_values(tag, measurement) or [""]

    def drop_dead_values_per_measurement(self, measurement, hostname=None, service=None, metric=None):
            
            hostnames = self.get_tag_set(measurement, "hostname", hostname)
            logger.info("Found hostnames %s", hostnames)
            services  = self.get_tag_set(measurement, "service", service)
            logger.info("Found services %s", services)
            metrics   = self.get_tag_set(measurement, "metric", metric)
            logger.info("Found metrics %s", metrics)

            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                for hostname, service, metric in product(hostnames, services, metrics):
                    executor.submit(self.drop_dead_values_specific, measurement, hostnames, service, metric)

    def drop_dead_values_specific(self, measurement, hostname, service, metric):
        logger.info("Analyzing %s %s %s %s", measurement, hostname, service, metric)
        for time_delta_old, time_delta_new in pair_times_scheduler(self.max_time):
            data = self.data_getter.exec_query(EXAMINE_TIME_INTERVAL.format(**locals()))
            if len(data) == 1:
                logger.info("Found values for measurement %s hostname %s service %s metric %s in the last %s", measurement, hostname, service, metric, naturaldelta(int(time_delta_new[:-1])))
                break
        else:   
            logger.warn("Not found values for measurement %s hostname %s service %s metric %s",  measurement, hostname, service, metric)
            logger.warn("Going to delete the values")
            if not self.dryrun:
                self.data_getter.exec_query(DELETE_VALUES.format(**locals()))