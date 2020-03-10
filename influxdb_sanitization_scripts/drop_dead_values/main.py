from ..core import logger
from itertools import product
from ..core import DataGetter
from humanize import naturaldelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

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
    def __init__(self, data_getter : DataGetter, dryrun : bool, max_time : int, workers : int, use_processes : bool, service_not_nullable: bool):
        self.dryrun = dryrun
        self.workers = workers
        self.max_time = max_time
        self.use_processes = use_processes
        self.data_getter = data_getter
        self.service_not_nullable = service_not_nullable

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

    def get_tag_set(self, measurement, tag, value, constraint=None, nullable=True):
        if value and value != "None":
            return [value]
        result = self.data_getter.get_tag_values(tag, measurement, constraint)
        if nullable:
            result += [""]
        return result

    def drop_dead_values_per_measurement(self, measurement, hostname=None, service=None, metric=None):
            
            hostnames = self.get_tag_set(measurement, "hostname", hostname, nullable=False)
            logger.info("Found hostnames %s", hostnames)
            
            if self.use_processes:
                pool = ProcessPoolExecutor
            else:
                pool = ThreadPoolExecutor

            with pool(max_workers=self.workers) as executor:
                for hostname in hostnames:
                    services  = self.get_tag_set(measurement, "service", service, {"hostname":hostname}, self.service_not_nullable)
                    logger.info("Found services %s for hostname %s", services, hostname)
                    for service in services:
                        executor.submit(self.drop_dead_values_specific, measurement, hostname, service, metric)

    def drop_dead_values_specific(self, measurement, hostname, service, metric):
        if not metric or metric == "None":
            metrics = self.get_tag_set(measurement, "metric", metric, {"hostname":hostname, "service":service})
        else:
            metrics = [metric]
        logger.info("Found metrics %s for hostname %s service %s", metrics, hostname, service)
        for metric in metrics:
            logger.info("Analyzing %s %s %s %s", measurement, hostname, service, metric)
            for time_delta_old, time_delta_new in pair_times_scheduler(self.max_time):
                data = self.data_getter.exec_query(EXAMINE_TIME_INTERVAL.format(**locals()))
                if len(data) == 1:
                    logger.info("Found values for measurement %s hostname %s service %s metric %s in the last %s", measurement, hostname, service, metric, naturaldelta(int(time_delta_new[:-1])))
                    break
            else:   
                logger.warn("Not found values for measurement %s hostname %s service %s metric %s",  measurement, hostname, service, metric)
                if not self.dryrun:
                    self.data_getter.exec_query(DELETE_VALUES.format(**locals()))