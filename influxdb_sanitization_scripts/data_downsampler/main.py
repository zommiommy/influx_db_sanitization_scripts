from itertools import product
from time import time
import pandas as pd
from ..core import logger, DataGetter, get_filtered_labels, epoch_to_time, time_chunks

BACKUP       = """SELECT * FROM "{measurement}" WHERE time <= {start} AND time >= {end}"""
AGGREGATE    = """SELECT MEAN(value) as "value" FROM "{measurement}" WHERE service = '{service}' AND hostname = '{hostname}' AND metric = '{metric}' AND time <= {start} AND time >= {end} GROUP BY time({window}) """
REMOVE_POINT = """DELETE FROM "{measurement}" WHERE time <= {start} AND time >= {end}"""


def get_clean_dataframe(data_getter, query):
    data = data_getter.exec_query(query)
    # Setup the dataframe
    df = pd.DataFrame(data)
    logger.info("Got %d datapoints", len(df))
    if len(df) == 0:
        return []
    # ensure that all values are floats
    df["value"] = df.value.astype(float)
    # Time index so it can be written
    df["time"] = pd.to_datetime(df.time, unit="s")
    return df.set_index("time")

class DataDownSampler:

    def __init__(
        self,
        data_getter: DataGetter,
        window: str="10m",
        start: str="1d",
        end: str="2d",
        interval: str="1h",
        dryrun: bool = False,
        backup: bool = False,
        service: str = None,
        hostname: str = None,
    ):
        self.backup = backup
        self.window = window
        self.dryrun = dryrun
        self._service = service
        self._hostname = hostname
        self.interval = interval
        self.data_getter = data_getter
        self.time_start, self.time_end = start, end

    def downsample_all_measurements(self):
        for measurement in self.data_getter.get_measurements():
            logger.info("Working on measurement [%s]", measurement)
            self.downsample_single_measurement(measurement)

    def get_tag_set(self, measurement, tag, value, constraint=None, nullable=True):
        if value and value != "None":
            return [value]
        result = self.data_getter.get_tag_values(tag, measurement, constraint)
        if nullable:
            result += [""]
        return result

    def get_tags_to_parse(self, measurement):
        self.hostnames = self.get_tag_set(measurement, "hostname", self._hostname, nullable=False)
        logger.info("Found hostnames %s", self.hostnames)
        self.services  = self.get_tag_set(measurement, "service", self._service, nullable=False)
        logger.info("Found services %s", self.services)
        self.metrics   = self.get_tag_set(measurement, "metric", None, nullable=False)
        logger.info("Found metrics %s", self.metrics)

    def downsample_single_measurement(self, measurement):
        if self.backup:
            df = get_clean_dataframe(self.data_getter, BACKUP.format(**locals(), **vars(self)))
            df.to_csv(measurement + str(epoch_to_time(time())) + "_backup.csv")

        self.get_tags_to_parse(measurement)

        for i_start, i_end in time_chunks(self.time_start, self.time_end, self.interval):
            self._interval_downsampler(measurement, i_start, i_end) 

    def _interval_downsampler(self, measurement, start, end):
        self.write_queue = []
        for hostname, service, metric in product(self.hostnames, self.services, self.metrics):
            logger.info("analyzing measurement:[%s] hostname:[%s] service:[%s] metric:[%s]", measurement, hostname, service, metric)
            # Get the data
            df = get_clean_dataframe(self.data_getter, AGGREGATE.format(**locals(), **vars(self)))

            if len(df) == 0:
                logger.info("No data so this interval will be skipped")
                continue
            # Add constant values
            tags = {
                "hostname":hostname,
                "service" :service,
                "metric"  :metric,
            }
        
            self.write_queue.append((df, measurement, tags))

        if len(self.window) == 0:
            logger.info("The mesurement %s has no data so it's skipped", measurement)
            return

        logger.info("Deleting the old points")
        self.data_getter.exec_query(REMOVE_POINT.format(**locals(), **vars(self)))

        logger.info("Writing the new downsampled values")
        for args in self.write_queue:
            try:
                self.data_getter.write_dataframe(*args)
            except:
                df, measurement, tags = args
                logger.error("Can't write data for %s with tags %s", measurement, tags)
                path = "./" 
                path += "_".join(measurement, *list(tags.values()))
                path += "_" + str(epoch_to_time(time())).replace(" ", "_")
                path += "_write_error.csv"
                logger.info("Saving the data which were not written to %s", path)
                df.to_csv(path)
