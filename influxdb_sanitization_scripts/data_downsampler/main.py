from itertools import product
from time import time
import pandas as pd
from ..core import logger, DataGetter, get_filtered_labels, epoch_to_time, time_chunks

BACKUP       = """SELECT * FROM "{measurement}" WHERE time <= now() - {start} AND time >= now() - {end}"""
AGGREGATE    = """SELECT MEAN(value) as "value" FROM "{measurement}" WHERE service = '{service}' AND hostname = '{hostname}' AND metric = '{metric}' AND time <= now() - {start} AND time >= now() - {end} GROUP BY time({window}) """
REMOVE_POINT = """DELETE FROM {measurement} WHERE service = '{service}' AND hostname = '{hostname}' AND metric = '{metric}' AND time <= now() - {start} AND time >= now() - {end}"""


def get_clean_dataframe(data_getter, query):
    data = data_getter.exec_query(query)
    # Setup the dataframe
    df = pd.DataFrame(data)
    logger.info("Got %d datapoints", len(df))
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
        backup: bool = False
    ):
        self.backup = backup
        self.window = window
        self.dryrun = dryrun
        self.interval = interval
        self.data_getter = data_getter
        self.time_start, self.time_end = start, end

    def downsample_all_measurements(self):
        for measurement in self.data_getter.get_measurements():
            self.downsample_single_measurement(measurement)

    def downsample_single_measurement(self, measurement):
        if self.backup:
            df = get_clean_dataframe(self.data_getter, BACKUP.format(**locals(), **vars(self)))
            df.to_csv(measurement + str(epoch_to_time(time())) + "_backup.csv")

        self.hostnames = self.data_getter.get_tag_values("hostname", measurement) or [""]
        self.services  = self.data_getter.get_tag_values("service",  measurement) or [""]
        self.metrics   = self.data_getter.get_tag_values("metric",   measurement) or [""]

        for i_start, i_end in time_chunks(self.time_start, self.time_end, self.interval):
            self._interval_downsampler(measurement, i_start, i_end) 

    def _interval_downsampler(self, measurement, start, end):
        for hostname, service, metric in product(self.hostnames, self.services, self.metrics):
            logger.info("analyzing hostname:[%s] service:[%s] metric:[%s]", hostname, service, metric)
            # Get the data
            df = get_clean_dataframe(self.data_getter, AGGREGATE.format(**locals(), **vars(self)))
            # Add constant values
            tags = {
                "hostname":hostname,
                "service" :service,
                "metric"  :metric,
            }
            
            if not self.dryrun:
                logger.info("Deleting the old points")
                self.data_getter.exec_query(REMOVE_POINT.format(**locals(), **vars(self)))

                logger.info("Writing the new downsampled values")
                self.data_getter.write_dataframe(df, measurement, tags)

            break
