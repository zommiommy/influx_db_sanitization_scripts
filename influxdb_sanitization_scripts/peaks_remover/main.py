

import time
import pandas as pd
from tqdm.auto import tqdm
from itertools import product
from ..core import logger, DataGetter, get_filtered_labels, consistent_groupby, time_chunks, epoch_to_time

FIND_QUERY = """SELECT time, service, hostname, value, metric FROM {measurement} WHERE metric = '{metric}' AND hostname = '{hostname}' AND service = '{service}' AND time >= {high:d} AND time < {low:d}"""
REMOVE_POINT = """DELETE FROM {measurement} WHERE service = '{service}' AND hostname = '{hostname}' AND metric = '{metric}' AND time = {time}"""

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


class PeaksRemover:

    def __init__(self,
        data_getter: DataGetter,
        measurement: str,
        coeff:float = 100,
        window: str="10m",
        range: str="1d",
        dryrun: bool = False,
        chunk_size: int = 1000,
        time_chunk: str = "6h",
        max_value: int = 1e8,
        service: str = None,
        hostname: str = None,
    ):
        self.service, self.hostname = service, hostname
        self.data_getter, self.measurement = data_getter, measurement
        self.coeff, self.window, self.range, self.dryrun = coeff, window, range, dryrun
        self.chunk_size, self.time_chunk, self.max_value = chunk_size, time_chunk, max_value

        self.get_tags_to_parse(measurement)


    def get_tag_set(self, measurement, tag, value, constraint=None, nullable=True):
        if value and value != "None":
            return [value]
        result = self.data_getter.get_tag_values(tag, measurement, constraint)
        if nullable:
            result += [""]
        return result

    def get_tags_to_parse(self, measurement):
        self.hostnames = self.get_tag_set(measurement, "hostname", self.hostname, False)
        logger.info("Found hostnames %s", self.hostnames)


    def peaks_remover(self):
        for low, high in time_chunks("0s", self.range, self.time_chunk):
            logger.info("Parsing the time intervals between %s and %s seconds since now", epoch_to_time(low / 1e9), epoch_to_time(high / 1e9))
            self.parse_time_slot(low, high)

    def parse_time_slot(self, low, high):
        for hostname in self.hostnames:
            services  = self.get_tag_set(self.measurement, "service", self.service, {"hostname":hostname}, False)
            logger.info("Found services %s", services)
            for service in services:
                for metric in ["inBandwidth", "outBandwidth"]:
                    logger.info("Checking measurement [%s] hostname [%s] service [%s] metric [%s]", self.measurement, hostname, service, metric)
                    tags = dict(zip(["hostname", "service", "metric"], [hostname, service, metric]))
                    self.parse_and_remove(low, high, tags)
        
        

    def parse_and_remove(self, low, high, indices):
        data = self.data_getter.exec_query(FIND_QUERY.format(measurement=self.measurement, high=high, low=low, **indices))
        df = pd.DataFrame(data)
        logger.info("Got %d datapoints", len(df))

        if len(df) == 0:
            return 

        df["pd_time"] = pd.to_datetime(df.time, unit="s")

        groups = df.groupby(pd.Grouper(key="pd_time", freq=self.window))

        outliers = pd.concat([
            group[(group.value > self.max_value) & (group.value > self.coeff * group.value.mean())]
            for index, group in groups
        ])

        means = [
            group.value.mean()
            for index, group in groups
        ]

        if len(outliers) == 0:
            return

        logger.warn("Found %d outliers for %s", len(outliers), indices)
        logger.warn("These outliers were at %s", [epoch_to_time(x) for x in outliers.time.values])
        logger.debug("outliers %s", outliers)
        logger.debug("means %s", means)

        if not self.dryrun:
            for outlier in outliers.iterrows():
                outlier = outlier[1].to_dict()
                self.data_getter.exec_query(
                    REMOVE_POINT.format(
                        time=(int(outlier["time"]) * 1_000_000_000),
                        service=outlier["service"],
                        hostname=outlier["hostname"],
                        metric=outliers["metric"],
                        measurement=self.measurement,
                    )
                )

                