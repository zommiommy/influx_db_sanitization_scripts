
import pandas as pd
import time
from tqdm.auto import tqdm
from ..core import logger, DataGetter, get_filtered_labels, consistent_groupby, time_chunks, epoch_to_time

FIND_QUERY = """SELECT time, service, hostname, value, metric FROM {measurement} WHERE (metric = 'inBandwidth' OR metric = 'outBandwidth') and time >= {high:d} and time < {low:d}"""
REMOVE_POINT = """DELETE FROM {measurement} WHERE service = '{service}' AND hostname = '{hostname}' AND time = {time}"""

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
    ):
        self.data_getter, self.measurement = data_getter, measurement
        self.coeff, self.window, self.range, self.dryrun = coeff, window, range, dryrun
        self.chunk_size, self.time_chunk, self.max_value = chunk_size, time_chunk, max_value


    def peaks_remover(self):
        for low, high in time_chunks("0s", self.range, self.time_chunk):
            logger.info("Parsing the time intervals between %s and %s seconds since now", epoch_to_time(low / 1e9), epoch_to_time(high / 1e9))
            data = self.data_getter.exec_query(FIND_QUERY.format(**{**vars(self), **locals()}))
            self.parse_time_slot(data)

    def parse_time_slot(self, data):
        df = pd.DataFrame(data)

        logger.info("Got %d datapoints", len(df))

        if len(df) == 0:
            return 


        df["pd_time"] = pd.to_datetime(df.time, unit="s")
    
        for indices, data in df.groupby(["hostname", "service"]):
            self.parse_and_remove(data, dict(zip(indices, indices)))
            
        

    def parse_and_remove(self, data, indices):
        groups = data.groupby(pd.Grouper(key="pd_time", freq=self.window))

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

        logger.info("Found %d outliers for %s", len(outliers), indices)
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
                        **vars(self)
                    )
                )

                