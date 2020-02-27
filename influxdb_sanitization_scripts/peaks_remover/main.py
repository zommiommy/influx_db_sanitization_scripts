
import pandas as pd
from tqdm.auto import tqdm
from ..core import logger, DataGetter, get_filtered_labels, consistent_groupby

FIND_QUERY = """SELECT * FROM "{measurement}" WHERE time > now() - {range}"""
REMOVE_POINT = """DELETE FROM {measurement} WHERE time = {time}"""

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


class PeaksRemover:

    def __init__(self,
        data_getter: DataGetter,
        measurement: str,
        field: str="value",
        coeff:float = 100,
        window: str="10m",
        range: str="1d",
        dryrun: bool = False,
        chunk_size: int = 1000,
    ):
        self.data_getter, self.measurement, self.field = data_getter, measurement, field
        self.coeff, self.window, self.range, self.dryrun = coeff, window, range, dryrun
        self.chunk_size = chunk_size

    def peaks_remover(self):
        data = self.data_getter.exec_query(FIND_QUERY.format(**vars(self)))
        df = pd.DataFrame(data)

        df["pd_time"] = pd.to_datetime(df.time, unit="s")
        
        logger.info("Got %d datapoints", len(df))

        if len(df) == 0:
            return 

        labels = get_filtered_labels(df, self.field)

        logger.info("Groupping by the values of %s", labels)
        
        consistent_groupby(df, labels, self.parse_and_remove)
            

    def parse_and_remove(self, data, indices):
        groups = data.groupby(pd.Grouper(key="pd_time", freq=self.window))

        outliers = pd.concat([
            group[group[self.field] > self.coeff * group[self.field].mean()]
            for index, group in tqdm(groups, total=len(groups))
        ])

        logger.info("Found %d outliers for %s", len(outliers), indices)

        if not self.dryrun and len(outliers) > 0:
            for chunk in tqdm(chunks(outliers.time, self.chunk_size), total=int(len(outliers) // self.chunk_size)):
                query = " ; ".join(
                    REMOVE_POINT.format(time=(int(timestamp) * 1_000_000_000), **vars(self))
                    for timestamp in chunk
                )
                self.data_getter.exec_query(query)

                