
import pandas as pd
from tqdm.auto import tqdm
from ..core import logger, DataGetter, get_filtered_labels

FIND_QUERY = """SELECT time, {field} as value FROM "{measurement}" WHERE time > now() - {range}"""
REMOVE_POINT = """DELETE FROM {measurement} WHERE time >= {min} and time <= {max}"""

def data_downsampler(data_getter: DataGetter, measurement: str, window: str="10m", field:str = "value", range: str="1d", dryrun: bool = False):
    data = data_getter.exec_query(FIND_QUERY.format(**locals()))
    df = pd.DataFrame(data)

    logger.info("Got %d datapoints", len(df))

    if len(df) > 0:

        df["pd_time"] = pd.to_datetime(df.time, unit="s")
        
        labels = get_filtered_labels(df, field)

        for indices, data in df.groupby(labels):
            groups = data.groupby(pd.Grouper(key="pd_time", freq=window))

            downsampled = pd.concat([
                (group.time.min(), group.value.mean()) 
                for index, group in tqdm(groups, total=len(groups))
            ])

            logger.info("Downsampled to %d points from %d for %s", len(downsampled), len(data), indices)

            if not dryrun:
                logger.info("Deleting old values")
                
                raise NotImplementedError("QUESTO VA CONTROLLATO INSIEME")

                data_getter.write_dataframe()

                data_getter.exec_query(REMOVE_POINT.format(
                    min=min(data.time) * 1_000_000, 
                    max=max(data.time) * 1_000_000
                ))
                
                # TODO write data to db

            