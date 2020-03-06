from itertools import product
import pandas as pd
from tqdm.auto import tqdm
from ..core import logger, DataGetter, get_filtered_labels

GET_TAG_VALUES = """SHOW TAG VALUES FROM "{measurement}" WITH KEY = "{tag}" """
FIND_QUERY = """SELECT time, service, hostname, metric, value FROM "{measurement}" WHERE time > now() - {range}"""
AGGREGATE  = """SELECT time, service, hostname, metric, value FROM "{measurement}" WHERE AND time >= {min} AND time <= {max} """
REMOVE_POINT = """DELETE FROM {measurement} WHERE service = '{service}' AND hostname = '{hostname}' AND time >= {min} AND time <= {max}"""

def data_downsampler(data_getter: DataGetter, measurement: str, window: str="10m", field:str = "value", min: str="1d", max: str="1d", dryrun: bool = False):
    combinations = [
         [
            x["value"]
            for x in data_getter.exec_query(GET_TAG_VALUES.format(measurement=measurement, tag=tag))
        ]
        for tag in ["hostname", "service", "metric"]
    ]
    
    logger.info("Got combinations %s", combinations)

    for hostname, service, metric in product(combinations):
        logger.info("%s %s %s", hostname, service, metric)

    raise NotImplementedError("QUESTO VA CONTROLLATO INSIEME")
    
    data = data_getter.exec_query(FIND_QUERY.format(**locals()))
    df = pd.DataFrame(data)

    logger.info("Got %d datapoints", len(df))

    if len(df) > 0:

        df["pd_time"] = pd.to_datetime(df.time, unit="s")
        
        for indices, data in df.groupby(["hostname", "service", "metric"]):
            groups = data.groupby(pd.Grouper(key="pd_time", freq=window))

            downsampled = pd.concat([
                (group.time.min(), group.value.mean()) 
                for index, group in tqdm(groups, total=len(groups))
            ])

            logger.info("Downsampled to %d points from %d for %s", len(downsampled), len(data), indices)

            if not dryrun:
                logger.info("Deleting old values")

                data_getter.write_dataframe(downsampled)
                
                raise NotImplementedError("QUESTO VA CONTROLLATO INSIEME")

                data_getter.exec_query(REMOVE_POINT.format(
                    min=min(data.time) * 1_000_000, 
                    max=max(data.time) * 1_000_000
                ))