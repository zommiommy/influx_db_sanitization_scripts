
import pandas as pd
from tqdm.auto import tqdm
from ..core import DataGetter
from ..core import logger

FIND_QUERY = """SELECT time, {field} as value FROM "{measurement}" WHERE time > now() - {range}"""
REMOVE_POINT = """DELETE FROM {measurement} WHERE time == {timestamp}"""

def peaks_remover(data_getter: DataGetter, measurement: str, field: str="value", coeff:float = 1.01, window: str="10m", range: str="1d", dryrun: bool = True):
    data = data_getter.exec_query(FIND_QUERY.format(**locals()))
    df = pd.DataFrame(data)

    logger.info("Got %d datapoints", len(df))

    if len(df) > 0:

        df["pd_time"] = pd.to_datetime(df.time, unit="s")
        groups = df.groupby(pd.Grouper(key="pd_time", freq=window))

        outliers = pd.concat([
            group[group.value > coeff * group.value.mean()]
            for index, group in tqdm(groups, total=len(groups))
        ])

        logger.info("Found %d outliers", len(outliers))

        if not dryrun:
            for timestamp in tqdm(outliers.time):
                data_getter.exec_query(REMOVE_POINT.format(**locals()))

        