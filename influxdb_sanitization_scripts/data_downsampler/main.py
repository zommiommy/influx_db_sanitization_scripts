from itertools import product
from time import time
import pandas as pd
from ..core import logger, DataGetter, get_filtered_labels, epoch_to_time

BACKUP       = """SELECT * FROM "{measurement}" WHERE time >= now() - {start} AND time <= now() - {end}"""
AGGREGATE    = """SELECT MEAN(value) as "value" FROM "{measurement}" WHERE service = '{service}' AND hostname = '{hostname}' AND metric = '{metric}' AND time >= now() - {start} AND time <= now() - {end} GROUP BY time({window}) """
REMOVE_POINT = """DELETE FROM {measurement} WHERE service = '{service}' AND hostname = '{hostname}' AND metric = '{metric}' AND time >= now() - {start} AND time <= now() - {end}"""


def get_clean_dataframe(data_getter, query):
    data = data_getter.exec_query()
    # Setup the dataframe
    df = pd.DataFrame(data)
    # Time index so it can be written
    df["time"] = pd.to_datetime(df.time, unit="s")
    df = df.set_index("time")
    return df

def data_downsampler(data_getter: DataGetter, measurement: str, window: str="10m", start: str="1d", end: str="2d", dryrun: bool = False, backup: bool = False):

    hostnames = data_getter.get_tag_values("hostname", measurement) or [""]
    services  = data_getter.get_tag_values("service" , measurement) or [""]
    metrics   = data_getter.get_tag_values("metric"  , measurement) or [""]

    if backup:
        df = get_clean_dataframe(data_getter, BACKUP.format(**locals()))
        df.to_csv(str(epoch_to_time(time())) + "_backup.csv")

    for hostname, service, metric in product(hostnames, services, metrics):
        logger.info("analyzing hostname:[%s] service:[%s] metric:[%s]", hostname, service, metric)
        # Get the data
        df = get_clean_dataframe(data_getter, AGGREGATE.format(**locals()))
        # Add constant values
        df["hostname"] = hostname
        df["service"]  = service
        df["metric"]   = metric
        logger.info("Got %d datapoints", len(df))
        # Write the new points
        logger.info("Deleting the old points")
        data_getter.exec_query(REMOVE_POINT.format(**locals()))
        logger.info("Writing the new downsampled values")
        data_getter.write_dataframe(df, measurement + "_test")
        break
