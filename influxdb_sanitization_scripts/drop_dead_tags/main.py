import pandas as pd
from time import time
from ..core import DataGetter
from humanize import naturaldelta
from ..core import logger

GET_SERVICES = """SHOW TAG VALUES WITH KEY = "service" """
GET_HOSTNAMES = """SHOW TAG VALUES WITH KEY = "hostname" """

MOST_RECENT_QUERY = """SELECT time, service FROM (SELECT * FROM /.*/ WHERE hostname = {hostname} AND time > now() - 2w) GROUP BY service ORDER BY time DESC LIMIT 1"""


# Add blacklist and or whitelist

def drop_dead_tags(data_getter: DataGetter, dryrun: bool = True, max_time: int = 3 * 365 * 24 * 60 * 60):


    services = data_getter.exec_query(GET_SERVICES)

    logger.info("Got %d services", len(services))
    logger.debug("Got services %s", services)

    hostnames = data_getter.exec_query(GET_HOSTNAMES)
    logger.info("Got %d hostnames", len(hostnames))
    logger.debug("Got hostnames %s", hostnames)


    measurements = data_getter.get_measurements()
    logger.info("The available measurements are %s", measurements)

