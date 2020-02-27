# CheckTime is a free software developed by Tommaso Fontana for Wurth Phoenix S.r.l. under GPL-2 License.

import os
import sys
import json
import logging
from typing import List, Tuple, Dict, Union
from influxdb import InfluxDBClient, DataFrameClient

from .logger import logger

class DataGetter:
    

    def __init__(self, setting_file= "db_settings.json"):
        """Load the settings file and connect to the DB"""
        # Get the current folder
        current_script_dir = "/".join(__file__.split("/")[:-3])
        
        path = current_script_dir + "/" + setting_file
        logger.info("Loading the DB settings from [%s]"%path)

        # Load the settings
        with open(path, "r") as f:
            self.settings = json.load(f)

        logger.info("Conneting to the DB on [{host}:{port}] for the database [{database}]".format(**self.settings))
        
        # Create the client passing the settings as kwargs
        self.client = InfluxDBClient(**self.settings)
        self.dfclient = DataFrameClient(**self.settings)

    def __del__(self):
        """On exit / delation close the client connetion"""
        if "client" in dir(self):
            self.client.close()

    def exec_query(self, query : str):
        # Construct the query to workaround the tags distinct constraint
        query = query.replace("\\", "\\\\")
        logger.debug("Executing query [%s]"%query)
        result = self.client.query(query, epoch="s")
        if type(result) == list:
            return [
                list(subres.get_points())
                for subres in result
            ]
            
        return list(result.get_points())

    def get_measurements(self) -> List[str]:
        """Get all the measurements sul DB"""
        result = [
            x["name"]
            for x in self.client.get_list_measurements()
        ]
        logger.info("Found the measurements %s"%result)
        return result

    def drop_measurement(self, measurement: str) -> None:
        self.client.drop_measurement(measurement)

    def write_dataframe(self, df, measurement):
        self.dfclient.write_points(df, measurement, time_precision="s")

    def get_tag_values(self, tag):
        result = self.exec_query("""SHOW TAG VALUES WITH KEY = "{tag}" """.format(tag=tag))
        return [
            x["value"].trim("'")
            for x in result
        ]