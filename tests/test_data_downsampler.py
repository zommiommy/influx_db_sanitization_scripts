
from influxdb_sanitization_scripts import DataGetter
from influxdb_sanitization_scripts import data_downsampler

def test_data_downsampler():
    dg = DataGetter("tests/test_db_settings.json")
    data_downsampler(dg, "h2o_temperature")
