
from influxdb_sanitization_scripts import DataGetter
from influxdb_sanitization_scripts import drop_dead_measurements

def test_drop_dead_measurements():
    dg = DataGetter("tests/test_db_settings.json")
    drop_dead_measurements(dg, "h2o_temperature")
