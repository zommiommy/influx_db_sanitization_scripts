
from influxdb_sanitization_scripts import DataGetter
from influxdb_sanitization_scripts import peaks_remover

def test_peaks_remover():
    dg = DataGetter("tests/test_db_settings.json")
    peaks_remover(dg, "h2o_temperature", range="9999w")