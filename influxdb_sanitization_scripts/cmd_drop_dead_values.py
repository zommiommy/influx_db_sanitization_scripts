import logging
from .core import DataGetter, get_common_parser, common_callback
from .drop_dead_values import drop_dead_values

description = """If a tag had no new data in the last x years, drop the values from the measurement (grouped by hostname, service, and metric)."""

def cmd_test_drop_dead_values():
    parser = get_common_parser(description)

    parser.add_argument("-m", "--max-time", type=int, default=1, help="Threshold time in seconds, if a measurement has no points newer than ")
    values = vars(parser.parse_args())

    common_callback(values)

    dg = DataGetter(values.pop("db_settings_path"))
    drop_dead_values(dg, **values)