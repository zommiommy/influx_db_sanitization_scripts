import logging
from .core import DataGetter, get_common_parser, common_callback, parse_time
from .drop_dead_values import drop_dead_values_dispatcher

description = """If a tag had no new data in the last x years, drop the values from the measurement (grouped by hostname, service, and metric)."""

def cmd_test_drop_dead_values():
    parser = get_common_parser(description)

    parser.add_argument("-t", "--max-time", type=str, default="52w", help="Threshold time, if a measurement has no points newer than the threshold")
    parser.add_argument("-H", "--hostname", type=str, default="None", help="The hostname to select")
    parser.add_argument("-s", "--service", type=str, default="None", help="The service to select")
    parser.add_argument("-m", "--metric", type=str, default="None", help="The metric to select")
    parser.add_argument("-M", "--measurement", type=str, default="None", help="The measurement to select")
    values = vars(parser.parse_args())

    common_callback(values)

    dg = DataGetter(values.pop("db_settings_path"))
    
    values["max_time"] = parse_time(values["max_time"])

    drop_dead_values_dispatcher(dg, **values)