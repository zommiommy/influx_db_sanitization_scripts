import logging
from .core import DataGetter, get_common_parser, common_callback
from .drop_dead_tags import drop_dead_tags

description = """If a tag had no new data in the last 2 years, drop the tag from all measurements.
The values are configurable."""

def cmd_test_drop_dead_tags():
    parser = get_common_parser(description)

    parser.add_argument("-m", "--max-time", type=int, default=1, help="Threshold time in seconds, if a measurement has no points newer than ")
    values = vars(parser.parse_args())

    common_callback(values)

    dg = DataGetter(values.pop("db_settings_path"))
    drop_dead_tags(dg, **values)