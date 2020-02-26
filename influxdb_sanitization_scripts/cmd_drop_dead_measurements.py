import logging
from .core import DataGetter, get_common_parser, common_callback
from .drop_dead_measurements import drop_dead_measurements

def cmd_test_drop_dead_measurements():
    parser = get_common_parser()

    parser.add_argument("-m", "--max-time", type=float, default=1, help="Threshold time, if a measurement has no points newer than ")
    values = vars(parser.parse_args())

    common_callback(values)

    dg = DataGetter(values.pop("db_settings_path"))
    drop_dead_measurements(dg, **values)