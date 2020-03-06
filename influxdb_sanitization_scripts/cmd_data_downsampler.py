import logging
from .core import DataGetter, get_common_parser, common_callback, validate_time, ask_user_to_continue
from .data_downsampler import DataDownSampler

description = """This scripts take the values between 6 month and 2 years and downsample them by aggregating values from windows of 15 minutes."""

def cmd_data_downsampler():
    parser = get_common_parser(description)

    parser.add_argument("-m", "--measurement", default=None, type=str, help="The measurement to use")
    parser.add_argument("-e", "--end",    type=str, help="Inclusive Upper-bound of the time to be parsed")
    parser.add_argument("-s", "--start",  type=str, help="Inclusive Lower-bound of the time to be parsed")
    parser.add_argument("-w", "--window", type=str, help="How big are the chunks with which the means are computed.")
    parser.add_argument("-b", "--backup", default=False, action="store_true", help="If setted, the script will save as csv all the value on which the analysis will work")
    parser.add_argument("-i", "--interval", type=str, default="7w", help="The analysis will be divided in intervals to bypass the timeout error")
    values = vars(parser.parse_args())

    validate_time(values["end"])
    validate_time(values["start"])

    common_callback(values)

    dg = DataGetter(values.pop("db_settings_path"))

    measurement = values.pop("measurement")

    ds = DataDownSampler(dg, **values)

    if measurement:
        ds.downsample_single_measurement(measurement)
    else:
        if not values["force"]:
            ask_user_to_continue("This will downsample ALL the measurements")
        ds.downsample_all_measurements()
