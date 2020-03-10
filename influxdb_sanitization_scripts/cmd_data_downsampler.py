import logging
from time import sleep
from .core import DataGetter, get_common_parser, common_callback, validate_time, ask_user_to_continue
from .data_downsampler import DataDownSampler

description = """
Example
./data_downsampler -v 2 -w 15m -s 26w -e 52w

This scripts take the values between 26 weeks and 52 weeks from now
and downsample them by aggregating values from windows of 15 minutes
and set the verbosity to 2 (INFO)"""

def cmd_data_downsampler():
    parser = get_common_parser(description)

    parser.add_argument("-m", "--measurement", default=None, type=str, help="The measurement to use")
    parser.add_argument("-H", "--hostname", type=str, nargs='+', default="None", help="The hostname to select")
    parser.add_argument("-S", "--service", type=str, nargs='+', default="None", help="The service to select")
    parser.add_argument("-e", "--end",    type=str, help="Inclusive Upper-bound of the time to be parsed")
    parser.add_argument("-s", "--start",  type=str, help="Inclusive Lower-bound of the time to be parsed")
    parser.add_argument("-w", "--window", type=str, help="How big are the chunks with which the means are computed.")
    parser.add_argument("-b", "--backup", default=False, action="store_true", help="If setted, the script will save as csv all the value on which the analysis will work")
    parser.add_argument("-i", "--interval", type=str, default="7w", help="The analysis will be divided in intervals to bypass the timeout error")
    values = vars(parser.parse_args())

    validate_time(values["end"])
    validate_time(values["start"])

    common_callback(values)

    if type(values["service"]) == list:
        values["service"] = " ".join(values["service"])
    if type(values["hostname"]) == list:
        values["hostname"] = " ".join(values["hostname"])

    dg = DataGetter(values.pop("db_settings_path"))

    measurement = values.pop("measurement")

    ds = DataDownSampler(dg, **values)

    if measurement:
        ds.downsample_single_measurement(measurement)
    else:
        logging.warn("Going to drop ALL measurements, press Ctrl-C if you want to stop")
        sleep(5)
        ds.downsample_all_measurements()
