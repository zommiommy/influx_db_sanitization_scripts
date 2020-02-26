import logging
from .core import DataGetter, get_common_parser, common_callback
from .data_downsampler import data_downsampler

description = """This scripts take the values between 6 month and 2 years and downsample them by aggregating values from windows of 15 minutes."""

def cmd_data_downsampler():
    parser = get_common_parser(description)

    parser.add_argument("measurement", type=str,help="The measurement to use")
    parser.add_argument("-w", "--window", type=str, default="1d", help="How big are the chunks with which the means are computed.")
    parser.add_argument("-M", "--max", type=str, default="104w", help="Inclusive Upper-bound of the time to be parsed")
    parser.add_argument("-m", "--min", type=str, default="24w",  help="Inclusive Lower-bound of the time to be parsed")
    parser.add_argument("-fi", "--field", type=str, default="value", help="The name of the column to use for peak deletion")
    values = vars(parser.parse_args())

    common_callback(values)

    dg = DataGetter(values.pop("db_settings_path"))
    data_downsampler(dg, **values)