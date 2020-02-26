import logging
from .core import DataGetter, get_common_parser, common_callback
from .peaks_remover import peaks_remover

description = """This scripts calculate the mean in 2 hours windows of data and remove peaks that are 3 times bigger than the mean of its window.
The values are configurable."""

def cmd_peaks_remover():
    parser = get_common_parser(description)

    parser.add_argument("measurement", type=str,help="The measurement to use")
    parser.add_argument("-c", "--coeff", type=float, default=1, help="How many time the point has to be over the mean to be considered a peak and removed.")
    parser.add_argument("-w", "--window", type=str, default="1d", help="How big are the chunks with which the means are computed.")
    parser.add_argument("-r", "--range", type=str, default="4w", help="How back the scripts goes to clean the data.")
    parser.add_argument("-fi", "--field", type=str, default="field", help="The name of the column to use for peak deletion")
    values = vars(parser.parse_args())

    common_callback(values)

    dg = DataGetter(values.pop("db_settings_path"))
    peaks_remover(dg, **values)