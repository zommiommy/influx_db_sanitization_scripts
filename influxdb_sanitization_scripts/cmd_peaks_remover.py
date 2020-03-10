import logging
from .core import DataGetter, get_common_parser, common_callback
from .peaks_remover import PeaksRemover

description = """
Example:
./peaks_remover -v 2 -c 30 -w 4h -r 1d -t 4h check_iftraffic64 -f -m 100000000

This scripts calculate the mean in 4 hours windows of data on 
the measurement check_iftraffic64 and remove peaks that are 30
times bigger than the mean of its window and bigger than 100000000.
The queries will be divided in chunks of 4 hours and set the verbosity
to 2 (INFO).
Since -f is passed, no security prompt will be shown.
"""

def cmd_peaks_remover():
    parser = get_common_parser(description)

    parser.add_argument("measurement", type=str,help="The measurement to use")
    parser.add_argument("-H", "--hostname", type=str, nargs='+', default="None", help="The hostname to select")
    parser.add_argument("-S", "--service", type=str, nargs='+', default="None", help="The service to select")
    parser.add_argument("-c", "--coeff", type=float, default=10, help="How many time the point has to be over the mean to be considered a peak and removed.")
    parser.add_argument("-w", "--window", type=str, default="1h", help="How big are the chunks with which the means are computed.")
    parser.add_argument("-r", "--range", type=str, default="4w", help="How back the scripts goes to clean the data.")
    parser.add_argument("-t", "--time-chunk", type=str, default="6h", help="The scripts analize separately chunks of time in order to don't timeout the queries.")
    parser.add_argument("-m", "--max-value", type=int, default=1e8, help="The max value the point may have without being checked if outlier")
    values = vars(parser.parse_args())

    common_callback(values)

    values["service"] = " ".join(values["service"])
    values["hostname"] = " ".join(values["hostname"])

    dg = DataGetter(values.pop("db_settings_path"))
    PeaksRemover(dg, **values).peaks_remover()