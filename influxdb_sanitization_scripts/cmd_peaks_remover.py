import logging
from .core import DataGetter, get_common_parser, common_callback
from .peaks_remover import peaks_remover

def cmd_peaks_remover():
    parser = get_common_parser()

    parser.add_argument("measurement", type=str,help="The measurement to use")
    parser.add_argument("-c", "--coeff", type=float, default=1, help="How many time the point has to be over the mean to be considered a peak and removed.")
    parser.add_argument("-w", "--window", type=str, default="1d", help="How big are the chunks with which the means are computed.")
    parser.add_argument("-r", "--range", type=str, default="4w", help="How many ")
    values = vars(parser.parse_args())

    common_callback(values)

    dg = DataGetter(values.pop("db_settings_path"))
    peaks_remover(dg, **values)