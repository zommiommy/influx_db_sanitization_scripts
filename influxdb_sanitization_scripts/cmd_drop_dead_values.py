import logging
from .core import DataGetter, get_common_parser, common_callback, parse_time
from .drop_dead_values import DropDeadValues

description = """
Example:
./drop_dead_values -dr -t 15w -v 1 -w 5 -M ping -snn

If had no new data in the last 15 weeks on the measurement "ping"
drop the values (grouped by hostname, service, and metric).
Set verbosity to 1 (WARNING) and set 5 workers for parallel analysis.
-ssn since in this case the service tag can't be null.
"""

def cmd_test_drop_dead_values():
    parser = get_common_parser(description)

    parser.add_argument("-t", "--max-time", type=str, default="52w", help="Threshold time, if a measurement has no points newer than the threshold")
    parser.add_argument("-H", "--hostname", type=str, default="None", help="The hostname to select")
    parser.add_argument("-s", "--service", type=str, default="None", help="The service to select")
    parser.add_argument("-m", "--metric", type=str, default="None", help="The metric to select")
    parser.add_argument("-M", "--measurement", type=str, default="None", help="The measurement to select")
    parser.add_argument("-w", "--workers", type=int, default=1, help="How many query to execute in parallel")
    parser.add_argument("-p", "--use-processes", default=False, action="store_true", help="If the parallelization should use threads or processes")
    parser.add_argument("-snn", "--service-not-nullable", default=False, action="store_true", help="if the service can be null or not")
    values = vars(parser.parse_args())

    common_callback(values)

    dg = DataGetter(values.pop("db_settings_path"))
    
    values["max_time"] = parse_time(values["max_time"])

    ddv = DropDeadValues(dg,
        values.pop("dryrun"),
        values.pop("max_time"),
        values.pop("workers"),
        values.pop("use_processes"),
        values.pop("service_not_nullable")
    )
    ddv.drop_dead_values_dispatcher(**values)