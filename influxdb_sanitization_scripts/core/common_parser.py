import sys
import argparse
from argparse import RawTextHelpFormatter

class MyParser(argparse.ArgumentParser):
    """Custom parser to ensure that the exit code on error is 1
        and that the error messages are printed on the stderr 
        so that the stdout is only for sucessfull data analysis"""

    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help(file=sys.stderr)
        sys.exit(1)

        
def get_common_parser(description):
    parser = MyParser(description=description, formatter_class=RawTextHelpFormatter)
    parser.add_argument("-v", "--verbosity", type=int, help="Verbosity of the program. 0 - Critical, 1 - Warnings, 2 - Info, 3 - Debug", default=0)
    parser.add_argument("-dr", "--dryrun", default=False, action="store_true", help="Test run, access the DB in read only mode.")
    parser.add_argument("-f", "--force", default=False, action="store_true", help="No not show the are you sure prompt.")
    parser.add_argument("-dsp", "--db-settings-path", type=str, default="db_settings.json", help="Path to the json with the settings for the DB connections.")
    return parser