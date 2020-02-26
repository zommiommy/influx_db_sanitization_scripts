import sys
import argparse

class MyParser(argparse.ArgumentParser):
    """Custom parser to ensure that the exit code on error is 1
        and that the error messages are printed on the stderr 
        so that the stdout is only for sucessfull data analysis"""

    def error(self, message):
        sys.stderr.write('error: %s\n' % message)
        self.print_help(file=sys.stderr)
        sys.exit(1)

        
def get_common_parser():
    parser = MyParser()
    parser.add_argument("-v", "--verbosity", type=int, help="Verbosity of the program. 0 - Critical, 1 - Info, 2 - Debug", default=0)
    parser.add_argument("-dr", "--dryrun", default=False, action="store_true")
    parser.add_argument("-f", "--force", default=False, action="store_true")
    parser.add_argument("-dsp", "--db-settings-path", type=str, default="db_settings.json", help="Path to the json with the settings for the DB connections.")
    return parser