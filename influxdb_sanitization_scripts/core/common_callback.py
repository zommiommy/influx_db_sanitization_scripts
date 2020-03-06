import sys
import logging
from .logger import logger, setLevel

def ask_user_to_continue():
    value = input("Do you want to continue? [y/N]\n")
    if not value.startswith("y"):
        sys.exit(0)


def common_callback(values):
    verbosity = values.pop("verbosity")
    if verbosity == 0:
        setLevel(logging.CRITICAL)
    elif verbosity == 1:
        setLevel(logging.INFO)
    else: 
        setLevel(logging.DEBUG)

    if not values.pop("force"):
        print("This could delete data. Please make a backup if you don't already have one.")
        ask_user_to_continue()
