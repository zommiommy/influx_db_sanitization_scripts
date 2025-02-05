
import re

def validate_time(value):
    value = value.strip()
    if not re.match(r"\d+[smhdw]?", value):
        raise ValueError("The time delta %s does not match the regex \d+[smhdw]?"%value)
    if value[-1].isdigit():
        value += "s"
    return value

def parse_time(value):
    value = validate_time(value)
    number = int(value[:-1])
    unit = value[-1]

    if unit == "s":
        return number
    elif unit == "m":
        return number * 60
    elif unit == "h":
        return number * 60 * 60
    elif unit == "d":
        return number * 60 * 60 * 24
    elif unit == "w":
        return number * 60 * 60 * 24 * 7
    else:
        raise ValueError("The time delta %s does not match the regex \d+[smhdw]?"%value)