
from .parse_time import parse_time

def time_chunks(start, delta, chunk):
    time = parse_time(delta)
    chunk = parse_time(chunk)
    low = 0
    high = min(chunk, time)

    yield int(start - low), int(start - high)

    while high < time:
        low = high
        high = min(low + chunk, time)
        yield int(start - low), int(start - high)