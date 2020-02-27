
from .parse_time import parse_time

def time_chunks(start, delta, chunk):
    k = 1_000_000_000
    time = parse_time(delta)
    chunk = parse_time(chunk)
    low = 0
    high = min(chunk, time)

    yield int(start - low) * k, int(start - high) * k

    while high < time:
        low = high
        high = min(low + chunk, time)
        yield int(start - low) * k, int(start - high) * k