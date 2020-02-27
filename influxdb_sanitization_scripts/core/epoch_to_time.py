import time

def epoch_to_time(epoch):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(epoch))