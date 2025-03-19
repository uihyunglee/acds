import datetime


def utc_now():
    now = datetime.datetime.now(datetime.timezone.utc)
    return now.isoformat(timespec='microseconds')
