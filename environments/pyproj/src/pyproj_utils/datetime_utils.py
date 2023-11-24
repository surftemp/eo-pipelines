import datetime

# YYYY-MM-DDThh:mm:ss<tz>
DATEFORMAT = "%Y-%m-%dT%H:%M:%S%z"

def date_format(dt):
    if dt is None:
        return None
    return dt.strftime(DATEFORMAT)

def date_parse(s):
    if s is None:
        return None
    return datetime.datetime.strptime(s,DATEFORMAT)

