#Author: Adrian J 2022-05
import datetime
from datetime import timedelta

def time_delta(hours: int):
    """
    Method used to get a timestamp used for a cassandra query
    @hours: hours to substract to the current time
    Current time is for development purposes 2016-11-07 00:00:00
    """
    #ct = datetime.now()
    ct = datetime.datetime(2016, 11, 7)
    delta_time = ct - timedelta(hours=hours)
    return delta_time