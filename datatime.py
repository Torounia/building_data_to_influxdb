import time
from datetime import datetime, timedelta

date_time_str = '200817162736'
date_time_obj = datetime.strptime(date_time_str, '%y%m%d%H%M%S')
date_time_obj.isoformat()

d = (datetime.now() - timedelta(hours=25)).strftime('%y%m%d%H%M%S')
print('Time-24h:', (datetime.now() - timedelta(hours=25)).strftime('%y%m%d%H%M%S'))


print('Date:', date_time_obj.date())
print('Time:', date_time_obj.time())
print('Date-time:', date_time_obj)
print('Date-time ISO:', date_time_obj.isoformat())
print(time.mktime(datetime.strptime(date_time_str, '%y%m%d%H%M%S').timetuple())) 