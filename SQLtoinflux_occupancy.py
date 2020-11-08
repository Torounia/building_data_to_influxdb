import pypyodbc 
import pandas as pd
from datetime import datetime, timedelta
import time
import argparse
from influxdb import InfluxDBClient

cnxn = pypyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                        "Server=LLSANDBOX-PC\SQLEXPRESS;"
                        "Database=HBT.ECD.Cognipoint;"
                        "uid=SQLadmin;pwd=***")

cursor = cnxn.cursor()
cursor.execute("SELECT * FROM [HBT.ECD.Cognipoint].[dbo].[HBT18015000_Data] where [DPTimeStamp] >" + ((datetime.now() - timedelta(hours=25)).strftime('%y%m%d%H%M%S')) + " order by [DPTimeStamp] desc")
rows = cursor.fetchall()

client = InfluxDBClient('localhost', 8086, 'root', 'root', 'building_systems')




def convert_timestamps(timestamp):
    return int(time.mktime(datetime.strptime(timestamp, '%y%m%d%H%M%S').timetuple())) 



'''
DeviceAddress	        DeviceName	DeviceDescription	DeviceState	    DeviceTimeStamp	DeviceType	           DPAddress	DPName	      DPDescription	   DPState	DPTimeStamp	DPType	DPValue	Comments
154	                    Trend_BMS	london_first_mtgr4		            200827220655	BMS_sensor  	        0	        Space R105	   DegC		                200827220655	AI	24.435	

DeviceAddress	        DeviceName	DeviceDescription	 DeviceState    DeviceTimeStamp	DeviceType	             DPAddress	DPName	       DPDescription	DPState	DPTimeStamp	DPType	DPValue	Comments
Qnc8LeIoRkC8DqiM3nF-Mg	1_REC1	    london_first_general	            200827210421	occupancy_sensor		             1_REC1	        TRAFFIC		            200827210421		0,0	    200827210421
uZ69pY5FQduC9A_KLqg9yA	MTG2	    london_first_mtgr2		            200827210451	occupancy_sensor		              MTG2	        COUNTING	    0	    200827210451		,	    200827210451

DeviceAddress	        DeviceName	DeviceDescription	DeviceState	    DeviceTimeStamp	DeviceType	            DPAddress	DPName	        DPDescription	DPState DPTimeStamp	DPType	DPValue	Comments
993	                    OMNI 001 - 	london_first_general_4A		        200827220502	environmental_sensor	temp	    OMNI 001 - WTS	temp		            200827220502	temp	24.9899997711182	
993	                    OMNI 001 - 	london_first_general_4A		        200827220502	environmental_sensor	pm25	    OMNI 001 - WTSy	pm25		            200827220502	pm25	2	


Device Name
Omni…
1_rec1
Trend_BMS

Device Description (location)


Device_Type = DeviceType

Datapoint_name
Environmental = DPDescrption
Occupancy = DPDescrption
BMS = DPname

'''

device_name = [] #DeviceName
device_location = [] #DeviceDescrption yes all
device_type = [] #DeviceType yes all
datapoint_name = [] #DPname 
datapoint_value_area = [] # dpstate Cognipoint only!
datapoint_time = [] #dptimestamp yes all
datapoint_value_traf = [] #dpvalue datapoint value for the rest
for row in rows:
    device_name.append(row[1])
    device_location.append(row[2])
    device_type.append(row[5])
    datapoint_name.append(row[8])
    datapoint_value_area.append(row[9])
    datapoint_time.append(convert_timestamps(row[10]))
    datapoint_value_traf.append(row[12])



influx_points = []

for row in range(len(datapoint_time)):
    influx_points.append(
        {
        "measurement": "Occupancy",
        "tags": {
            "device_name": device_name[row],
            "device_location_office": device_location[row].split('_')[0],
            "device_location_floor": device_location[row].split('_')[1],
            "device_location_area": device_location[row].split('_')[2],
            "device_type": device_type[row],
            "datapoint-name": datapoint_name[row]

        },
        "time": datapoint_time[row],
        "fields": {
            "area_counter": int('0' if datapoint_value_area[row] == '' else datapoint_value_area[row]),
            "traffic_counter_in": int('0' if datapoint_value_traf[row].split(',')[0] == '' else datapoint_value_traf[row].split(',')[0]),
            "traffic_counter_out": int('0' if datapoint_value_traf[row].split(',')[1] == '' else datapoint_value_traf[row].split(',')[1]),
            }
    })
    

client_write_start_time = time.perf_counter()

client.write_points(influx_points, database='building_systems', time_precision='s', batch_size=10000)

client_write_end_time = time.perf_counter()

print('write complete')

print("Client Library Write: {time}s".format(time=client_write_end_time - client_write_start_time))

    
