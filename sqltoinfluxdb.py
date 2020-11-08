#TODOS:
# Windows Scheduler https://stackoverflow.com/questions/34622514/run-a-python-script-in-virtual-environment-from-windows-task-scheduler
# Timestamp convert https://stackabuse.com/converting-strings-to-datetime-in-python/ & https://www.geeksforgeeks.org/convert-date-string-to-timestamp-in-python/
# How to Quary SQL in Python
#


import pypyodbc 
import pandas as pd
import datetime
import time

cnxn = pypyodbc.connect("Driver={SQL Server};"
                        "Server=LLSANDBOX-PC\SQLEXPRESS;"
                        "Database=HBT.ECD.Cognipoint;"
                        "uid=SQLadmin;pwd=LL22@@")



### InfluxDB info ####
from influxdb import InfluxDBClient
influx_db_name = ""
influxClient = InfluxDBClient("<INFLUX_HOST>", "<INFLUX_PORT>")
influxClient.delete_database(influx_db_name)
influxClient.create_database(influx_db_name)

# dictates how columns will be mapped to key/fields in InfluxDB
schema = {
    "time_column": "", # the column that will be used as the time stamp in influx
    "columns_to_fields" : ["",...], # columns that will map to fields 
    "columns_to_tags" : ["",...], # columns that will map to tags
    "table_name_to_measurement" : "", # table name that will be mapped to measurement
    }

'''
Generates an collection of influxdb points from the given SQL records
'''
def generate_influx_points(records):
    influx_points = []
    for record in records:
        tags = {}, fields = {}
        for tag_label in schema['columns_to_tags']:
            tags[tag_label] = record[tag_label]
        for field_label in schema['columns_to_fields']:
            fields[field_label] = record[field_label]
        influx_points.append({
            "measurement": schema['table_name_to_measurement'],
            "tags": tags,
            "time": record[schema['time_column']],
            "fields": fields
        })
    return influx_points



# query relational DB for all records
curr = conn.cursor('cursor', cursor_factory=psycopg2.extras.RealDictCursor)
# curr = conn.cursor(dictionary=True)
curr.execute('SELECT TOP (1000) * FROM [HBT.ECD.Cognipoint].[dbo].[HBT18015000_State]')
#curr.execute("SELECT * FROM " + schema['table_name_to_measurement'] + "ORDER BY " + schema['column_to_time'] + " DESC;")
row_count = 0
# process 1000 records at a time
while True:
    print("Processing row #" + (row_count + 1))
    selected_rows = curr.fetchmany(1000)
    #(influxClient.write_points(generate_influx_points(selected_rows))
    row_count += 1000
    if len(selected_rows) < 1000:
        break
cnxn.close()
