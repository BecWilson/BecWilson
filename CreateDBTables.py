#
import numpy   as np             #
import pandas  as pd             # dataframe support...

import pyarrow as pa             # in-memory analytics 
import pyarrow.parquet as pq     # reading and writing apache parquet

import openpyxl
import psycopg2 as sql           # Postgres Support and /copy command. copy command utility is faster than
                                 # JDBC/ ODBC and native database writes

import subprocess                # Submit Spark processes

# Connect to an existing database
connection = sql.connect(user="postgres",
                                  password="admin",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="example")
# Create a cursor to perform database operations
cursor = connection.cursor()


cursor.execute("drop table if exists last_run_timespan;")
cursor.execute("commit;")

cursor.execute("""CREATE TABLE run_time_metrics 
               (last_runtime_stamp timestamptz Not NULL,
               data_start_timestamp timestamptz Not Null,
               run_type char(8) Not Null);""")
cursor.execute("commit;")

#cursor.execute("drop table if exists test_sensor_data;")
#cursor.execute("commit;")

# cursor.execute("""CREATE TABLE test_sensor_data 
#  (time_stamp  timestamptz NOT NULL, 
#  bat1_temp    numeric(6,3) NULL, 
#  bat1_charge  numeric(6,3) NULL, 
#  bat1_voltage numeric(6,3) NULL, 
#  bat2_temp    numeric(6,3) NULL, 
#  bat2_charge  numeric(6,3) NULL, 
#  bat2_voltage numeric(6,3) NULL, 
#  bat3_temp    numeric(6,3) NULL, 
#  bat3_charge  numeric(6,3) NULL, 
#  bat3_voltage numeric(6,3) NULL, 
#  bat4_temp    numeric(6,3) NULL, 
#  bat4_charge  numeric(6,3) NULL, 
#  bat4_voltage numeric(6,3) NULL, 
#  bat5_temp    numeric(6,3) NULL, 
#  bat5_charge  numeric(6,3) NULL, 
#  bat5_voltage numeric(6,3) NULL, 
#  bat6_temp    numeric(6,3) NULL, 
#  bat6_charge  numeric(6,3) NULL, 
#  bat6_voltage numeric(6,3) NULL);""")

cursor.close()
connection.close()