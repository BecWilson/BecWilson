#
import numpy   as np             #
import pandas  as pd             # dataframe support...

import pyarrow as pa             # in-memory analytics 
import pyarrow.parquet as pq     # reading and writing apache parquet

import openpyxl
import psycopg2 as sql           # Postgres Support and /copy command. copy command utility is faster than
                                 # JDBC/ ODBC and native database wr

def copy_Proc():                 # define a proc that can be run by name
    "\copy test_sensor_data from 'C:\Data\Caurus\Fluence\test_sensor_data.csv' DELIMITER ',' csv header;"
 
# Connect to an existing database
connection = sql.connect(user="postgres",
                                  password="admin",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="example")
# Create a cursor to perform database operations
cursor = connection.cursor()

read_file = pd.read_excel (r'C:\Data\Caurus\Fluence\schema1.xlsx', sheet_name='Sheet1')
read_file.to_csv (r'C:\\Data\\Caurus\\Fluence\\test_sensor_data.csv', index = None, header=True)

# loads the data into memory / a variable and then uses a copy_expert command to do the copy.  will have to test
# the speed of the two approaches

# f = StringIO(my_tsv_string)
# cursor.copy_expert("COPY my_table FROM STDIN WITH CSV DELIMITER AS E'\t' ENCODING 'utf-8' QUOTE E'\b' NULL ''", f)

cursor.execute("copy test_sensor_data from 'C:\\Data\\Caurus\\Fluence\\test_sensor_data.csv' DELIMITER ',' csv header;")
cursor.execute("commit;")

cursor.execute("select * from test_sensor_data;")

print(cursor.rowcount)

cursor.close()
connection.close()
