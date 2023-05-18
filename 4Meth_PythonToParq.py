# shebang
#     _____        5 Methods to write data from Python to parque files.
#    /  _  |
#   /  / | |       1 - pandas.to_parquet with pyarrow engine    (Panda)
#  /  /__| |__     2 - pandas and fastParquet.write             (fastP)
# |___________|    3 - pyarrow schema and pyarrow.ParquetWriter (pyArr)
#        | |       4 - spark dataframe.write.parquet            (Spark)
#        | |       
#        |_|       



# Methods #1 - #3 start with a pandas dataframe
# Method  #4 - needs a spark session and a spark dataframe
#
# done: 1 - separate schema description from data load in #4 spark
# done: 2 - Get overwrite working in #4 spark
# done: 3 - load #4 spark dataframe from from pandas dataframe like #1-#3
# todo: 4 - implement #5 adrian
# todo: 4 - spark overwrite deletes the file first, if its open this will fail, (catch, move on)
# todo: 6 - random error - (TID 0) (10.0.0.26 executor driver): org.apache.spark.SparkException: Python worker failed to connect back.
#           (catch, retry)
# todo: 7 - john's docker setup.
# done: 8 - install vscode database client extension
# todo: 9 - when #4 is moved to ReadTSDB_GroupTo_#4Spark.py add the code to load the spark.parquet from the database.  i guess now
#           that im documenting this, i have to do that for #1-#3 anyway so it will probably use most of that code with a tweak
#           or two.
#

import pandas as pd
import numpy
import pyarrow as pa             # in-memory analytics 
import pyarrow.parquet as pq  

# print("") 
# print("       ________       _          _             __          __  ______    _          _         ______      " )
# print("      / ____ _/      / \        / \           /  \        / / / ____/   / \        / \       / ____ \     " )
# print("     / /            /   \      /   \         / /\ \      / / / /__     /   \      /   \     / /    \ \    " )
# print("    / /            / /\  \    /  /\ \       / /  \ \    / / / ___/    / /\  \    /  /\ \   |  |    |  |   " )
# print("   / /            / /  \  \  /  /  \ \     / /    \ \  / / / /       / /  \  \  /  /  \ \  |  |    |  |   " )
# print("  / /_____   __  / /    \  \/  /    \ \   / /      \ \/ / / /_____  / /    \  \/  /    \ \  \ \____/ /    " )
# print(" /_______/  /_/ /_/      \____/      \_\ /_/        \__/ /_______/ /_/      \____/      \_\  \______/     " )
# print("                                                                                   #1 PANDAS Only         " )
# print("")

df = pd.read_excel("c:\\data\\caurus\\fluence\\schema1.xlsx")

# Method # 1: Using Plain Pandas (pandas dataframe to parquet)
# Methods 1 - 3 start with a pandas dataframe

print("Method 1 starting - Plain Pandas")

dtypes = numpy.dtype(
    [
        ("a", str),
        ("b", int),
        ("c", float),
        ("d", numpy.datetime64('2015-01-31T00:00:00.000','ms')),
    ]
)
dfNumpy = pd.DataFrame(numpy.empty(0, dtype=dtypes))

df.to_parquet("c:\\data\\caurus\\fluence\\meth1_PyToPArq.parquet", engine = "pyarrow", compression = 'gzip')
print("Method 1 ending - Plain Pandas")


# Method # 2: Using Pandas & FastParquet (you can append to parquet files so you can write in batches)
# Methods 1 - 3 start with a pandas dataframe

import fastparquet as fp

print("Method 2 starting - Pandas annd fastparquet")
fp.write("c:\\data\\caurus\\fluence\\meth2_PyToPArq.parquet", df, compression = 'GZIP')
print("Method 2 ending - Pandas annd fastparquet")

# Method # 3: Using Pandas & PyArrow (you can append to parquet files so you can write in batches)
#             It needs schemas defined!!! ###
# Methods 1 - 3 start with a pandas dataframe  

parquet_schema = pa.schema([('timestamp', pa.timestamp('ns')),
                            ('bat 1 temp', pa.float64()),
                            ('bat 1 charge', pa.int64()),
                            (' bat 1 voltage', pa.float64()),
                            ('bat 2 temp', pa.float64()),
                            ('bat 2 charge', pa.int64()),
                            (' bat 2 voltage', pa.float64()),
                            ('bat 3 temp', pa.float64()),
                            ('bat 3 charge', pa.int64()),
                            (' bat 3 voltage', pa.float64()),
                            ('bat 4 temp', pa.float64()),
                            ('bat 4 charge', pa.int64()),
                            (' bat 4 voltage', pa.float64()),
                            ('bat 5 temp', pa.float64()),
                            ('bat 5 charge', pa.int64()),
                            (' bat 5 voltage', pa.float64()),
                            ('bat 6 temp', pa.float64()),
                            ('bat 6 charge', pa.int64()),
                            (' bat 6 voltage', pa.float64())
                           ])

print("Method 3 starting - Pandas and PyArrow")

table = pa.Table.from_pandas(df)
pqObj = pq.ParquetWriter("c:\\data\\caurus\\fluence\\meth3_PyToPArq.parquet", parquet_schema, compression='gzip')
pqObj.write_table(table)
pqObj.close

print("Method 3 ending - Pandas and PyArrow")

###########################################################################################
# Method # 4: Using Pyspark (can also batch up appends)
#
# Method #4 - needs a spark session and a spark dataframe

import pyspark   
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkConf

print("Method 4 starting - PySpark - Session Builder")

spark = SparkSession.builder.config("spark.some.config.option", "some-value") \
	.master("local").appName("PySpark_Parquet_test").getOrCreate()

print("Method 4 loading schema")
               
PySpark_session_schema_Df = {   'timestamp':      'datetime64[ns]',
                                'bat 1 temp':     'float64',
                                'bat 1 charge':   'float64',
                                ' bat 1 voltage': 'float64',
                                'bat 2 temp':     'float64',
                                'bat 2 charge':   'float64',
                                ' bat 2 voltage': 'float64',
                                'bat 3 temp':     'float64',
                                'bat 3 charge':   'float64',
                                ' bat 3 voltage': 'float64',
                                'bat 4 temp':     'float64',
                                'bat 4 charge':   'float64',
                                ' bat 4 voltage': 'float64',
                                'bat 5 temp':     'float64',
                                'bat 5 charge':   'float64',
                                ' bat 5 voltage': 'float64',
                                'bat 6 temp':     'float64',
                                'bat 6 charge':   'float64',
                                ' bat 6 voltage': 'float64'}


print("Method 4 creating student dataframe")
studentDfx = spark.createDataFrame([
 	Row(id=1,name='vijay',marks=67),
 	Row(id=2,name='Ajay',marks=88),
 	Row(id=3,name='jay',marks=79),
 	Row(id=4,name='vinay',marks=67),
])
print("Method 4 student dataframe done")

data = [("vijay",67),("ajay",33),("jay",79),("vinay",67)]

schema = StructType([ 
	StructField("name",StringType(),True),
   	StructField("marks",IntegerType(),True)
])

#studentDf = spark.createDataFrame(data=data,schema=schema)
print("Method 4 create dataframe from df")
studentDf = spark.createDataFrame(df)
studentDf.show(5)

print("spark writing to parquet")

studentDf.write.mode("overwrite").parquet("people.parquet")

print("completed write to parquet")

print("Method 4 Ending - PySpark")


