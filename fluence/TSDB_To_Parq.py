# TSDB_To_Parq_#1_Panda.py
# 
# 5/3/23
# todo:  1 - document more - much more before starting method #2
# fixme: 2 - when the pandas datetime stamps are written to parquet the utc-5:00 is converted to utc+0:00 
# done:  3 - get #1 pandas python to parq in here.
# done:  4 - get overwrite working for #4-spark in pythontospark.py
# done:  5 - separate #4 spark schema def from data load
# done:  6 - complete CreateOpenParquetFile() This may not have to be done as most of the writers open,write and close 
#               automatically
# done:  7 - groupsensorrecordsbycount should write the records to a dataframe
# done:  8 - groupsensorrecordsbytime should write the records to a dataframe or table
# done:  9 - complete WriteRecordToParquetFile(). It should read a dataframe.
# done: 10 - in GroupSensorRecordsByCount(), load the dataframe we just created from numpy datetime64, with 
#               x records from the sql read
# 5/9/23
# done: 11 - loop and clear the DF and reload it with another set of records from the fetch
# done: 12 - indexing on the tuple to load the dataframe is off, they are 0 based indexes so I think this is what is causing
#               the record counts in the dataframes to be off.
# todo: 13 - build the full schema back into the process not the two field abrieviated schema of "bucket", and "bat1 temp" 
# todo: 14 - get the real timestamp in and not just the bucket
# todo: 15 - fix the fact that we loose the UTC offset when we write to parquet. the time is still correct but
#               its converted to UTC+0:00
# todo: 16 - add formatting for the case when total runtime is greater than a second
# todo: 17 - add total files created counter at the end and use the write in place routine. try importing it
# 5/10/23
# canceled: 18 - in the .py file PandasParqoader.py get the PandasRecCountLoadWriteParquet function to import
#            currently it hangs up on the two global veriables of startrecord and something else.  eliminate
#            the need for these globals by passing them around. test first just by commenting them out and see
#            if we can at least run with an error then. also return the new values as a tuple out of the routine
#            so they can be further used by main. MAYBE??? Think about this!!!!
# done:      Sync all three sections. Add timing to each and make sure each of them has matched output.
#               Group sensor records by record count?: 1")
#               Group sensor records by time buckets - SQL 'where clause'?: 2")
#               Group sensor records by time buckets - SQL 'time_bucket()' method?: 3")
##############################################################################################################################

import pandas  as pd               # dataframe support...
import numpy   as np               # better dataframes, more types ...

import pyarrow as pa               # in-memory analytics, data types for parquet schemas 
import pyarrow.parquet as pq       # reading and writing apache parquet

import openpyxl                    # more python Excel support
import psycopg2 as sql             # Postgres Support and /copy command. copy command utility is faster than
                                   # JDBC/ ODBC and native database writes, sql cursors

import subprocess                  # Submit Spark processes

import psycopg2.extras             # some even faster psyco routines

import time                      
from   datetime import datetime    # datetime.now() and UTC offset support, time delta math support
from   datetime import timedelta   # time delta math support

import fastparquet as fp
from   fastparquet import ParquetFile

import csv
import decimal

from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkConf

import warnings                     # suppress warnings i was getting from something in pandas
warnings.filterwarnings("ignore")

class SensorRecordProcessor:

    ### constructor
    def __init__(self, db_host, db_user, db_password, db_name, db_port):
        self.connection = sql.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            port=db_port
        )

    
        self.tsTuple = []
        self.batchCount = 1
        self.startRecord = 0
        self.dtypes = [
            ('timestamp', float),
            ('bat 1 temp', float),
            ('bat 1 charge', float),
            ('bat 1 voltage', float),
            ('bat 2 temp', float),
            ('bat 2 charge', float),
            ('bat 2 voltage', float),
            ('bat 3 temp', float),
            ('bat 3 charge', float),
            ('bat 3 voltage', float),
            ('bat 4 temp', float),
            ('bat 4 charge', float),
            ('bat 4 voltage', float),
            ('bat 5 temp', float),
            ('bat 5 charge', float),
            ('bat 5 voltage', float),
            ('bat 6 temp', float),
            ('bat 6 charge', float),
            ('bat 6 voltage', float)
        ]
    ### destructor
    def __del__(self):
        self.connection.close()
        
    ##### moderator method that directs calls to other methods based on arguments passed in
    def group_sensor_records(self, engine, groupType, str_record_count, timeFrame):
        if(engine == "panda" and groupType=="count"):
            self.group_sensor_records_by_count_pandas(str_record_count,self.tsTuple)
        elif(engine == "fastp" and groupType=="count"):
            self.group_sensor_records_by_count_fastparquet(str_record_count,self.tsTuple)
        elif(engine == "spark" and groupType=="count"):
            self.group_sensor_records_by_count_spark(str_record_count,self.tsTuple)
        elif(engine == "panda" and groupType=="bucket"):
            self.group_sensor_records_by_bucket_pandas(timeFrame)
        elif(engine == "fastp" and groupType=="bucket"):
            self.group_sensor_records_by_bucket_fastparquet(timeFrame)
        else:
            print("############ method does not exist #############")
    
    ##### query the sensor table and order by time_stamp
    def query_db(self):
        with self.connection.cursor() as curs:
            curs.execute("""select * from test_sensor_data order by time_stamp asc;""")
            curs.scroll(0,mode='absolute')
            self.tsTuple = curs.fetchall()
            return(self.tsTuple)

    ############################################################ 
################################################################  

    ####     #     #     # ####     #
    #  #    # #    # #   # #   #   # #          #### ##  # #####   
    ####   # # #   #  #  # #   #  # # #         #    # # #   #     
    #     #     #  #   # # #   # #     #        #    #  ##   #                          
    #    #       # #     # #### #       #       #### #  ##   #
        
################################################################  
    ############################################################ 
    
    def group_sensor_records_by_count_pandas(self, str_record_count, _lclTuple):
        record_count = int(str_record_count)

        # _lclTuple is passed in but it is just self.tuple
        # _tuple is created by running self.querydb below

        #_tuple = self.query_db()

        with self.connection.cursor() as curs:
            curs.execute("""select * from test_sensor_data order by time_stamp asc;""")
            curs.scroll(0,mode='absolute')
            _tuple = curs.fetchall()
            
        len_tuple = len(_tuple)
        whole_batches = (len_tuple / record_count)
        last_batch = (len_tuple % record_count)

        print("")
        print("")
        print("                                                                       : PANDA COUNT Class")
        int_whole_batches = int(whole_batches)
        print(f"                                                     Total records read: {len_tuple}")
        print(f"                   Complete parque files (files with full record count): {int_whole_batches}")
        print(f"                                                  Record count for each: {record_count}")
        if last_batch != 0:
            print(f"                                 One partial parquet file. Record count: {last_batch}")
        print("")

        start_time = datetime.now()
        while self.batchCount <= int_whole_batches:
            print(f"                                                          Files created: {self.batchCount}", end="\r")
            self.start_record = self.batchCount * record_count
            self.pandas_group_by_count_create_parquet_files(record_count,_tuple)
            self.batchCount += 1

        if last_batch > 0:
            print(f"                                                          Files created: {self.batchCount}", end="\r")
            self.pandas_group_by_count_create_parquet_files(last_batch,_tuple) #self.tuple _lclTuple _tuple same data three ways
        end_time = datetime.now()

        tot_time = end_time - start_time
        tot_time_d = 0
        if tot_time < timedelta(seconds=1):
            tot_time = tot_time * 1000
            tot_time_d = tot_time.total_seconds()
        else:
            tot_time_d = tot_time
        print("")
        print(f"                                                             Total time: {tot_time_d} milliseconds")

        print("")
        print("")

    def pandas_group_by_count_create_parquet_files(self, record_count,_tuple):
        file_tuple = _tuple[self.startRecord:record_count * self.batchCount]
        df_numpy = pd.DataFrame(file_tuple, columns=self.dtypes)
        #print(df_numpy)
        df_numpy.to_parquet(f"c:\\data\\caurus\\fluence\\parquetFilesByRecCount\\meth1_Panda-Parq_{self.batchCount}.parquet", engine="pyarrow")

    
    
    ############################################################ 
################################################################  

    ####     #     ##### ####### #####    
    #       # #    #        #    #    #             #### ##  # #####   
    ####   # # #   #####    #    #####              #    # # #   #     
    #     #     #      #    #    #                  #    #  ##   #                          
    #    #       # #####    #    #                  #### #  ##   #
        
################################################################  
    ############################################################ 

    def group_sensor_records_by_count_fastparquet(self, str_record_count, _lclTuple):
        
        _lclTuple = self.query_db()

        record_count = int(str_record_count)
        len_tuple = len(_lclTuple)
        whole_batches = (len_tuple / record_count)
        last_batch = (len_tuple % record_count)

        
        print("")
        print("                                                                       : FASTP COUNT Class")
        int_wholeBatches = int(whole_batches)
        print( "                                                     Total records read: "    + str(len_tuple))
        print( "                   Complete parque files (files with full record count): "    + str(int_wholeBatches))
        print( "                                                  Record count for each: "    + str(record_count) )
        if (last_batch != 0):
            print("                                 One partial parquet file. Record count: " + str(last_batch))
        print("")
        
        dtypes =  ([    ('timestamp',       np.dtype("datetime64[ns]")),
                        ('bat 1 temp',      float),
                        ('bat 1 charge',    float),
                        ('bat 1 voltage',   float),
                        ('bat 2 temp',      float),
                        ('bat 2 charge',    float),
                        ('bat 2 voltage',   float),
                        ('bat 3 temp',      float),
                        ('bat 3 charge',    float),
                        ('bat 3 voltage',   float),
                        ('bat 4 temp',      float),
                        ('bat 4 charge',    float),
                        ('bat 4 voltage',   float),
                        ('bat 5 temp',      float),
                        ('bat 5 charge',    float),
                        ('bat 5 voltage',   float),
                        ('bat 6 temp',      float),
                        ('bat 6 charge',    float),
                        ('bat 6 voltage',   float)])
    
        names_only =   ('timestamp', 'bat 1 temp', 'bat 1 charge', 'bat 1 voltage', 'bat 2 temp', 'bat 2 charge',
                        'bat 2 voltage', 'bat 3 temp', 'bat 3 charge', 'bat 3 voltage', 'bat 4 temp', 'bat 4 charge', 
                        'bat 4 voltage', 'bat 5 temp', 'bat 5 charge', 'bat 5 voltage', 'bat 6 temp', 'bat 6 charge', 
                        'bat 6 voltage')

        self.start_record = 0
        self.batchCount = 1
        start_time=datetime.now() # start the timer so we can calculate how long it takes to create files
        while  (self.batchCount <= int_wholeBatches):
            print("                                                          Files created: " + str(self.batchCount), end="\r")   
            self.create_parquet_files_fastparquet(record_count, names_only, _lclTuple)
            
        if(last_batch > 0):
            print("                                                          Files created: " + str(self.batchCount), end="\r")
            self.create_parquet_files_fastparquet(record_count, names_only, _lclTuple)

        end_time = datetime.now()
        # subtract start_time that we captured above from end_time (now)
        # if it is less than a second, do some formatting on it so we  
        # can show it cleanly in milli-seconds. when it gets up over
        # a second (and it will), I wil have to come back here and
        # catch that case and do some formatting to show seconds
        # and milliseconds. that a todo.
        tot_time = end_time - start_time
        tot_time_d = 0
        if tot_time < timedelta(seconds=1):
            tot_time = tot_time * 1000
            tot_time_d = tot_time.total_seconds()
        else:
            #print("more than a second")
            tot_time_d = tot_time
        print("")
        print(f"                                                             Total time: {tot_time_d} milliseconds")

        print("")
        print("")   
        return()


    def create_parquet_files_fastparquet(self, record_count, names_only,_lclTuple):
        # important note[1:5] start with the tuple at the first index, this makes sense
        #                     but the second index # is read up to but not including
        #                     [1:5] will return 4 tuples
        #                     so the second # will always need to be 1 more than the 
        #                     last index you want to return

        #fileTuple = self.tsTuple[self.startRecord:record_count * self.batchCount]
        fileTuple = _lclTuple[self.startRecord:record_count * self.batchCount]
        
        dfNumpy = pd.DataFrame(fileTuple, columns=names_only)
        #print(dfNumpy)

        fp.write("c:\\data\\caurus\\fluence\\parquetFilesByRecCount\\meth2_fastP-Parq_" + str(self.batchCount) + ".parquet", dfNumpy, compression="UNCOMPRESSED")
        
        self.start_record = record_count * self.batchCount
        self.batchCount = self.batchCount + 1 
        return()

   
    ############################################################ 
################################################################  

                   #####  #####    
                   #      #    #             #### ##  # #####   
                   #####  #####              #    # # #   #     
                       #  #                  #    #  ##   #                          
                   #####  #                  #### #  ##   #
        
################################################################  
    ############################################################ 

    def group_sensor_records_by_count_spark(self, str_record_count, _lclTuple):
        
        _lclTuple = self.query_db()

        record_count = int(str_record_count)
        len_tuple = len(_lclTuple)
        whole_batches = (len_tuple / record_count)
        last_batch = (len_tuple % record_count)

        
        print("")
        print("                                                                       : SPARK COUNT Class")
        int_wholeBatches = int(whole_batches)
        print( "                                                     Total records read: "    + str(len_tuple))
        print( "                   Complete parque files (files with full record count): "    + str(int_wholeBatches))
        print( "                                                  Record count for each: "    + str(record_count) )
        if (last_batch != 0):
            print("                                 One partial parquet file. Record count: " + str(last_batch))
        print("")
        
        dtypes =  ([    ('timestamp',       np.dtype("datetime64[ns]")),
                        ('bat 1 temp',      float),
                        ('bat 1 charge',    float),
                        ('bat 1 voltage',   float),
                        ('bat 2 temp',      float),
                        ('bat 2 charge',    float),
                        ('bat 2 voltage',   float),
                        ('bat 3 temp',      float),
                        ('bat 3 charge',    float),
                        ('bat 3 voltage',   float),
                        ('bat 4 temp',      float),
                        ('bat 4 charge',    float),
                        ('bat 4 voltage',   float),
                        ('bat 5 temp',      float),
                        ('bat 5 charge',    float),
                        ('bat 5 voltage',   float),
                        ('bat 6 temp',      float),
                        ('bat 6 charge',    float),
                        ('bat 6 voltage',   float)])
    
        names_only =   ('timestamp', 'bat 1 temp', 'bat 1 charge', 'bat 1 voltage', 'bat 2 temp', 'bat 2 charge',
                        'bat 2 voltage', 'bat 3 temp', 'bat 3 charge', 'bat 3 voltage', 'bat 4 temp', 'bat 4 charge', 
                        'bat 4 voltage', 'bat 5 temp', 'bat 5 charge', 'bat 5 voltage', 'bat 6 temp', 'bat 6 charge', 
                        'bat 6 voltage')

        self.start_record = 0
        self.batchCount = 1
        start_time=datetime.now() # start the timer so we can calculate how long it takes to create files
        while  (self.batchCount <= int_wholeBatches):
            print("                                                          Files created: " + str(self.batchCount), end="\r")   
            #self.create_parquet_files_spark(record_count, names_only, _lclTuple)
            self.batchCount+=1

        if(last_batch > 0):
            print("                                                          Files created: " + str(self.batchCount), end="\r")
            #self.create_parquet_files_spark(record_count, names_only, _lclTuple)

        end_time = datetime.now()
        # subtract start_time that we captured above from end_time (now)
        # if it is less than a second, do some formatting on it so we  
        # can show it cleanly in milli-seconds. when it gets up over
        # a second (and it will), I wil have to come back here and
        # catch that case and do some formatting to show seconds
        # and milliseconds. that a todo.
        tot_time = end_time - start_time
        tot_time_d = 0
        if tot_time < timedelta(seconds=1):
            tot_time = tot_time * 1000
            tot_time_d = tot_time.total_seconds()
        else:
            #print("more than a second")
            tot_time_d = tot_time
        print("")
        print(f"                                                             Total time: {tot_time_d} milliseconds")

        print("")
        print("")   
        return()


    ############################################################ 
################################################################  

    ####     #     #     # ####     #
    #  #    # #    # #   # #   #   # #          #### #   # #####   
    ####   # # #   #  #  # #   #  # # #         #  # #   # #        
    #     #     #  #   # # #   # #     #        #### #   # #                              
    #    #       # #     # #### #       #       #  # #   # #   
                                                #### ##### #####        
################################################################  
    ############################################################ 

    def group_sensor_records_by_bucket_pandas(self, timeBucketSize):
        
        
        print("                                                                       : PANDA BUCKET Class")
        print("                                            Grouping by time buckets of: " + str(timeBucketSize) + " seconds")
        
        parquet_schema = pa.schema([('bucket', pa.timestamp('ns')),
                                    ('time_stamp', pa.timestamp('ns')),
                                    ('bat 1 temp', pa.decimal128(5,3)),                            
                                    ('bat 1 charge', pa.int64()),
                                    ('bat 1 voltage', pa.decimal128(5,3)),
                                    ('bat 2 temp', pa.decimal128(5,3)),
                                    ('bat 2 charge', pa.int64()),
                                    ('bat 2 voltage', pa.decimal128(5,3)),
                                    ('bat 3 temp', pa.decimal128(5,3)),
                                    ('bat 3 charge', pa.int64()),
                                    ('bat 3 voltage', pa.decimal128(5,3)),
                                    ('bat 4 temp', pa.decimal128(5,3)),
                                    ('bat 4 charge', pa.int64()),
                                    ('bat 4 voltage', pa.decimal128(5,3)),
                                    ('bat 5 temp', pa.decimal128(5,3)),
                                    ('bat 5 charge', pa.int64()),
                                    ('bat 5 voltage', pa.decimal128(5,3)),
                                    ('bat 6 temp', pa.decimal128(5,3)),
                                    ('bat 6 charge', pa.int64()),
                                    ('bat 6 voltage', pa.decimal128(5,3))
                                    ])
        
        pandas_columns =            ['bucket', 
                                    'time_stamp', 
                                    'bat 1 temp', 
                                    'bat 1 charge', 
                                    'bat 1 voltage', 
                                    'bat 2 temp', 
                                    'bat 2 charge', 
                                    'bat 2 voltage', 
                                    'bat 3 temp', 
                                    'bat 3 charge', 
                                    'bat 3 voltage', 
                                    'bat 4 temp', 
                                    'bat 4 charge', 
                                    'bat 4 voltage', 
                                    'bat 5 temp', 
                                    'bat 5 charge', 
                                    'bat 5 voltage', 
                                    'bat 6 temp', 
                                    'bat 6 charge', 
                                    'bat 6 voltage'
                                    ]

        timeBucketSize = timeBucketSize + " second"
        with self.connection.cursor() as curs:
            curs.execute("""select time_bucket(%s, time_stamp) AS bucket,
            *
            FROM test_sensor_data 
            where 
            (time_stamp < now())
            ORDER BY bucket, bat1_temp ; """,(timeBucketSize,))
            #                                 timeBucketSize is a tuple "3 second" so put it 
            #                                 in () with a comma following to indicate tuple

            # {} initialize an empty dictionary. [] is a list. {} is a dict.
            # now have a dictionary called groups. nothing in it.
            groups = {}

            dbRows = curs.fetchall()

            # loop through every row in the cursor. each row currently looks like:
            # (datetime.datetime(2023, 4, 26, 21, 43, 34, 
            # tzinfo=datetime.timezone(datetime.timedelta(days=-1,seconds=68400))),Decimal('98.800'))
            # notice there are no column names in the row, this may be a problem
            
            # this is the "group by key" pattern
            prev_interval_start_str = None
            batchCount = 0
            for row in dbRows:

                interval_start = row[0]  # the first column is the interval start time

                # Convert the interval start time to a string for use in the file name
                # it is already in date time format as you can see from the string above so
                # all we have to do is convert it to a string with the strftime() method
                # so we will get an easy to read dateTime string of "yyyy-mm-dd_hh-mm-ss"
                interval_start_str = interval_start.strftime('%Y-%m-%d_%H-%M-%S')

                # start the timer so we can calculate how long it takes to create these 
                # parquet files
                start_time=datetime.now()

                # The if statement below tests to see if the interval_start_str (which is the 
                # bucket column) is already a key in the groups dictionary. if it is already a 
                # key in the dictionary that means that we are already processing that bucket so
                # just move on.  However if this "bucket" is not a key already, that means its a
                # new bucket so add it to the dictionary as a new key and its value is an empty 
                # list.  We will add all rows in this bucket to the list with this new key.
                # dictionary.  So we have a dictionary of [lists] named "groups".
                
                if interval_start_str not in groups:
                    # then add this new interval_start_str from a row in the cursor.
                    # it is the time bucket column created by the sql query
                    # this key did not exist before
                    groups[interval_start_str] = []

                    # now that we added a new key (bucket value) we have completed processing all
                    # the records in the previous "bucket". We know this because the records are
                    # sorted by "bucket" column.  So lets print the bucket list we just completed.
                    self.pandas_group_by_timebuckets_create_parquet_files(prev_interval_start_str, groups, parquet_schema, pandas_columns, batchCount)
                    prev_interval_start_str = interval_start_str
                    batchCount += 1

                # in the groups dictionary at the key location of interval_start_str append row 
                # which is a list of all the records in that time bucket.
                groups[interval_start_str].append(row)
                        
        self.pandas_group_by_timebuckets_create_parquet_files(prev_interval_start_str, groups, parquet_schema, pandas_columns, batchCount)
        
        end_time = datetime.now()
        # subtract start_time that we captured above from end_time (now)
        # if it is less than a second, do some formatting on it so we  
        # can show it cleanly in milli-seconds. when it gets up over
        # a second (and it will), I wil have to come back here and
        # catch that case and do some formatting to show seconds
        # and milliseconds. that a todo.
        tot_time = end_time - start_time
        if (tot_time < timedelta(seconds = 1)):
            tot_time = tot_time * 1000
            tot_time_d = tot_time.total_seconds()
        print("")
        print("                                                             total time: " + str(tot_time_d) + " milliseconds")    
        return()

    def pandas_group_by_timebuckets_create_parquet_files(self, prev_interval_start_str,groups, parquet_schema, pandas_columns, batchCount):
        if(prev_interval_start_str == None):
            return()
        
        data_ = groups[prev_interval_start_str]
        
        df = pd.DataFrame(data=data_,columns=pandas_columns)
        
        filename = "c:\\data\\caurus\\fluence\\parquetFilesByTimeBuckets\\meth1_Panda-Parq_" + str(batchCount) + "_" \
        + f"{prev_interval_start_str}.parquet"

        df.to_parquet(filename,schema = parquet_schema, engine="pyarrow")
        
        print("                                                          files created: " + str(batchCount), end="\r")
        
        return()    

    ############################################################ 
  ################################################################  

    ####     #     ##### ####### #####    
    #       # #    #        #    #    #             #### #  #  ###   
    ####   # # #   #####    #    #####              #  # #  # #        
    #     #     #      #    #    #                  #### #  # #                             
    #    #       # #####    #    #                  #  # #  # #   
                                                    #### ####  ###       
  ################################################################  
    ############################################################ 


    def group_sensor_records_by_bucket_fastparquet(self, timeBucketSize):
        timeBucketSize = timeBucketSize + " second"

        print("")
        print("")
        print("                                                                      : FASTP Bucket Class")
        print("                                           Grouping by time buckets of: " + str(timeBucketSize) + " seconds")
        
        
        parquet_schema = pa.schema([('bucket', pa.timestamp('ns')),
                                    ('time_stamp', pa.timestamp('ns')),
                                    ('bat 1 temp', pa.decimal128(5,3)),                            
                                    ('bat 1 charge', pa.int64()),
                                    ('bat 1 voltage', pa.decimal128(5,3)),
                                    ('bat 2 temp', pa.decimal128(5,3)),
                                    ('bat 2 charge', pa.int64()),
                                    ('bat 2 voltage', pa.decimal128(5,3)),
                                    ('bat 3 temp', pa.decimal128(5,3)),
                                    ('bat 3 charge', pa.int64()),
                                    ('bat 3 voltage', pa.decimal128(5,3)),
                                    ('bat 4 temp', pa.decimal128(5,3)),
                                    ('bat 4 charge', pa.int64()),
                                    ('bat 4 voltage', pa.decimal128(5,3)),
                                    ('bat 5 temp', pa.decimal128(5,3)),
                                    ('bat 5 charge', pa.int64()),
                                    ('bat 5 voltage', pa.decimal128(5,3)),
                                    ('bat 6 temp', pa.decimal128(5,3)),
                                    ('bat 6 charge', pa.int64()),
                                    ('bat 6 voltage', pa.decimal128(5,3))
                                    ])
        
        pandas_columns =            ['bucket', 
                                    'time_stamp', 
                                    'bat 1 temp', 
                                    'bat 1 charge', 
                                    'bat 1 voltage', 
                                    'bat 2 temp', 
                                    'bat 2 charge', 
                                    'bat 2 voltage', 
                                    'bat 3 temp', 
                                    'bat 3 charge', 
                                    'bat 3 voltage', 
                                    'bat 4 temp', 
                                    'bat 4 charge', 
                                    'bat 4 voltage', 
                                    'bat 5 temp', 
                                    'bat 5 charge', 
                                    'bat 5 voltage', 
                                    'bat 6 temp', 
                                    'bat 6 charge', 
                                    'bat 6 voltage'
                                    ]


        with self.connection.cursor() as curs:
            curs.execute("""select time_bucket(%s, time_stamp) AS bucket,
            *
            FROM test_sensor_data 
            where 
            (time_stamp < now())
            ORDER BY bucket, bat1_temp ; """,(timeBucketSize,))
            #                                 timeBucketSize is a tuple "3 second" so put it 
            #                                 in () with a comma following to indicate tuple

            # {} initialize an empty dictionary. [] is a list. {} is a dict.
            # now have a dictionary called groups. nothing in it.
            groups = {}

            dbRows = curs.fetchall()

            # loop through every row in the cursor. each row currently looks like:
            # (datetime.datetime(2023, 4, 26, 21, 43, 34, fixme
            # tzinfo=datetime.timezone(datetime.timedelta(days=-1,seconds=68400))),Decimal('98.800'))
            # notice there are no column names in the row, this may be a problem
            
            # this is the "group by key" pattern
            prev_interval_start_str = None
            batchCount = 0
            for row in dbRows:

                interval_start = row[0]  # the first column is the interval start time

                # Convert the interval start time to a string for use in the file name
                # it is already in date time format as you can see from the string above so
                # all we have to do is convert it to a string with the strftime() method
                # so we will get an easy to read dateTime string of "yyyy-mm-dd_hh-mm-ss"
                interval_start_str = interval_start.strftime('%Y-%m-%d_%H-%M-%S')

                # start the timer so we can calculate how long it takes to create these 
                # parquet files
                start_time=datetime.now()

                # The if statement below tests to see if the interval_start_str (which is the 
                # bucket column) is already a key in the groups dictionary. if it is already a 
                # key in the dictionary that means that we are already processing that bucket so
                # just move on.  However if this "bucket" is not a key already, that means its a
                # new bucket so add it to the dictionary as a new key and its value is an empty 
                # list.  We will add all rows in this bucket to the list with this new key.
                # dictionary.  So we have a dictionary of [lists] named "groups".
                
                if interval_start_str not in groups:
                    # then add this new interval_start_str from a row in the cursor.
                    # it is the time bucket column created by the sql query
                    # this key did not exist before
                    groups[interval_start_str] = []

                    # now that we added a new key (bucket value) we have completed processing all
                    # the records in the previous "bucket". We know this because the records are
                    # sorted by "bucket" column.  So lets print the bucket list we just completed.
                    self.FastParquetTimeBucketsWriteParquet(prev_interval_start_str, groups, parquet_schema, pandas_columns, batchCount)
                    prev_interval_start_str = interval_start_str
                    batchCount += 1

                # in the groups dictionary at the key location of interval_start_str append row 
                # which is a list of all the records in that time bucket.
                groups[interval_start_str].append(row)
                        
        self.FastParquetTimeBucketsWriteParquet(prev_interval_start_str, groups, parquet_schema, pandas_columns, batchCount)
        
        end_time = datetime.now()
        # subtract start_time that we captured above from end_time (now)
        # if it is less than a second, do some formatting on it so we  
        # can show it cleanly in milli-seconds. when it gets up over
        # a second (and it will), I wil have to come back here and
        # catch that case and do some formatting to show seconds
        # and milliseconds. that a todo.
        tot_time = end_time - start_time
        if (tot_time < timedelta(seconds = 1)):
            tot_time = tot_time * 1000
            tot_time_d = tot_time.total_seconds()
        
        print("")
        print("                                                            total time: " + str(tot_time_d) + " milliseconds")    
        return()

    def FastParquetTimeBucketsWriteParquet(self, prev_interval_start_str,groups, parquet_schema, pandas_columns, batchCount):
        if(prev_interval_start_str == None):
            return()
        
        data_ = groups[prev_interval_start_str]
        #print(data_)

        df = pd.DataFrame(data=data_,columns=pandas_columns)
        #print(df)

        filename = "c:\\data\\caurus\\fluence\\parquetFilesByTimeBuckets\\meth2_fastP_" + str(batchCount) + "_" \
        + f"{prev_interval_start_str}.parquet"
        
        fp.write(filename, df, compression="UNCOMPRESSED")

        pf = ParquetFile(filename)

        df_ = pf.to_pandas(columns=None, categories=None, filters=[], index=None, row_filter=False, dtypes=None)
        #print(df)

        print("                                                         files created: " + str(batchCount), end="\r")
        
        return()    
    

##################################################################################################################### 
###                                                                                                               ###   
###                                                                                                               ###        
                                ####      ####     ##       ########  ####     ##
                                ## ##    ## ##    ## ##        ##     ## ##    ##
                                ##  ##  ##  ##   ##   ##       ##     ##  ##   ##
                                ##    ##    ##   #######       ##     ##   ##  ##
                                ##    ##    ##   ##   ##       ##     ##    ## ## 
                                ##    ##    ##   ##   ##    ########  ##     ####
###                                                                                                               ###     
###                                                                                                               ###

#       main - Read the database and create files based on two main criteria.
#
# criteria 1 - by batch record count (fixed # of records for each parque file created).
#
# criteria 2 - by time bucket, you choose the time boundraries for each file created. (currently in # of seconds)
#####################################################################################################################
print("") 
print("       ________                          __        __  ______                    ______                    " )
print("      / ____ _/      /\      /\         /  \      / / / ____/   /\      /\      / ____ \                   " )
print("     / /            /  \    /  \       / /\ \    / / / /__     /  \    /  \    / /    \ \                  " )
print("    / /            / /\ \  / /\ \     / /  \ \  / / / ___/    / /\ \  / /\ \   | |     | |                 " )
print("   / /_____   __  / /  \ \/ /  \ \   / /    \ \/ / / /_____  / /  \ \/ /  \ \  \ \____/ /                  " )
print("  /_______/  /_/ /_/    \__/    \_\ /_/      \__/ /_______/ /_/    \__/    \_\  \______/                   " )
print("                                                                                   #1-4 All Engines        " )

# Instantiate the class
sensor_processor = SensorRecordProcessor(db_host="127.0.0.1", db_user='postgres', db_password='admin', db_name='example',db_port="5432")

# Call the method to group sensor records by count
sensor_processor.group_sensor_records('panda','count','10','0')
sensor_processor.group_sensor_records('fastp','count','10','0')

# get a spark session
print("                                                                      : Starting Spark Session")
spark = SparkSession.builder.config("spark.some.config.option", "some-value") \
	.master("local").appName("PySpark_Parquet_test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

sensor_processor.group_sensor_records('spark','count','10','0')

print("                                           : Closing Spark Session")
spark.stop()

sensor_processor.group_sensor_records('panda','bucket','0','5')
sensor_processor.group_sensor_records('fastp','bucket','0','5')

exit()


 
