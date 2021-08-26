##################################################
## {Generate parquet file containg moving average for brent and wti oil from 2000 to date}
##################################################
## Author: {Juan Manuel Cruz Reyes}
## Copyright: Copyright {2021}, {challenge_abraxas}
## Email: {juanm_cr@outlook.com}
## Status: {review_status}
## (C) 2021 Juan Cruz, CDMX ,Mexico
##################################################

import os 
from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import lit
from datetime import datetime


def read_files(file: str):
    return spark.createDataFrame(pd.read_excel(os.getcwd()+'/Files/'+str(file)))

def filter_dates(df):
    return df.filter(df.Date > '2000-01-01')

def moving_average(df):
    days = lambda i: i * 86400
    w = (Window.orderBy(F.col("Date").cast('long')).rangeBetween(-days(7), 0))
    return df.withColumn('moving_average', F.avg("Price").over(w))

def create_rdd(df):
    return df.select("moving_average").rdd.map(lambda l : round(l[0],4))

def create_date_rdd(df):
    return df.select("Date").rdd.map(lambda l : l[0])

def export_file(df):
    return df.write.parquet(os.getcwd()+'/moving_average_oil_type.parquet')


if __name__ == "__main__":
    #Start Spark Session
    spark = SparkSession.builder.appName("Challenge").master("local").getOrCreate()
    #Load Files
    wti_df = read_files('wti-daily_csv.xlsx')
    brent_df = read_files('brent-daily_csv.xlsx')
    #Filter Dates 
    wti_df = filter_dates(wti_df)
    brent_df = filter_dates(brent_df)
    #Calculate moving average
    wti_df = moving_average(wti_df)
    brent_df = moving_average(brent_df)

    #Create dataframe
    #Create moving_avg RDDs
    wti_rdd = create_rdd(wti_df)
    brent_rdd = create_rdd(brent_df)
    #Create dates RDDs
    date_wti = create_date_rdd(wti_df)
    date_brent = create_date_rdd(brent_df)
    #Merge RDDs
    wti = spark.createDataFrame(date_wti.zip(wti_rdd))\
    .toDF("Date","moving_average")\
    .withColumn("oil_type", lit('WTI'))

    brent = spark.createDataFrame(date_brent.zip(brent_rdd))\
    .toDF("Date","moving_average")\
    .withColumn("oil_type", lit('Brent'))

    #Union Dataframes
    oil_df = wti.union(brent).sort("Date")
    #Export parquet file
    export_file(oil_df)
    #Stop Spark Session
    spark.stop()







