import pandas as pd
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from functools import reduce
from past.builtins import xrange
from datetime import datetime
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DateType
from pyspark.sql.functions import *

sc = SparkContext("local","gsod")
spark = SparkSession(sc)
#import gsod data into dataframe
df = spark.read.format("csv").option("delimiter",";").load("hdfs:///poc/data/gsod.csv")
#import station history data into dataframe
hi = spark.read.format("csv").option("header","true").load("hdfs:///poc/data/isd-history.csv")
#import country list data into dataframe
cn = spark.read.format("csv").option("delimiter",";").option("header","true").load("hdfs:///poc/data/country-list.csv")
#import continent list into dataframe
co = spark.read.format("csv").option("delimiter",";").option("header","true").load("hdfs:///poc/data/continent.csv")

# Add header
oldColumns = df.schema.names
newColumns = ["STN", "WBAN","YEARMODA", "TEMP" , "DEWP" , "SLP" , "STP" , "VISIB" , "WDSP" , "MXSPD" , "GUST" , "MAX" , "MIN" , "PRCP" , "SNDP" , "F", "R", "S", "H", "TH", "TOO"]
df = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)), df)
# set column types
types=["string","string","string","float","float","float","float","float","float","float","float","float","float","float","float","string","string","String","string","string","String"]
df = reduce(lambda df, idx: df.withColumn(newColumns[idx], df[newColumns[idx]].cast(types[idx])),xrange(len(newColumns)), df)
# set date type column
func = udf(lambda x: datetime.strptime(x,'%Y%m%d'), DateType())
df = df.withColumn('YEARMODA', func(col('YEARMODA')))
# change temp from F to C
tocColumns=[ "TEMP" , "DEWP" , "MAX" , "MIN" ]
df= reduce(lambda df, idx: df.withColumn(tocColumns[idx],when(df[tocColumns[idx]] <9999.9,round((df[tocColumns[idx]]-32)*5/9,1))),xrange(len(tocColumns)), df)
#join continent table to country
cn=cn.join(co,on='ID',how='left')
# join country table to station history
df3 = hi.join(cn, hi.CTRY == cn.ID,how='left').drop(cn.ID)
# join all data
df4 = df.join(df3, (df.STN == df3.USAF) & (df.WBAN == df3.WBAN),how='left').drop(df3.USAF).drop(df3.WBAN) 

# basic data by year
df7=df4.select('STN','WBAN','YEARMODA','COUNTRY').groupby(year('YEARMODA').alias('Year')).agg(countDistinct('STN','WBAN').alias('No of station'),count('STN').alias('No of lines'),countDistinct('COUNTRY').alias('No of countries')).orderBy('Year')
df7.toPandas().to_csv('year.csv',index=False)

# mean temp by decade
df5=df4.groupby(floor(year('YEARMODA')/10) %10).agg(mean('TEMP').alias("Mean Temp"),count('TEMP')).withColumnRenamed('(FLOOR((year(YEARMODA) / 10)) % 10)', 'decade')
df5.toPandas().to_csv('mean.csv',index=False)
#mean temp by decade per continent
df5c=df4.groupby(floor(year('YEARMODA')/10) %10,'CONTINENT').agg(mean('TEMP').alias("Mean Temp"),count('TEMP')).withColumnRenamed('(FLOOR((year(YEARMODA) / 10)) % 10)', 'decade').orderBy('decade')
df5c.toPandas().to_csv('mean_cont.csv',index=False)

# station by year
df8=df4.select('STN','WBAN',year('YEARMODA').alias('year'),'STATION NAME','COUNTRY','LAT','LON').distinct()
df8.toPandas().to_csv('station.csv',index=False)

# Temp with decade and continent 
dfbb=df4.where(year('YEARMODA')>1969)
dfB=dfbb.select((floor(year('YEARMODA')/10) %10).alias('decade'),'TEMP','CONTINENT')
dfB.write.csv("hdfs:///poc/data/decade_cont.csv")
#dfB.toPandas().to_csv('decade_cont.csv',index=False)

# Min and Max temp every year 
df21=df4.select(year('YEARMODA').alias('Year'),'STATION NAME','COUNTRY','MIN').groupBy('Year').agg(min('MIN').alias('MIN')).orderBy('Year')
df22=df4.select(year('YEARMODA').alias('Year'),'STATION NAME','COUNTRY','MAX').groupBy('Year').agg(max('MAX').alias('MAX')).orderBy('Year')
df11=df4.select(year('YEARMODA').alias('Year'),'YEARMODA','STATION NAME','COUNTRY','MIN')
df12=df4.select(year('YEARMODA').alias('Year'),'YEARMODA','STATION NAME','COUNTRY','MAX')
df13=df4.select(year('YEARMODA').alias('Year'),'STATION NAME','COUNTRY','TEMP')
df31=df21.join(df11,(df21.MIN==df11.MIN)&(df21.Year==df11.Year),how='left').drop(df11.Year).drop(df11.MIN).orderBy('Year')
df32=df22.join(df12,(df22.MAX==df12.MAX)&(df22.Year==df12.Year),how='left').drop(df12.Year).drop(df12.MAX).orderBy('Year')
df31.toPandas().to_csv('min.csv',index=False)
df32.toPandas().to_csv('max.csv',index=False)

# rainy days per year
dfr=df4.select(year('YEARMODA').alias('Year'),'R').crosstab('Year','R').orderBy('Year_R')
dfr.toPandas().to_csv('rain.csv',index=False)

# snowy days per year
dfs=df4.select(year('YEARMODA').alias('Year'),'S').crosstab('Year','S').orderBy('Year_S')
dfs.toPandas().to_csv('snow.csv',index=False)

