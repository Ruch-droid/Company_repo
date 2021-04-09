from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

#import csv file
df = spark.read.csv("/home/kkr/222/archive/yellow_tripdata_2019-01.csv",header = 'true')
df2 = spark.read.csv("/home/kkr/222/archive/yellow_tripdata_2019-02.csv",header = 'true')
df3 = spark.read.csv("/home/kkr/222/archive/yellow_tripdata_2019-03.csv",header = 'true')
df4 = spark.read.csv("/home/kkr/222/archive/yellow_tripdata_2019-04.csv",header = 'true')

#merging csv files into one
df5 = df.union(df2)
df6 = df5.union(df3)
df7 = df6.union(df4)

dataset=df7.select(['tpep_pickup_datetime','tpep_dropoff_datetime','passenger_count','trip_distance','payment_type','improvement_surcharge','tip_amount','total_amount'])

amount = dataset.select(['tpep_pickup_datetime','tpep_dropoff_datetime','improvement_surcharge','trip_distance','total_amount'])

#spiltting columns
from pyspark.sql.functions import split


df1 = amount.withColumn('pickupdate', split(amount['tpep_pickup_datetime'], ' ').getItem(0))
df2 = df1.withColumn('pickuptime', split(df1['tpep_pickup_datetime'], ' ').getItem(1))
df3 = df2.withColumn('dropoffdate', split(df2['tpep_dropoff_datetime'], ' ').getItem(0))
df4 = df3.withColumn('dropofftime', split(df3['tpep_dropoff_datetime'], ' ').getItem(1))

df = df4.select(['pickupdate','pickuptime','dropoffdate','dropofftime','trip_distance','improvement_surcharge','total_amount'])

df.show()

#a = amount.filter(amount["trip_distance"]>4).show()
#amount.select(amount["tpep_dropoff_datetime"],amount["trip_distance"]+ 5).show()

#data between 2 values
#a = df[(df['trip_distance'] >= 3) & (df['trip_distance'] <= 5)]
#split column 
df = df4.withColumn('day', split(df3['pickupdate'], '-').getItem(2))
df8 = df4.withColumn('day', split(df3['pickupdate'], '-').getItem(2))
df8.show()
x = df8.groupby('day').agg({'total_amount' : 'avg'})
x.show()
#dataframe to csv
#x.write.format('csv').option('header',False).mode('overwrite').option('sep',',').save('/home/kkr/inn.csv')

#sorting csv file
import pandas as pd
a =pd.read_csv("/home/kkr/abc.csv")

b= a.sort_values(by = ['day'] , ascending = True)
b
#plotting graph using plotly
import plotly.express as px
fig = px.line(a , x = 'day', y = 'amount')
fig.show()


#ploting graph
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
ip = pd.read_csv('/home/kkr/abc.csv')
fig = plt.figure()
plt.yscale('log')
plt.title(' Earings according to days ')

plt.xlabel('Days')

plt.ylabel('tatal_amount')

plt.scatter(ip['day'], ip['amount'])

fig.tight_layout()
fig.show()


#splitting time in hr and min & date in years month and days
df1 = df.withColumn('month', split(df['pickupdate'], '-').getItem(1))
>>> df2 = df1.withColumn('month', split(df1['pickupdate'], '-').getItem(2))
>>> df2 = df1.withColumn('days', split(df1['pickupdate'], '-').getItem(2))
>>> df3 = df2.withColumn('days', split(df2['pickuptime'], ':').getItem(0))
>>> df3 = df2.withColumn('pickup_hours', split(df2['pickuptime'], ':').getItem(0))
>>> x = df3.withColumn('pickup_min', split(df3['pickuptime'], ':').getItem(1))
>>> df = x.select(['years','month','days','Pickup_hours','pickup_min','trip_distance','total_amount'])
>>> df.show()

#giving header in exel column
import pandas as pd                                                         
file = pd.read_csv("/home/kkr/new.csv/nee.csv")
headerList = ['years', 'month', 'days','pickup_hours','pickup_min','trip_distance','total_amount']
file.to_csv("/home/kkr/new.csv/nee.csv", header=headerList, index=False)

