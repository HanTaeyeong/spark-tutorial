import sys
from operator import add

import lib.sparkLib as sparkLib 

from pyspark.sql import SparkSession

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType,BooleanType,DateType

spark = sparkLib.createSpark('tutorial')

flightDataPath = "../data/flight-data/csv/2015-summary.csv"
flightData2015 = spark.read.option("header","true").csv(flightDataPath)
flightData2015.printSchema()

flightData2015 = sparkLib.getTypedDataFrame(flightData2015,[{"count":IntegerType()}])  

flightData2015.createOrReplaceTempView("flight_data_2015")

#flightData2015.show()

# query="""
#     select DEST_COUNTRY_NAME, sum(count) as destination_total
#     from flight_data_2015
#     group by DEST_COUNTRY_NAME
#     order by sum(count) DESC
#     limit 10
# """

# sqlway=spark.sql(query)
# sqlway.show()

# from pyspark.sql.functions import desc
# flightData2015.groupBy("DEST_COUNTRY_NAME")\
#     .sum("count")\
#     .withColumnRenamed("sum(count)","destination_total")\
#     .sort(desc("destination_total"))\
#     .limit(10)\
#     .show()

# partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
# n = 100000 * partitions

# def f(_: int) -> float:
#     x = random() * 2 - 1
#     y = random() * 2 - 1
#     return 1 if x ** 2 + y ** 2 <= 1 else 0

# count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
# print("Pi is roughly %f" % (4.0 * count / n))

spark.stop()
