import lib.sparkLib as sparkLib 

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType,DecimalType,TimestampType

spark = sparkLib.createSpark()

path = "../data/retail-data/by-day/*.csv"
dataFrame = spark.read.format('csv').option("header","true").load(path)
dataFrame.show()

schemaInfo = [{"InvoiceNo":IntegerType()},
            {"Quantity":IntegerType()},
            {"InvoiceDate":TimestampType()},
            {"UnitPrice":DecimalType()},
            ]

dataFrame = sparkLib.getTypedDataFrame(dataFrame,schemaInfo)

dataFrame.printSchema()

#dataFrame.createGlobalTempView("retail-data")

from pyspark.sql.functions import window,col

from pyspark.sql.functions import desc
dataFrame.selectExpr("CustomerId", "(UnitPrice * Quantity) as total_price","InvoiceDate")\
        .where("total_price > 0")\
        .groupBy(col("CustomerId"), window(col("InvoiceDate"), "1 day"))\
        .sum("total_price")\
        .sort(desc('sum(total_price)'))\
        .show(5)

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


spark.stop()
