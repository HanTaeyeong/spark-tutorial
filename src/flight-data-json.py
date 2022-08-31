import lib.sparkLib as sparkLib 

from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

spark = sparkLib.createSpark()

path = "../data/flight-data/json/2015-summary.json"
dataFrame = spark.read.format('json').load(path)

schemaInfo=[{"count":IntegerType()}]

dataFrame = sparkLib.getTypedDataFrame(dataFrame,schemaInfo)

#dataFrame.createGlobalTempView("retail-data")

from pyspark.sql.functions import window,col

from pyspark.sql.functions import desc, expr
dataFrame.orderBy(col("count").desc()).show(5)

dataFrame.printSchema()

spark.stop()
