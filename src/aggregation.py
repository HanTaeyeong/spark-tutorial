import lib.sparkLib as sparkLib 

from pyspark.sql.functions import col,count,countDistinct,approx_count_distinct,sum,avg, stddev_pop,stddev_samp,expr
from pyspark.sql.types import IntegerType,DecimalType,TimestampType,FloatType

spark = sparkLib.createSpark()

path = "../data/retail-data/by-day/*.csv"
schemaInfo = [{"InvoiceNo":IntegerType()},
            {"Quantity":IntegerType()},
            {"InvoiceDate":TimestampType()},
            {"UnitPrice":DecimalType()},
            ]


dataFrame = spark.read.format('csv').option("header","true").load(path).coalesce(5)
dataFrame.cache()
dataFrame = sparkLib.getTypedDataFrame(dataFrame,schemaInfo)

#dataFrame.createOrReplaceTempView('dfTable')

#541909       
#dataFrame.show()

# print(dataFrame.select(count('CustomerID')).show())

# print(dataFrame.select(countDistinct('CustomerID')).show())


#count distinct 가 안되어서 생각해보니 실수형이 disinct 말이안댐 그래서 실험해보니 int형은 가능한듯
#decimal 타입과 float 타입이 다른 결과를 나타냄.

#print(dataFrame.select(countDistinct('Quantity')).show())
#print(dataFrame.select(approx_count_distinct('unitPrice',0.1)).show())


# df.select(first("StockCode"),last(StockCode)).show();
# query="""SELECT first(StockCode), last(StockCode) FROM retail_data"""

# dataFrame.createOrReplaceGlobalTempView("retail_data")
# sqlway=spark.sql(query)
# sqlway.show()

#print(dataFrame.select(sum("Quantity")).show());
#sumDistinct

#dataFrame.selectExpr("(UnitPrice * Quantity) as total_price").select(sum('total_price')).show();

# revenue per person
#dataFrame.selectExpr("(UnitPrice * Quantity) as total_price").select(sum("total_price")/count('total_price'),avg('total_price')).show()


#dataFrame.select(stddev_pop('CustomerID'),stddev_samp('CustomerID')).show()
#비대칭도와 첨도
#https://dining-developer.tistory.com/17

#Mapping
# exprs=["avg(UnitPrice)","avg(Quantity)","stddev_pop(Quantity)","stddev_pop(UnitPrice)"]
# exprList =  list(map(lambda x: expr(x) ,exprs))
# dataFrame.groupBy("Country").agg(*exprList).show()

#국가별 매출 현황
# dataFrame.selectExpr("(UnitPrice * Quantity) as total_price","Country")\
#     .groupBy("Country")\
#     .agg(expr("avg(total_price)"),expr("sum(total_price)"))\
#     .show()



#window function
#https://sparkbyexamples.com/spark/spark-sql-window-functions/

simpleData = (("James", "Sales", 3000), \
    ("Michael", "Sales", 4600),  \
    ("Robert", "Sales", 4100),   \
    ("Maria", "Finance", 3000),  \
    ("James", "Sales", 3000),    \
    ("Scott", "Finance", 3300),  \
    ("Jen", "Finance", 3900),    \
    ("Jeff", "Marketing", 3000), \
    ("Kumar", "Marketing", 2000),\
    ("Saif", "Sales", 4100) \
  )

columns= ["employee_name", "department", "salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)

#df.printSchema()
#df.show(truncate=False)

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
windowSpec  = Window.partitionBy("department").orderBy("salary")

df.withColumn("row_number",row_number().over(windowSpec)) \
    .show(truncate=False)
















spark.stop()
