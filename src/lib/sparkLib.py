from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *

def createSpark(appName = "sparkApp"):
    spark = SparkSession\
        .builder\
        .appName(appName)\
        .getOrCreate()
    return spark

def getTypedDataFrame(dataFrame, schemaInfo):
    for info in schemaInfo:
        for columnName,type in info.items():
            dataFrame=dataFrame.withColumn(columnName,col(columnName).cast(type))
    
    return dataFrame

# schemaInfo2=[{"InvoiceNo":IntegerType()},
#             {"Quantity":IntegerType()},
#             {"InvoiceDate":TimestampType()},
#             {"UnitPrice":DecimalType()},
#             ]

# for object in schemaInfo2:
    
#         print(columnName,type)
