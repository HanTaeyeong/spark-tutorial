from ntpath import join
import lib.sparkLib as sparkLib

from pyspark.sql.functions import col, broadcast
from pyspark.sql.types import IntegerType, DecimalType, TimestampType, FloatType

spark = sparkLib.createSpark()

personData = [(0, "Bill Chambers", 0, [100]),
              (1, "Matei Zaharia", 1, [500, 250, 100]),
              (2, "Michael Armbrust", 1, [250, 100]),
              (5,'TYTY',5,[250,200])
              ]
personHeader = ["id", "name", "graduate_program", "spark_status"]
personDF = spark.createDataFrame(personData).toDF(*personHeader)

graduateProgramData = [(0, "Masters", "School of Information", "UC Buckeley"),
                       (2, "Matsers", "EECS", "UC Berkeley"),
                       (1, "Ph.D.", "EECS", "UC Berkeley"),
                       (3, "Bechelors.", "Materials Science", "CNU")]
graduateProgramHeader = ["id", "degree", "department", "school"]
graduateProgramDF = spark.createDataFrame(
    graduateProgramData).toDF(*graduateProgramHeader)

sparkStatusData = [(500, "Vice President"),
                   (250, "PMC Member"),
                   (100, "Contributor")]
sparkStatusHeader = ["id", "status"]
sparkStatusDF = spark.createDataFrame(sparkStatusData).toDF(*sparkStatusHeader)


personDF.createOrReplaceGlobalTempView('person')
graduateProgramDF.createOrReplaceGlobalTempView('graduateProgram')
sparkStatusDF.createOrReplaceGlobalTempView('sparkStatus')

graduateProgramDF=graduateProgramDF.withColumnRenamed('id','graduate_id')

joinExpression = personDF['graduate_program'] == graduateProgramDF['graduate_id']
wrongJoinExpression =  personDF['name'] == graduateProgramDF['school']
joinTypes=['inner','outer', 'left_outer', 'left_semi','left_anti','cross'] 

#personDF.join(graduateProgramDF,joinExpression,joinTypes[5]).show()
#personDF.crossJoin(graduateProgramDF).show();

graduateProgramDF.show();

personDF.join(graduateProgramDF,joinExpression,joinTypes[1]).show()

spark.stop()
