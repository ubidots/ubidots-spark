from pyspark.sql.types import *
from pyspark.sql import SQLContext
from pyspark import SparkContext
import ubidots


strc = StructType([
    StructField("url", StringType(),True),
    StructField("timestamp",LongType(),True),
    StructField("created_at",StringType(),True),
    StructField("context",MapType(NullType(),NullType(),True),True),
    StructField("value",DoubleType(),True)
])


ubi = ubidots.ApiClient("APICLIENT")
v = ubi.get_variable('ID_VARIABLE')
data = v.get_values()
df = SQLContext.getOrCreate(SparkContext.getOrCreate()).createDataFrame([i.values() for i in data], schema=strc)
df.registerTempTable("values")

display(SQLContext.getOrCreate(SparkContext.getOrCreate()).sql("SELECT value FROM values ORDER BY value LIMIT 10"))
