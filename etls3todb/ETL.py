

# COMMAND ----------

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, explode
import pyspark.sql.functions as f

# COMMAND ----------

# COMMAND ----------

# Extract
spark = SparkSession.builder.appName("ETL Pipeline").getOrCreate()
df = spark.read.text("s3://amirthadharun/etlWordCOunt/WordData.txt")

# COMMAND ----------

# Transformation
df2 = df.withColumn("splitedData", f.split("value"," "))
df3 = df2.withColumn("words", explode("splitedData"))
wordsDF = df3.select("words")
wordCount = wordsDF.groupBy("words").count()

# COMMAND ----------

display(wordCount)

# COMMAND ----------

driver = "org.postgresql.Driver"
url = "jdbc:postgresql://database-1.cbm2qa64a8t4.us-east-2.rds.amazonaws.com/"
table = "rr_schema_pysprk.Gitcode"
user = "postgres"
password = "Admin12345"


# COMMAND ----------

wordCount.write.format("jdbc").option("driver", driver).option("url",url).option("dbtable", table).option("mode", "append").option("user",user).option("password", password).save()
