import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['input_path', 'output_path'])
input_path = args['input_path']
output_path = args['output_path']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

df = spark.read.option("header", "true").csv(input_path)
df_cleaned = df.dropna()

df_cleaned.write.mode("overwrite") \
    .option("header", "true") \
    .option("sep", ",") \
    .csv(output_path)

print("Saved cleaned data as CSV")
spark.stop()
