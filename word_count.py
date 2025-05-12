import sys
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

# Create GlueContext and SparkSession
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

args = getResolvedOptions(sys.argv, ['input_path', 'output_path'])

print ("The input path is: ", args['input_path'])
print ("The output path is: ", args['output_path'])

input_path = args['input_path']
output_path = args['output_path']

text_rdd = spark.sparkContext.textFile(input_path)

# Split by words and count
word_counts = (
    text_rdd.flatMap(lambda line: line.split())
            .map(lambda word: (word.lower(), 1))
            .reduceByKey(lambda a, b: a + b)
)

# Convert to DataFrame
word_df = word_counts.toDF(["word", "count"])

# Repartition the DataFrame to 1 partition
word_df_repartitioned = word_df.repartition(1)

# Save output as CSV to S3 in a single partition
word_df_repartitioned.write.mode("overwrite").option("header", "true").csv(output_path)

spark.stop()