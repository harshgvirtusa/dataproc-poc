import sys
from pyspark.sql import SparkSession
 
# Retrieve the JOB_NAME, INPUT_PATH and OUTPUT_PATH arguments passed to the job
args = {arg.split('=')[0]: arg.split('=')[1] for arg in sys.argv[1:]}
job_name = args['JOB_NAME']
input_path = args['INPUT_PATH']
output_path = args['OUTPUT_PATH']
 
# Initialize Spark Session
spark = SparkSession.builder.appName(job_name).getOrCreate()
 
# Reading data from Google Cloud Storage using the input path passed as a job argument
dataframe = spark.read.format("csv").option("header", "true").load(input_path)
 
# Write down to Google Cloud Storage using the DataFrame API to the output path passed as a job argument
dataframe.write.format("csv").option("header", "true").save(output_path)
 
spark.stop()
