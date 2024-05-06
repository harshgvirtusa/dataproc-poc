import sys
from pyspark.sql import SparkSession
 
# Retrieve the JOB_NAME, INPUT_PATH, and OUTPUT_PATH arguments
args = sys.argv[1:]
# required_keys = ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH']
for arg in args:
    print("arg:", arg)
 
job_name = args[0]
input_path = args[1]
output_path = args[2]
 
# Initialize Spark Session
spark = SparkSession.builder.appName(job_name).getOrCreate()
 
# Read data from Google Cloud Storage using the input path passed as a job argument
dataframe = spark.read.format("csv").option("header", "true").load(input_path)
 
# Write down to Google Cloud Storage using the DataFrame API to the output path passed as a job argument
dataframe.write.format("csv").option("header", "true").save(output_path)
 
# Stop the Spark session
spark.stop()
