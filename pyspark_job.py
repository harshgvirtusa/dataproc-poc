import sys
from pyspark.sql import SparkSession
 
# Function to parse key-value arguments safely
def parse_args(args):
    parsed_args = {}
    for arg in args:
        print("arg:", arg)
        if '=' in arg:
            key, value = arg.split('=', 1)
            parsed_args[key] = value
    return parsed_args
 
# Retrieve the JOB_NAME, INPUT_PATH, and OUTPUT_PATH arguments
args = parse_args(sys.argv[1:])
required_keys = ['JOB_NAME', 'INPUT_PATH', 'OUTPUT_PATH']
 
# Check if all required arguments are present
missing_keys = [key for key in required_keys if key not in args]
if missing_keys:
    print(f"Error: Missing argument(s): {', '.join(missing_keys)}")
    sys.exit(1)
 
job_name = args['JOB_NAME']
input_path = args['INPUT_PATH']
output_path = args['OUTPUT_PATH']
 
# Initialize Spark Session
spark = SparkSession.builder.appName(job_name).getOrCreate()
 
# Read data from Google Cloud Storage using the input path passed as a job argument
dataframe = spark.read.format("csv").option("header", "true").load(input_path)
 
# Write down to Google Cloud Storage using the DataFrame API to the output path passed as a job argument
dataframe.write.format("csv").option("header", "true").save(output_path)
 
# Stop the Spark session
spark.stop()
