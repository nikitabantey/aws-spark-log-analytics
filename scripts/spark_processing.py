import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.streaming import StreamingContext
from pyspark.sql.types import StructType, StructField, StringType
# from awsglue.utils import getResolvedOptions
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KinesisLogProcessor") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

sc = spark.sparkContext
ssc = StreamingContext(sc, batchDuration=5)  # Process every 5 seconds

# AWS Kinesis Stream Config
STREAM_NAME = "log-stream"
REGION_NAME = "us-east-1"

# Define Schema for JSON Logs
schema = StructType([
    StructField("level", StringType(), True),
    StructField("message", StringType(), True),
    StructField("user", StringType(), True)
])

# Read from Kinesis Stream
kinesis_stream = KinesisUtils.createStream(
    ssc, STREAM_NAME, STREAM_NAME, REGION_NAME, 
    InitialPositionInStream.LATEST, 
    checkpointInterval=10
)

# Parse the records from JSON
def process_record(record):
    data = json.loads(record)
    return (data["level"], data["message"], data["user"])

parsed_stream = kinesis_stream.map(lambda x: json.loads(x)).map(process_record)

# Convert to DataFrame
df = spark.createDataFrame(parsed_stream, schema=schema)

# Process the logs (Example: Filter errors)
error_logs = df.filter(col("level") == "ERROR")

# Show the logs
error_logs.show()

# Store in S3
error_logs.write.mode("append").json("s3://log-storage-bucket-nikita/processed-logs/")

# Start the Spark Streaming Context
ssc.start()
ssc.awaitTermination()