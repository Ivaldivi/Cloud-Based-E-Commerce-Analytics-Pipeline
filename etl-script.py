import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import regexp_extract, col, to_timestamp, year, month, dayofmonth, hour, when

# Initialize Glue context
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SOURCE_BUCKET', 'OUTPUT_BUCKET', 'OUTPUT_DATABASE'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configuration
input_path = args['SOURCE_BUCKET']
output_path = args['OUTPUT_BUCKET']
output_database = args['OUTPUT_DATABASE']

input_path = f"s3://{input_path}/events/"
output_path = f"s3://{output_path}/capstone-analytics/"

# Read JSON files using DynamicFrame (required for bookmarks)
print(f"Reading JSON files from  {input_path}")
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [input_path], "recurse": True, "compressionType":"gzip"},
    format="json",
    transformation_ctx="read_json_logs"  # REQUIRED for bookmark tracking
)

# Convert to DataFrame for transformations
df = dynamic_frame.toDF()
print(f"Total records read: {df.count()}")

# Convert timestamp
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Derive partition columns
df = df.withColumn("year", year(col("timestamp")).cast("string"))
df = df.withColumn("month", month(col("timestamp")).cast("string"))
df = df.withColumn("day", dayofmonth(col("timestamp")).cast("string"))
df = df.withColumn("hour", hour(col("timestamp")).cast("string"))

# Select final columns (excluding temporary bytes_str)
final_df = df.select(
    "timestamp", "user_id", "session_id", "event_type", "product_id", "quantity", "price","category", "search_query",
    "year", "month", "day", "hour"
)

print(f"Parsed records: {final_df.count()}")
print("Sample parsed data:")
final_df.show(5, truncate=False)

# Convert back to DynamicFrame before writing (required for bookmarks)
output_dyf = DynamicFrame.fromDF(final_df, glueContext, "output")

# Write Parquet with partitions using Glue writer (preserves bookmark state)
print("Writing Parquet to S3 with partitioning...")
glueContext.write_dynamic_frame.from_options(
    frame=output_dyf,
    connection_type="s3",
    connection_options={
        "path": output_path,
        "partitionKeys": ["year", "month", "day", "hour"]
    },
    format="parquet",
    transformation_ctx="write_parquet_logs"  # REQUIRED for bookmark tracking
)

print("Glue job completed")
job.commit()

