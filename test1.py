'''
Previously:
input_temp → S3 path of raw file
raw_df = spark.read.text(input_temp)
raw_df now holds unstructured text
header_df = spark.read.csv(HDR_PATH)
header_df holds metadata
Loop over header_rows
Create new columns in parsed_df
parsed_df.createOrReplaceTempView(...)
CREATE OR REPLACE TABLE {CATALOG}.{TGT_DB}.{TGT_TBL}
Spark writes parsed_df to Iceberg

Now:
input_temp is removed
raw_df is read directly from S3 path of raw file

Description:
AWS Glue Spark job that reads raw text data from S3, parses it based on metadata headers,
and writes the processed data to an Iceberg table in the data catalog.
Includes logging, error handling, and column transformations using PySpark.

'''

import sys
import logging
import boto3,json, re, traceback
from datetime import datetime
from urllib.parse import urlparse
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql.utils import AnalysisException
from awsglue.utils import getResolvedOptions
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, date_format,substring, trim
from pyspark.sql.types import StructType, StructField, StringType


logger = logging.getLogger()
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
if logger.hasHandlers():
    logger.handlers.clear()
logger.addHandler(handler)
logger.setLevel(logging.INFO)

print("==== JOB STARTED ====")

# -------------------------------------------------------------------------
# Resolve arguments
# -------------------------------------------------------------------------
print("Resolving job parameters...")

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "input_path",
        "input_file",
        "HEADER_S3_PATH",
        "database_name",
        "table_name",
        "CATALOG",
        "raw_file_folder",
        "data_bucket",
        "log_bucket",
        "input_temp"
    ]
)

print("Resolved parameters:")
for k, v in args.items():
    print(f"  {k} = {v}")

SRC_PATH = args["input_path"]
SRC_FILE = args["input_file"]
HDR_PATH = args["HEADER_S3_PATH"]
TGT_DB = args["database_name"]
TGT_TBL = args["table_name"]
CATALOG = args["CATALOG"]
log_bucket = args["log_bucket"]
raw_file_folder = args["raw_file_folder"].strip().rstrip("/")
data_bucket = args["data_bucket"].strip().rstrip("/")
# input_temp=args["input_temp"]  # Not used anymore

# -------------------------------------------------------------------------
# Validate critical parameters early
# -------------------------------------------------------------------------
print("Validating required parameters...")

for k in ["input_path", "HEADER_S3_PATH", "database_name", "table_name", "CATALOG"]:
    if not args.get(k):
        raise ValueError(f"Missing or empty required job parameter: {k}")
        
sc = SparkContext.getOrCreate()
s3 = boto3.client("s3")
glue_client = boto3.client("glue")

SRC_FILE_FULL_PATH = f"{SRC_PATH}/{SRC_FILE}"
TARGET_IDENTIFIER = f"{CATALOG}.{TGT_DB}.{TGT_TBL}"
print(f"Target Iceberg identifier resolved as: {TARGET_IDENTIFIER}")

# -------------------------------------------------------------------------
# Spark / Iceberg initialization
# -------------------------------------------------------------------------
print("Initializing Spark session with Iceberg configs...")

spark = (
    SparkSession.builder
    .config("spark.sql.defaultCatalog", CATALOG)
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
    .getOrCreate()
)

glueContext = GlueContext(sc)
job = Job(glueContext)

def write_log(target_table, message):
    #s3 = boto3.client("s3")
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    safe_table = re.sub(r'[^A-Za-z0-9_]+', '_', target_table)

    LOG_BUCKET = args["log_bucket"]
    log_key = f"glue_logs/{safe_table}_{timestamp}.txt"

    resp = s3.list_objects_v2(Bucket=LOG_BUCKET, Prefix="glue_logs/", MaxKeys=1)
    if 'Contents' not in resp:
        s3.put_object(Bucket=LOG_BUCKET, Key="glue_logs/", Body=b'')

    s3.put_object(
        Bucket=LOG_BUCKET,
        Key=log_key,
        Body=message.encode('utf-8')
    )
    logger.info(f"Log written to s3://{LOG_BUCKET}/{log_key}")

# Archive input file upon success
def archive_raw_file(target_table, csv_key):
    ts = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    filename = csv_key.split("/")[-1]
    archive_key = f"{raw_file_folder}_archive/{target_table}/{target_table}_{ts}_{filename}"

    s3.copy_object(
        Bucket=data_bucket,
        CopySource={"Bucket": data_bucket, "Key": csv_key},
        Key=archive_key
    )
    s3.delete_object(Bucket=data_bucket, Key=csv_key)
    logger.info(f"Archived TXT → s3://{data_bucket}/{archive_key}")

# -------------------------------------------------------------------------
# Lookup TXT in input folder (must be exactly one)
# -------------------------------------------------------------------------
def get_raw_file(bucket, prefix):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" not in resp:
        raise ValueError(f"No data found under: s3://{bucket}/{prefix}")
        
    txt_files = [obj["Key"] for obj in resp["Contents"] if obj["Key"].lower().endswith((".txt"))] # Only consider .txt files
    if len(txt_files) != 1:
        raise ValueError(f"Expected 1 TXT under {prefix}, found {len(txt_files)}")

    return txt_files[0]  # key only
 
# Convert JSON schema → Spark StructType
# def get_table_schema(entry):
#     schema = entry["spark_schema"]
#     for f in schema["fields"]:
#         f.setdefault("metadata", {})
#     return StructType.fromJson(schema)
    
#print("==== JOB STARTED ====")

print("Spark session created successfully")

# -------------------------------------------------------------------------
# Read header / mapping file
# -------------------------------------------------------------------------
def read_file(csv_key):
    print(f"Reading header file from: {HDR_PATH}")

    header_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(HDR_PATH)  
    )

    print(f"Header columns detected: {header_df.columns}")

    header_df = header_df.select([col(c).alias(c.strip()) for c in header_df.columns])
    header_df = header_df.orderBy(col("Start").cast("int"))

    print("Collecting header rows...")
    header_rows = header_df.collect()
    print(f"Number of header rows: {len(header_rows)}")

    if not header_rows:
        raise Exception("Header file is empty")

    # -------------------------------------------------------------------------
    # Read source data (.txt)
    # -------------------------------------------------------------------------
    # print(f"Reading source data file from: {SRC_PATH}")
    # #prefix = f"{raw_file_folder}/{SRC_PATH}/"
    # SRC_FILE=f"{SRC_PATH}/{csv_key}"
    #try:
    #    spark_schema = get_table_schema(entry)
    #    schema_json = entry["spark_schema"]
    #    csv_options = entry.get("options", {})

    # #try:
    # raw_df = spark.read.text(input_temp)
    # print(f"Raw source row count (sampled): {raw_df.limit(10).count()}")
    
    
    full_path = f"s3://{data_bucket}/{csv_key}"
    print(f"Reading TXT source file from: {full_path}")

    raw_df = spark.read.text(full_path)

    # -------------------------------------------------------------------------
    # Parse fixed-width records
    # -------------------------------------------------------------------------
    print("Parsing fixed-width file using header metadata...")

    parsed_df = raw_df
    for r in header_rows:
        start = int(r["Start"])
        length = int(r["End"]) - start + 1
        name = r["Field_Name"]
        print(f"Extracting column '{name}' (start={start}, length={length})")
        parsed_df = parsed_df.withColumn(name, trim(substring(col("value"), start, length)))
    parsed_df = parsed_df.drop("value")
    #except Exception:
    #    error_details = traceback.format_exc()
    #    log_error_to_s3(table_name, error_details)
    #    raise
    #except ValueError as e:
    #    logger.warning(f"[SKIPPED] TGT_TBL: {str(e)}")
    #    write_log(TGT_TBL, f"SKIPPED: {str(e)}")
    #    return
    #print("Parsed DataFrame schema:")
    #parsed_df.printSchema()

    #print("Parsed DataFrame sample rows:")
    #parsed_df.show(5, truncate=False)
    #return parsed_df
    # -------------------------------------------------------------------------
    # Write to Iceberg
    # -------------------------------------------------------------------------
    #print(f"Writing data to Iceberg table: {TARGET_IDENTIFIER}")

    
    #def create_table(parsed_df):
    
    parsed_df.createOrReplaceTempView("staging_hunpn_wkly")

    spark.sql(f"""
    CREATE OR REPLACE TABLE {CATALOG}.{TGT_DB}.{TGT_TBL}
    USING iceberg
    AS
    SELECT * FROM staging_hunpn_wkly
    """)
    #df.writeTo(TARGET_IDENTIFIER).overwritePartitions()
    write_log(TGT_TBL, "Job completed successfully")
    logger.info("ETL job completed successfully.")
    print("Write completed successfully")
    print("==== JOB FINISHED ====")


def main():
    prefix = f"{raw_file_folder}/{TGT_TBL}/"
    #table_name = entry["table_name"].lower()
    #prefix = f"{raw_csv_folder}/{table_name}/"
    try:
        csv_key = get_raw_file(data_bucket, prefix)
    except ValueError as e:
        logger.warning(f"[SKIPPED] {TGT_TBL}: {str(e)}")
        write_log(TGT_TBL, f"SKIPPED: {str(e)}")
        return

    try:
        read_file(csv_key)
        #if partition_flag == 'Y':
        #    df = df.withColumn(partition_col, F.col(partition_col))
        #create_table(df)
        # Archive only after successful write
        archive_raw_file(TGT_TBL, csv_key)

        job.commit()
        logger.info("Glue job completed successfully")
    except Exception as e:
        err = f"Job failed: {str(e)}\n{traceback.format_exc()}"
        write_log(TGT_TBL, err)
        raise

# -------------------------------------------------------------------------
# Driver
# -------------------------------------------------------------------------
if __name__ == "__main__":
    main()