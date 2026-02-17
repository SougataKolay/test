import sys
import logging
from urllib import response
import boto3, re, traceback
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
#from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType,
    DecimalType, DateType
)

# -------------------------------------------------------------------------
# Logging Setup
# -------------------------------------------------------------------------
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
# Resolve Arguments
# -------------------------------------------------------------------------
print("Resolving job parameters...")

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "database_name",
        "table_name",
        "catalog",
        "raw_file_folder",
        "data_bucket",
        "log_bucket"
    ]
)

# print("Resolved parameters:")
# for k, v in args.items():
#     print(f"  {k} = {v}")

TGT_DB = args["database_name"]
TGT_TBL = args["table_name"]
CATALOG = args["catalog"]
raw_file_folder = args["raw_file_folder"].strip().rstrip("/")
data_bucket = args["data_bucket"].strip().rstrip("/")
log_bucket = args["log_bucket"]

W1_TABLE = f"{CATALOG}.{TGT_DB}.mbrship_sales_trans_w1"
BASE_TABLE = f"{CATALOG}.{TGT_DB}.mbrship_sales_trans_by_week"

# -------------------------------------------------------------------------
# Spark / Iceberg Initialization
# -------------------------------------------------------------------------
print("Initializing Spark session with Iceberg configs...")

sc = SparkContext.getOrCreate()
s3 = boto3.client("s3")

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
job.init(args["JOB_NAME"], args)

# -------------------------------------------------------------------------
# Utility Functions
# -------------------------------------------------------------------------

def write_log(target_table, message):
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    safe_table = re.sub(r'[^A-Za-z0-9_]+', '_', target_table)
    
    LOG_BUCKET = args["log_bucket"]
    log_key = f"glue_logs/{safe_table}_{timestamp}.txt"

    resp = s3.list_objects_v2(Bucket=LOG_BUCKET, Prefix="glue_logs/", MaxKeys=1)
    if 'Contents' not in resp:
        s3.put_object(Bucket=LOG_BUCKET, Key="glue_logs/", Body=b'')
        
    s3.put_object(
        Bucket=LOG_BUCKET,
        Key=log_key,
        Body=message.encode("utf-8")
    )

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

# -------------------------------------------------------------------------
# Get TXT File
# -------------------------------------------------------------------------

def get_raw_file(bucket, prefix):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    txt_files = [
        obj["Key"]
        for obj in resp.get("Contents", [])
        if obj["Key"].endswith(".txt")
    ]

    if len(txt_files) != 1:
        raise ValueError(f"Expected 1 TXT file under {prefix}, found {len(txt_files)}")

    return txt_files[0]

# -------------------------------------------------------------------------
# MAIN ETL LOGIC
# -------------------------------------------------------------------------

def process_file(csv_key):

    full_path = f"s3://{data_bucket}/{csv_key}"
    logger.info(f"Reading file: {full_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        #.option("trimValues", "true")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .csv(full_path)
    )

    num_cols = len(df.columns)

    # if num_cols not in (28, 29):
        # raise ValueError(f"Unexpected column count: {num_cols}")

    if num_cols == 29:
        extra_column_name = df.columns[-1]
        df = df.drop(extra_column_name)

    structured_df = (
        df
        .withColumn("ClubName", col("CLUB NAME").cast(StringType()))
        .withColumn("ClubNo", col("CLUB NO").cast(StringType()))
        .withColumn("RegionNo", col("CLUB REGION").cast(StringType()))
        .withColumn("GroupCode", col("GROUP").cast(StringType()))
        .withColumn("SubGroupCode", col("SUB GROUP").cast(StringType()))
        .withColumn("AgentType", col("AGENT").cast(StringType()))
        .withColumn("CampaignCode", col("CAMPAIGN").cast(StringType()))
        .withColumn("CampaignType", col("CAMPAIGN TYPE").cast(StringType()))
        .withColumn("TransactionDate", col("TRAN DATE").cast(DateType()))
        .withColumn("AdminFee", col("ADMIN FEE").cast(DecimalType(18,2)))
        .withColumn("PrimaryIndicator", col("PRIMARY").cast(StringType()))
        .withColumn("AdultIndicator", col("ADULT").cast(StringType()))
        .withColumn("DependantIndicator", col("DEPENDENT").cast(StringType()))
        .withColumn("PlusProductIndicator", col("IND PLUS").cast(StringType()))
        .withColumn("FamilyPlusIndicator", col("FAM PLUS").cast(StringType()))
        .withColumn("PremierProductIndicator", col("IND-PREMIER").cast(StringType()))
        .withColumn("FamilyPremierIndicator", col("FAM PREMIER").cast(StringType()))
        .withColumn("RVCyIIndicator", col("RV/CYL").cast(StringType()))
        .withColumn("ARIndicator", col("A/R").cast(StringType()))
        .withColumn("MemberNo", col("MEMBER #").cast(StringType()))
        .withColumn("SalesRegion", col("SALES REGION").cast(StringType()))
        .withColumn("OfficeNo", col("DO").cast(StringType()))
        .withColumn("OfficeName", col("DO NAME").cast(StringType()))
        .withColumn("EmployeeNo", col("EMPLOYEE #").cast(StringType()))
        .withColumn("AgentID", col("AGENT ID").cast(StringType()))
        .withColumn("JobCode", col("JOB").cast(StringType()))
        .withColumn("Rolecode", col("ROLE").cast(StringType()))
        .withColumn("TransType", col("TRANS").cast(StringType()))
        #.withColumn("Extract_T", current_timestamp())
        .withColumn("Extract_T", lit(None).cast(TimestampType()))
        .select("*")
    )

    # structured_df.createOrReplaceTempView("staging_table")

    # ------------------------------------------------------------
    # 1. CREATE OR REPLACE W1 TABLE (NO PARTITION)
    # ------------------------------------------------------------
    # spark.sql(f"""
    #     CREATE OR REPLACE TABLE {W1_TABLE}
    #     USING iceberg
    #     AS
    #     SELECT * FROM staging_table
    # """)
    
    structured_df.writeTo(W1_TABLE).using("iceberg").createOrReplace()

    # ------------------------------------------------------------
    # 2. DELETE BASE TABLE DATA (BASED ON (TRANSACTIONDATE div 100))
    # ------------------------------------------------------------
    spark.sql(f"""
        Delete From {BASE_TABLE} where (transactiondate div 100) in (select distinct (transactiondate div 100) from {W1_TABLE})
    """)

    # ------------------------------------------------------------
    # 3. INSERT FROM W1 TO BASE TABLE
    # ------------------------------------------------------------
    spark.sql(f"""
        INSERT INTO {BASE_TABLE}
        SELECT * FROM {W1_TABLE}
    """)

    archive_raw_file(TGT_TBL, csv_key)
    write_log(TGT_TBL, "Job completed successfully")

# -------------------------------------------------------------------------
# DRIVER
# -------------------------------------------------------------------------

def main():
    prefix = f"{raw_file_folder}/{TGT_TBL}/"

    try:
        csv_key = get_raw_file(data_bucket, prefix)
        process_file(csv_key)
        job.commit()

    except Exception as e:
        error_message = f"Job failed: {str(e)}\n{traceback.format_exc()}"
        write_log(TGT_TBL, error_message)
        raise

if __name__ == "__main__":
    main()
