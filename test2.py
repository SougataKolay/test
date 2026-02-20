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
BASE_TABLE = f"{CATALOG}.{TGT_DB}.{TGT_TBL}"

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
        # if obj["Key"].endswith(".txt")
        if obj["Key"].endswith(".G0875V00")
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

    if num_cols not in (28, 29):
        raise ValueError(f"Unexpected column count: {num_cols}")

    if num_cols == 29:
        extra_column_name = df.columns[-1]
        df = df.drop(extra_column_name)

    # structured_df = (
    #     df
    #     .withColumnRenamed("ClubName", col("CLUB NAME").cast(StringType()))
    #     .withColumnRenamed("ClubNo", col("CLUB NO").cast(StringType()))
    #     .withColumnRenamed("RegionNo", col("CLUB REGION").cast(StringType()))
    #     .withColumnRenamed("GroupCode", col("GROUP").cast(StringType()))
    #     .withColumnRenamed("SubGroupCode", col("SUB GROUP").cast(StringType()))
    #     .withColumnRenamed("AgentType", col("AGENT").cast(StringType()))
    #     .withColumnRenamed("CampaignCode", col("CAMPAIGN").cast(StringType()))
    #     .withColumnRenamed("CampaignType", col("CAMPAIGN TYPE").cast(StringType()))
    #     .withColumnRenamed("TransactionDate", col("TRAN DATE").cast(DateType()))
    #     .withColumnRenamed("AdminFee", col("ADMIN FEE").cast(DecimalType(18,2)))
    #     .withColumnRenamed("PrimaryIndicator", col("PRIMARY").cast(StringType()))
    #     .withColumnRenamed("AdultIndicator", col("ADULT").cast(StringType()))
    #     .withColumnRenamed("DependantIndicator", col("DEPENDENT").cast(StringType()))
    #     .withColumnRenamed("PlusProductIndicator", col("IND PLUS").cast(StringType()))
    #     .withColumnRenamed("FamilyPlusIndicator", col("FAM PLUS").cast(StringType()))
    #     .withColumnRenamed("PremierProductIndicator", col("IND-PREMIER").cast(StringType()))
    #     .withColumnRenamed("FamilyPremierIndicator", col("FAM PREMIER").cast(StringType()))
    #     .withColumnRenamed("RVCyIIndicator", col("RV/CYL").cast(StringType()))
    #     .withColumnRenamed("ARIndicator", col("A/R").cast(StringType()))
    #     .withColumnRenamed("MemberNo", col("MEMBER #").cast(StringType()))
    #     .withColumnRenamed("SalesRegion", col("SALES REGION").cast(StringType()))
    #     .withColumnRenamed("OfficeNo", col("DO").cast(StringType()))
    #     .withColumnRenamed("OfficeName", col("DO NAME").cast(StringType()))
    #     .withColumnRenamed("EmployeeNo", col("EMPLOYEE #").cast(StringType()))
    #     .withColumnRenamed("AgentID", col("AGENT ID").cast(StringType()))
    #     .withColumnRenamed("JobCode", col("JOB").cast(StringType()))
    #     .withColumnRenamed("Rolecode", col("ROLE").cast(StringType()))
    #     .withColumnRenamed("TransType", col("TRANS").cast(StringType()))
    #     #.withColumn("Extract_T", current_timestamp())
    #     .withColumn("Extract_T", lit(None).cast(TimestampType()))
    #     #.select("*")
    # )
    
    # structured_df = df.selectExpr(
    # "CAST(ClubName AS StringType) AS CLUB NAME" )
    
        
    structured_df = df.selectExpr(
        "CAST(`CLUB NAME` AS STRING) AS ClubName",
        "CAST(`CLUB NO` AS STRING) AS ClubNo",
        "CAST(`CLUB REGION` AS STRING) AS RegionNo",
        "CAST(`GROUP` AS STRING) AS GroupCode",
        "CAST(`SUB GROUP` AS STRING) AS SubGroupCode",
        "CAST(`AGENT` AS STRING) AS AgentType",
        "CAST(`CAMPAIGN` AS STRING) AS CampaignCode",
        "CAST(`CAMPAIGN TYPE` AS STRING) AS CampaignType",
        "CAST(`TRAN DATE` AS DATE) AS TransactionDate",
        "CAST(`ADMIN FEE` AS DECIMAL(18,2)) AS AdminFee",
        "CAST(`PRIMARY` AS STRING) AS PrimaryIndicator",
        "CAST(`ADULT` AS STRING) AS AdultIndicator",
        "CAST(`DEPENDENT` AS STRING) AS DependantIndicator",
        "CAST(`IND PLUS` AS STRING) AS PlusProductIndicator",
        "CAST(`FAM PLUS` AS STRING) AS FamilyPlusIndicator",
        "CAST(`IND-PREMIER` AS STRING) AS PremierProductIndicator",
        "CAST(`FAM PREMIER` AS STRING) AS FamilyPremierIndicator",
        "CAST(`RV/CYL` AS STRING) AS RVCyIIndicator",
        "CAST(`A/R` AS STRING) AS ARIndicator",
        "CAST(`MEMBER #` AS STRING) AS MemberNo",
        "CAST(`SALES REGION` AS STRING) AS SalesRegion",
        "CAST(`DO` AS STRING) AS OfficeNo",
        "CAST(`DO NAME` AS STRING) AS OfficeName",
        "CAST(`EMPLOYEE #` AS STRING) AS EmployeeNo",
        "CAST(`AGENT ID` AS STRING) AS AgentID",
        "CAST(`JOB` AS STRING) AS JobCode",
        "CAST(`ROLE` AS STRING) AS Rolecode",
        "CAST(`TRANS` AS STRING) AS TransType",
        "CAST(NULL AS TIMESTAMP) AS Extract_T"
    )

    
    
    

    # structured_df.createOrReplaceTempView("staging_table")

    # ------------------------------------------------------------
    # 1. Truncate load W1 TABLE (NO PARTITION)
    # ------------------------------------------------------------
    spark.sql(f"""
        drop table if exists {W1_TABLE} purge;
    """)
    
    structured_df.writeTo(W1_TABLE).using("iceberg").create()

    # ------------------------------------------------------------
    # 2. DELETE BASE TABLE DATA (BASED ON (TRANSACTIONDATE div 100))
    # ------------------------------------------------------------
    spark.sql(f"""
     DELETE FROM {BASE_TABLE}
     WHERE date_format(TransactionDate, 'yyyyMM')
     IN (
        SELECT DISTINCT date_format(TransactionDate, 'yyyyMM')
        FROM {W1_TABLE}
         )
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
