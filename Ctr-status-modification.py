import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.context import DynamicFrame
from raw_to_access_recon import *
import sys
import configparser
from io import StringIO
import os
from awsglue.job import Job
import boto3
import re
import logging_service

CONFIG_BUCKET_NAME_KEY = "config_bucket"
SYS_CONFIG_KEY = "sys_config_file"
FEED_CONFIG_KEY = "feed_config_file"
GUID_KEY = "guid"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
BATCH_RUN_DATE = 'batch_date'
PROCESS_KEY = 'raw_to_access'
JOB_KEY = "raw-to-access"


def main():
    global BOTO3_AWS_REGION

    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME', GUID_KEY,
                               CONFIG_BUCKET_NAME_KEY,
                               SYS_CONFIG_KEY,
                               FEED_CONFIG_KEY,
                               REGION_KEY])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    feed_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_KEY])

    guid = args[GUID_KEY]
    athena_raw_database = sys_config.get(args[REGION_KEY], 'database')
    athena_access_database = sys_config.get(args[REGION_KEY], 'access_athena_db')
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    pre_raw_s3_bucket = sys_config.get(args[REGION_KEY], 'pre_raw_bucket')

    changed_records_sql = feed_config.get('transformation-rules', 'changed-records-sql')
    unchanged_records_sql = feed_config.get('transformation-rules', 'unchanged-records-sql')
    temp_table_s3_path = feed_config.get('transformation-rules', 'temp-table-s3-path')
    ctr_sts_modification_tbl_s3_path = feed_config.get('transformation-rules', 'ctr-sts-modification-tbl-s3-path')
    ctr_sts_modification_tlb_name = feed_config.get('transformation-rules', 'ctr-sts-modification-tlb-name')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    client = boto3.client('logs', region_name=boto3_aws_region)

    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=guid,
                                             process_key=PROCESS_KEY, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-ctr-status-modification')
    log_manager.log(message="Starting contract modification daily job",
                    args={"environment": args[REGION_KEY], 'raw_db': athena_raw_database,
                          'access_db': athena_access_database, "job": JOB_KEY})

    changed_records_sql = changed_records_sql.replace("\n", " ").replace("\t", " ").replace("\r", " ").replace("    ",
                                                                                                               " ")
    changed_records_sql = re.sub('{athena_raw_db}', athena_raw_database, changed_records_sql)
    changed_records_sql = re.sub('{athena_access_db}', athena_access_database, changed_records_sql)
    log_manager.log(message="SQL query", args={"status_modified_records_sql": changed_records_sql})
    print("status_modified_records_sql : " + changed_records_sql)
    status_modified_df = spark.sql(changed_records_sql)

    # status_modified_df.show()

    status_modified_df.registerTempTable("ctr_sts_modified_records")

    unchanged_records_sql = unchanged_records_sql.replace("\n", " ").replace("\t", " ").replace("\r", " ").replace(
        "    ", " ")
    unchanged_records_sql = re.sub('{athena_raw_db}', athena_raw_database, unchanged_records_sql)
    unchanged_records_sql = re.sub('{athena_access_db}', athena_access_database, unchanged_records_sql)

    print("unchanged_records_sql : " + unchanged_records_sql)
    log_manager.log(message="SQL query", args={"unchanged_records_sql": unchanged_records_sql})

    unmodified_records_df = spark.sql(unchanged_records_sql)
    unmodified_count = unmodified_records_df.count()
    print("count is : " + str(unmodified_count))
    log_manager.log(message="SQL query", args={"unmodified_records count is : ": str(unmodified_count)})
    # unmodified_records_df.show()
    print("dataframe print")

    temp_table_s3_path = re.sub('{pre-raw-bucket}', pre_raw_s3_bucket, temp_table_s3_path)
    ctr_sts_modification_tbl_s3_path = re.sub('{pre-raw-bucket}', pre_raw_s3_bucket, ctr_sts_modification_tbl_s3_path)

    if (unmodified_count > 0):
        unmodified_records_df.write.mode("overwrite").format("parquet").save(temp_table_s3_path)
    status_modified_df.write.mode("overwrite").format("parquet").save(ctr_sts_modification_tbl_s3_path)

    if (unmodified_count > 0):
        spark.read.parquet(temp_table_s3_path).write.mode("append").format("parquet").save(
            ctr_sts_modification_tbl_s3_path)

    spark.sql("msck repair table " + athena_access_database + "." + ctr_sts_modification_tlb_name)


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
