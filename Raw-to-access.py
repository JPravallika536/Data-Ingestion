import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.context import DynamicFrame
#from raw_to_access_recon import *
import sys
import configparser
from io import StringIO
import os
from awsglue.job import Job
import boto3
import re
import logging_service
import sys
from pyspark.sql.functions import col,lit,create_map,to_json,expr,struct,collect_set,regexp_replace
from pyspark.sql.types import *


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
    logger = glueContext.get_logger()

    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    feed_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_KEY])

    guid = args[GUID_KEY]
    athena_raw_database = sys_config.get(args[REGION_KEY], 'database')
    athena_access_database = sys_config.get(args[REGION_KEY], 'access_athena_db')
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    client = boto3.client('logs', region_name=boto3_aws_region)

    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=guid,
                                             process_key=PROCESS_KEY, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-raw-to-access')
    log_manager.log(message="Starting the raw-to-access job",
                   args={"environment": args[REGION_KEY], 'raw_db': athena_raw_database,
                         'access_db': athena_access_database, "job": JOB_KEY})

    mapping_sql = feed_config.get('transformation-rules', 'mapping-sql')
    athena_write_sql = feed_config.get('transformation-rules', 'athena-write-sql')

    if feed_config.has_section('dynamic-partition-rules'):
        dyn_mode_prprty = feed_config.get('dynamic-partition-rules', 'dynamic-partition-mode')
        dyn_mode_prprty = update_dbname_placeholders(dyn_mode_prprty, athena_raw_database, athena_access_database)
        log_manager.log(message='executing dynamic mode property', args={'dyn_mode_prprty': dyn_mode_prprty, "job": JOB_KEY})
        dyn_mode_prprty_response = spark.sql(dyn_mode_prprty)

    if feed_config.has_section('temp-tables-sql'):
        for temp_table_name in feed_config['temp-tables-sql']:
            print('keys :' + temp_table_name)
            log_manager.log(message='Temp table name',
                            args={'name': temp_table_name, "job": JOB_KEY})
            tmp_table_sql_query = feed_config.get('temp-tables-sql', 'temp_table_name')
            tmp_table_sql_query = update_dbname_placeholders(tmp_table_sql_query, athena_raw_database, athena_access_database)
            log_manager.log(message='executing temp table query', args={'tmp_table_sql': tmp_table_sql_query, "job": JOB_KEY})
            tmp_tbl_df = spark.sql(tmp_table_sql_query)
            tmp_tbl_df.registerTempTable("temp_table_name")


    conf_file_name = args[FEED_CONFIG_KEY].split('/')[-1]

    if conf_file_name in ('crm_ann_activity_event.conf', 'crm_ann_activity_task.conf'):

        temp_table_df = tmp_tbl_df

        temp_table_assoc_df = temp_table_df.groupby('crm_object_id').agg(collect_set("associations").alias('associations'))

        temp_table_assoc_df.registerTempTable("mappedAssociationsTable")

        mapping_sql= update_dbname_placeholders(mapping_sql,athena_raw_database,athena_access_database)

        log_manager.log(message='executing mapping query', args={'mapping_sql': mapping_sql, "job": JOB_KEY})
        # Run mapping query
        mapped_output_df = spark.sql(mapping_sql)

        columns_list = [column for column in mapped_output_df.columns[1:]]
        mapped_output_df = mapped_output_df.withColumn("payload",struct(*columns_list))
        mapped_output_df = mapped_output_df.drop(*columns_list)

        mapped_output_df.registerTempTable("mappedOutputTable")

    else:

        mapping_sql= update_dbname_placeholders(mapping_sql,athena_raw_database,athena_access_database)

        log_manager.log(message='executing mapping query', args={'mapping_sql': mapping_sql, "job": JOB_KEY})
        # Run mapping query
        mapped_output_df = spark.sql(mapping_sql)

        mapped_output_df.registerTempTable("mappedOutputTable")

    # Write to athena table
    athena_write_sql= update_dbname_placeholders(athena_write_sql,athena_raw_database,athena_access_database)

    log_manager.log(message='executing query to write to Athena', args={'write_sql': athena_write_sql, "job": JOB_KEY})
    response = spark.sql(athena_write_sql)
 
    if response is 200:
         log_manager.log(message='Athena write query executed successfully', args={'HTTPStatusCode': str(response)})

    if feed_config.has_section('archival'):

        athena_write_hist_sql = feed_config.get('archival', 'athena-write-hist-sql')
        # Write to athena hist table
        athena_write_hist_sql= update_dbname_placeholders(athena_write_hist_sql,athena_raw_database,athena_access_database)

        log_manager.log(message='executing query to write to Athena hist', args={'write_sql': athena_write_hist_sql, "job": JOB_KEY})
        response = spark.sql(athena_write_hist_sql)
     
        if response is 200:
             log_manager.log(message='Athena write hist query executed successfully', args={'HTTPStatusCode': str(response)})

    job.commit()


def read_config(bucket, file_prefix):
    s3 = boto3.resource('s3')
    i = 0
    bucket = s3.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix=file_prefix):
        buf = StringIO(obj.get()['Body'].read().decode('utf-8'))
        config = configparser.ConfigParser()
        config.readfp(buf)
        return config

def update_dbname_placeholders(sql_query, raw_db_name, access_db_name):
    sql_query = sql_query.replace("\n", " ").replace("\t", " ").replace("\r", " ")
    sql_query = re.sub('{athena_raw_db}', raw_db_name, sql_query)
    sql_query = re.sub('{athena_access_db}', access_db_name, sql_query)
    return sql_query
# entry point for PySpark ETL application
if __name__ == '__main__':
    main()