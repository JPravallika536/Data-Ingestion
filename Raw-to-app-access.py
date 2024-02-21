import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.context import DynamicFrame
import sys
import configparser
from io import StringIO
import os
from awsglue.job import Job
import boto3
import re
import logging_service
import sys
from pyspark.sql.utils import AnalysisException

CONFIG_BUCKET_NAME_KEY = "config_bucket"
SYS_CONFIG_KEY = "sys_config_file"
APP_CONFIG_KEY = "app_config_file"
GUID_KEY = "guid"
FEED_CONFIG_KEY = "feed_config_file"
REGION_KEY = "region"
JOB_KEY = "raw-to-app-access"
BATCHID_KEY = 'batchid'


def main():
    global BOTO3_AWS_REGION
    if ("--batchid" in sys.argv):
        args = getResolvedOptions(sys.argv,
                            ['JOB_NAME', GUID_KEY,
                               CONFIG_BUCKET_NAME_KEY,
                               SYS_CONFIG_KEY,
                               FEED_CONFIG_KEY,
                               REGION_KEY,APP_CONFIG_KEY,BATCHID_KEY])
        batchid = args[BATCHID_KEY]
    else:
        args = getResolvedOptions(sys.argv,
                            ['JOB_NAME', GUID_KEY,
                               CONFIG_BUCKET_NAME_KEY,
                               SYS_CONFIG_KEY,
                               FEED_CONFIG_KEY,
                               REGION_KEY,APP_CONFIG_KEY])
        batchid = None



    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    
    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    app_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[APP_CONFIG_KEY])
    feed_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_KEY])

    guid = args[GUID_KEY]
    
    athena_raw_database = sys_config.get(args[REGION_KEY], 'database')
    athena_access_database = app_config.get(args[REGION_KEY], 'access_athena_db')
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    client = boto3.client('logs', region_name=boto3_aws_region)
    feed = args[FEED_CONFIG_KEY].split('/')[-1].split('.')[0]

    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=feed + '_' + guid,
                                             process_key="Access Transformation", client=client,
                                             job=args[REGION_KEY] + '-isg-ie-raw-to-app-access')
    log_manager.log(message="Starting the raw-to-app-access job",
                   args={"environment": args[REGION_KEY], 'raw_db': athena_raw_database,
                         'access_db': athena_access_database, "job": JOB_KEY})

    if feed_config.has_section('temp-tables-sql'):
        for temp_table_name in feed_config['temp-tables-sql']:
            log_manager.log(message='Temp table name',
                            args={'name': temp_table_name, "job": JOB_KEY})
            tmp_table_sql_query = feed_config.get('temp-tables-sql', temp_table_name)
            tmp_table_sql_query = update_dbname_placeholders(tmp_table_sql_query, athena_raw_database, athena_access_database,batchid)
            log_manager.log(message='executing temp table query', args={'tmp_table_sql': tmp_table_sql_query, "job": JOB_KEY})
            tmp_tbl_df = spark.sql(tmp_table_sql_query)
            tmp_tbl_df.registerTempTable(temp_table_name)
            
    mapping_sql = feed_config.get('transformation-rules', 'mapping-sql')
    mapping_sql= update_dbname_placeholders(mapping_sql,athena_raw_database,athena_access_database,batchid)

    log_manager.log(message='executing mapping query', args={'mapping_sql': mapping_sql, "job": JOB_KEY})
    # Run mapping query
    mapped_output_df = spark.sql(mapping_sql)
    mapped_output_df.registerTempTable("mappedOutputTable")
    mapped_output_df.show(2)
    
    for transformation_sql_name in feed_config['transformation-rules']:
        if 'athena-write' in transformation_sql_name:
            athena_write_sql = feed_config.get('transformation-rules', transformation_sql_name)
            athena_write_sql = update_dbname_placeholders(athena_write_sql,athena_raw_database,athena_access_database,batchid)
            log_manager.log(message='executing query to write to Athena', args={'write_sql': athena_write_sql, "job": JOB_KEY})
            try:
                # Write to athena table
                response = spark.sql(athena_write_sql)
            except AnalysisException as e:
                if 'Can not create a Path from an empty string' in str(e):
                    print("ignore analysis exception")
                else:
                    log_manager.log_error(message='Caught analysis exception in writing to Athena', args={'exception': str(e)})
                    sys.exit(str(e))
            log_manager.log(message='Athena write query executed successfully')
            
        
            
    job.commit()


def read_config(bucket, file_prefix):
    s3 = boto3.resource('s3')
    i = 0
    bucket = s3.Bucket(bucket)
    for obj in bucket.objects.filter(Prefix=file_prefix):
        buf = StringIO(obj.get()['Body'].read().decode('utf-8'))
        config = configparser.ConfigParser(interpolation=None)
        config.readfp(buf)
        return config

def update_dbname_placeholders(sql_query, raw_db_name, access_db_name, inp_batch_id=None):
    sql_query = sql_query.replace("\n", " ").replace("\t", " ").replace("\r", " ")
    sql_query = re.sub('{athena_raw_db}', raw_db_name, sql_query)
    sql_query = re.sub('{athena_access_db}', access_db_name, sql_query)
    if inp_batch_id != None:
        sql_query = re.sub('{batchid}', inp_batch_id, sql_query)
    return sql_query
# entry point for PySpark ETL application
if __name__ == '__main__':
    main()