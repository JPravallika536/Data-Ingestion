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
import botocore
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
    athena_access_bucket = sys_config.get(args[REGION_KEY], 'pre_raw_bucket')
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    client = boto3.client('logs', region_name=boto3_aws_region)
    client_s3 = boto3.client('s3')

    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=guid,
                                             process_key=PROCESS_KEY, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-raw2access-append')
    log_manager.log(message="Starting the raw-to-access job",
                    args={"environment": args[REGION_KEY], 'raw_db': athena_raw_database,
                          'access_db': athena_access_database, "job": JOB_KEY})

    mapping_sql = feed_config.get('transformation-rules', 'mapping-sql')
    athena_write_sql = feed_config.get('transformation-rules', 'athena-write-sql')
    athena_write_sql_date = feed_config.get('transformation-rules', 'athena-write-sql-date')

    mapping_sql = mapping_sql.replace("\n", " ").replace("\t", " ").replace("\r", " ")
    mapping_sql = re.sub('{athena_raw_db}', athena_raw_database, mapping_sql)
    mapping_sql = re.sub('{athena_access_db}', athena_access_database, mapping_sql)

    print(mapping_sql)
    log_manager.log(message='executing mapping query', args={'mapping_sql': mapping_sql, "job": JOB_KEY})

    # Run the mapping query
    mapped_output_df = spark.sql(mapping_sql)
    mapped_output_count = mapped_output_df.count()
    print("count is : " + str(mapped_output_count))
    log_manager.log(message="SQL query", args={"mapped output count is : ": str(mapped_output_count)})

    # mapped_output_df.show()
    mapped_output_df.registerTempTable("mappedOutputTable")

    #if mapped_output_count > 0:
        # Run reconciliation job
       #if (feed_config.has_option('transformation-rules', 'run-reconciliation')):

            #log_manager.log(message='Executing Reconciliation job', args={"job": JOB_KEY})
            #feed_config_parts = args[FEED_CONFIG_KEY].split('.')
            #reconc_config = feed_config_parts[0] + '_recon.conf'
            #recon_database = ""
            #recon_table = ""

            #if (sys_config.has_option(args[REGION_KEY], 'reconciliation_database')):
            #    recon_database = sys_config.get(args[REGION_KEY], 'reconciliation_database')

            #if (sys_config.has_option(args[REGION_KEY], 'reconciliation_table')):
            #    recon_table = sys_config.get(args[REGION_KEY], 'reconciliation_table')

            #run_reconciliation(args[CONFIG_BUCKET_NAME_KEY], reconc_config, spark, guid, recon_database, recon_table,
            #                   athena_raw_database, athena_access_database, cloudwatch_log_group, client)

        #log_manager.log(message='After executing Reconciliation job', args={"job": JOB_KEY})

    #log_manager.log(message='After executing Recon step', args={"job": JOB_KEY})

    # Write to athena table
    athena_write_sql = athena_write_sql.replace("\n", " ").replace("\t", " ").replace("\r", " ")
    athena_write_sql = re.sub('{athena_raw_db}', athena_raw_database, athena_write_sql)
    athena_write_sql = re.sub('{athena_access_db}', athena_access_database, athena_write_sql)

    log_manager.log(message='executing query to write to Athena', args={'write_sql': athena_write_sql, "job": JOB_KEY})
    spark.sql(athena_write_sql)
    # response = spark.sql(athena_write_sql)['HTTPStatusCode']
    # if response is 200:
    # log_manager.log(message='Athena write query executed successfully', args={'HTTPStatusCode': str(response)})

    if mapped_output_count > 0:
        # Write date table to athena
        athena_write_sql_date = athena_write_sql_date.replace("\n", " ").replace("\t", " ").replace("\r", " ")
        athena_write_sql_date = re.sub('{athena_access_db}', athena_access_database, athena_write_sql_date)

        log_manager.log(message='executing query to write date to Athena',
                        args={'write_sql': athena_write_sql_date, "job": JOB_KEY})
        spark.sql(athena_write_sql_date)
    log_manager.log(message='After executing date query', args={'write_sql': athena_write_sql_date, "job": JOB_KEY})

    if (feed_config.has_option('transformation-rules', 'prefix-name')):
        log_manager.log(message='Executing upsert job', args={"job": JOB_KEY})
        athena_select_sql = feed_config.get('transformation-rules', 'athena-select-sql')
        prefix_name = feed_config.get('transformation-rules', 'prefix-name')
        copy_key = feed_config.get('transformation-rules', 'copy-key')
        athena_select_sql = re.sub('{athena_access_db}', athena_access_database, athena_select_sql)
        print(athena_select_sql)
        select_df = spark.sql(athena_select_sql)
        final_df = select_df.repartition(1).write.format('csv').option('header', True).mode('overwrite').option('sep',
                                                                                                                ',').csv(
            's3://' + athena_access_bucket + '/' + prefix_name)
        response = client_s3.list_objects(
            Bucket=athena_access_bucket,
            Prefix=prefix_name,
        )
        name = response["Contents"][0]["Key"]
        copy_source = {'Bucket': athena_access_bucket, 'Key': name}
        # copy_key = prefix_name + '.csv'
        print(copy_key)
        tries2 = 2
        for i in range(tries2):
            try:
                client_s3.copy(CopySource=copy_source, Bucket=athena_access_bucket, Key=copy_key)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    continue
                else:
                    raise
            print('new object copied')
            break
        log_manager.log(message='After executing copy loop', args={'try': i, "job": JOB_KEY})
        tries3 = 2
        for i in range(tries3):
            try:
                client_s3.delete_object(Bucket=athena_access_bucket, Key=name)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    continue
                else:
                    raise
            print('delete after copy')
            break
        log_manager.log(message='After executing delete loop', args={'try': i, "job": JOB_KEY})
    log_manager.log(message='After executing upsert step', args={"job": JOB_KEY})

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


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()