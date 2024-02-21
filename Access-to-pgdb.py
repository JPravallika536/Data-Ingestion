import sys
import boto3
import sys
import configparser
from io import StringIO
import os
import logging_service
import uuid
from awsglue.utils import getResolvedOptions
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.context import DynamicFrame
from awsglue.job import Job
import json

CONFIG_BUCKET_NAME_KEY = "config_bucket"
FEED_CONFIG_FILE_KEY = "feed_config_file"
SYS_CONFIG_KEY = "sys_config_file"
GUID_KEY = "guid"
REGION_KEY = "region"
PROCESS_KEY = 'access_to_pgdb'
JOB_KEY = "raw-to-access"
BATCH_SIZE = "jdbc_batch_size"
NUM_PARTITIONS = "jdbc_num_partitions"
DB_HOSTNAME_OVERRIDE = "pgdb_hostname_override"


def main():
    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME', CONFIG_BUCKET_NAME_KEY, FEED_CONFIG_FILE_KEY, SYS_CONFIG_KEY, REGION_KEY,
                               GUID_KEY, BATCH_SIZE, NUM_PARTITIONS, DB_HOSTNAME_OVERRIDE])

    # setting up variables from configs and job arguments
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    print('args are' + str(args))
    print ('bucket is ' + args[CONFIG_BUCKET_NAME_KEY])
    print('key is ' + args[FEED_CONFIG_FILE_KEY])

    pgdb_hostname_override = args[DB_HOSTNAME_OVERRIDE]

    guid = args[GUID_KEY]
    feed_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_FILE_KEY])
    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    athena_access_database = sys_config.get(args[REGION_KEY], 'access_athena_db')
    athena_raw_database = sys_config.get(args[REGION_KEY], 'database')
    batch_size = args[BATCH_SIZE]
    num_partitions = args[NUM_PARTITIONS]

    pgdb_schema = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db_schema')
    pgres_database = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db')
    pgdb_insert_table = feed_config.get('transformation-rules', 'pgdb-insert-table')
    pgdb_insert_table = re.sub('{pgdb_schema}', pgdb_schema, pgdb_insert_table)
    batch_update_table = feed_config.get('transformation-rules', 'batch-update-table')
    batch_update_table = re.sub('{pgdb_schema}', pgdb_schema, batch_update_table)
    # user = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db_user')
    # password = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db_pwd')
    glue_connection_name = sys_config.get(args[REGION_KEY], 'glue_pgdb_connection_name')
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')

    pgdb_schema = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db_schema')
    pgdb_secret_name = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_secret_name')
    pgres_database = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db')
    secret = get_secret(pgdb_secret_name, boto3_aws_region)
    secret = json.loads(secret)
    db_hostname = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db_host')
    db_port = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db_port')

    client = boto3.client('logs', region_name=boto3_aws_region)

    # set up log manager object
    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=guid,
                                             process_key=PROCESS_KEY, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-access-athena-to-pgdb')
    log_manager.log(message="Starting the athena-to-pgdb job",
                    args={"environment": args[REGION_KEY], "table": pgdb_insert_table, "job": JOB_KEY})

    # Override postgres database hostname if passed from jumpbox script
    if pgdb_hostname_override.strip().strip() != 'none' :
        log_manager.log(message="Overriding Postgres db hostname",
                    args={"environment": args[REGION_KEY], "new hostname": pgdb_hostname_override, "job": JOB_KEY})
        db_hostname= pgdb_hostname_override.strip()

    # Read config file
    pgdb_insert_sql = feed_config.get('transformation-rules', 'pgdb-insert-sql')
    pgdb_insert_sql = re.sub('{athena_access_db}', athena_access_database, pgdb_insert_sql)

    print(pgdb_insert_sql)
    access_table_df = spark.sql(pgdb_insert_sql)

    # batch completion table  update
    log_manager.log(message="Starting the batch-pgdb job",
                    args={"environment": args[REGION_KEY], "table": batch_update_table, "job": JOB_KEY})
    batch_update_sql = feed_config.get('transformation-rules', 'batch-update-sql')
    batch_update_sql = re.sub('{athena_raw_db}', athena_raw_database, batch_update_sql)
    batch_update_sql = re.sub('{athena_access_db}', athena_access_database, batch_update_sql)

    print(batch_update_sql)


    batch_table_df = spark.sql(batch_update_sql)
    
    # access_table_df.show()
    
    # jdbcUrl = jdbcUrl_base_url+'/'+pgres_database
    
    # dynamicFrame = DynamicFrame.fromDF(access_table_df, glueContext, "none")
    # dynamicbatch = DynamicFrame.fromDF(batch_table_df, glueContext, "none")
    
    # create postgresdb connection string and set property values needed to connect
    jdbc_conn_string = "jdbc:postgresql://" + db_hostname + ":" + db_port + "/" + pgres_database + "?rewriteBatchedStatements=true&reWri    teBatchedInserts=true"
    mode = "append"
    properties = {"user": secret.get('username'), "password": secret.get('password'), "driver": "org.postgresql.Driver",
                  "batc    hsize": batch_size, "IsolationLevel": "READ_COMMITTED", "rewriteBatchedStatements": "true",
                  "numPartitions": num_partitions, "sslmode": "require", "stringtype":"unspecified"}
    
    # result = glueContext.write_dynamic_frame.from_jdbc_conf(dynamicFrame, glue_connection_name, connection_options={"dbtab    le": pgdb_insert_table, "database": pgres_database}, redshift_tmp_dir="", transformation_ctx="")
    result = access_table_df.write.jdbc(url=jdbc_conn_string, table=pgdb_insert_table, mode=mode, properties=properties)
    print('count is : ' + str(result))
    log_manager.log(message='data written to pgdb table ' + pgdb_insert_table, args={'count': str(result), "job": JOB_KEY})
    
    # batch_result = glueContext.write_dynamic_frame.from_jdbc_conf(dynamicbatch, glue_connection_name, connection_options={    "dbtable": batch_update_table, "database": pgres_database}, redshift_tmp_dir="", transformation_ctx="")
    batch_result = batch_table_df.write.jdbc(url=jdbc_conn_string, table=batch_update_table, mode=mode, properties=properties)
    print('count is : ' + str(batch_result))
    
    log_manager.log(message='data written to batch pgdb table ' + batch_update_table,
                    args={'count': str(batch_result), "j    ob": JOB_KEY})
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


def get_secret(secret_name, region_name):
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(e)
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.

        if 'SecretString' in get_secret_value_response:
            secret_value = get_secret_value_response['SecretString']
        else:
            secret_value = base64.b64decode(get_secret_value_response['SecretBinary'])
        return secret_value


if __name__ == '__main__':
    main()
