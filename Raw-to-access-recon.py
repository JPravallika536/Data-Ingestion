import sys
import unittest
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.context import SparkConf
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException
from io import StringIO
import configparser
import boto3
import os
import logging_service
import json
import re

CONFIG_BUCKET_NAME_KEY = "config_bucket"
FEED_CONFIG_FILE_KEY = "feed_config_file"
SYS_CONFIG_KEY = "sys_config_file"
GUID_KEY = "guid"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
PROCESS_KEY = "access_to_pgdb"
JOB_KEY = "raw-to-access-recon"
DB_HOSTNAME_OVERRIDE = "pgdb_hostname_override"



def main():
    global BOTO3_AWS_REGION
    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME', GUID_KEY,
                               CONFIG_BUCKET_NAME_KEY,
                               SYS_CONFIG_KEY,
                               FEED_CONFIG_FILE_KEY,
                               REGION_KEY,
                               DB_HOSTNAME_OVERRIDE])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    logger = glueContext.get_logger()

    pgdb_hostname_override = args[DB_HOSTNAME_OVERRIDE]


    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    reconf = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_FILE_KEY])
    guid = args[GUID_KEY]
    athena_raw_database = sys_config.get(args[REGION_KEY], 'database')
    athena_access_database = sys_config.get(args[REGION_KEY], 'access_athena_db')
    recon_database = sys_config.get(args[REGION_KEY], 'reconciliation_database')
    recon_table = sys_config.get(args[REGION_KEY], 'reconciliation_table')

    glue_connection_name = sys_config.get(args[REGION_KEY], 'glue_pgdb_connection_name')
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    client = boto3.client('logs', region_name=boto3_aws_region)
    source = args[FEED_CONFIG_FILE_KEY].split('/')[-1].split('.')[0]
    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=source+"_"+guid,
                                             process_key=PROCESS_KEY, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-raw-to-access-recon')

    pgdb_schema = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db_schema')
    pgres_database = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db')
    pgdb_secret_name = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_secret_name')
    secret = get_secret(pgdb_secret_name, boto3_aws_region)
    secret = json.loads(secret)
    db_hostname = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db_host')
    db_port = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db_port')

    #Override postgres database hostname if passed from jumpbox script
    if pgdb_hostname_override.strip().strip() != 'none' :
        log_manager.log(message="Overriding Postgres db hostname",
                    args={"environment": args[REGION_KEY], "new hostname": pgdb_hostname_override, "job": JOB_KEY})
        db_hostname= pgdb_hostname_override.strip()

    for section_name in reconf.sections():
        log_manager.log(message="Inside section:", args={'Section name': section_name})
        print("Section name: ", section_name)
        audit_fields = ""
        access_zone_query = ""
        pgdb_query = ""
        business_rule_name = section_name
        try:
            pgdb_recon_type = business_rule_name.startswith('athena_to_pgdb')
            job_name = reconf.get(section_name, 'job_name')
            src_sys_abbr = reconf.get(section_name, 'src_sys_abbr')
            if (reconf.has_option(business_rule_name, 'join_fields')):
                join_fields = reconf.get(section_name, 'join_fields')
            if (reconf.has_option(business_rule_name, 'audit_fields')):
                audit_fields = reconf.get(section_name, 'audit_fields')
            if not pgdb_recon_type:
                raw_zone_query = reconf.get(section_name, 'raw_zone_query')
                access_zone_query = reconf.get(section_name, 'access_zone_query')
                raw_zone_query = re.sub('{athena_raw_db}', athena_raw_database, raw_zone_query)
                raw_zone_query = re.sub('{athena_access_db}', athena_access_database, raw_zone_query)
                access_zone_query = re.sub('{athena_access_db}', athena_access_database, access_zone_query)

                print('job_name: ' + job_name)
                print('raw_zone_query: ' + raw_zone_query)
                print('access_zone_query: ' + access_zone_query)
                log_manager.log(message="Reconciliation details:",
                                args={'Job name': job_name, 'raw_zone_query': raw_zone_query,
                                      'access_zone_query': access_zone_query})

                raw_zone_df = spark.sql(raw_zone_query)
                raw_zone_df.registerTempTable("raw_zone_table")

                target_zone_df = spark.sql(access_zone_query)
                target_zone_df.registerTempTable("target_zone_table")

                selectquery = " select '" + job_name + "','" + business_rule_name + "','" + src_sys_abbr + "', " + \
                              audit_fields + \
                             ", CURRENT_TIMESTAMP,'" + guid + "' from raw_zone_table r left join target_zone_table a on (" + \
                              "r." + join_fields + " = a." + join_fields + ")"
                target_select_df = spark.sql(selectquery)
                outputjson = target_select_df.toJSON().collect()
                log_manager.log(message=outputjson, args={'Job name': job_name, 'content_type': 'reconciliation_data'})
                audit_write_sql = "insert into " + recon_database + "." + recon_table + \
                      " select '" + job_name + "','" + business_rule_name + "','" + src_sys_abbr + "', " + \
                      audit_fields + \
                      ", CURRENT_TIMESTAMP,'" + guid + "' from raw_zone_table r left join target_zone_table a on (" + \
                      "r." + join_fields + " = a." + join_fields + ")"
                log_manager.log(message="Executing audit sql for raw to access:", args={'audit_write_sql': audit_write_sql})
                print("Audit write sql:", audit_write_sql)
                spark.sql(audit_write_sql)
            if pgdb_recon_type:
                pgdb_query = reconf.get(section_name, 'pgdb_query')
                pgdb_query = re.sub('{postgres_db}', pgdb_schema, pgdb_query)
                jdbc_conn_string = "jdbc:postgresql://" + db_hostname + ":" + db_port + "/" + pgres_database + "?rewriteBatchedStatements=true&reWriteBatchedInserts=true"
                properties = {"user": secret.get('username'), "password": secret.get('password'),
                              "driver": "org.postgresql.Driver"}
                print('job_name: ' + job_name)
                print('pgdb_query: ' + pgdb_query)
                if (reconf.has_option(business_rule_name, 'access_zone_query')):
                    access_zone_query = reconf.get(section_name, 'access_zone_query')
                    access_zone_query = re.sub('{athena_access_db}', athena_access_database, access_zone_query)
                    print('access_zone_query: ' + access_zone_query)
                    log_manager.log(message="Reconciliation details:",
                                    args={'Job name': job_name, 'access_zone_query': access_zone_query,
                                      'pgdb_query': pgdb_query})
                    access_df = spark.sql(access_zone_query)
                    access_count = str(access_df.head().access_count)
                    print("Access Count:", access_count)
                    result = spark.read.jdbc(url=jdbc_conn_string, table=pgdb_query, properties=properties)
                    pgdb_count = str(result.head().pgdb_count)
                    print('Postgres Count: ' + str(pgdb_count))
                    diff_ct = int(access_count) - int(pgdb_count)
                    log_manager.log(message="Athena to Postgres Reconciliation:",
                                    args={'Job name': job_name, 'Access Count': access_count,
                                          'Postgres Count':pgdb_count, 'Count Difference':diff_ct})
                    audit_write_sql2 = "insert into " + recon_database + "." + recon_table + \
                                       " select '" + job_name + "','" + business_rule_name + "','" + src_sys_abbr + "'," + \
                                       "'access_count'"+",'"+ access_count + "','Postgres_count','"+pgdb_count+"',"+"'',"+"'',"+"'',"+"'',"+"'',"+"'',"+\
                                       "CURRENT_TIMESTAMP,'" + guid + "' "
                    print(" Recon Insert Sql:", audit_write_sql2)
                    spark.sql(audit_write_sql2)
                else:
                    log_manager.log(message="Reconciliation details:",
                                    args={'Job name': job_name, 'pgdb_query': pgdb_query})
                    result_df = spark.read.jdbc(url=jdbc_conn_string, table=pgdb_query, properties=properties)
                    result_df.registerTempTable("pgdb_table")
                    selectquery = " select '" + job_name + "','" + business_rule_name + "','" + src_sys_abbr + "', " + \
                                  audit_fields + \
                                  ", CURRENT_TIMESTAMP,'" + guid + "' from pgdb_table "
                    target_select_df = spark.sql(selectquery)
                    outputjson = target_select_df.toJSON().collect()
                    log_manager.log(message=outputjson,
                                    args={'Job name': job_name, 'content_type': 'reconciliation_data'})
                    audit_write_sql3 = "insert into " + recon_database + "." + recon_table + \
                                       " select '" + job_name + "','" + business_rule_name + "','" + src_sys_abbr + "', " + \
                                       audit_fields + \
                                       ", CURRENT_TIMESTAMP,'" + guid + "' from pgdb_table "
                    log_manager.log(message="Executing audit sql for raw to access:",
                                    args={'audit_write_sql': audit_write_sql3})
                    print("Audit write sql:", audit_write_sql3)
                    spark.sql(audit_write_sql3)

        except AnalysisException as e:
            log_manager.log_error(message="Analysis error while executing audit sql: ", args={'exception': str(e)})
            print("Analysis Error:" + str(e))
        except:
            log_manager.log_error(message="Error while executing Reconciliation job",args={'section name': section_name})
            print("Error while executing Reconciliation job:")

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


# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
