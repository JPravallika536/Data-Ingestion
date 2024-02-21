import sys
import boto3
import sys
import configparser
from io import StringIO
import os
import logging_service
import uuid
import pg
import pgdb
from pg import DB
from awsglue.utils import getResolvedOptions
import re
import json
import time

CONFIG_BUCKET_NAME_KEY = "config_bucket"
FEED_CONFIG_FILE_KEY = "feed_config_file"
SYS_CONFIG_KEY = "sys_config_file"
GUID_KEY = "guid"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
PROCESS_KEY = "access_to_pgdb"
JOB_KEY = "access_pgdb_preaction"
DB_HOSTNAME_OVERRIDE = "pgdb_hostname_override"

args = getResolvedOptions(sys.argv,
                          [CONFIG_BUCKET_NAME_KEY, FEED_CONFIG_FILE_KEY, SYS_CONFIG_KEY, REGION_KEY, GUID_KEY, DB_HOSTNAME_OVERRIDE])


def main():
    global BOTO3_AWS_REGION

    print('Config file prefix : ', args[FEED_CONFIG_FILE_KEY])
    pgdb_hostname_override = args[DB_HOSTNAME_OVERRIDE]
    guid = args[GUID_KEY]
    feed_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[FEED_CONFIG_FILE_KEY])
    sys_config = read_config(args[CONFIG_BUCKET_NAME_KEY], args[SYS_CONFIG_KEY])
    cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
    client = boto3.client('logs', region_name=boto3_aws_region)
    client_s3 = boto3.client('s3')

    # log that the preaction job is starting
    log_manager = logging_service.LogManager(cw_loggroup=cloudwatch_log_group, cw_logstream=guid,
                                             process_key=PROCESS_KEY, client=client,
                                             job=args[REGION_KEY] + '-isg-ie-access-pgdb-preaction')
    log_manager.log(message="Starting the preaction job", args={"environment": args[REGION_KEY], "job": JOB_KEY})

    athena_access_bucket = sys_config.get(args[REGION_KEY], 'pre_raw_bucket')
    athena_raw_bucket = sys_config.get(args[REGION_KEY], 'raw_bucket')
    pgdb_schema = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db_schema')
    pgdb_secret_name = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_secret_name')
    pgres_database = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db')
    access_athena_db = sys_config.get(args[REGION_KEY], 'access_athena_db')
    work_group = sys_config.get(args[REGION_KEY], 'work_group')
    pgdb_delete_sql = feed_config.get('transformation-rules', 'pgdb-delete-sql')
    batch_delete_sql = feed_config.get('transformation-rules', 'batch-delete-sql')

    pgdb_delete_sql = re.sub('{pgdb_schema}', pgdb_schema, pgdb_delete_sql)
    batch_delete_sql = re.sub('{pgdb_schema}', pgdb_schema, batch_delete_sql)

    secret = get_secret(pgdb_secret_name, boto3_aws_region)
    secret = json.loads(secret)

    db_hostname = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db_host')
    db_port = sys_config.get(args[REGION_KEY], 'access_ierpt_pgres_db_port')

    # Override postgres database hostname if passed from jumpbox script
    if pgdb_hostname_override.strip().strip() != 'none':
        log_manager.log(message="Overriding Postgres db hostname",
                        args={"environment": args[REGION_KEY], "new hostname": pgdb_hostname_override, "job": JOB_KEY})
        db_hostname = pgdb_hostname_override.strip()

    db = DB(dbname=pgres_database, host=db_hostname, port=int(db_port), user=secret.get('username'),
            passwd=secret.get('password'))
    # print(db.query(pgdb_delete_sql))

    # run_data_length_validation(feed_config, db, log_manager, athena_raw_bucket, boto3_aws_region, access_athena_db, work_group)

    if (feed_config.has_option('transformation-rules', 'dup-prmkey-sql')):
        log_manager.log(message='Checking for duplicates in access table', args={"job": JOB_KEY})
        dup_prm_sql = feed_config.get('transformation-rules', 'dup-prmkey-sql')
        dup_prm_sql = re.sub('{athena_access_db}', access_athena_db, dup_prm_sql)
        raw_bucket_path = 's3://' + athena_raw_bucket + '/tmp/athena-query-results/duplicate_check/'
        duplicate_flag = get_query_results(boto3_aws_region, dup_prm_sql, access_athena_db, raw_bucket_path, work_group)
        if (duplicate_flag == True):
            print("Found Duplicates in Access table")
            log_manager.log(message='Duplicates found in access table', args={"job": JOB_KEY})
            raise Exception("Duplicates Found in Access table:")
        else:
            print("No duplicates")
            log_manager.log(message='No duplicates in access table', args={"job": JOB_KEY})

    if (feed_config.has_option('transformation-rules', 'column-name')):
        log_manager.log(message='Executing pgdb selective delete job', args={"job": JOB_KEY})
        copy_key = feed_config.get('transformation-rules', 'copy-key')
        table_name = feed_config.get('transformation-rules', 'table-name')
        column_name = feed_config.get('transformation-rules', 'column-name')
        filename = client_s3.get_object(Bucket=athena_access_bucket, Key=copy_key)
        linesf = filename['Body'].read().decode('utf-8').splitlines(True)
        print(linesf)
        lines = list(map(str.strip, linesf))
        print(lines)

        length = len(lines)
        print(length)
        if length > 0:
            i = 1
            while i < length:
                value = lines[i]
                pgdb_delete_sql1 = 'delete from ' + pgdb_schema + '.' + table_name + ' where ' + column_name + ' = \'' + value + '\''
                print(pgdb_delete_sql1)
                i += 1
                #log_manager.log(message='starting pgdb delete query', args={'batch_delete_sql': pgdb_delete_sql1, "job": JOB_KEY})
                query = db.query(pgdb_delete_sql1)
                print(query)
        else:
            log_manager.log(message='starting pgdb delete query', args={'delete_sql': pgdb_delete_sql, "job": JOB_KEY})
            query = db.query(pgdb_delete_sql)
            print(query)

    elif (feed_config.has_option('transformation-rules', 'column-name2')):
        log_manager.log(message='Executing pgdb selective delete job', args={"job": JOB_KEY})
        copy_key = feed_config.get('transformation-rules', 'copy-key')
        table_name = feed_config.get('transformation-rules', 'table-name')
        column_name1 = feed_config.get('transformation-rules', 'column-name1')
        column_name2 = feed_config.get('transformation-rules', 'column-name2')
        filename = client_s3.get_object(Bucket=athena_access_bucket, Key=copy_key)
        linesf = filename['Body'].read().decode('utf-8').splitlines(True)
        print(linesf)
        lines = list(map(str.strip, linesf))
        print(lines)

        length = len(lines)
        print(length)
        if length > 0:
            i = 1
            while i < length:
                mainvalue = lines[i].split(',')
                value1 = mainvalue[0]
                value2 = mainvalue[1]
                pgdb_delete_sql1 = 'delete from ' + pgdb_schema + '.' + table_name + ' where ' + column_name1 + ' = \'' + value1 + '\' and ' + column_name2 + ' = \'' + value2 + '\''
                print(pgdb_delete_sql1)
                i += 1
                #log_manager.log(message='starting pgdb delete query', args={'batch_delete_sql': pgdb_delete_sql1, "job": JOB_KEY})
                query = db.query(pgdb_delete_sql1)
                print(query)
        else:
            log_manager.log(message='starting pgdb delete query', args={'delete_sql': pgdb_delete_sql, "job": JOB_KEY})
            query = db.query(pgdb_delete_sql)
            print(query)
    else:
        log_manager.log(message='starting pgdb delete query', args={'delete_sql': pgdb_delete_sql, "job": JOB_KEY})
        query = db.query(pgdb_delete_sql)
        print(query)
    log_manager.log(message='After pgdb delete query', args={'delete_sql': pgdb_delete_sql, "job": JOB_KEY})

    log_manager.log(message='starting pgdb batch delete query',
                    args={'batch_delete_sql': batch_delete_sql, "job": JOB_KEY})
    batchquery = db.query(batch_delete_sql)
    print(batchquery)


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


def get_query_results(boto3_aws_region, dup_prm_sql, access_athena_db, raw_bucket_path, work_group):
    client = boto3.client('athena', boto3_aws_region)
    response = client.start_query_execution(
        QueryString=dup_prm_sql,
        QueryExecutionContext={
            'Database': access_athena_db
        },
        ResultConfiguration={
            'OutputLocation': raw_bucket_path
        },
        WorkGroup=work_group
    )
    QueryExecutionId = response['QueryExecutionId']
    if not response:
        return
    while True:
        stats = client.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
        status = stats['QueryExecution']['Status']['State']
        print(status)
        if status in ('FAILED', 'CANCELLED'):
            print("Query execution failed for the query:",dup_prm_sql)
            return False
        elif status == 'SUCCEEDED':
            location = stats['QueryExecution']['ResultConfiguration']['OutputLocation']
            client = boto3.client('s3', boto3_aws_region)
            bucket_name = location.split('//')[-1].split('/')[0].strip()
            copy_key = location.split(bucket_name + '/')[1].strip()
            filename = client.get_object(Bucket=bucket_name, Key=copy_key)
            result_data = filename['Body'].read().decode('utf-8').splitlines(True)
            lines = list(map(str.strip, result_data))
            length = len(lines)
            if length > 1:
                return True
            else:
                return False
        else:
            time.sleep(1)


def run_athena_query(boto3_aws_region, dup_prm_sql, access_athena_db, raw_bucket_path, work_group):
    client = boto3.client('athena', boto3_aws_region)
    response = client.start_query_execution(
        QueryString=dup_prm_sql,
        QueryExecutionContext={
            'Database': access_athena_db
        },
        ResultConfiguration={
            'OutputLocation': raw_bucket_path
        },
        WorkGroup=work_group
    )
    QueryExecutionId = response['QueryExecutionId']
    if not response:
        return
    while True:
        stats = client.get_query_execution(QueryExecutionId=response['QueryExecutionId'])
        status = stats['QueryExecution']['Status']['State']
        print(status)
        if status in ('FAILED', 'CANCELLED'):
            print("Query execution failed for the query:",dup_prm_sql)
            return False
        elif status == 'SUCCEEDED':
            location = stats['QueryExecution']['ResultConfiguration']['OutputLocation']
            client = boto3.client('s3', boto3_aws_region)
            bucket_name = location.split('//')[-1].split('/')[0].strip()
            copy_key = location.split(bucket_name + '/')[1].strip()
            filename = client.get_object(Bucket=bucket_name, Key=copy_key)
            result_data = filename['Body'].read().decode('utf-8').splitlines(True)
            lines = list(map(str.strip, result_data))
            length = len(lines)
            return lines
            # if length > 1:
            #     return True
            # else:
            #     return False
        else:
            time.sleep(1)


def run_data_length_validation(feed_config, db, log_manager, athena_raw_bucket, boto3_aws_region, access_athena_db, work_group):
    
    log_manager.log(message="beginning data length validation", args={"environment": args[REGION_KEY], "job": JOB_KEY})
    
    access_table = feed_config.get('transformation-rules', 'pgdb-insert-table').split('.')[1]
    pgtable_column_result = db.query("SELECT table_name, column_name, data_type, character_maximum_length, numeric_precision FROM information_schema.columns where table_name = '"+access_table+"'").getresult()
    for rec in pgtable_column_result:
        print(rec)
    # print(pgtable_column_result)
    # print(type(result))

    max_len_sql = 'SELECT '
    max_len_dict = {}
    datatype_dict = {}
    for item in pgtable_column_result:
        # select max(length(aggregateplan)) max(length(aggregateplan)), 
        # max(agtreplaceind) agtreplaceind, max(annuitzdate)  annuitzdate from vpas_contract_raw
        column = item[1]
        if (item[3] is not None):
            max_len_sql += 'MAX(LENGTH('+ column + ')) ' + column + ','
            max_len_dict[column] = item[3]
        elif (item[4] is not None):
            max_len_sql += 'MAX('+ column + ') ' + column + ','
            max_len_dict[column] = item[4]
        datatype_dict[column] = item[2]
    max_len_sql = max_len_sql[:-1] + ' FROM ' + access_table
    print(max_len_sql)
    print(max_len_dict)
    print(datatype_dict)

    data_length_validation_bucket = 's3://' + athena_raw_bucket + '/tmp/athena-query-results/data-length-validation/'
    # print(access_athena_db)
    athena_max_len_results = run_athena_query(boto3_aws_region, max_len_sql, access_athena_db, data_length_validation_bucket, work_group)
    header = athena_max_len_results[0].split(',')
    values = athena_max_len_results[1].split(',')
    print(header, len(header))
    print(values, len(values))

    longest_length_dict = {}
    for i in range(len(header)):
        val = values[i].strip('"')
        if (datatype_dict[header[i].strip('"')] == 'numeric' or val == ''):
            if ('.' in val):
                val = len(val)-1
            else:
                val = len(val)
        longest_length_dict[header[i].strip('"')] = val

    print(max_len_dict)
    print(longest_length_dict)

    result_list = []
    for key in max_len_dict:
        column_name = key
        max_len = max_len_dict[column_name]
        longest_len = int(longest_length_dict[column_name])
        valid = (max_len >= longest_len)
        if (not valid):
            result_list.append((column_name, max_len, longest_len, valid))

    for tuple_record in result_list:
        print(tuple_record)
    if (len(result_list) > 0):
        log_manager.log(message="Data length validation failed with " + str(len(result_list)) + " columns with longer max length record than expected", args={"environment": args[REGION_KEY], "job": JOB_KEY, "result_list": result_list})
    else:
        log_manager.log(message="Data lengths validated successfully", args={"environment": args[REGION_KEY], "job": JOB_KEY})

# entry point for PySpark ETL application
if __name__ == '__main__':
        main()