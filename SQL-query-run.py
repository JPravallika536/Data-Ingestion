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
from pyspark.sql import HiveContext

GUID_KEY = "guid"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
JOB_KEY = "raw-to-access"
RESULT_OUTPUT_LOCATION_KEY = "result_ouput_location"
QUERY_KEY = "query"
from pyspark.sql.utils import AnalysisException


def main():
    global BOTO3_AWS_REGION

    args = getResolvedOptions(sys.argv,
                              ['JOB_NAME', GUID_KEY,
                               RESULT_OUTPUT_LOCATION_KEY,
                               QUERY_KEY,
                               REGION_KEY])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
	
    sqlContext = HiveContext(sc)
    sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sqlContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

    result_output_location = args[RESULT_OUTPUT_LOCATION_KEY]
    query = args[QUERY_KEY]

    guid = args[GUID_KEY]

    print('Query is  :' + query)

    try:
        output_df = spark.sql(query)
			
        output_df.write.format("csv").save(result_output_location + "/" + guid)
    except AnalysisException as e:
        if 'Can not create a Path from an empty string' in str(e):
            print('Caught analysis exception to move full snapshot from raw table to new partition in history...' + str(e))
        elif 'Datasource does not support writing empty or nested empty schemas' in str(e):
            print('Caught analysis exception while doing only table inserts...' + str(e))
        else:
            print('Caught analysis exception to move full snapshot from raw table to new partition in history...' + str(e))
            sys.exit(str(e))
    print('Save location : ' + result_output_location + "/" + guid)

    job.commit()

# entry point for PySpark ETL application
if __name__ == '__main__':
    main()
