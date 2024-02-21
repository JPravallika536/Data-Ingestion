import sys
import boto3
import configparser
from io import StringIO
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.context import DynamicFrame
from pyspark.sql.functions import col,lit,create_map,to_json,expr,struct,collect_set,regexp_replace
from pyspark.sql.types import *
from awsglue.job import Job
import re
from datetime import date

CONFIG_BUCKET_NAME_KEY = "config_bucket"
SYS_CONFIG_KEY = "sys_config_file"
FEED_CONFIG_KEY = "feed_config_file"
GUID_KEY = "guid"
REGION_KEY = "region"
BOTO3_AWS_REGION = ""
BATCH_RUN_DATE = 'batch_date'
PROCESS_KEY = 'raw_to_report'
JOB_KEY = "raw-to-report"

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
	
	#sys_config = read_config('pruvpcaws060-isg-usbie-tooling', 'dev/code/config-files/app_sys.conf')
	#feed_config = read_config('pruvpcaws060-isg-usbie-tooling', 'dev/gt_isgie/etl_domain_distribution/config-files/raw-to-access/burst_report_testing.conf')
	print(feed_config)
	
	guid = args[GUID_KEY]
	athena_raw_database = sys_config.get(args[REGION_KEY], 'database')
	athena_access_database = sys_config.get(args[REGION_KEY], 'access_athena_db')
	cloudwatch_log_group = sys_config.get(args[REGION_KEY], 'cloudwatch_log_group')
    pre_raw_bucket = sys_config.get(args[REGION_KEY], 'pre_raw_bucket')
	boto3_aws_region = sys_config.get(args[REGION_KEY], 'boto3_aws_region')
	client = boto3.client('logs', region_name=boto3_aws_region)
	
	tmplist=[]
	#report_bucket = feed_config.get('transformation-rules', 'report_bucket')
	report_location = feed_config.get('transformation-rules', 'report_location')
	report_foldername = feed_config.get('transformation-rules', 'report_foldername')
	num_of_sheets = feed_config.get('transformation-rules', 'number-of-sheets')
	cntrl_file_requirement = feed_config.get('transformation-rules', 'cntrl_file_requirement')
	cntrl_file_name = feed_config.get('transformation-rules', 'cntrl_file_name')
	cntrl_start_date = feed_config.get('transformation-rules', 'cntrl_start_date')
	cntrl_end_date = feed_config.get('transformation-rules', 'cntrl_end_date')
	
	for i in range(int(num_of_sheets)):
		mapping_sqls = "mapping-sql-{}".format(i+1)
		mapping_sql = feed_config.get('transformation-rules', mapping_sqls)
		#print("mapping_sql from file::::"+mapping_sql)
		mapping_file_names = "mapping-sql-{}-filename".format(i+1)
		mapping_file_name = feed_config.get('transformation-rules', mapping_file_names)
		
		mapping_sql = mapping_sql.replace("\n", " ").replace("\t", " ").replace("\r", " ")
		mapping_sql = re.sub('{athena_raw_db}', athena_raw_database, mapping_sql)
		mapping_sql = re.sub('{athena_access_db}', athena_access_database, mapping_sql)
		
		report_df = spark.sql(mapping_sql)
		mapped_output_count = report_df.count()
		athena_access_bucket = pre_raw_bucket
		prefix_name = "/" + report_location + "temp{}".format(i+1)
		prefix_name_w = report_location + "temp{}".format(i+1)
		destination_loc = report_location + report_foldername
		report_df.repartition(1).write.option("quoteAll", "true").format("csv").mode("overwrite").save("s3://" + athena_access_bucket + prefix_name)
		
		rename_csv(athena_access_bucket,prefix_name_w, destination_loc, mapping_file_name)
		if "Yes" == str(cntrl_file_requirement):
			gen_contrl_file_w_csv_names(i+1,mapping_file_name,cntrl_file_name,int(num_of_sheets))
	if "Yes" == str(cntrl_file_requirement):
		start_date = str(spark.sql("select " + str(cntrl_start_date)).collect()[0][0])
		end_date = str(spark.sql("select " + str(cntrl_end_date)).collect()[0][0])
		add_strt_end_date_2_contrl_file(start_date,end_date,cntrl_file_name)
		cp_cntl_2_s3(athena_access_bucket, destination_loc, cntrl_file_name)
	
def cp_cntl_2_s3(athena_access_bucket, destination_loc, contrl_file_name):
	s3 = boto3.resource('s3')
	s3.meta.client.upload_file(Filename = contrl_file_name, Bucket = athena_access_bucket, Key = destination_loc + "/" + contrl_file_name)
		
def add_strt_end_date_2_contrl_file(strt_date,end_date,contrl_file_name):
	f = open(contrl_file_name, "a")
	f.write("strt_date: " +strt_date + "\n")
	f.write("end_date: " +end_date)
	f.close()

def gen_contrl_file_w_csv_names(num,csv_file_name,contrl_file_name,num_of_sheets):
	f = open(contrl_file_name, "a")
	if num == num_of_sheets:
		f.write("file_names: "+ csv_file_name +"\n")
	elif num == 1:
		f.write("file_names: "+ csv_file_name +",")
	elif num < num_of_sheets:
		f.write(csv_file_name +",")
	else:
		f.write(csv_file_name +"\n")
	f.close()
	
def rename_csv(src_bucket, source_dir, destination_loc, dest_filename):
	s3 = boto3.resource('s3')
	i = 0
	bucket = s3.Bucket(src_bucket)
	for obj in bucket.objects.filter(Prefix=source_dir):
		copy_source = {
			'Bucket': src_bucket,
			'Key': obj.key
		}
		print("dest:"+source_dir+"/"+dest_filename)
		dest = destination_loc+"/"+dest_filename
		s3.meta.client.copy(copy_source, src_bucket , dest)
		obj.delete()

def read_config(bucket, file_prefix):
    print("in read_config")
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