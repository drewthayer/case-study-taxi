import boto3
import pyspark as ps

%matplotlib inline

spark = ps.sql.SparkSession.builder.master("local")\
                                    .appName("casestudy-taxi")\
                                    .getOrCreate()

# connect to s3 bucket with bucket
bucket = 'nyc-tlc'

import boto3
s3 = boto3.client('s3')
all_objects = s3.list_objects(Bucket = bucket)

# key for object
key_yellow = 'trip data/yellow_tripdata_2015-07.csv'

''' option: Download the file from S3 '''
try:
    s3.download_file(bucket, key_yellow, 'yellow_test.csv')
except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '484':
            print('the object does not exist')
        else:
            raise
