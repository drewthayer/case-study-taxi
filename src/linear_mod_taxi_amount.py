import pyspark as ps
from pyspark.sql.functions import *
#from pyspark.ml.classification import NaiveBayes
from pyspark.ml.feature import OneHotEncoder, VectorAssembler, StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import functions as F
from pyspark.sql import types
import pyspark as ps
import boto3

spark = ps.sql.SparkSession.builder.master('local').appName('caseStudy').getOrCreate()

def cast_to_float(df):
    df.registerTempTable('df')
    select_and_cast = '''
        SELECT
           float(year(tpep_pickup_datetime)) as year,
           float(month(tpep_pickup_datetime)) as month,
           float(dayofyear(tpep_pickup_datetime)) as dayofyear,
           float(dayofmonth(tpep_pickup_datetime)) as dayofmonth,
           float(dayofweek(tpep_pickup_datetime)) as dayofweek,
           float(hour(tpep_pickup_datetime)) as hour,
           float(minute(tpep_pickup_datetime)) as minute,
           CAST(tpep_pickup_datetime AS TIMESTAMP),
           CAST(tpep_dropoff_datetime AS TIMESTAMP),
           float(passenger_count),
           trip_distance,
           pickup_longitude,
           pickup_latitude,
           float(RateCodeID),
           dropoff_longitude,
           dropoff_latitude,
           payment_type,
           float(trip_time),
           fare_amount
        FROM
           df
        WHERE
           fare_amount IS NOT NULL
        '''
    return spark.sql(select_and_cast)


def create_trip_time(df):
    # Create datetime features
    timeFmt = "yyyy-MM-dd'T'HH:mm:ss"
    timeDiff = (F.unix_timestamp('tpep_dropoff_datetime', format=timeFmt)- F.unix_timestamp('tpep_pickup_datetime', format=timeFmt))
    return df.withColumn("trip_time", timeDiff)

def load_file():
    #s3 = boto3.client('3')
    bucket = 'nyc-tlc'
    #all_objects = s3.list_objects(Bucket = bucket)
    year_list = ['2015']
    month_list = ['05']
    first = True
    for year in year_list:
        for month in month_list:
            key_yellow =  f'trip data/yellow_tripdata_{year}-{month}.csv'
            file = f's3a://{bucket}/{key_yellow}'
            if first:
                df = spark.read.load(file,
                                      format='com.databricks.spark.csv',
                                      header='true',
                                      inferSchema='true')
                first = False
            else:
                df = df.join(spark.read.load(file,
                                      format='com.databricks.spark.csv',
                                      header='true',
                                      inferSchema='true'))
    return df

def open_file()
    df = spark.read.load('yellow_2015-05.csv',
                      format='com.databricks.spark.csv',
                      header='true',
                      inferSchema='true')

#df = load_file()
df = open_file()
df.printSchema()

df_2015 = cast_to_float(create_trip_time(df))
df_2015.printSchema()


encode_columns = ['month', 'dayofweek', 'RateCodeID', 'payment_type']
non_encoded=['year', 'dayofyear', 'dayofmonth', 'hour', 'minute',
             'passenger_count', 'trip_distance', 'pickup_longitude',
             'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude',
             'trip_time' ]
indexers = [StringIndexer(inputCol=column, outputCol=column+"_index", handleInvalid="skip") for column in encode_columns]
encoders = [OneHotEncoder(inputCol=column+"_index", outputCol= column+"_encoder", dropLast=False) for column in encode_columns]
assembler = VectorAssembler(inputCols=[encoder.getOutputCol() for encoder in encoders]+non_encoded, outputCol='features')
lin_reg_mod = LinearRegression(featuresCol = 'features', labelCol='fare_amount')


train, test = df_2015.randomSplit([.7,.3])
pipeline = Pipeline(stages=indexers+encoders + [assembler, lin_reg_mod])
model = pipeline.fit(train)

train_prediction = model.transform(train)
test_prediction = model.transform(test)
train_prediction.show(3)
test_prediction.show(3)


train_predictionAndLabels = train_prediction.select("prediction", "fare_amount")
test_predictionAndLabels = test_prediction.select("prediction", "fare_amount")
train_predictionAndLabels.show(3)
test_predictionAndLabels.show(3)

evaluator = RegressionEvaluator(labelCol="fare_amount", predictionCol = "prediction", metricName = "rmse")
print("Train RMSE: {}".format(evaluator.evaluate(train_predictionAndLabels)))
print("Test RMSE: {}".format(evaluator.evaluate(test_predictionAndLabels)))
