import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from py4j.java_gateway import java_import

## @params: [JOB_NAME, URL, ACCOUNT, WAREHOUSE, DB, SCHEMA, USERNAME, PASSWORD]
SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'URL', 'ACCOUNT', 'WAREHOUSE', 'DB', 'SCHEMA', 'USERNAME', 'PASSWORD'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
java_import(spark._jvm, SNOWFLAKE_SOURCE_NAME)
spark._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.enablePushdownSession(spark._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())
sfOptions = {
"sfURL" : args['URL'],
"sfAccount" : args['ACCOUNT'],
"sfUser" : args['USERNAME'],
"sfPassword" : args['PASSWORD'],
"sfDatabase" : args['DB'],
"sfSchema" : args['SCHEMA'],
"sfWarehouse" : args['WAREHOUSE'],
}

if __name__ == '__main__':
    s3_path = 's3://bucket/prefix/*.parquet'
    poke_df = spark.read.parquet(s3_path)
    poke_df.write.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "[tableName]").mode("overwrite").save()