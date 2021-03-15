from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import FloatType


def create_dataframe():
    s3_path = 's3://bucket/prefix/*.json'
    poke_df = spark.read.json(s3_path)
    read_df = poke_df.withColumn('all_pokemon', f.explode('pokemon')).select('all_pokemon.*')
    clean_df = read_df \
        .withColumn('multipliers', f.array_join('multipliers', ',')) \
        .withColumn('type', f.array_join('type', ' | ')) \
        .withColumn('weaknesses', f.array_join('weaknesses', ' | ')) \
        .withColumn('next_evolution', f.array_join('next_evolution.name', ' ,')) \
        .withColumn('prev_evolution', f.array_join('prev_evolution.name', ' ,'))
    return clean_df


def bmi_calculation():
    bmi_df = create_dataframe() \
        .withColumn('bmi',
                    f.round((f.substring_index('weight', ' ', 1) / (f.substring_index('height', ' ', 1) ** 2)), 2)
                    .cast(FloatType()))
    return bmi_df


def obesity():
    obesity_df = bmi_calculation().withColumn('obesity', f.when(f.col('bmi') < 18.5, 'Underweight')
                                              .when((f.col('bmi') >= 18.5) & (f.col('bmi') <= 24.9), 'Normal weight')
                                              .when((f.col('bmi') >= 25) & (f.col('bmi') <= 29.9), 'Overweight')
                                              .when(f.col('bmi') >= 30, 'Obesity'))
    return obesity_df


def spawn_rate():
    spawn_df = obesity().filter((f.col('avg_spawns') >= 1) & (f.col('avg_spawns') <= 200))
    return spawn_df


def eggs_filter():
    eggs_df = spawn_rate().filter(f.col('egg') != 'Not in Eggs')
    return eggs_df


def name_update():
    name_df = eggs_filter().withColumn('name', f.concat(f.col('name'), f.lit(' Vidal')))
    return name_df


def drop_columns():
    columns_df = name_update().drop('img', 'num', 'candy_count')
    return columns_df


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Pokemon") \
        .config("hive.metastore.connect.retries", 5) \
        .config("hive.metastore.client.factory.class",
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport() \
        .getOrCreate()

    df = drop_columns()

    table_exist = spark.sql('show tables in database').where(f.col('tableName') == 'table_name').count() == 1

    if table_exist:
        df.write.insertInto('database.table_name', overwrite=True)
    else:
        df.write.saveAsTable('database.table_name', path='s3://bucket/schema-table/')
