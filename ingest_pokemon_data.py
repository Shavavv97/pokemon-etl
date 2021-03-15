import requests
from pyspark.sql import SparkSession


def get_data():
    url = "https://raw.githubusercontent.com/ClaviHaze/CDMX009-Data-Lovers/master/src/data/pokemon/pokemon.json"
    request = requests.get(url)
    data = request.text
    return data


def create_dataframe():
    sc = spark.sparkContext
    df = sc.parallelize([get_data()])
    poke_df = spark.read.json(df)
    return poke_df


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('Pokemon') \
        .getOrCreate()
    new_df = create_dataframe().repartition(1)
    new_df.write.format('json').mode('overwrite').save('data')

