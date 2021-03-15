# Pokemon ETL

### Ingest pokemon data:
Get data from a pokemon JSON file from [HERE](https://raw.githubusercontent.com/ClaviHaze/CDMX009-Data-Lovers/master/src/data/pokemon/pokemon.json).
And upload it to an AWS S3 bucket.

### Upload data to glue:
Using AWS S3 and AWS Glue, get the data from the JSON file from the AWS S3 bucket and do the corresponding transformations and upload it to a table in an AWS Glue Database.

### Write the data to Snowflake
Having the transformed data in the table in AWS Glue database, you will take the data and upload it into a table in Snowflake
