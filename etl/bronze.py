from pyspark.sql.functions import explode, col
from datetime import datetime, timedelta
from awsglue.context import GlueContext
from pyspark.context import SparkContext

glueContext = GlueContext(SparkContext())
spark = glueContext.spark_session

timestamp_str = (datetime.now() - timedelta(days=0)).strftime('%d%m%Y')
print(f'Extracting data for {timestamp_str}')
bucket = "housekg-etl-bucket"
key = f"ingestions/apartments_{timestamp_str}.json"
path = f"s3://{bucket}/{key}"

df_bronze = spark.read.json(path).select(explode(col("list")).alias("house")).select('house.*').select(
    col('slug'),
    col('longitude'),
    col('latitude'),
    col('prices')[1]['price'].alias('price'),
    col('square'),
    col('kitchen_square'),
    col('district'),
    col('micro_district'),
    col('updated_at'),
    col('year'),
    col('toilet'),
    col('serie'),
    col('rooms'),
    # col('parking'),
    # col('material'),
    # col('heating'),
    # col('gas'),
    # col('furniture'),
    # col('floor'),
    # col('flooring'),
    col('condition'),
    # col('balcony'),
    col('ceiling_height')
)
df_bronze.write.format("delta") \
    .mode("overwrite") \
    .save(f"s3://housekg-etl-bucket/bronze/apartments_{timestamp_str}")