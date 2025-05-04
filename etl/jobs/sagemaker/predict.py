# from awsglue.context import GlueContext
# from awsglue.utils import getResolvedOptions
# from pyspark.context import SparkContext
# import boto3
# import sys
# from datetime import datetime
#
# args = getResolvedOptions(sys.argv, ['JOB_NAME'])
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
# sagemaker = boto3.client("sagemaker")
#
# timestamp_str = datetime.now().strftime("%d.%m.%Y")
# # Configuration
# bucket = args['BUCKET']
# model_name_prefix = args['MODEL_NAME_PREFIX']
# model_name = f"{model_name_prefix}-{timestamp_str}"
# input_s3_path = "s3://housekg-etl-bucket/input_data/"
# output_s3_path = "s3://housekg-etl-bucket/predictions/"
# transform_job_name = f"housekg-transform"
#
# # Read from Glue Catalog
# datasource = glueContext.create_dynamic_frame.from_catalog(
#     database="housekg_database",
#     table_name="ingestions"
# )
#
# # Write input data to S3 for SageMaker
# datasource.toDF().write.format("csv").option("header", "true").mode("overwrite").save(input_s3_path)
#
# # Create SageMaker batch transform job
# sagemaker.create_transform_job(
#     TransformJobName=transform_job_name,
#     ModelName=model_name,
#     TransformInput={
#         "DataSource": {
#             "S3DataSource": {
#                 "S3DataType": "S3Prefix",
#                 "S3Uri": input_s3_path
#             }
#         },
#         "ContentType": "text/csv"
#     },
#     TransformOutput={
#         "S3OutputPath": output_s3_path
#     },
#     TransformResources={
#         "InstanceType": "ml.m5.large",
#         "InstanceCount": 1
#     }
# )
#
# # Wait for transform job to complete
# while sagemaker.describe_transform_job(TransformJobName=transform_job_name)["TransformJobStatus"] != "Completed":
#     time.sleep(30)
#
# # Read predictions from S3
# predictions_df = spark.read.csv(f"{output_s3_path}*.csv.out")
#
# # Write to fact table in Glue Catalog
# predictions_df.write.mode("overwrite").saveAsTable(
#     database="housekg_database",
#     table_name="predictions_fact",
#     format="parquet"
# )