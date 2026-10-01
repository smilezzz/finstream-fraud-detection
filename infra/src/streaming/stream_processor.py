import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# 1. Fetch environment variables injected by Docker
KAFKA_BROKER = os.getenv("KAFKA_BROKER_INTERNAL")
TOPIC_NAME = os.getenv("TRANSACTIONS_TOPIC")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")

def main():
    # 2. Initialize SparkSession
    # We don't set master here; it's passed via spark-submit
    spark = SparkSession.builder \
        .appName("FinStream-Kafka-to-S3") \
        .getOrCreate()

    # 3. Configure Hadoop S3A connector for AWS access
    spark.sparkContext.setLogLevel("WARN")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # Fetch credentials injected at runtime
    aws_access = os.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    aws_session = os.getenv("AWS_SESSION_TOKEN", "")

    # Switch provider based on whether a session token exists
    if aws_session:
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        hadoop_conf.set("fs.s3a.access.key", aws_access)
        hadoop_conf.set("fs.s3a.secret.key", aws_secret)
        hadoop_conf.set("fs.s3a.session.token", aws_session)
    else:
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        hadoop_conf.set("fs.s3a.access.key", aws_access)
        hadoop_conf.set("fs.s3a.secret.key", aws_secret)

    # 4. Define the schema matching our Python generator
    # Note: We read timestamp as a String first to handle ISO 8601 formatting safely
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("timestamp", StringType(), True), 
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("is_flagged", BooleanType(), True)
    ])

    # 5. Read stream from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER or "kafka:9092") \
        .option("subscribe", TOPIC_NAME or "transactions") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # 6. Parse JSON and enrich
    # Kafka 'value' is binary; cast to string, parse JSON, and extract fields
    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Cast timestamp correctly and add a date column for S3 partitioning
    enriched_df = parsed_df \
        .withColumn("timestamp", to_timestamp(col("timestamp"))) \
        .withColumn("date", to_date(col("timestamp")))

    # 7. Write stream to S3
    s3_path = f"s3a://{S3_BUCKET}/raw_transactions/"
    checkpoint_path = f"s3a://{S3_BUCKET}/checkpoints/raw_transactions/"

    print(f"Starting stream... Writing to {s3_path}")

    # We use a 1-minute trigger to avoid creating thousands of tiny files in S3
    query = enriched_df.writeStream \
        .format("json") \
        .partitionBy("date") \
        .option("path", s3_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="1 minute") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()