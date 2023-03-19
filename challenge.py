# import libraries
from delta.tables import *
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.types import DateType, StringType, TimestampType, DecimalType, LongType, IntegerType
from pyspark.sql.functions import current_timestamp, col, lit, to_json, unix_timestamp, from_utc_timestamp, format_number, to_date,to_timestamp
from dotenv import load_dotenv
import os
load_dotenv()


# function add metadata bronze
def add_metadata_bronze(df):
    return df.withColumn(
        "bronze_ingestion_time", from_utc_timestamp(current_timestamp(), "UTC")
    ).withColumn(
        "bronze_source_system", lit("demo_users")
    ).withColumn(
        "bronze_schema", lit(df.schema.json())
    ).withColumn(
        "bronze_user_name", lit("Jaime")
    ).withColumn(
        "bronze_rows_written", lit(df.count())
    ).withColumn(
        "bronze_base_format", lit("json")
    )

# function add metadata silver
def add_metadata_silver(df):  
    # Add silver metadata
    silver_ingestion_time_utc = from_utc_timestamp(current_timestamp(), "UTC")
    df = df.withColumn("silver_ingestion_time", silver_ingestion_time_utc)
    return df

# function add metadata silver
def add_metadata_gold(df):  
    # Add silver metadata
    gold_ingestion_time_utc = from_utc_timestamp(current_timestamp(), "UTC")
    df = df.withColumn("gold_ingestion_time", gold_ingestion_time_utc)
    return df

# main spark program
# init application
if __name__ == '__main__':

    # init session
    # set configs
    spark = SparkSession \
            .builder \
            .appName("challenge-py") \
            .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("ENDPOINT")) \
            .config("spark.hadoop.fs.s3a.access.key", os.environ.get("ACCESS_KEY")) \
            .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("SECRET_KEY")) \
            .config("spark.hadoop.fs.s3a.path.style.access", True) \
            .config("spark.hadoop.fs.s3a.fast.upload", True) \
            .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
            .config("fs.s3a.connection.maximum", 100) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .getOrCreate()
    

    # show configured parameters
    print(SparkConf().getAll())

    # set log level
    spark.sparkContext.setLogLevel("INFO")


    # [landing zone area]
    # device and subscription
    get_device_file = "s3a://landing/device/*.json"
    get_subscription_file = "s3a://landing/subscription/*.json"


    # read device data
    # json file from landing zone
    df_device = spark.read \
            .format("json") \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .json(get_device_file)

    # read subscription data
    # json file from landing zone
    df_subscription = spark.read \
            .format("json") \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .json(get_subscription_file)
    

    # get number of partitions
    print(df_device.rdd.getNumPartitions())
    # get number of partitions
    print(df_subscription.rdd.getNumPartitions())



    df_device.printSchema()
    df_subscription.printSchema()

    # add metadata bronze in df
    df_device = add_metadata_bronze(df_device)
    df_subscription = add_metadata_bronze(df_subscription)

    ## writing into [bronze zone] 

    # [bronze zone area]
    write_delta_mode = "append"
    delta_bronze_zone = "s3a://lakehouse/bronze"
    df_device.write.mode(write_delta_mode).format("delta").save(delta_bronze_zone + "/devices/")


    # [bronze zone area]
    write_delta_mode = "append"
    delta_bronze_zone = "s3a://lakehouse/bronze"
    df_subscription.write.mode(write_delta_mode).format("delta").save(delta_bronze_zone + "/subscriptions/")


    ## [Read from delta bronze]

    #devices
    delta_lake_location_devices = "s3a://lakehouse/bronze/devices"
    df_device = DeltaTable.forPath(spark, delta_lake_location_devices)
    #df_devices.toDF().select("bronze_ingestion_time").show(truncate=False)

    #subscriptions
    delta_lake_location_subscriptions = "s3a://lakehouse/bronze/subscriptions"
    df_subscription = DeltaTable.forPath(spark, delta_lake_location_subscriptions)
    #df_subscription.toDF().select("bronze_ingestion_time").show(truncate=False)


    ## [Silver transformations]

    # [silver zone area]
    # [devices]
    df_device_transformation = df_device.toDF().select(
        col("build_number").alias("build_number"),
        col("dt_current_timestamp").alias("dt_current_timestamp"),
        col("id").alias("id"),
        col("manufacturer").alias("manufacturer"),
        col("model").alias("model"),
        col("platform").alias("platform"),
        col("serial_number").alias("serial_number"),
        col("uid").alias("uid"),
        col("user_id").alias("user_id"),
        col("version").alias("version"),
        col("bronze_ingestion_time").alias("bronze_ingestion_time"),
        col("bronze_source_system").alias("bronze_source_system"),
        col("bronze_schema").alias("bronze_schema"),
        col("bronze_user_name").alias("bronze_user_name"),
        col("bronze_rows_written").alias("bronze_rows_written"),
        col("bronze_base_format").alias("bronze_base_format")
    )

    # [silver zone area]
    # [subscriptions]

    df_subscription_transformation = df_subscription.toDF().select(
        col("dt_current_timestamp").alias("dt_current_timestamp"),
        col("id").alias("id"),
        col("payment_method").alias("payment_method"),
        col("payment_term").alias("payment_term"),
        col("plan").alias("plan"),
        col("status").alias("status"),
        col("subscription_term").alias("subscription_term"),
        col("uid").alias("uid"),
        col("user_id").alias("user_id"),
        col("bronze_ingestion_time").alias("bronze_ingestion_time"),
        col("bronze_source_system").alias("bronze_source_system"),
        col("bronze_schema").alias("bronze_schema"),
        col("bronze_user_name").alias("bronze_user_name"),
        col("bronze_rows_written").alias("bronze_rows_written"),
        col("bronze_base_format").alias("bronze_base_format")
    )

    # add metadata silver in df
    df_device_transformation = add_metadata_silver(df_device_transformation)
    df_subscription_transformation = add_metadata_silver(df_subscription_transformation)


    ## writing into [silver zone] 
    # devices

    write_delta_mode = "append"
    delta_silver_zone = "s3a://lakehouse/silver"
    df_device_transformation.write.format("delta").mode(write_delta_mode).save(delta_silver_zone + "/devices/")

    # read delta table
    delta_lake_location_devices = "s3a://lakehouse/silver/devices"
    df_devices = DeltaTable.forPath(spark, delta_lake_location_devices)
    df_devices.toDF().show()

    # subscriptions

    write_delta_mode = "append"
    delta_silver_zone = "s3a://lakehouse/silver"
    df_subscription_transformation.write.format("delta").mode(write_delta_mode).save(delta_silver_zone + "/subscriptions/")

    # read delta table
    delta_lake_location_subscriptions = "s3a://lakehouse/silver/subscriptions"
    df_subscriptions = DeltaTable.forPath(spark, delta_lake_location_subscriptions)
    df_subscriptions.toDF().show()


    ## Create view from delta Silver

    df_subscriptions.toDF().createOrReplaceTempView("vw_subscription")
    df_devices.toDF().createOrReplaceTempView("vw_devices")

    #check data
    spark.sql("select to_date(FROM_UNIXTIME(dt_current_timestamp/1000)) AS date_time from  vw_subscription GROUP BY to_date(FROM_UNIXTIME(dt_current_timestamp/1000))").show()
    spark.sql("select to_date(FROM_UNIXTIME(dt_current_timestamp/1000)) AS date_time from  vw_devices GROUP BY to_date(FROM_UNIXTIME(dt_current_timestamp/1000))").show()

    ## [Gold aggregations]

    # Gold ds team

    df_gold_ds = spark.sql("""
                SELECT devices.user_id, devices.manufacturer ,devices.model ,devices.platform, devices.version, devices.build_number, \
                devices.serial_number, \
                subscription.payment_method,subscription.payment_term, subscription.plan, subscription.status
                FROM vw_devices devices \
                INNER JOIN vw_subscription subscription \
                ON devices.user_id = subscription.user_id  
    """)

    # Gold external partnership team by payment_method
    df_gold_partnerships = spark.sql("""
            SELECT count(1) AS purchases_per_user_id ,sub.payment_method
            FROM vw_subscription sub \
            GROUP BY sub.payment_method
            ORDER BY COUNT(1) DESC
    """)

    # Gold users count devices by same user_id
    df_gold_devices_count_by_users = spark.sql(""" SELECT user_id, device_count, RANK() OVER (ORDER BY device_count DESC) AS rank
    FROM (
    SELECT user_id, COUNT(*) AS device_count
    FROM vw_devices
    GROUP BY user_id
    ) subquery""")

    # Gold devices 
    df_gold_devices = spark.sql("""
            SELECT user_id, manufacturer, model, platform, version, build_number, \
            serial_number, to_date(FROM_UNIXTIME(dt_current_timestamp/1000)) AS dt_current, \
            year(to_date(FROM_UNIXTIME(dt_current_timestamp/1000))) AS year, \
            month(to_date(FROM_UNIXTIME(dt_current_timestamp/1000))) AS month, \
            day(to_date(FROM_UNIXTIME(dt_current_timestamp/1000))) AS day, \
            bronze_ingestion_time, silver_ingestion_time
            FROM vw_devices """)

    # add metadata gold in df
    df_gold_ds = add_metadata_gold(df_gold_ds)
    df_gold_partnerships = add_metadata_gold(df_gold_partnerships)
    df_gold_devices_count_by_users = add_metadata_gold(df_gold_devices_count_by_users)
    df_gold_devices = add_metadata_gold(df_gold_devices)


    # building [gold zone area]
    # create table in metastore
    # deltatablebuilder api

    # Minio OK
    delta_gold_tb_obt_ds_location = "s3a://lakehouse/gold/challenge/obt"
    DeltaTable.createIfNotExists(spark) \
            .tableName("obt") \
            .addColumn("user_id", LongType()) \
            .addColumn("manufacturer", StringType()) \
            .addColumn("model", StringType()) \
            .addColumn("platform", StringType()) \
            .addColumn("version",LongType()) \
            .addColumn("build_number", LongType()) \
            .addColumn("serial_number", StringType()) \
            .addColumn("payment_method", StringType()) \
            .addColumn("payment_term", StringType()) \
            .addColumn("plan", StringType()) \
            .addColumn("status", StringType()) \
            .addColumn("gold_ingestion_time", TimestampType()) \
            .addColumn("date", DateType(), generatedAlwaysAs="CAST(gold_ingestion_time AS DATE)") \
            .partitionedBy("date") \
            .location(delta_gold_tb_obt_ds_location) \
            .execute()
    

    # building [gold zone area]
    # create table in metastore
    # deltatablebuilder api

    # Minio OK
    delta_gold_tb_partnerships_location = "s3a://lakehouse/gold/challenge/partnerships"
    DeltaTable.createIfNotExists(spark) \
            .tableName("partnerships") \
            .addColumn("purchases_per_user_id", LongType()) \
            .addColumn("payment_method", StringType()) \
            .addColumn("gold_ingestion_time", TimestampType()) \
            .addColumn("date", DateType(), generatedAlwaysAs="CAST(gold_ingestion_time AS DATE)") \
            .partitionedBy("date") \
            .location(delta_gold_tb_partnerships_location) \
            .execute()
    

    # building [gold zone area]
    # create table in metastore
    # deltatablebuilder api

    # Minio OK
    delta_gold_tb_devices_count_by_users_location = "s3a://lakehouse/gold/challenge/devices_count_by_users"
    DeltaTable.createIfNotExists(spark) \
            .tableName("devices_count_by_users") \
            .addColumn("user_id", LongType()) \
            .addColumn("device_count", LongType()) \
            .addColumn("rank", IntegerType()) \
            .addColumn("gold_ingestion_time", TimestampType()) \
            .addColumn("date", DateType(), generatedAlwaysAs="CAST(gold_ingestion_time AS DATE)") \
            .partitionedBy("date") \
            .location(delta_gold_tb_devices_count_by_users_location) \
            .execute()
    
    # building [gold zone area]
    # create table in metastore
    # deltatablebuilder api

    # windows not working create delta table
    # Minio OK
    delta_gold_tb_devices_location = "s3a://lakehouse/gold/challenge/devices"
    DeltaTable.createIfNotExists(spark) \
            .tableName("devices7") \
            .addColumn("user_id", LongType()) \
            .addColumn("manufacturer", StringType()) \
            .addColumn("model", StringType()) \
            .addColumn("platform", StringType()) \
            .addColumn("version", LongType()) \
            .addColumn("build_number", LongType()) \
            .addColumn("serial_number", StringType()) \
            .addColumn("dt_current", DateType()) \
            .addColumn("year", IntegerType()) \
            .addColumn("month",IntegerType()) \
            .addColumn("day",IntegerType()) \
            .addColumn("bronze_ingestion_time", TimestampType()) \
            .addColumn("silver_ingestion_time", TimestampType()) \
            .addColumn("gold_ingestion_time", TimestampType()) \
            .partitionedBy("year","month","day") \
            .location(delta_gold_tb_devices_location) \
            .execute()
    

    ## writing into [gold zone] 
    df_gold_ds.write.format("delta").mode("overwrite").save(delta_gold_tb_obt_ds_location)
    df_gold_partnerships.write.format("delta").mode("overwrite").save(delta_gold_tb_partnerships_location)
    df_gold_devices_count_by_users.write.format("delta").mode("overwrite").save(delta_gold_tb_devices_count_by_users_location)
    df_gold_devices.write.format("delta").mode("overwrite").save(delta_gold_tb_devices_location)


    ## Business Case

    # delta table name = obt
    # read latest rows added
    dt_obt_delta = DeltaTable.forPath(spark, delta_gold_tb_obt_ds_location)
    dt_obt_delta.toDF().show()

    # delta table name = partnerships
    # read latest rows added
    dt_partnerships_delta = DeltaTable.forPath(spark, delta_gold_tb_partnerships_location)
    dt_partnerships_delta.toDF().show()

    # delta table name = devices_count_by_users
    # read latest rows added
    dt_devices_count_by_users_delta = DeltaTable.forPath(spark, delta_gold_tb_devices_count_by_users_location)
    dt_devices_count_by_users_delta.toDF().show()

    # delta table name = devices
    # read latest rows added
    dt_devices_delta = DeltaTable.forPath(spark, delta_gold_tb_devices_location)
    dt_devices_delta.toDF().show()

    # data retention
    # retain commit history for 30 days
    # not run VACUUM, if so, lose the ability to
    # go back older than 7 days
    # configure retention period
    # delta.logRetentionDuration = "interval <interval>":
    # delta.deletedFileRetentionDuration = "interval <interval>":

    # dt_obt_delta.vacuum()
    # dt_partnerships_delta.vacuum()
    # dt_devices_count_by_users_delta.vacuum()
    # dt_devices_delta.vacuum()

    # stop session
    spark.stop()