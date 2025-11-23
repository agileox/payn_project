#!/usr/bin/env python
# coding: utf-8
# Imports libraries & Spark Session

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import (
    from_json, col, split, trim, regexp_replace, when, regexp_extract, size, length, lit, current_date, from_unixtime
)

# This is required when running on my laptop with Fedora 43 and 4 CPUs, 8 GB ram
spark = (
    SparkSession.builder
    .appName("ETL_Flatten_Clean_Push_Postgres")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Read downloaded JSON from kaggel & Define Schemas

df = spark.read.json("/home/agileox/Project/payn_project/data/cc_sample_transaction.json")

address_schema = StructType([
    StructField("street", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip", StringType(), True)
])

personal_schema = StructType([
    StructField("person_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("address", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("long", StringType(), True),
    StructField("city_pop", StringType(), True),
    StructField("job", StringType(), True),
    StructField("dob", StringType(), True)
])

# Process to Flatten Nested JSON

df_level1 = df.withColumn("personal_detail", from_json(col("personal_detail"), personal_schema))
df_level2 = df_level1.withColumn("address", from_json(col("personal_detail.address"), address_schema))

df_flat = df_level2.select(
    col("Unnamed: 0").alias("unnamed_id"),   # rename here
    "amt", "category", "cc_bic", "cc_num", "is_fraud",
    "merch_eff_time", "merch_last_update_time", "merch_lat", "merch_long",
    "merch_zipcode", "merchant", "trans_date_trans_time", "trans_num",
    col("personal_detail.person_name").alias("raw_person_name"),
    col("personal_detail.gender").alias("gender"),
    col("personal_detail.lat").alias("lat"),
    col("personal_detail.long").alias("long"),
    col("personal_detail.city_pop").alias("city_pop"),
    col("personal_detail.job").alias("job"),
    col("personal_detail.dob").alias("dob"),
    col("address.street").alias("address_street"),
    col("address.city").alias("address_city"),
    col("address.state").alias("address_state"),
    col("address.zip").alias("address_zip")
)

# Identified not that clean data and splitting the Person Name

df_names = df_flat.withColumn("raw_name", regexp_replace(col("raw_person_name"), "/", "@"))

df_cleaned = df_names \
    .withColumn("name", regexp_replace(col("raw_name"), r"[@|!]+", ",")) \
    .withColumn("name", regexp_replace(col("name"), r"\bNOOOO\b", "")) \
    .withColumn("name", regexp_replace(col("name"), r"\beeeee\b", "")) \
    .withColumn("name", regexp_replace(col("name"), r",\s*,", ",")) \
    .withColumn("name", regexp_replace(col("name"), r"\s+", " ")) \
    .withColumn("name", trim(col("name"))) \
    .withColumn("name", regexp_replace(col("name"), r"^,+|,+$", ""))

tokens = split(col("name"), r"[ ,]+")
first_tok = tokens.getItem(0)
second_tok = tokens.getItem(1)

df_split = df_cleaned \
    .withColumn("first_tok", first_tok) \
    .withColumn("second_tok", when((size(tokens) >= 2) & (length(second_tok) > 0), second_tok))

df_final = df_split \
    .withColumn(
        "first_name",
        when(col("second_tok").isNull(),
             regexp_extract(col("name"), r"^([A-Z][a-z]+)", 1)
        ).otherwise(col("first_tok"))
    ) \
    .withColumn(
        "last_name",
        when(col("second_tok").isNull(),
             regexp_extract(col("name"), r"^[A-Z][a-z]+([A-Z][a-z]+)", 1)
        ).otherwise(col("second_tok"))
    ) \
    .drop("first_tok", "second_tok", "raw_person_name", "raw_name", "name")

# Normalize Gender to Female and Male

df_final = df_final.withColumn(
    "gender",
    when(col("gender") == "F", "Female")
    .when(col("gender") == "M", "Male")
    .otherwise(col("gender"))
)

# Mask Credit Card Number (keep last 4 digits) & Postcode (keep 2 digits)

cc_digits = regexp_replace(col("cc_num").cast("string"), r"\D", "")
df_final = df_final.withColumn(
    "cc_num_masked",
    when(length(cc_digits) >= 4,
         regexp_replace(cc_digits, r"\d(?=\d{4})", "*")
    ).otherwise(lit(None))
).drop("cc_num")

addr_zip = regexp_replace(col("address_zip").cast("string"), r"\D", "")
df_final = df_final.withColumn(
    "address_zip_masked",
    when(length(addr_zip) >= 2,
         regexp_replace(addr_zip, r"\d(?=\d{2})", "*")
    ).otherwise(lit(None))
).drop("address_zip")

addr_street = col("address_street").cast("string")
df_final = df_final.withColumn(
    "address_street_masked",
    when(length(addr_street) >= 3,
         # replace first 3 digits with '*'
         regexp_replace(addr_street, r"^\d{3}", "***")
    ).otherwise(lit(None))
).drop("address_street")

# Casting the proper data type for the used of inserting into Postgresql DB

df_final = df_final \
    .withColumn("amt", col("amt").cast("double")) \
    .withColumn("city_pop", col("city_pop").cast("int")) \
    .withColumn("dob", col("dob").cast("date")) \
    .withColumn("is_fraud", col("is_fraud").cast("boolean")) \
    .withColumn("trans_date_trans_time", col("trans_date_trans_time").cast("timestamp"))

# handling the timestamp matters

df_final = df_final.withColumn(
    "merch_eff_time",
    when(length(col("merch_eff_time").cast("string")) == 16,  # microseconds
         from_unixtime((col("merch_eff_time").cast("bigint")/1000000).cast("bigint")))
    .when(length(col("merch_eff_time").cast("string")) == 13,  # milliseconds
         from_unixtime((col("merch_eff_time").cast("bigint")/1000).cast("bigint")))
    .when(length(col("merch_eff_time").cast("string")) == 10,  # seconds
         from_unixtime(col("merch_eff_time").cast("bigint")))
    .otherwise(None)
    .cast("timestamp")
)

df_final = df_final.withColumn(
    "merch_last_update_time",
    when(length(col("merch_last_update_time").cast("string")) == 16,  # microseconds
         from_unixtime((col("merch_last_update_time").cast("bigint")/1000000).cast("bigint")))
    .when(length(col("merch_last_update_time").cast("string")) == 13,  # milliseconds
         from_unixtime((col("merch_last_update_time").cast("bigint")/1000).cast("bigint")))
    .when(length(col("merch_last_update_time").cast("string")) == 10,  # seconds
         from_unixtime(col("merch_last_update_time").cast("bigint")))
    .otherwise(None)
    .cast("timestamp")
)

# preparing the ingestion date before pushing to PG DB

df_final = df_final.withColumn("ingestion_date", current_date())
#df_final = df_final.withColumn("ingestion_date", lit("2025-11-23").cast("date"))

# Verify

#print("Row count:", df_final.count())
#df_final.show(100, truncate=False)

# Push to PostgreSQL DB

jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
connection_properties = {
    "user": "payn_user",
    "password": "welcome1",
    "driver": "org.postgresql.Driver"
}

df_final.write \
    .jdbc(
        url=jdbc_url,
        table="payn_etl.cleaned_transactions_partition",   # schema-qualified
        mode="append",
        properties=connection_properties
    )
