#!/usr/bin/env python3
"""
Потоковое чтение данных о кредитах из топика Apache Kafka® (Structured Streaming).
Результат сохраняется в Yandex Object Storage (бакет etl-mel).

Перед запуском укажите FQDN брокера Kafka вместо <KAFKA_FQDN>.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType


KAFKA_BOOTSTRAP = "<KAFKA_FQDN>:9091"
KAFKA_TOPIC     = "dataproc-kafka-topic"
KAFKA_USER      = "user1"
KAFKA_PASSWORD  = "password1"
OUTPUT_PATH     = "s3a://etl-mel/output/kafka-read-stream-output"
CHECKPOINT_PATH = "s3a://etl-mel/checkpoints/kafka-stream"


def main():
    spark = SparkSession.builder \
        .appName("dataproc-kafka-read-stream-loans") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("loan_id",   IntegerType()),
        StructField("borrower",  StringType()),
        StructField("amount",    LongType()),
        StructField("status",    StringType()),
    ])

    query = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers",  KAFKA_BOOTSTRAP) \
        .option("subscribe",                KAFKA_TOPIC) \
        .option("kafka.security.protocol",  "SASL_SSL") \
        .option("kafka.sasl.mechanism",     "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                f"username={KAFKA_USER} "
                f"password={KAFKA_PASSWORD} "
                ";") \
        .option("startingOffsets", "earliest") \
        .load() \
        .select(
            from_json(col("value").cast("string"), schema).alias("data"),
            col("timestamp")
        ) \
        .select("data.*", "timestamp") \
        .where(col("loan_id").isNotNull()) \
        .writeStream \
        .trigger(once=True) \
        .queryName("loans-stream") \
        .format("memory") \
        .start()

    query.awaitTermination()

    df = spark.sql("SELECT * FROM `loans-stream`")
    df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(OUTPUT_PATH)

    print(f"[INFO] Стрим завершён. Результат сохранён в {OUTPUT_PATH}")
    spark.stop()


if __name__ == "__main__":
    main()