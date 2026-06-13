#!/usr/bin/env python3
"""
Запись данных о кредитах в топик Apache Kafka®.
Кластер Kafka: dataproc-kafka  |  Топик: dataproc-kafka-topic
Запускается как PySpark-задание на кластере Yandex Data Processing (dataproc102).

Перед запуском укажите FQDN брокера Kafka вместо <KAFKA_FQDN>.
"""

from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import to_json, col, struct


KAFKA_BOOTSTRAP = "<KAFKA_FQDN>:9091" 
KAFKA_TOPIC     = "dataproc-kafka-topic"
KAFKA_USER      = "user1"
KAFKA_PASSWORD  = "password1"


def main():
    spark = SparkSession.builder \
        .appName("dataproc-kafka-write-loans") \
        .getOrCreate()

    df = spark.createDataFrame([
        Row(loan_id=1,  borrower="Иванов Иван",       amount=500000, status="active"),
        Row(loan_id=2,  borrower="Петрова Мария",      amount=250000, status="paid"),
        Row(loan_id=3,  borrower="Сидоров Алексей",    amount=750000, status="active"),
        Row(loan_id=4,  borrower="Козлова Анна",       amount=100000, status="paid"),
        Row(loan_id=5,  borrower="Новиков Дмитрий",    amount=1200000, status="active"),
    ])

    df_kafka = df.select(
        to_json(struct([col(c).alias(c) for c in df.columns])).alias("value")
    )

    df_kafka.write.format("kafka") \
        .option("kafka.bootstrap.servers",  KAFKA_BOOTSTRAP) \
        .option("topic",                    KAFKA_TOPIC) \
        .option("kafka.security.protocol",  "SASL_SSL") \
        .option("kafka.sasl.mechanism",     "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                f"username={KAFKA_USER} "
                f"password={KAFKA_PASSWORD} "
                ";") \
        .save()

    print(f"[INFO] Записано {df_kafka.count()} сообщений в топик '{KAFKA_TOPIC}'")
    spark.stop()


if __name__ == "__main__":
    main()