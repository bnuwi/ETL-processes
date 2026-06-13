"""
PySpark ETL скрипт для обработки данных по кредитам.
Читает CSV из Yandex Object Storage, выполняет трансформации и сохраняет результат.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def main():
    input_path  = sys.argv[1] if len(sys.argv) > 1 else "s3a://etl-mel/input/loans_input.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "s3a://etl-mel/output/loans_processed"

    spark = SparkSession.builder \
        .appName("LoanETL") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .csv(input_path)

    print(f"[INFO] Прочитано строк: {df.count()}")
    df.printSchema()

    df = df \
        .withColumn("loan_amount",    F.col("loan_amount").cast(DoubleType())) \
        .withColumn("interest_rate",  F.col("interest_rate").cast(DoubleType())) \
        .withColumn("loan_term_months", F.col("loan_term_months").cast("int")) \
        .withColumn("issue_date",     F.to_date(F.col("issue_date"), "yyyy-MM-dd"))

    df = df.withColumn(
        "total_payment",
        F.round(
            F.col("loan_amount") * (1 + F.col("interest_rate") / 100 * F.col("loan_term_months") / 12),
            2
        )
    )

    df = df.withColumn(
        "loan_category",
        F.when(F.col("loan_amount") <  300000, "small")
         .when(F.col("loan_amount") <  700000, "medium")
         .otherwise("large")
    )

    df = df.withColumn(
        "years_since_issue",
        F.round(F.datediff(F.current_date(), F.col("issue_date")) / 365.0, 1)
    )

    df = df.withColumn(
        "is_problem",
        F.when(F.col("status") == "overdue", True).otherwise(False)
    )

    region_stats = df.groupBy("region").agg(
        F.count("*")                          .alias("total_loans"),
        F.round(F.sum("loan_amount"), 2)      .alias("total_amount"),
        F.round(F.avg("loan_amount"), 2)      .alias("avg_amount"),
        F.round(F.avg("interest_rate"), 2)    .alias("avg_rate"),
        F.sum(F.col("is_problem").cast("int")).alias("problem_loans")
    ).orderBy(F.col("total_amount").desc())

    print("[INFO] Статистика по регионам:")
    region_stats.show(truncate=False)

    df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path + "/details")

    region_stats.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path + "/region_stats")

    print(f"[INFO] Результат сохранён в {output_path}")
    spark.stop()


if __name__ == "__main__":
    main()