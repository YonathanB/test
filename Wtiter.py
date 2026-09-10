from pyspark.sql import functions as F

UNRESOLVED_BUCKETS = 512
WRITERS_PER_BUCKET_MONTH = 8

def bucket_expr(column_name: str, buckets: int):
    # Réutiliser exactement l’algorithme déjà normalisé dans le projet.
    return F.pmod(
        F.xxhash64(F.col(column_name)),
        F.lit(buckets)
    ).cast("smallint")

prepared = (
    iceberg_snapshot_df
    .withColumn(
        "entity_bucket",
        bucket_expr("entitykey", UNRESOLVED_BUCKETS)
    )
    .withColumn(
        "event_month",
        F.trunc(F.to_date("event_ts"), "month")
    )
    .withColumn(
        "_write_salt",
        F.pmod(
            F.xxhash64(
                F.col("entitykey"),
                F.col("event_ts"),
                F.lit("hive-write-v1")
            ),
            F.lit(WRITERS_PER_BUCKET_MONTH)
        )
    )
)

# Pour un full de deux mois : 512 × 2 × 8 = 8192 tâches potentielles.
number_of_writers = (
    UNRESOLVED_BUCKETS
    * len(months_in_batch)
    * WRITERS_PER_BUCKET_MONTH
)

hive_output = (
    prepared
    .repartition(
        number_of_writers,
        "entity_bucket",
        "event_month",
        "_write_salt"
    )
    .sortWithinPartitions("entitykey", "event_ts")
    .drop("_write_salt")
)

# Écriture dans une zone de staging, jamais directement sur la table publiée.
(
    hive_output.write
    .mode("overwrite")
    .format("orc")
    .option("compression", "zstd")
    .partitionBy("entity_bucket", "event_month")
    .save(hive_staging_path)
)
