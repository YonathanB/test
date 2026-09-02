from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def build_person_day(location_segment: DataFrame) -> DataFrame:

    # ========================================================
    # 1. Découper chaque segment aux frontières des jours
    #
    # Les segments sont semi-ouverts [segment_from, segment_to)
    # ========================================================

    daily_slices = (
        location_segment

        .where(
            F.col("segment_from").isNotNull()
            & F.col("segment_to").isNotNull()
            & (F.col("segment_to") > F.col("segment_from"))
            & F.col("country_code").isNotNull()
        )

        # Si segment_to = exactement minuit,
        # ne pas créer une journée vide supplémentaire.
        .withColumn(
            "_last_day",
            F.to_date(
                F.from_unixtime(
                    F.unix_timestamp("segment_to") - F.lit(1)
                )
            ),
        )

        .withColumn(
            "day",
            F.explode(
                F.sequence(
                    F.to_date("segment_from"),
                    F.col("_last_day"),
                    F.expr("INTERVAL 1 DAY"),
                )
            ),
        )

        .withColumn(
            "day_from",
            F.greatest(
                F.col("segment_from"),
                F.col("day").cast("timestamp"),
            ),
        )

        .withColumn(
            "day_to",
            F.least(
                F.col("segment_to"),
                F.date_add("day", 1).cast("timestamp"),
            ),
        )

        .withColumn(
            "duration_seconds",
            F.unix_timestamp("day_to")
            - F.unix_timestamp("day_from"),
        )

        .where(F.col("duration_seconds") > 0)
    )

    # ========================================================
    # 2. Agréger les éventuels segments du même pays
    #    dans la même journée
    # ========================================================

    daily_country = (
        daily_slices

        .groupBy(
            "entity_key",
            "day",
            "country_code",
        )

        .agg(
            F.min("day_from").alias("presence_from"),
            F.max("day_to").alias("presence_to"),

            F.sum("duration_seconds").alias(
                "duration_seconds"
            ),

            F.collect_set("segment_id").alias(
                "segment_ids"
            ),

            F.sum("evidence_count").alias(
                "evidence_count"
            ),

            F.max("has_temporal_inference").alias(
                "has_temporal_inference"
            ),

            F.min("is_fully_inferred").alias(
                "is_fully_inferred"
            ),

            F.max("has_gap_alternatives").alias(
                "has_gap_alternatives"
            ),

            F.max("max_candidate_count").alias(
                "max_candidate_count"
            ),

            F.min("min_gap_candidate_share").alias(
                "min_gap_candidate_share"
            ),

            F.min("min_support_score").alias(
                "min_support_score"
            ),

            # Score du segment pondéré par sa durée
            F.sum(
                F.when(
                    F.col("weighted_support_score").isNotNull(),
                    F.col("weighted_support_score")
                    * F.col("duration_seconds"),
                ).otherwise(F.lit(0.0))
            ).alias("_weighted_score_sum"),

            F.sum(
                F.when(
                    F.col("weighted_support_score").isNotNull(),
                    F.col("duration_seconds"),
                ).otherwise(F.lit(0))
            ).alias("_weighted_score_duration"),

            F.array_sort(
                F.array_distinct(
                    F.flatten(
                        F.collect_list("resolution_methods")
                    )
                )
            ).alias("resolution_methods"),
        )

        .withColumn(
            "weighted_support_score",
            F.when(
                F.col("_weighted_score_duration") > 0,
                F.col("_weighted_score_sum")
                / F.col("_weighted_score_duration"),
            ),
        )

        .drop(
            "_weighted_score_sum",
            "_weighted_score_duration",
        )
    )

    # ========================================================
    # 3. Type de résultat
    # ========================================================

    daily_country = (
        daily_country

        .withColumn(
            "result_type",
            F.when(
                F.col("is_fully_inferred"),
                F.lit("INFERRED"),
            )
            .when(
                F.col("has_temporal_inference"),
                F.lit("MIXED"),
            )
            .otherwise(
                F.lit("RESOLVED"),
            ),
        )
    )

    # ========================================================
    # 4. Classement des pays dans la journée
    #
    # Pour l'instant :
    # priorité = durée couverte,
    # puis support,
    # puis country_code pour déterminisme.
    # ========================================================

    day_window = Window.partitionBy(
        "entity_key",
        "day",
    )

    rank_window = (
        day_window
        .orderBy(
            F.col("duration_seconds").desc(),
            F.col("weighted_support_score").desc_nulls_last(),
            F.col("country_code").asc(),
        )
    )

    return (
        daily_country

        .withColumn(
            "day_total_seconds",
            F.sum("duration_seconds").over(day_window),
        )

        .withColumn(
            "day_share",
            F.col("duration_seconds")
            / F.col("day_total_seconds"),
        )

        .withColumn(
            "country_count",
            F.count(F.lit(1)).over(day_window),
        )

        .withColumn(
            "country_rank",
            F.row_number().over(rank_window),
        )

        .withColumn(
            "is_primary",
            F.col("country_rank") == 1,
        )

        .select(
            "entity_key",
            "day",
            "country_code",

            "country_rank",
            "is_primary",
            "country_count",

            "presence_from",
            "presence_to",
            "duration_seconds",
            "day_share",

            "result_type",
            "weighted_support_score",
            "min_support_score",

            "evidence_count",

            "has_temporal_inference",
            "is_fully_inferred",

            "has_gap_alternatives",
            "max_candidate_count",
            "min_gap_candidate_share",

            "resolution_methods",
            "segment_ids",
        )
    )
