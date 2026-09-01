from pyspark.sql import functions as F
from pyspark.sql.window import Window


# =========================
# CALIBRATION
# =========================

TIME_DECAY_DAYS = 3.0
MAX_INFERENCE_GAP_DAYS = 30


def build_gap_candidates(resolved_ranges):

    # On préfère le score ajusté s'il existe
    score_col = (
        "adjusted_relative_score"
        if "adjusted_relative_score" in resolved_ranges.columns
        else "relative_score"
    )

    w = (
        Window
        .partitionBy("entity_key")
        .orderBy("range_from", "range_to", "range_id")
    )

    # -----------------------------------------
    # 1. Trouver les observations avant/après
    # -----------------------------------------

    gaps = (
        resolved_ranges

        .withColumn(
            "next_range_from",
            F.lead("range_from").over(w),
        )
        .withColumn(
            "next_country",
            F.lead("country_code").over(w),
        )
        .withColumn(
            "next_score",
            F.lead(score_col).over(w),
        )

        .where(
            F.col("next_range_from").isNotNull()
            & (F.col("range_to") < F.col("next_range_from"))
        )

        .select(
            "entity_key",

            F.col("range_to").alias("gap_from"),
            F.col("next_range_from").alias("gap_to"),

            F.col("country_code").alias("left_country"),
            F.col(score_col).alias("left_score"),

            F.col("next_country").alias("right_country"),
            F.col("next_score").alias("right_score"),
        )

        .withColumn(
            "gap_days",
            F.datediff(
                F.to_date("gap_to"),
                F.to_date("gap_from"),
            ),
        )

        # On n'infère pas indéfiniment
        .where(
            F.col("gap_days")
            <= F.lit(MAX_INFERENCE_GAP_DAYS)
        )
    )

    # -----------------------------------------
    # 2. Générer les jours du trou
    # -----------------------------------------

    days = (
        gaps

        .withColumn(
            "day",
            F.explode(
                F.sequence(
                    F.to_date("gap_from"),
                    F.to_date("gap_to"),
                )
            ),
        )

        .withColumn(
            "distance_left_days",
            F.greatest(
                F.datediff(
                    F.col("day"),
                    F.to_date("gap_from"),
                ),
                F.lit(0),
            ),
        )

        .withColumn(
            "distance_right_days",
            F.greatest(
                F.datediff(
                    F.to_date("gap_to"),
                    F.col("day"),
                ),
                F.lit(0),
            ),
        )

        .withColumn(
            "left_influence",
            F.col("left_score")
            * F.exp(
                -F.col("distance_left_days")
                / F.lit(TIME_DECAY_DAYS)
            ),
        )

        .withColumn(
            "right_influence",
            F.col("right_score")
            * F.exp(
                -F.col("distance_right_days")
                / F.lit(TIME_DECAY_DAYS)
            ),
        )
    )

    # -----------------------------------------
    # 3. Candidat venant de la gauche
    # -----------------------------------------

    left = (
        days
        .select(
            "entity_key",
            "day",
            "gap_from",
            "gap_to",

            F.col("left_country").alias("country_code"),
            F.col("left_influence").alias("inferred_score"),

            "distance_left_days",
            "distance_right_days",

            "left_country",
            "right_country",
        )
    )

    # -----------------------------------------
    # 4. Candidat venant de la droite
    # -----------------------------------------

    right = (
        days
        .select(
            "entity_key",
            "day",
            "gap_from",
            "gap_to",

            F.col("right_country").alias("country_code"),
            F.col("right_influence").alias("inferred_score"),

            "distance_left_days",
            "distance_right_days",

            "left_country",
            "right_country",
        )
    )

    # -----------------------------------------
    # 5. Fusionner
    #
    # Si FR ... FR :
    # les deux influences deviennent un seul
    # candidat FR.
    # -----------------------------------------

    candidates = (
        left
        .unionByName(right)

        .groupBy(
            "entity_key",
            "day",
            "gap_from",
            "gap_to",
            "country_code",
            "left_country",
            "right_country",
        )

        .agg(
            F.max("inferred_score").alias("inferred_score"),
            F.min("distance_left_days").alias("distance_left_days"),
            F.min("distance_right_days").alias("distance_right_days"),
        )
    )

    # -----------------------------------------
    # 6. Poids relatif entre les candidats
    # -----------------------------------------

    wc = Window.partitionBy(
        "entity_key",
        "day",
        "gap_from",
        "gap_to",
    )

    return (
        candidates

        .withColumn(
            "total_inferred_score",
            F.sum("inferred_score").over(wc),
        )

        .withColumn(
            "candidate_share",
            F.when(
                F.col("total_inferred_score") > 0,
                F.col("inferred_score")
                / F.col("total_inferred_score"),
            ),
        )

        .withColumn(
            "result_type",
            F.lit("INFERRED"),
        )

        .withColumn(
            "resolution_method",
            F.lit("TEMPORAL_INTERPOLATION"),
        )
    )
