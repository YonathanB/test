from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# CALIBRATION
# ============================================================

MAX_CONTINUITY_GAP_HOURS = 72
MAX_TRANSITION_BRIDGE_GAP_HOURS = 72


# ============================================================
# FILL TEMPORAL GAPS
# ============================================================

def fill_temporal_gaps(
    resolved_ranges,
    transition_anchors,
):

    w = (
        Window
        .partitionBy("entity_key")
        .orderBy(
            "range_from",
            "range_to",
            "range_id",
        )
    )

    # --------------------------------------------------------
    # 1. Trouver les trous entre ranges résolus
    # --------------------------------------------------------

    adj = (
        resolved_ranges
        .withColumn(
            "next_range_from",
            F.lead("range_from").over(w),
        )
        .withColumn(
            "next_country_code",
            F.lead("country_code").over(w),
        )
        .withColumn(
            "next_range_id",
            F.lead("range_id").over(w),
        )
    )

    gaps = (
        adj
        .where(
            F.col("next_range_from").isNotNull()
            & (F.col("range_to") < F.col("next_range_from"))
        )
        .select(
            "entity_key",
            "person_id",

            F.col("range_id").alias("left_range_id"),
            F.col("next_range_id").alias("right_range_id"),

            F.col("range_to").alias("gap_from"),
            F.col("next_range_from").alias("gap_to"),

            F.col("country_code").alias("country_before"),
            F.col("next_country_code").alias("country_after"),
        )
        .withColumn(
            "gap_seconds",
            F.col("gap_to").cast("long")
            - F.col("gap_from").cast("long"),
        )
        .withColumn(
            "gap_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.col("entity_key"),
                    F.col("gap_from").cast("string"),
                    F.col("gap_to").cast("string"),
                ),
                256,
            ),
        )
    )

    # --------------------------------------------------------
    # 2. Chercher transition explicite compatible
    #
    # FR ----- trou ----- DE
    #
    # On cherche :
    # FR -> DE
    # avec transition_ts dans le trou.
    # --------------------------------------------------------

    compatible_transitions = (
        gaps.alias("g")
        .join(
            transition_anchors.alias("t"),
            (
                (F.col("g.entity_key") == F.col("t.entity_key"))
                & (
                    F.col("t.transition_ts")
                    >= F.col("g.gap_from")
                )
                & (
                    F.col("t.transition_ts")
                    <= F.col("g.gap_to")
                )
                & (
                    F.col("t.country_from")
                    == F.col("g.country_before")
                )
                & (
                    F.col("t.country_to")
                    == F.col("g.country_after")
                )
            ),
            "inner",
        )
        .select(
            F.col("g.gap_id"),
            F.col("t.silver_event_id")
                .alias("transition_silver_event_id"),
            F.col("t.transition_ts"),
            F.col("t.evidence_weight")
                .alias("transition_weight"),
            F.col("t.evidence_count")
                .alias("transition_evidence_count"),
        )
    )

    # S'il y en a plusieurs, garder la plus forte
    wt = (
        Window
        .partitionBy("gap_id")
        .orderBy(
            F.col("transition_weight").desc(),
            F.col("transition_evidence_count").desc(),
            F.col("transition_ts").asc(),
        )
    )

    best_transition = (
        compatible_transitions
        .withColumn(
            "_rank",
            F.row_number().over(wt),
        )
        .where(F.col("_rank") == 1)
        .drop("_rank")
    )

    gaps = (
        gaps
        .join(
            best_transition,
            "gap_id",
            "left",
        )
    )

    # ========================================================
    # 3. CAS A : FR ... TROU ... FR
    #
    # Continuité du même pays
    # ========================================================

    same_country_gaps = (
        gaps
        .where(
            (F.col("country_before") == F.col("country_after"))
            & (
                F.col("gap_seconds")
                <= F.lit(MAX_CONTINUITY_GAP_HOURS * 3600)
            )
        )
        .select(
            "gap_id",
            "entity_key",
            "person_id",

            F.col("gap_from").alias("range_from"),
            F.col("gap_to").alias("range_to"),

            F.col("country_before").alias("country_code"),

            "left_range_id",
            "right_range_id",
        )
        .withColumn(
            "range_id",
            F.concat(
                F.lit("GAP_CONTINUITY_"),
                F.col("gap_id"),
            ),
        )
        .withColumn(
            "result_type",
            F.lit("INFERRED"),
        )
        .withColumn(
            "resolution_method",
            F.lit("SAME_COUNTRY_CONTINUITY"),
        )
        .withColumn(
            "is_inferred",
            F.lit(True),
        )
    )

    # ========================================================
    # 4. CAS B : FR ... TROU ... DE
    #            transition FR -> DE à T
    #
    # Produit :
    #
    # gap_from -> T   FR
    # T -> gap_to     DE
    # ========================================================

    transition_gaps = (
        gaps
        .where(
            (F.col("country_before") != F.col("country_after"))
            & F.col("transition_ts").isNotNull()
            & (
                F.col("gap_seconds")
                <= F.lit(
                    MAX_TRANSITION_BRIDGE_GAP_HOURS * 3600
                )
            )
        )
    )

    before_transition = (
        transition_gaps
        .where(
            F.col("gap_from") < F.col("transition_ts")
        )
        .select(
            "gap_id",
            "entity_key",
            "person_id",

            F.col("gap_from").alias("range_from"),
            F.col("transition_ts").alias("range_to"),

            F.col("country_before").alias("country_code"),

            "left_range_id",
            "right_range_id",

            "transition_silver_event_id",
            "transition_ts",
            "transition_weight",
        )
        .withColumn(
            "range_id",
            F.concat(
                F.lit("GAP_TRANSITION_BEFORE_"),
                F.col("gap_id"),
            ),
        )
        .withColumn(
            "result_type",
            F.lit("INFERRED"),
        )
        .withColumn(
            "resolution_method",
            F.lit("EXPLICIT_TRANSITION_BRIDGE"),
        )
        .withColumn(
            "is_inferred",
            F.lit(True),
        )
    )

    after_transition = (
        transition_gaps
        .where(
            F.col("transition_ts") < F.col("gap_to")
        )
        .select(
            "gap_id",
            "entity_key",
            "person_id",

            F.col("transition_ts").alias("range_from"),
            F.col("gap_to").alias("range_to"),

            F.col("country_after").alias("country_code"),

            "left_range_id",
            "right_range_id",

            "transition_silver_event_id",
            "transition_ts",
            "transition_weight",
        )
        .withColumn(
            "range_id",
            F.concat(
                F.lit("GAP_TRANSITION_AFTER_"),
                F.col("gap_id"),
            ),
        )
        .withColumn(
            "result_type",
            F.lit("INFERRED"),
        )
        .withColumn(
            "resolution_method",
            F.lit("EXPLICIT_TRANSITION_BRIDGE"),
        )
        .withColumn(
            "is_inferred",
            F.lit(True),
        )
    )

    # ========================================================
    # 5. CAS C : impossible de conclure
    #
    # On garde explicitement le trou UNKNOWN
    # ========================================================

    unknown_gaps = (
        gaps
        .where(
            ~(
                (
                    (F.col("country_before") == F.col("country_after"))
                    & (
                        F.col("gap_seconds")
                        <= F.lit(MAX_CONTINUITY_GAP_HOURS * 3600)
                    )
                )
                |
                (
                    (F.col("country_before") != F.col("country_after"))
                    & F.col("transition_ts").isNotNull()
                    & (
                        F.col("gap_seconds")
                        <= F.lit(
                            MAX_TRANSITION_BRIDGE_GAP_HOURS * 3600
                        )
                    )
                )
            )
        )
        .select(
            "gap_id",
            "entity_key",
            "person_id",

            F.col("gap_from").alias("range_from"),
            F.col("gap_to").alias("range_to"),

            "left_range_id",
            "right_range_id",
        )
        .withColumn(
            "country_code",
            F.lit(None).cast("string"),
        )
        .withColumn(
            "range_id",
            F.concat(
                F.lit("GAP_UNKNOWN_"),
                F.col("gap_id"),
            ),
        )
        .withColumn(
            "result_type",
            F.lit("UNKNOWN"),
        )
        .withColumn(
            "resolution_method",
            F.lit("UNRESOLVED_GAP"),
        )
        .withColumn(
            "is_inferred",
            F.lit(True),
        )
    )

    # ========================================================
    # 6. RANGES EXISTANTS
    # ========================================================

    observed = (
        resolved_ranges
        .withColumn(
            "result_type",
            F.lit("RESOLVED"),
        )
        .withColumn(
            "is_inferred",
            F.lit(False),
        )
    )

    # ========================================================
    # 7. TIMELINE COMPLETE
    # ========================================================

    resolved_timeline = (
        observed
        .unionByName(
            same_country_gaps,
            allowMissingColumns=True,
        )
        .unionByName(
            before_transition,
            allowMissingColumns=True,
        )
        .unionByName(
            after_transition,
            allowMissingColumns=True,
        )
        .unionByName(
            unknown_gaps,
            allowMissingColumns=True,
        )
        .orderBy(
            "entity_key",
            "range_from",
            "range_to",
        )
    )

    return resolved_timeline
