# REMPLACER build_gap_candidates() PAR CETTE VERSION

def build_gap_candidates(
    unresolved_gaps: DataFrame,
    time_decay_days: float = TIME_DECAY_DAYS,
    max_inference_gap_days: float = MAX_INFERENCE_GAP_DAYS,
    transition_anchors: DataFrame | None = None,
) -> DataFrame:

    required = {
        "entity_key",
        "gap_id",
        "gap_from",
        "gap_to",
        "gap_duration_seconds",
        "gap_duration_days",
        "country_before",
        "country_after",
        "score_before",
        "score_after",
    }
    _require_columns(unresolved_gaps, required, "unresolved_gaps")

    if time_decay_days <= 0:
        raise ValueError("time_decay_days doit être strictement positif")

    if max_inference_gap_days <= 0:
        raise ValueError(
            "max_inference_gap_days doit être strictement positif"
        )

    eligible_gaps = (
        unresolved_gaps
        .where(
            (F.col("gap_duration_seconds") > 0)
            & (
                F.col("gap_duration_days")
                <= F.lit(max_inference_gap_days)
            )
        )
        .withColumn(
            "_last_gap_day",
            F.to_date(
                F.from_unixtime(
                    F.unix_timestamp("gap_to") - F.lit(1)
                )
            ),
        )
    )

    # ---------------------------------------------------------------
    # 1. Découpage initial aux frontières de jours
    # ---------------------------------------------------------------

    day_slices = (
        eligible_gaps
        .withColumn(
            "_slice_day",
            F.explode(
                F.sequence(
                    F.to_date("gap_from"),
                    F.col("_last_gap_day"),
                    F.expr("INTERVAL 1 DAY"),
                )
            ),
        )
        .withColumn(
            "slice_from",
            F.greatest(
                F.col("gap_from"),
                F.col("_slice_day").cast("timestamp"),
            ),
        )
        .withColumn(
            "slice_to",
            F.least(
                F.col("gap_to"),
                F.date_add(
                    F.col("_slice_day"),
                    1,
                ).cast("timestamp"),
            ),
        )
        .where(
            F.col("slice_to") > F.col("slice_from")
        )
    )

    transition_sequences = None

    # ---------------------------------------------------------------
    # 2. Les transitions deviennent également des points de coupure
    # ---------------------------------------------------------------

    if transition_anchors is not None:

        _require_columns(
            transition_anchors,
            _REQUIRED_TRANSITION_COLUMNS,
            "transition_anchors",
        )

        transition_weight = (
            F.col("evidence_weight").cast("double")
            if "evidence_weight" in transition_anchors.columns
            else F.lit(None).cast("double")
        )

        transitions = (
            transition_anchors
            .select(
                "entity_key",
                F.col("transition_ts")
                .cast("timestamp")
                .alias("transition_ts"),
                "country_from",
                "country_to",
                transition_weight.alias("evidence_weight"),
            )
            .where(
                F.col("transition_ts").isNotNull()
                & F.col("country_from").isNotNull()
                & F.col("country_to").isNotNull()
            )
            .groupBy(
                "entity_key",
                "transition_ts",
                "country_from",
                "country_to",
            )
            .agg(
                F.max("evidence_weight")
                .alias("evidence_weight")
            )
        )

        # Si plusieurs transitions différentes existent exactement
        # au même instant, on ne fabrique pas une séquence artificielle.
        same_ts_window = Window.partitionBy(
            "entity_key",
            "transition_ts",
        )

        unique_transitions = (
            transitions
            .withColumn(
                "_transition_count_at_ts",
                F.count(F.lit(1)).over(same_ts_window),
            )
            .where(
                F.col("_transition_count_at_ts") == 1
            )
            .drop("_transition_count_at_ts")
        )

        # -----------------------------------------------------------
        # Séquences cohérentes :
        #
        # T1 : A -> B
        # T2 : B -> C
        #
        # => B est une preuve temporelle sur [T1, T2)
        # -----------------------------------------------------------

        transition_window = (
            Window
            .partitionBy("entity_key")
            .orderBy("transition_ts")
        )

        transition_sequences = (
            unique_transitions
            .withColumn(
                "_next_transition_ts",
                F.lead("transition_ts")
                .over(transition_window),
            )
            .withColumn(
                "_next_country_from",
                F.lead("country_from")
                .over(transition_window),
            )
            .withColumn(
                "_next_evidence_weight",
                F.lead("evidence_weight")
                .over(transition_window),
            )
            .where(
                F.col("_next_transition_ts").isNotNull()
                & (
                    F.col("_next_transition_ts")
                    > F.col("transition_ts")
                )
                & (
                    F.col("country_to")
                    == F.col("_next_country_from")
                )
            )
            .withColumn(
                "_current_weight",
                F.coalesce(
                    F.col("evidence_weight"),
                    F.lit(TRANSITION_SEQUENCE_WEIGHT),
                ),
            )
            .withColumn(
                "_next_weight",
                F.coalesce(
                    F.col("_next_evidence_weight"),
                    F.lit(TRANSITION_SEQUENCE_WEIGHT),
                ),
            )
            .select(
                "entity_key",
                F.col("transition_ts")
                .alias("anchor_from"),
                F.col("_next_transition_ts")
                .alias("anchor_to"),
                F.col("country_to")
                .alias("country_code"),
                F.least(
                    F.col("_current_weight"),
                    F.col("_next_weight"),
                ).alias("sequence_score"),
            )
            .where(
                F.col("sequence_score") > 0
            )
        )

        # -----------------------------------------------------------
        # Ajout des timestamps de transition aux frontières des slices.
        #
        # Avant :
        #     jour entier
        #
        # Après :
        #     début jour -> transition -> transition -> fin jour
        # -----------------------------------------------------------

        slice_identity_columns = [
            "entity_key",
            "gap_id",
            "gap_from",
            "gap_to",
            "country_before",
            "country_after",
            "score_before",
            "score_after",
        ]

        base_cuts = (
            day_slices
            .select(
                *slice_identity_columns,
                F.col("slice_from").alias("_cut"),
            )
            .unionByName(
                day_slices.select(
                    *slice_identity_columns,
                    F.col("slice_to").alias("_cut"),
                )
            )
        )

        transition_cuts = (
            eligible_gaps.alias("g")
            .join(
                unique_transitions.alias("t"),
                (
                    F.col("g.entity_key")
                    == F.col("t.entity_key")
                )
                & (
                    F.col("t.transition_ts")
                    > F.col("g.gap_from")
                )
                & (
                    F.col("t.transition_ts")
                    < F.col("g.gap_to")
                ),
                "inner",
            )
            .select(
                *[
                    F.col(f"g.{c}").alias(c)
                    for c in slice_identity_columns
                ],
                F.col("t.transition_ts").alias("_cut"),
            )
        )

        cuts = (
            base_cuts
            .unionByName(transition_cuts)
            .distinct()
        )

        cut_window = (
            Window
            .partitionBy(
                "entity_key",
                "gap_id",
            )
            .orderBy("_cut")
        )

        slices = (
            cuts
            .withColumn(
                "slice_to",
                F.lead("_cut").over(cut_window),
            )
            .withColumnRenamed(
                "_cut",
                "slice_from",
            )
            .where(
                F.col("slice_to").isNotNull()
                & (
                    F.col("slice_to")
                    > F.col("slice_from")
                )
            )
        )

    else:
        slices = day_slices

    # ---------------------------------------------------------------
    # 3. Distance temporelle aux bornes du trou
    # ---------------------------------------------------------------

    slices = (
        slices
        .withColumn(
            "_slice_mid_epoch",
            (
                F.unix_timestamp("slice_from")
                + F.unix_timestamp("slice_to")
            )
            / F.lit(2.0),
        )
        .withColumn(
            "_distance_before_days",
            (
                F.col("_slice_mid_epoch")
                - F.unix_timestamp("gap_from")
            )
            / F.lit(86400.0),
        )
        .withColumn(
            "_distance_after_days",
            (
                F.unix_timestamp("gap_to")
                - F.col("_slice_mid_epoch")
            )
            / F.lit(86400.0),
        )
    )

    common_columns = [
        "entity_key",
        "gap_id",
        "gap_from",
        "gap_to",
        "slice_from",
        "slice_to",
    ]

    # ---------------------------------------------------------------
    # 4. Candidats provenant des deux présences voisines
    # ---------------------------------------------------------------

    candidates_before = (
        slices
        .select(
            *common_columns,
            F.col("country_before")
            .alias("country_code"),
            F.col("score_before")
            .cast("double")
            .alias("boundary_score"),
            F.col("_distance_before_days")
            .alias("distance_days"),
            F.lit("BEFORE")
            .alias("supported_by"),
        )
    )

    candidates_after = (
        slices
        .select(
            *common_columns,
            F.col("country_after")
            .alias("country_code"),
            F.col("score_after")
            .cast("double")
            .alias("boundary_score"),
            F.col("_distance_after_days")
            .alias("distance_days"),
            F.lit("AFTER")
            .alias("supported_by"),
        )
    )

    boundary_candidates = (
        candidates_before
        .unionByName(candidates_after)
        .where(
            F.col("country_code").isNotNull()
            & F.col("boundary_score").isNotNull()
            & (
                F.col("boundary_score") > 0
            )
        )
        .withColumn(
            "influence",
            F.col("boundary_score")
            * F.exp(
                -F.col("distance_days")
                / F.lit(float(time_decay_days))
            ),
        )
        .select(
            *common_columns,
            "country_code",
            "influence",
            "distance_days",
            "supported_by",
        )
    )

    # ---------------------------------------------------------------
    # 5. NOUVEAU :
    #    candidats provenant d'une séquence de transitions cohérentes
    # ---------------------------------------------------------------

    if transition_sequences is not None:

        transition_candidates = (
            slices.alias("s")
            .join(
                transition_sequences.alias("t"),
                (
                    F.col("s.entity_key")
                    == F.col("t.entity_key")
                )
                & (
                    F.col("s.slice_from")
                    >= F.col("t.anchor_from")
                )
                & (
                    F.col("s.slice_to")
                    <= F.col("t.anchor_to")
                ),
                "inner",
            )
            .select(
                *[
                    F.col(f"s.{c}").alias(c)
                    for c in common_columns
                ],
                F.col("t.country_code")
                .alias("country_code"),
                F.col("t.sequence_score")
                .alias("influence"),
                F.lit(0.0)
                .alias("distance_days"),
                F.lit("TRANSITION_SEQUENCE")
                .alias("supported_by"),
            )
        )

        raw_candidates = (
            boundary_candidates
            .unionByName(transition_candidates)
        )

    else:
        raw_candidates = boundary_candidates

    # ---------------------------------------------------------------
    # 6. Agrégation de toutes les preuves du même pays
    # ---------------------------------------------------------------

    candidates = (
        raw_candidates
        .groupBy(
            "entity_key",
            "gap_id",
            "gap_from",
            "gap_to",
            "slice_from",
            "slice_to",
            "country_code",
        )
        .agg(
            F.sum("influence")
            .alias("candidate_score"),

            F.min("distance_days")
            .alias("distance_days"),

            F.sort_array(
                F.collect_set("supported_by")
            ).alias("supported_by"),
        )
    )

    candidate_window = Window.partitionBy(
        "entity_key",
        "gap_id",
        "slice_from",
        "slice_to",
    )

    candidate_rank_window = (
        candidate_window
        .orderBy(
            F.col("candidate_score").desc(),
            F.col("country_code").asc(),
        )
    )

    return (
        candidates
        .withColumn(
            "candidate_total_score",
            F.sum("candidate_score")
            .over(candidate_window),
        )
        .withColumn(
            "candidate_share",
            F.col("candidate_score")
            / F.col("candidate_total_score"),
        )
        .withColumn(
            "candidate_count",
            F.count(F.lit(1))
            .over(candidate_window),
        )
        .withColumn(
            "candidate_rank",
            F.row_number()
            .over(candidate_rank_window),
        )
        .withColumn(
            "range_id",
            _stable_id(
                F.col("gap_id"),
                F.col("slice_from"),
                F.col("slice_to"),
                F.col("country_code"),
            ),
        )
        .withColumnRenamed(
            "slice_from",
            "range_from",
        )
        .withColumnRenamed(
            "slice_to",
            "range_to",
        )
        .withColumn(
            "evidence_score",
            F.lit(None).cast("double"),
        )
        .withColumn(
            "relative_score",
            F.col("candidate_share"),
        )
        .withColumn(
            "evidence_count",
            F.lit(0).cast("long"),
        )
        .withColumn(
            "source_count",
            F.lit(0).cast("long"),
        )
        .withColumn(
            "logic_count",
            F.lit(0).cast("long"),
        )
        .withColumn(
            "total_evidence_score",
            F.lit(None).cast("double"),
        )
        .withColumn(
            "transition_support",
            F.when(
                F.array_contains(
                    F.col("supported_by"),
                    "TRANSITION_SEQUENCE",
                ),
                F.col("candidate_score"),
            ).otherwise(
                F.lit(None).cast("double")
            ),
        )
        .withColumn(
            "adjusted_score",
            F.col("candidate_score"),
        )
        .withColumn(
            "adjusted_rank",
            F.col("candidate_rank"),
        )
        .withColumn(
            "original_country_code",
            F.col("country_code"),
        )
        .withColumn(
            "original_adjusted_score",
            F.col("candidate_score"),
        )

        # On conserve TEMPORAL_DECAY_CANDIDATE pour rester compatible
        # avec location_segments_v1.
        .withColumn(
            "resolution_method",
            F.lit("TEMPORAL_DECAY_CANDIDATE"),
        )

        .withColumn(
            "is_temporally_inferred",
            F.lit(True),
        )
        .withColumn(
            "temporal_resolution_method",
            F.when(
                F.array_contains(
                    F.col("supported_by"),
                    "TRANSITION_SEQUENCE",
                ),
                F.lit("TRANSITION_SEQUENCE"),
            ).otherwise(
                F.lit("TEMPORAL_DECAY")
            ),
        )
        .withColumn(
            "temporal_support_score",
            F.col("candidate_score"),
        )
        .withColumn(
            "inference_method",
            F.when(
                F.array_contains(
                    F.col("supported_by"),
                    "TRANSITION_SEQUENCE",
                ),
                F.lit("TRANSITION_SEQUENCE"),
            ).otherwise(
                F.lit("TIME_DECAY")
            ),
        )
                )



# REMPLACER attach_gap_candidates() PAR :

def attach_gap_candidates(
    filled_ranges: DataFrame,
    unresolved_gaps: DataFrame,
    time_decay_days: float = TIME_DECAY_DAYS,
    max_inference_gap_days: float = MAX_INFERENCE_GAP_DAYS,
    transition_anchors: DataFrame | None = None,
) -> tuple[DataFrame, DataFrame]:

    gap_candidates = build_gap_candidates(
        unresolved_gaps,
        time_decay_days=time_decay_days,
        max_inference_gap_days=max_inference_gap_days,
        transition_anchors=transition_anchors,
    )

    resolved_timeline = (
        filled_ranges
        .unionByName(
            gap_candidates,
            allowMissingColumns=True,
        )
    )

    return gap_candidates, resolved_timeline


# DANS L'ORCHESTRATION :
# ancien appel

gap_candidates, resolved_timeline = attach_gap_candidates(
    filled_ranges,
    unresolved_gaps,
)


# devient

gap_candidates, resolved_timeline = attach_gap_candidates(
    filled_ranges,
    unresolved_gaps,
    transition_anchors=transition_anchors,
)
