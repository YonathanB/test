# ============================================================
# NOUVEAU
# Corrige uniquement les sauts isolés A -> B -> A.
# On ne supprime B que si :
#   - A existe aussi comme candidat sur cette range
#   - B n'est pas fortement meilleur
#   - aucune transition ne soutient B
# ============================================================

def smooth_isolated_jumps(
    winners,
    adjusted_candidates,
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

    x = (
        winners
        .withColumn(
            "prev_country",
            F.lag("country_code").over(w),
        )
        .withColumn(
            "next_country",
            F.lead("country_code").over(w),
        )
        .withColumn(
            "is_isolated_jump",
            (
                F.col("prev_country").isNotNull()
                & (F.col("prev_country") == F.col("next_country"))
                & (F.col("country_code") != F.col("prev_country"))
            ),
        )
    )

    # Chercher le pays A comme candidat alternatif
    alternatives = (
        x.where(F.col("is_isolated_jump"))
        .select(
            "range_id",
            F.col("prev_country").alias("alternative_country"),
        )
        .join(
            adjusted_candidates.select(
                "range_id",
                F.col("country_code").alias("alternative_country"),
                F.col("evidence_score").alias("alt_evidence_score"),
                F.col("relative_score").alias("alt_relative_score"),
                F.col("adjusted_score").alias("alt_adjusted_score"),
                F.col("candidate_rank").alias("alt_candidate_rank"),
            ),
            ["range_id", "alternative_country"],
            "inner",
        )
    )

    x = (
        x.join(
            alternatives,
            "range_id",
            "left",
        )
        .withColumn(
            "should_smooth",
            F.col("is_isolated_jump")
            & F.col("alt_adjusted_score").isNotNull()

            # Une transition soutient le pays actuel :
            # on ne le corrige pas.
            & (F.col("transition_support") == 0)

            # Le pays actuel ne domine pas suffisamment.
            & (
                (
                    F.col("adjusted_score")
                    - F.col("alt_adjusted_score")
                )
                <= F.lit(ISOLATED_JUMP_MIN_ADVANTAGE)
            ),
        )

        .withColumn(
            "country_code",
            F.when(
                F.col("should_smooth"),
                F.col("alternative_country"),
            ).otherwise(F.col("country_code")),
        )

        .withColumn(
            "evidence_score",
            F.when(
                F.col("should_smooth"),
                F.col("alt_evidence_score"),
            ).otherwise(F.col("evidence_score")),
        )

        .withColumn(
            "relative_score",
            F.when(
                F.col("should_smooth"),
                F.col("alt_relative_score"),
            ).otherwise(F.col("relative_score")),
        )

        .withColumn(
            "adjusted_score",
            F.when(
                F.col("should_smooth"),
                F.col("alt_adjusted_score"),
            ).otherwise(F.col("adjusted_score")),
        )

        .withColumn(
            "candidate_rank",
            F.when(
                F.col("should_smooth"),
                F.col("alt_candidate_rank"),
            ).otherwise(F.col("candidate_rank")),
        )

        .withColumn(
            "resolution_method",
            F.when(
                F.col("should_smooth"),
                F.lit("ISOLATED_JUMP_CORRECTION"),
            ).otherwise(F.col("resolution_method")),
        )

        .drop(
            "prev_country",
            "next_country",
            "is_isolated_jump",
            "alternative_country",
            "alt_evidence_score",
            "alt_relative_score",
            "alt_adjusted_score",
            "alt_candidate_rank",
            "should_smooth",
        )
    )

    return x# ============================================================
# MODIFICATION : nouveau temporal_engine simple
# ============================================================

def temporal_engine(stage1):

    adjusted_candidates = apply_transition_support(
        stage1["local_candidates"],
        stage1["transition_anchors"],
    )

    local_winners = choose_local_winner(
        adjusted_candidates
    )

    resolved_ranges = smooth_isolated_jumps(
        local_winners,
        adjusted_candidates,
    )

    return {
        "adjusted_candidates": adjusted_candidates,
        "resolved_ranges": resolved_ranges,
    }# ============================================================
# NOUVEAU
# On choisit le meilleur candidat APRES prise en compte
# des transitions.
# ============================================================

def choose_local_winner(adjusted_candidates):

    w = (
        Window
        .partitionBy("range_id")
        .orderBy(
            F.col("adjusted_score").desc(),
            F.col("logic_count").desc(),
            F.col("country_code").asc(),
        )
    )

    return (
        adjusted_candidates
        .withColumn(
            "adjusted_rank",
            F.row_number().over(w),
        )
        .where(F.col("adjusted_rank") == 1)
        .withColumn(
            "original_country_code",
            F.col("country_code"),
        )
        .withColumn(
            "resolution_method",
            F.lit("LOCAL_WINNER"),
        )
    )# ============================================================
# NOUVEAU
# Les transitions renforcent directement les candidats locaux.
# ============================================================

def apply_transition_support(
    local_candidates,
    transition_anchors,
):

    c = local_candidates.alias("c")
    t = transition_anchors.alias("t")

    # Pays de départ : range immédiatement AVANT la transition
    support_before = (
        c.join(
            t,
            (
                (F.col("c.entity_key") == F.col("t.entity_key"))
                & (F.col("c.range_to") == F.col("t.transition_ts"))
                & (F.col("c.country_code") == F.col("t.country_from"))
            ),
            "inner",
        )
        .select(
            F.col("c.range_id").alias("range_id"),
            F.col("c.country_code").alias("country_code"),
            (
                F.col("t.evidence_weight")
                * F.lit(TRANSITION_FROM_BONUS)
            ).alias("transition_support"),
        )
    )

    # Pays d'arrivée : range immédiatement APRES la transition
    support_after = (
        c.join(
            t,
            (
                (F.col("c.entity_key") == F.col("t.entity_key"))
                & (F.col("c.range_from") == F.col("t.transition_ts"))
                & (F.col("c.country_code") == F.col("t.country_to"))
            ),
            "inner",
        )
        .select(
            F.col("c.range_id").alias("range_id"),
            F.col("c.country_code").alias("country_code"),
            (
                F.col("t.evidence_weight")
                * F.lit(TRANSITION_TO_BONUS)
            ).alias("transition_support"),
        )
    )

    support = (
        support_before
        .unionByName(support_after)
        .groupBy("range_id", "country_code")
        .agg(
            F.sum("transition_support")
            .alias("transition_support")
        )
    )

    return (
        local_candidates
        .join(
            support,
            ["range_id", "country_code"],
            "left",
        )
        .withColumn(
            "transition_support",
            F.coalesce(
                F.col("transition_support"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "adjusted_score",
            F.col("evidence_score")
            + F.col("transition_support"),
        )
    )# ============================================================
# PARAMETRES TEMPORAL RESOLVER
# ============================================================

TRANSITION_FROM_BONUS = 0.50
TRANSITION_TO_BONUS = 1.00

# Un saut A -> B -> A est corrigé si B ne dépasse pas
# le candidat A de plus de cette valeur.
ISOLATED_JUMP_MIN_ADVANTAGE = 0.30
