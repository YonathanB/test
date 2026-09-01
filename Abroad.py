"""Résolution conservative des trous temporels du pipeline Gold.

Ce module complète la chaîne suivante, au grain ``entity_key``::

    resolved_ranges
        -> fill_temporal_gaps
        -> filled_ranges + unresolved_gaps
        -> build_gap_candidates(unresolved_gaps)

Les intervalles sont considérés comme semi-ouverts : [range_from, range_to).
Un trou non résolu n'est jamais transformé en une ligne finale dont
``country_code`` vaut NULL. Il est décrit séparément dans ``unresolved_gaps``.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


_REQUIRED_RANGE_COLUMNS = {
    "entity_key",
    "range_id",
    "range_from",
    "range_to",
    "country_code",
    "adjusted_score",
}

_REQUIRED_TRANSITION_COLUMNS = {
    "entity_key",
    "transition_ts",
    "country_from",
    "country_to",
}


def _require_columns(df: DataFrame, required: set[str], name: str) -> None:
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"{name}: colonnes manquantes: {', '.join(missing)}")


def _stable_id(*columns):
    """Construit un identifiant déterministe sans dépendre du partitionnement."""
    return F.sha2(
        F.concat_ws(
            "||",
            *[
                F.coalesce(column.cast("string"), F.lit("<NULL>"))
                for column in columns
            ],
        ),
        256,
    )


def _project_like(
    source: DataFrame,
    template: DataFrame,
    overrides: dict,
    extras: dict | None = None,
) -> DataFrame:
    """Projette des lignes inférées sur le schéma exact de resolved_ranges."""
    expressions = []
    for field in template.schema.fields:
        value = overrides.get(field.name)
        if value is None:
            value = F.lit(None)
        expressions.append(value.cast(field.dataType).alias(field.name))
    for name, value in (extras or {}).items():
        expressions.append(value.alias(name))
    return source.select(*expressions)


def _decorate_ranges(
    ranges: DataFrame,
    *,
    gap_id,
    is_inferred: bool,
    method: str,
    temporal_support_score,
) -> DataFrame:
    """Ajoute la provenance temporelle sans écraser le score de preuve local."""
    return (
        ranges
        .withColumn("gap_id", gap_id.cast("string"))
        .withColumn("is_temporally_inferred", F.lit(is_inferred))
        .withColumn("temporal_resolution_method", F.lit(method))
        .withColumn(
            "temporal_support_score",
            temporal_support_score.cast("double"),
        )
    )


def fill_temporal_gaps(
    resolved_ranges: DataFrame,
    transition_anchors: DataFrame | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Sépare les intervalles certains des trous encore ambigus.

    Règles de résolution :

    1. Deux plages voisines portent le même pays et aucune transition n'est
       observée dans le trou : le trou est rempli avec ce pays.
    2. Les pays diffèrent et il existe exactement une transition distincte
       dans le trou, compatible ``country_before -> country_after`` : le trou
       est coupé au timestamp de cette transition.
    3. Tous les autres cas sont retournés dans ``unresolved_gaps``. Ce second
       DataFrame ne possède volontairement aucune colonne ``country_code`` :
       il décrit les deux bornes et laisse ``build_gap_candidates()`` produire
       les hypothèses temporelles.

    ``filled_ranges`` contient les plages initiales et les seules plages
    temporelles résolues avec certitude. Les métriques de preuve locale des
    plages inférées restent NULL ; ``temporal_support_score`` porte séparément
    la force héritée des bornes.
    """
    _require_columns(
        resolved_ranges,
        _REQUIRED_RANGE_COLUMNS,
        "resolved_ranges",
    )

    order_window = Window.partitionBy("entity_key").orderBy(
        F.col("range_from"),
        F.col("range_to"),
        F.col("range_id"),
    )

    neighbours = (
        resolved_ranges
        .withColumn("_next_range_id", F.lead("range_id").over(order_window))
        .withColumn("_next_range_from", F.lead("range_from").over(order_window))
        .withColumn("_next_country_code", F.lead("country_code").over(order_window))
        .withColumn("_next_adjusted_score", F.lead("adjusted_score").over(order_window))
    )

    gaps = (
        neighbours
        .where(F.col("_next_range_from") > F.col("range_to"))
        .select(
            "entity_key",
            _stable_id(
                F.col("entity_key"),
                F.col("range_to"),
                F.col("_next_range_from"),
            ).alias("gap_id"),
            F.col("range_to").alias("gap_from"),
            F.col("_next_range_from").alias("gap_to"),
            F.col("range_id").alias("range_id_before"),
            F.col("_next_range_id").alias("range_id_after"),
            F.col("country_code").alias("country_before"),
            F.col("_next_country_code").alias("country_after"),
            F.col("adjusted_score").cast("double").alias("score_before"),
            F.col("_next_adjusted_score").cast("double").alias("score_after"),
        )
        .withColumn(
            "gap_duration_seconds",
            F.unix_timestamp("gap_to") - F.unix_timestamp("gap_from"),
        )
        .withColumn(
            "gap_duration_days",
            F.col("gap_duration_seconds") / F.lit(86400.0),
        )
    )

    if transition_anchors is None:
        classified = (
            gaps
            .withColumn("transition_count", F.lit(0).cast("long"))
            .withColumn("compatible_transition_count", F.lit(0).cast("long"))
            .withColumn("bridge_ts", F.lit(None).cast("timestamp"))
            .withColumn("bridge_weight", F.lit(None).cast("double"))
        )
    else:
        _require_columns(
            transition_anchors,
            _REQUIRED_TRANSITION_COLUMNS,
            "transition_anchors",
        )

        weight = (
            F.col("evidence_weight").cast("double")
            if "evidence_weight" in transition_anchors.columns
            else F.lit(None).cast("double")
        )

        transitions = (
            transition_anchors
            .select(
                "entity_key",
                F.col("transition_ts").cast("timestamp").alias("transition_ts"),
                "country_from",
                "country_to",
                weight.alias("evidence_weight"),
            )
            .where(
                F.col("transition_ts").isNotNull()
                & F.col("country_from").isNotNull()
                & F.col("country_to").isNotNull()
            )
            # Plusieurs sources peuvent décrire la même transition logique.
            .groupBy(
                "entity_key",
                "transition_ts",
                "country_from",
                "country_to",
            )
            .agg(F.max("evidence_weight").alias("evidence_weight"))
        )

        joined = gaps.alias("g").join(
            transitions.alias("t"),
            (F.col("g.entity_key") == F.col("t.entity_key"))
            & (F.col("t.transition_ts") >= F.col("g.gap_from"))
            & (F.col("t.transition_ts") <= F.col("g.gap_to")),
            "left",
        )

        compatible = (
            (F.col("t.country_from") == F.col("g.country_before"))
            & (F.col("t.country_to") == F.col("g.country_after"))
        )

        logical_transition = F.struct(
            F.col("t.transition_ts"),
            F.col("t.country_from"),
            F.col("t.country_to"),
        )

        transition_stats = (
            joined
            .groupBy(F.col("g.gap_id").alias("gap_id"))
            .agg(
                F.countDistinct(
                    F.when(F.col("t.transition_ts").isNotNull(), logical_transition)
                ).alias("transition_count"),
                F.countDistinct(
                    F.when(compatible, logical_transition)
                ).alias("compatible_transition_count"),
                F.min(F.when(compatible, F.col("t.transition_ts"))).alias("bridge_ts"),
                F.max(F.when(compatible, F.col("t.evidence_weight"))).alias("bridge_weight"),
            )
        )

        classified = (
            gaps.join(transition_stats, "gap_id", "left")
            .fillna(
                {
                    "transition_count": 0,
                    "compatible_transition_count": 0,
                }
            )
        )

    same_country_condition = (
        (F.col("country_before") == F.col("country_after"))
        & (F.col("transition_count") == 0)
    )

    explicit_bridge_condition = (
        (F.col("country_before") != F.col("country_after"))
        & (F.col("transition_count") == 1)
        & (F.col("compatible_transition_count") == 1)
    )

    same_country_gaps = classified.where(same_country_condition)
    explicit_bridge_gaps = classified.where(explicit_bridge_condition)

    same_country_score = F.least(
        F.col("score_before"),
        F.col("score_after"),
    )

    same_country_ranges = _project_like(
        same_country_gaps,
        resolved_ranges,
        {
            "entity_key": F.col("entity_key"),
            "range_id": _stable_id(F.col("gap_id"), F.lit("SAME_COUNTRY")),
            "range_from": F.col("gap_from"),
            "range_to": F.col("gap_to"),
            "country_code": F.col("country_before"),
            "relative_score": F.lit(1.0),
            "candidate_count": F.lit(1),
            "candidate_rank": F.lit(1),
            "adjusted_score": same_country_score,
            "adjusted_rank": F.lit(1),
            "original_country_code": F.col("country_before"),
            "original_adjusted_score": same_country_score,
            "resolution_method": F.lit("TEMPORAL_SAME_COUNTRY"),
        },
        extras={"gap_id": F.col("gap_id")},
    )
    same_country_ranges = _decorate_ranges(
        same_country_ranges,
        gap_id=F.col("gap_id"),
        is_inferred=True,
        method="TEMPORAL_SAME_COUNTRY",
        temporal_support_score=F.col("adjusted_score"),
    )

    bridge_before_ranges = _project_like(
        explicit_bridge_gaps.where(F.col("bridge_ts") > F.col("gap_from")),
        resolved_ranges,
        {
            "entity_key": F.col("entity_key"),
            "range_id": _stable_id(F.col("gap_id"), F.lit("TRANSITION_BEFORE")),
            "range_from": F.col("gap_from"),
            "range_to": F.col("bridge_ts"),
            "country_code": F.col("country_before"),
            "relative_score": F.lit(1.0),
            "candidate_count": F.lit(1),
            "candidate_rank": F.lit(1),
            "transition_support": F.col("bridge_weight"),
            "adjusted_score": F.col("score_before"),
            "adjusted_rank": F.lit(1),
            "original_country_code": F.col("country_before"),
            "original_adjusted_score": F.col("score_before"),
            "resolution_method": F.lit("EXPLICIT_TRANSITION"),
        },
        extras={"gap_id": F.col("gap_id")},
    )
    bridge_before_ranges = _decorate_ranges(
        bridge_before_ranges,
        gap_id=F.col("gap_id"),
        is_inferred=True,
        method="EXPLICIT_TRANSITION_BEFORE",
        temporal_support_score=F.col("adjusted_score"),
    )

    bridge_after_ranges = _project_like(
        explicit_bridge_gaps.where(F.col("bridge_ts") < F.col("gap_to")),
        resolved_ranges,
        {
            "entity_key": F.col("entity_key"),
            "range_id": _stable_id(F.col("gap_id"), F.lit("TRANSITION_AFTER")),
            "range_from": F.col("bridge_ts"),
            "range_to": F.col("gap_to"),
            "country_code": F.col("country_after"),
            "relative_score": F.lit(1.0),
            "candidate_count": F.lit(1),
            "candidate_rank": F.lit(1),
            "transition_support": F.col("bridge_weight"),
            "adjusted_score": F.col("score_after"),
            "adjusted_rank": F.lit(1),
            "original_country_code": F.col("country_after"),
            "original_adjusted_score": F.col("score_after"),
            "resolution_method": F.lit("EXPLICIT_TRANSITION"),
        },
        extras={"gap_id": F.col("gap_id")},
    )
    bridge_after_ranges = _decorate_ranges(
        bridge_after_ranges,
        gap_id=F.col("gap_id"),
        is_inferred=True,
        method="EXPLICIT_TRANSITION_AFTER",
        temporal_support_score=F.col("adjusted_score"),
    )

    original_ranges = _decorate_ranges(
        resolved_ranges,
        gap_id=F.lit(None),
        is_inferred=False,
        method="RESOLVED_RANGE",
        temporal_support_score=F.col("adjusted_score"),
    )

    filled_ranges = (
        original_ranges
        .unionByName(same_country_ranges, allowMissingColumns=True)
        .unionByName(bridge_before_ranges, allowMissingColumns=True)
        .unionByName(bridge_after_ranges, allowMissingColumns=True)
    )

    unresolved_gaps = (
        classified
        .where(~same_country_condition & ~explicit_bridge_condition)
        .withColumn(
            "unresolved_reason",
            F.when(
                (F.col("country_before") == F.col("country_after"))
                & (F.col("transition_count") > 0),
                F.lit("TRANSITION_INSIDE_SAME_COUNTRY_GAP"),
            )
            .when(
                (F.col("country_before") != F.col("country_after"))
                & (F.col("transition_count") == 0),
                F.lit("DIFFERENT_COUNTRIES_NO_TRANSITION"),
            )
            .when(
                F.col("compatible_transition_count") == 0,
                F.lit("INCOMPATIBLE_TRANSITION"),
            )
            .otherwise(F.lit("AMBIGUOUS_MULTIPLE_TRANSITIONS")),
        )
        .select(
            "entity_key",
            "gap_id",
            "gap_from",
            "gap_to",
            "gap_duration_seconds",
            "gap_duration_days",
            "range_id_before",
            "range_id_after",
            "country_before",
            "country_after",
            "score_before",
            "score_after",
            "transition_count",
            "compatible_transition_count",
            "unresolved_reason",
        )
    )

    return filled_ranges, unresolved_gaps


def attach_gap_candidates(
    filled_ranges: DataFrame,
    unresolved_gaps: DataFrame,
    build_gap_candidates,
) -> tuple[DataFrame, DataFrame]:
    """Branchement minimal à utiliser dans ``temporal_engine(stage1)``.

    ``build_gap_candidates`` doit désormais recevoir ``unresolved_gaps`` et
    non ``resolved_ranges``. Il reste ainsi l'unique responsable des pays
    plausibles, de la décroissance temporelle et de ``candidate_share``.
    """
    gap_candidates = build_gap_candidates(unresolved_gaps)
    resolved_timeline = filled_ranges.unionByName(
        gap_candidates,
        allowMissingColumns=True,
    )
    return gap_candidates, resolved_timeline
