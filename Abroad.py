from location_segments_v1 import build_location_segments

temporal = temporal_engine(stage1)

segment_outputs = build_location_segments(
    filled_ranges=temporal["filled_ranges"],
    gap_candidates=temporal["gap_candidates"],
)

location_segment = segment_outputs["location_segment"]



"""Construction de ``gold.location_segment`` au grain ``entity_key``.

Entrées :
    - ``filled_ranges`` : plages observées ou résolues avec certitude ;
    - ``gap_candidates`` : hypothèses calculées dans les trous ambigus.

Seul ``candidate_rank = 1`` entre dans le parcours principal. Les alternatives
restent reliées au segment correspondant pour préserver l'explicabilité sans
fabriquer plusieurs séjours simultanés.

Les intervalles sont semi-ouverts : [range_from, range_to).
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


_REQUIRED_FILLED_COLUMNS = {
    "entity_key",
    "range_id",
    "range_from",
    "range_to",
    "country_code",
}

_REQUIRED_GAP_CANDIDATE_COLUMNS = {
    "entity_key",
    "range_id",
    "range_from",
    "range_to",
    "country_code",
    "gap_id",
    "candidate_rank",
    "candidate_count",
    "candidate_share",
    "candidate_score",
    "distance_days",
    "supported_by",
}


def _require_columns(df: DataFrame, required: set[str], name: str) -> None:
    missing = sorted(required.difference(df.columns))
    if missing:
        raise ValueError(f"{name}: colonnes manquantes: {', '.join(missing)}")


def _stable_id(*columns):
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


def _column_or_null(df: DataFrame, name: str, data_type: str):
    if name in df.columns:
        return F.col(name).cast(data_type)
    return F.lit(None).cast(data_type)


def _column_or_value(
    df: DataFrame,
    name: str,
    default,
    data_type: str,
):
    if name in df.columns:
        return F.coalesce(F.col(name).cast(data_type), F.lit(default))
    return F.lit(default).cast(data_type)


def build_location_segments(
    filled_ranges: DataFrame,
    gap_candidates: DataFrame,
) -> dict[str, DataFrame]:
    """Construit les séjours continus et leurs liens d'explicabilité.

    Deux plages sont fusionnées uniquement si :
      - elles appartiennent au même ``entity_key`` ;
      - elles portent le même ``country_code`` ;
      - elles sont adjacentes ou se chevauchent.

    Un vrai trou temporel, même entre deux plages du même pays, démarre donc un
    nouveau segment. Aucun seuil arbitraire de tolérance n'est introduit ici.
    """
    _require_columns(
        filled_ranges,
        _REQUIRED_FILLED_COLUMNS,
        "filled_ranges",
    )
    _require_columns(
        gap_candidates,
        _REQUIRED_GAP_CANDIDATE_COLUMNS,
        "gap_candidates",
    )

    gap_winners = gap_candidates.where(F.col("candidate_rank") == 1)
    gap_alternatives = gap_candidates.where(F.col("candidate_rank") > 1)

    primary_timeline = filled_ranges.unionByName(
        gap_winners,
        allowMissingColumns=True,
    )

    prepared = (
        primary_timeline
        .withColumn(
            "_duration_seconds",
            F.unix_timestamp("range_to") - F.unix_timestamp("range_from"),
        )
        .withColumn(
            "_is_temporally_inferred",
            _column_or_value(
                primary_timeline,
                "is_temporally_inferred",
                False,
                "boolean",
            ),
        )
        .withColumn(
            "_is_gap_candidate",
            F.coalesce(
                _column_or_null(
                    primary_timeline,
                    "resolution_method",
                    "string",
                ) == F.lit("TEMPORAL_DECAY_CANDIDATE"),
                F.lit(False),
            ),
        )
        .withColumn(
            "_support_score",
            F.coalesce(
                _column_or_null(
                    primary_timeline,
                    "temporal_support_score",
                    "double",
                ),
                _column_or_null(
                    primary_timeline,
                    "adjusted_score",
                    "double",
                ),
            ),
        )
        .withColumn(
            "_candidate_share",
            _column_or_null(
                primary_timeline,
                "candidate_share",
                "double",
            ),
        )
        .withColumn(
            "_candidate_count",
            _column_or_value(
                primary_timeline,
                "candidate_count",
                1,
                "long",
            ),
        )
        .withColumn(
            "_evidence_count",
            _column_or_value(
                primary_timeline,
                "evidence_count",
                0,
                "long",
            ),
        )
        .withColumn(
            "_resolution_method",
            F.coalesce(
                _column_or_null(
                    primary_timeline,
                    "temporal_resolution_method",
                    "string",
                ),
                _column_or_null(
                    primary_timeline,
                    "resolution_method",
                    "string",
                ),
                F.lit("UNKNOWN"),
            ),
        )
    )

    invalid_condition = (
        F.col("entity_key").isNull()
        | F.col("range_id").isNull()
        | F.col("range_from").isNull()
        | F.col("range_to").isNull()
        | F.col("country_code").isNull()
        | (F.col("range_to") <= F.col("range_from"))
    )

    invalid_ranges = (
        prepared
        .where(invalid_condition)
        .withColumn(
            "invalid_reason",
            F.when(F.col("entity_key").isNull(), F.lit("NULL_ENTITY_KEY"))
            .when(F.col("range_id").isNull(), F.lit("NULL_RANGE_ID"))
            .when(F.col("range_from").isNull(), F.lit("NULL_RANGE_FROM"))
            .when(F.col("range_to").isNull(), F.lit("NULL_RANGE_TO"))
            .when(F.col("country_code").isNull(), F.lit("NULL_COUNTRY_CODE"))
            .otherwise(F.lit("NON_POSITIVE_DURATION")),
        )
    )

    valid_ranges = prepared.where(~invalid_condition)

    order_window = Window.partitionBy("entity_key").orderBy(
        F.col("range_from"),
        F.col("range_to"),
        F.col("range_id"),
    )

    ordered = (
        valid_ranges
        .withColumn("_previous_country", F.lag("country_code").over(order_window))
        .withColumn("_previous_range_to", F.lag("range_to").over(order_window))
    )

    overlap_anomalies = (
        ordered
        .where(
            F.col("_previous_range_to").isNotNull()
            & (F.col("range_from") < F.col("_previous_range_to"))
        )
        .withColumn(
            "overlap_type",
            F.when(
                F.col("country_code") == F.col("_previous_country"),
                F.lit("SAME_COUNTRY_OVERLAP"),
            ).otherwise(F.lit("DIFFERENT_COUNTRY_OVERLAP")),
        )
    )

    # Une frontière de segment apparaît au premier rang, lors d'un changement
    # de pays ou lorsqu'il reste un véritable espace entre deux plages.
    segmented = (
        ordered
        .withColumn(
            "_starts_new_segment",
            F.when(F.col("_previous_range_to").isNull(), F.lit(1))
            .when(
                F.col("country_code") != F.col("_previous_country"),
                F.lit(1),
            )
            .when(
                F.col("range_from") > F.col("_previous_range_to"),
                F.lit(1),
            )
            .otherwise(F.lit(0)),
        )
        .withColumn(
            "_segment_seq",
            F.sum("_starts_new_segment").over(
                order_window.rowsBetween(
                    Window.unboundedPreceding,
                    Window.currentRow,
                )
            ),
        )
    )

    segment_aggregates = (
        segmented
        .groupBy("entity_key", "_segment_seq", "country_code")
        .agg(
            F.min("range_from").alias("segment_from"),
            F.max("range_to").alias("segment_to"),
            F.count(F.lit(1)).alias("range_count"),
            F.sum("_duration_seconds").alias("covered_duration_seconds"),
            F.sum(
                F.col("_is_temporally_inferred").cast("long")
            ).alias("inferred_range_count"),
            F.sum(
                F.col("_is_gap_candidate").cast("long")
            ).alias("gap_candidate_range_count"),
            F.sum("_evidence_count").alias("evidence_count"),
            F.max("_candidate_count").alias("max_candidate_count"),
            F.min(
                F.when(
                    F.col("_is_gap_candidate"),
                    F.col("_candidate_share"),
                )
            ).alias("min_gap_candidate_share"),
            F.min("_support_score").alias("min_support_score"),
            F.sum(
                F.when(
                    F.col("_support_score").isNotNull(),
                    F.col("_support_score") * F.col("_duration_seconds"),
                ).otherwise(F.lit(0.0))
            ).alias("_weighted_support_sum"),
            F.sum(
                F.when(
                    F.col("_support_score").isNotNull(),
                    F.col("_duration_seconds"),
                ).otherwise(F.lit(0))
            ).alias("_weighted_support_duration"),
            F.sort_array(
                F.collect_set("_resolution_method")
            ).alias("resolution_methods"),
        )
        .withColumn(
            "segment_id",
            _stable_id(
                F.col("entity_key"),
                F.col("country_code"),
                F.col("segment_from"),
                F.col("segment_to"),
            ),
        )
        .withColumn(
            "segment_duration_seconds",
            F.unix_timestamp("segment_to")
            - F.unix_timestamp("segment_from"),
        )
        .withColumn(
            "weighted_support_score",
            F.when(
                F.col("_weighted_support_duration") > 0,
                F.col("_weighted_support_sum")
                / F.col("_weighted_support_duration"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "has_temporal_inference",
            F.col("inferred_range_count") > 0,
        )
        .withColumn(
            "is_fully_inferred",
            F.col("inferred_range_count") == F.col("range_count"),
        )
        .withColumn(
            "has_gap_alternatives",
            F.col("max_candidate_count") > 1,
        )
    )

    location_segments = segment_aggregates.select(
        "segment_id",
        "entity_key",
        "country_code",
        "segment_from",
        "segment_to",
        "segment_duration_seconds",
        "covered_duration_seconds",
        "range_count",
        "inferred_range_count",
        "gap_candidate_range_count",
        "evidence_count",
        "min_support_score",
        "weighted_support_score",
        "max_candidate_count",
        "min_gap_candidate_share",
        "has_temporal_inference",
        "is_fully_inferred",
        "has_gap_alternatives",
        "resolution_methods",
    )

    segment_keys = segment_aggregates.select(
        "entity_key",
        "_segment_seq",
        "segment_id",
    )

    linked_primary_ranges = segmented.join(
        segment_keys,
        ["entity_key", "_segment_seq"],
        "inner",
    )

    link_order_window = Window.partitionBy("segment_id").orderBy(
        F.col("range_from"),
        F.col("range_to"),
        F.col("range_id"),
    )

    link_columns = [
        F.col("segment_id"),
        F.col("entity_key"),
        F.col("range_id"),
        _column_or_null(linked_primary_ranges, "gap_id", "string").alias("gap_id"),
        F.col("range_from"),
        F.col("range_to"),
        F.col("country_code"),
        _column_or_null(
            linked_primary_ranges,
            "adjusted_score",
            "double",
        ).alias("adjusted_score"),
        _column_or_null(
            linked_primary_ranges,
            "candidate_share",
            "double",
        ).alias("candidate_share"),
        _column_or_null(
            linked_primary_ranges,
            "candidate_rank",
            "long",
        ).alias("candidate_rank"),
        _column_or_null(
            linked_primary_ranges,
            "candidate_count",
            "long",
        ).alias("candidate_count"),
        _column_or_null(
            linked_primary_ranges,
            "resolution_method",
            "string",
        ).alias("resolution_method"),
        _column_or_null(
            linked_primary_ranges,
            "temporal_resolution_method",
            "string",
        ).alias("temporal_resolution_method"),
        _column_or_value(
            linked_primary_ranges,
            "is_temporally_inferred",
            False,
            "boolean",
        ).alias("is_temporally_inferred"),
    ]

    segment_range_links = (
        linked_primary_ranges
        .select(*link_columns)
        .withColumn(
            "range_order",
            F.row_number().over(link_order_window),
        )
    )

    # Une alternative est rattachée au segment dont la plage gagnante couvre
    # exactement la même tranche du même gap.
    primary_gap_slices = (
        segment_range_links
        .where(F.col("gap_id").isNotNull())
        .select(
            "segment_id",
            "entity_key",
            "gap_id",
            "range_from",
            "range_to",
        )
        .distinct()
    )

    segment_candidate_links = (
        gap_alternatives.alias("a")
        .join(
            primary_gap_slices.alias("p"),
            (F.col("a.entity_key") == F.col("p.entity_key"))
            & (F.col("a.gap_id") == F.col("p.gap_id"))
            & (F.col("a.range_from") == F.col("p.range_from"))
            & (F.col("a.range_to") == F.col("p.range_to")),
            "left",
        )
        .select(
            F.col("p.segment_id").alias("segment_id"),
            F.col("a.entity_key").alias("entity_key"),
            F.col("a.gap_id").alias("gap_id"),
            F.col("a.range_id").alias("candidate_range_id"),
            F.col("a.range_from").alias("range_from"),
            F.col("a.range_to").alias("range_to"),
            F.col("a.country_code").alias("alternative_country_code"),
            F.col("a.candidate_score").alias("candidate_score"),
            F.col("a.candidate_share").alias("candidate_share"),
            F.col("a.candidate_rank").alias("candidate_rank"),
            F.col("a.distance_days").alias("distance_days"),
            F.col("a.supported_by").alias("supported_by"),
        )
    )

    return {
        "primary_timeline": primary_timeline,
        "location_segment": location_segments,
        "location_segment_range_link": segment_range_links,
        "location_segment_candidate_link": segment_candidate_links,
        "invalid_ranges": invalid_ranges,
        "overlap_anomalies": overlap_anomalies,
    }
