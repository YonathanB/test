from pyspark.sql import functions as F
from pyspark.sql.window import Window


def extend_recompute_bounds(impacts, gold_segments):
    """
    impacts:
      entity_key, impacted_from, impacted_to

    gold_segments:
      entity_key, range_from, range_to

    Les intervalles sont supposés semi-ouverts :
      [range_from, range_to[
    """

    i = impacts.alias("i")
    g = gold_segments.alias("g")

    # 1. Segment précédent le plus proche :
    # celui dont la fin est la plus grande parmi les segments
    # terminés avant impacted_from.
    previous = (
        i.join(
            g,
            (F.col("i.entity_key") == F.col("g.entity_key"))
            & (F.col("g.range_to") <= F.col("i.impacted_from")),
            "left"
        )
        .select(
            F.col("i.entity_key").alias("entity_key"),
            F.col("g.range_from").alias("previous_from"),
            F.col("g.range_to").alias("previous_to")
        )
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("entity_key")
                .orderBy(F.col("previous_to").desc_nulls_last())
            )
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # 2. Segment suivant le plus proche :
    # celui dont le début est le plus petit parmi les segments
    # commençant après impacted_to.
    following = (
        i.join(
            g,
            (F.col("i.entity_key") == F.col("g.entity_key"))
            & (F.col("g.range_from") >= F.col("i.impacted_to")),
            "left"
        )
        .select(
            F.col("i.entity_key").alias("entity_key"),
            F.col("g.range_from").alias("following_from"),
            F.col("g.range_to").alias("following_to")
        )
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("entity_key")
                .orderBy(F.col("following_from").asc_nulls_last())
            )
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # 3. Segments existants chevauchant directement la période impactée.
    overlapping = (
        i.join(
            g,
            (F.col("i.entity_key") == F.col("g.entity_key"))
            & (F.col("g.range_from") < F.col("i.impacted_to"))
            & (F.col("g.range_to") > F.col("i.impacted_from")),
            "inner"
        )
        .select(
            F.col("i.entity_key").alias("entity_key"),
            F.col("g.range_from").alias("range_from"),
            F.col("g.range_to").alias("range_to")
        )
        .groupBy("entity_key")
        .agg(
            F.min("range_from").alias("overlapping_from"),
            F.max("range_to").alias("overlapping_to")
        )
    )

    return (
        impacts
        .join(previous, "entity_key", "left")
        .join(following, "entity_key", "left")
        .join(overlapping, "entity_key", "left")
        .withColumn(
            "recompute_from",
            F.least(
                F.col("impacted_from"),
                F.coalesce(
                    F.col("previous_from"),
                    F.col("impacted_from")
                ),
                F.coalesce(
                    F.col("overlapping_from"),
                    F.col("impacted_from")
                )
            )
        )
        .withColumn(
            "recompute_to",
            F.greatest(
                F.col("impacted_to"),
                F.coalesce(
                    F.col("following_to"),
                    F.col("impacted_to")
                ),
                F.coalesce(
                    F.col("overlapping_to"),
                    F.col("impacted_to")
                )
            )
        )
    )
