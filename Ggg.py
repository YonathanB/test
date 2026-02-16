"""
Convergence Engine — Version corrigée (changements minimaux)
=============================================================
Corrections par rapport à l'original :
  1. PHASE 3: localCheckpoint() sur conv_agg pour éviter le bug de plan partagé
  2. PHASE 5: Remplacement du "primary_anchor" par des composantes connexes
             (label propagation) pour gérer plusieurs clusters par caméra
  3. PHASE 6: Tous les clusters primary ont is_best=True (pas un seul)
"""

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType, IntegerType
import math


# =============================================================================
# CONFIG
# =============================================================================

class Config:
    CONVERGENCE_RADIUS_METERS = 100
    MAIN_LOGIC_NAME = "mainLogic"
    MAIN_LOGIC_SCORE = 100.0
    BASE_SCORE_SINGLE_LOGIC = 20.0
    CONVERGENCE_BONUS_PER_LOGIC = 15.0
    MAX_SCORE_WITHOUT_MAIN = 85.0


# =============================================================================
# HAVERSINE UDF
# =============================================================================

@F.udf(DoubleType())
def haversine_meters(lat1, lon1, lat2, lon2):
    if any(v is None for v in [lat1, lon1, lat2, lon2]):
        return None
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = (math.sin(dphi / 2) ** 2
         + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# =============================================================================
# CONVERGENCE ENGINE
# =============================================================================

class ConvergenceEngine:

    def __init__(self, config: Config):
        self.config = config

    def score_and_merge(self, all_candidates: DataFrame, spark: SparkSession) -> DataFrame:
        config = self.config

        # =================================================================
        # PHASE 1: Assign a unique row_id to every candidate
        # =================================================================
        candidates = all_candidates.withColumn(
            "row_id",
            F.monotonically_increasing_id()
        )

        # =================================================================
        # PHASE 2: Pairwise distances (ALL logics, ALL cameras)
        # =================================================================
        left = candidates.alias("a")
        right = candidates.alias("b")

        pairs = (
            left.join(
                right,
                (F.col("a.camera_id") == F.col("b.camera_id"))
                & (F.col("a.row_id") < F.col("b.row_id")),
                "inner"
            )
            .withColumn(
                "distance_m",
                haversine_meters(
                    F.col("a.lat"), F.col("a.lon"),
                    F.col("b.lat"), F.col("b.lon")
                )
            )
            .withColumn(
                "within_radius",
                F.col("distance_m") <= config.CONVERGENCE_RADIUS_METERS
            )
            .select(
                F.col("a.camera_id").alias("camera_id"),
                F.col("a.logic_name").alias("logic_a"),
                F.col("b.logic_name").alias("logic_b"),
                "distance_m",
                "within_radius",
            )
        )

        # =================================================================
        # PHASE 3: Build convergence/divergence per logic
        #
        # FIX: localCheckpoint() sur conv_agg et div_agg pour couper le
        # lineage partagé avec candidates. Sans ça, le left join en
        # PHASE 4 peut produire des valeurs fantômes.
        # =================================================================

        conv_from_a = (
            pairs.filter(F.col("within_radius"))
            .select("camera_id",
                    F.col("logic_a").alias("logic_name"),
                    F.col("logic_b").alias("converges_with_logic"))
        )
        conv_from_b = (
            pairs.filter(F.col("within_radius"))
            .select("camera_id",
                    F.col("logic_b").alias("logic_name"),
                    F.col("logic_a").alias("converges_with_logic"))
        )
        all_convergences = conv_from_a.unionByName(conv_from_b)

        conv_agg = (
            all_convergences
            .groupBy("camera_id", "logic_name")
            .agg(
                F.collect_set("converges_with_logic").alias("converges_with"),
                F.count("*").alias("num_converging"),
            )
            .localCheckpoint()  # FIX: coupe le lineage
        )

        div_from_a = (
            pairs.filter(~F.col("within_radius"))
            .select("camera_id",
                    F.col("logic_a").alias("logic_name"),
                    F.col("logic_b").alias("diverges_from_logic"))
        )
        div_from_b = (
            pairs.filter(~F.col("within_radius"))
            .select("camera_id",
                    F.col("logic_b").alias("logic_name"),
                    F.col("logic_a").alias("diverges_from_logic"))
        )

        div_agg = (
            div_from_a.unionByName(div_from_b)
            .groupBy("camera_id", "logic_name")
            .agg(F.collect_set("diverges_from_logic").alias("diverges_from"))
            .localCheckpoint()  # FIX: coupe le lineage
        )

        # =================================================================
        # PHASE 4: Enrich every candidate row
        # =================================================================
        enriched = (
            candidates
            .join(conv_agg, ["camera_id", "logic_name"], "left")
            .join(div_agg, ["camera_id", "logic_name"], "left")
            .withColumn("converges_with",
                        F.coalesce(F.col("converges_with"), F.array()))
            .withColumn("diverges_from",
                        F.coalesce(F.col("diverges_from"), F.array()))
            .withColumn("num_converging",
                        F.coalesce(F.col("num_converging"), F.lit(0)))
        )

        # Count logics per camera
        logic_count_per_camera = (
            candidates.groupBy("camera_id")
            .agg(F.count("*").alias("total_logics_for_camera"))
        )
        enriched = enriched.join(logic_count_per_camera, "camera_id", "left")

        # Has mainLogic?
        has_main = (
            candidates
            .filter(F.col("logic_name") == config.MAIN_LOGIC_NAME)
            .select("camera_id").distinct()
            .withColumn("has_main_logic", F.lit(True))
        )
        enriched = (
            enriched.join(has_main, "camera_id", "left")
            .withColumn("has_main_logic",
                        F.coalesce(F.col("has_main_logic"), F.lit(False)))
        )

        # =================================================================
        # PHASE 5: Connected components via label propagation
        #
        # FIX: Remplace le "primary_anchor" qui ne gérait qu'un seul
        # groupe. Maintenant chaque cluster convergent reçoit son propre
        # cluster_id (= min(logic_name) dans la composante connexe).
        # A↔B et C↔D donnent deux clusters distincts.
        # =================================================================

        # Edges: convergence pairs (bidirectional)
        edges = (
            all_convergences
            .select(
                "camera_id",
                F.col("logic_name").alias("node"),
                F.col("converges_with_logic").alias("neighbor")
            )
        )

        # All nodes that have at least one convergence
        converging_nodes = (
            all_convergences
            .select("camera_id", F.col("logic_name").alias("node"))
            .distinct()
        )

        # Initialize: each node's label = its own name
        labels = converging_nodes.withColumn("cluster_id", F.col("node"))

        # Iterate: propagate min label through edges
        for _ in range(10):
            neighbor_labels = (
                edges.alias("e")
                .join(
                    labels.alias("l"),
                    (F.col("e.camera_id") == F.col("l.camera_id"))
                    & (F.col("e.neighbor") == F.col("l.node")),
                    "inner"
                )
                .select(
                    F.col("e.camera_id"),
                    F.col("e.node"),
                    F.col("l.cluster_id").alias("neighbor_label")
                )
            )

            labels = (
                labels
                .unionByName(
                    neighbor_labels.select(
                        "camera_id", "node",
                        F.col("neighbor_label").alias("cluster_id")
                    )
                )
                .groupBy("camera_id", "node")
                .agg(F.min("cluster_id").alias("cluster_id"))
            )

        # Join cluster_id back to enriched
        enriched = (
            enriched
            .join(
                labels.select(
                    F.col("camera_id").alias("_cc_cam"),
                    F.col("node").alias("_cc_logic"),
                    "cluster_id"
                ),
                (F.col("camera_id") == F.col("_cc_cam"))
                & (F.col("logic_name") == F.col("_cc_logic")),
                "left"
            )
            .drop("_cc_cam", "_cc_logic")
        )

        # =================================================================
        # PHASE 6: Assign convergence_group, score, is_best, cross-refs
        # =================================================================

        # Role assignment: mainLogic first, then cluster, then alternative/isolated
        scored = enriched.withColumn(
            "convergence_group",
            F.when(
                F.col("has_main_logic") & (F.col("logic_name") == config.MAIN_LOGIC_NAME),
                F.lit("primary")
            ).when(
                F.col("has_main_logic"),
                F.lit("alternative")  # mainLogic écrase tout
            ).when(
                F.col("cluster_id").isNotNull(),
                F.lit("primary")      # member of a convergence cluster
            ).otherwise(
                F.lit("isolated")
            )
        )

        # Score
        scored = scored.withColumn(
            "final_score",
            F.when(
                F.col("logic_name") == config.MAIN_LOGIC_NAME,
                F.lit(config.MAIN_LOGIC_SCORE)
            ).when(
                F.col("convergence_group") == "primary",
                F.least(
                    F.lit(config.MAX_SCORE_WITHOUT_MAIN),
                    F.lit(config.BASE_SCORE_SINGLE_LOGIC)
                    + F.lit(config.CONVERGENCE_BONUS_PER_LOGIC) * F.col("num_converging")
                )
            ).when(
                F.col("convergence_group") == "alternative",
                F.lit(config.BASE_SCORE_SINGLE_LOGIC)
            ).otherwise(
                F.lit(config.BASE_SCORE_SINGLE_LOGIC)  # isolated
            )
        )

        # is_best_location:
        #   - all primary rows = True
        #   - isolated = True ONLY if no primary exists for this camera
        has_primary = (
            scored.filter(F.col("convergence_group") == "primary")
            .select("camera_id").distinct()
            .withColumn("_has_primary", F.lit(True))
        )
        scored = (
            scored.join(has_primary, "camera_id", "left")
            .withColumn("_has_primary",
                        F.coalesce(F.col("_has_primary"), F.lit(False)))
            .withColumn(
                "is_best_location",
                F.when(F.col("convergence_group") == "primary", F.lit(True))
                 .when(~F.col("_has_primary") & (F.col("convergence_group") == "isolated"),
                       F.lit(True))
                 .otherwise(F.lit(False))
            )
        )

        # =================================================================
        # PHASE 7: Cross-references (bidirectional)
        # =================================================================

        best_logics_per_camera = (
            scored
            .filter(F.col("convergence_group") == "primary")
            .groupBy("camera_id")
            .agg(F.collect_set("logic_name").alias("best_location_logics"))
        )

        alt_logics_per_camera = (
            scored
            .filter(F.col("convergence_group").isin("alternative", "isolated"))
            .groupBy("camera_id")
            .agg(F.collect_set("logic_name").alias("alternative_location_logics"))
        )

        scored = (
            scored
            .join(best_logics_per_camera, "camera_id", "left")
            .join(alt_logics_per_camera, "camera_id", "left")
        )

        scored = (
            scored
            .withColumn(
                "alternative_locations",
                F.when(
                    F.col("convergence_group") == "primary",
                    F.coalesce(F.col("alternative_location_logics"), F.array())
                ).otherwise(F.array())
            )
            .withColumn(
                "best_location_logics",
                F.when(
                    F.col("convergence_group").isin("alternative", "isolated"),
                    F.coalesce(F.col("best_location_logics"), F.array())
                ).otherwise(F.array())
            )
        )

        # Confidence flag
        scored = scored.withColumn(
            "confidence_flag",
            F.when(F.col("logic_name") == config.MAIN_LOGIC_NAME, F.lit("high"))
             .when((F.col("convergence_group") == "primary") & (F.col("num_converging") >= 2),
                   F.lit("high"))
             .when((F.col("convergence_group") == "primary") & (F.col("num_converging") == 1),
                   F.lit("medium"))
             .when(F.col("convergence_group") == "alternative", F.lit("low_confidence"))
             .otherwise(F.lit("low_confidence"))
        )

        # =================================================================
        # PHASE 8: Final select
        # =================================================================
        final = (
            scored
            .withColumn("converges_with_str", F.concat_ws(",", "converges_with"))
            .withColumn("alternative_locations_str", F.concat_ws(",", "alternative_locations"))
            .withColumn("best_location_logics_str", F.concat_ws(",", "best_location_logics"))
            .select(
                "camera_id",
                "lat",
                "lon",
                "logic_name",
                "logic_confidence",
                "num_evidence_points",
                "final_score",
                "is_best_location",
                "convergence_group",
                "cluster_id",
                F.col("converges_with_str").alias("converges_with"),
                F.col("alternative_locations_str").alias("alternative_locations"),
                F.col("best_location_logics_str").alias("best_location_logics"),
                "num_converging",
                "total_logics_for_camera",
                "confidence_flag",
                "metadata",
            )
        )

        return final
