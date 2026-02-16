"""
Multi-Logic Convergence Engine — Scalable & Exact
===================================================
Identifies natural spatial clusters of camera positions across multiple
localization logics, without O(N²) blowup, with exact Haversine checks.

Architecture:
  1. Grid bucketing (cell = R meters) → candidate pairs only
  2. Haversine check on candidate pairs → exact edges
  3. Union-Find connected components → natural clusters
  4. Weighted centroid fusion per cluster
  5. Proportional scoring + bidirectional cross-references
"""

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession, Window


# =============================================================================
# HAVERSINE (Column expression, no UDF overhead)
# =============================================================================

def haversine_meters(lat1, lon1, lat2, lon2):
    """Returns a Column expression — computed by Spark, not Python."""
    r = F.lit(6_371_000.0)
    phi1, phi2 = F.radians(lat1), F.radians(lat2)
    dphi = F.radians(lat2 - lat1)
    dl = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dphi / 2), 2) + F.cos(phi1) * F.cos(phi2) * F.pow(F.sin(dl / 2), 2)
    return 2 * r * F.asin(F.sqrt(a))


# =============================================================================
# UNION-FIND UDF (scalar, not Pandas — runs on driver-side small data)
# =============================================================================

CC_RESULT = T.ArrayType(T.StructType([
    T.StructField("node", T.StringType(), False),
    T.StructField("cluster_id", T.StringType(), False),
]))


@F.udf(CC_RESULT)
def union_find_components(edges, all_nodes):
    """
    Given edges [(a,b), ...] and all node names,
    returns [(node, cluster_id), ...] where cluster_id = min(node) in component.
    Nodes with no edges get their own cluster_id = themselves.
    """
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]  # path compression
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            # always attach to lexicographically smaller root
            if ra > rb:
                ra, rb = rb, ra
            parent[rb] = ra

    # Initialize all nodes
    if all_nodes:
        for n in all_nodes:
            parent.setdefault(n, n)

    # Process edges
    if edges:
        for e in edges:
            union(e["a"], e["b"])

    # Build result
    result = []
    for n in parent:
        result.append({"node": n, "cluster_id": find(n)})
    return result


# =============================================================================
# CONVERGENCE ENGINE
# =============================================================================

class ConvergenceEngine:

    def __init__(self, config):
        self.config = config

    def score_and_merge(self, all_candidates: DataFrame, spark: SparkSession) -> DataFrame:
        c = self.config
        R = float(c.CONVERGENCE_RADIUS_METERS)
        MAIN = c.MAIN_LOGIC_NAME

        # =============================================================
        # STEP 0: Normalize input
        # =============================================================
        base = (
            all_candidates
            .filter(
                F.col("camera_id").isNotNull()
                & F.col("logic_name").isNotNull()
                & F.col("lat").isNotNull()
                & F.col("lon").isNotNull()
            )
            .withColumn("w", F.coalesce(F.col("logic_confidence"), F.lit(1.0)))
            .withColumn("_detail", F.struct(
                F.col("logic_name").alias("logic"),
                "lat", "lon", "logic_confidence",
                "num_evidence_points", "metadata",
            ))
        )

        # =============================================================
        # STEP 1: Separate mainLogic — it wins unconditionally
        # =============================================================
        has_main = (
            base.filter(F.col("logic_name") == MAIN)
            .select("camera_id").distinct()
        )

        main_candidates = base.join(has_main, "camera_id", "inner")
        non_main_candidates = base.join(has_main, "camera_id", "left_anti")

        # mainLogic row
        main_w = Window.partitionBy("camera_id").orderBy(
            F.col("logic_confidence").desc_nulls_last()
        )
        main_best = (
            main_candidates
            .filter(F.col("logic_name") == MAIN)
            .withColumn("rn", F.row_number().over(main_w))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

        # Other logics for cameras with mainLogic → alternative rows
        main_others = main_candidates.filter(F.col("logic_name") != MAIN)

        # Total logics per main camera
        main_totals = (
            main_candidates.groupBy("camera_id")
            .agg(F.count("*").alias("total_logics_for_camera"))
        )

        # All other logic names per main camera (for other_locations)
        main_other_names = (
            main_others.groupBy("camera_id")
            .agg(F.collect_set("logic_name").alias("_other_names"))
        )

        main_rows = (
            main_best
            .join(main_totals, "camera_id", "left")
            .join(main_other_names, "camera_id", "left")
            .select(
                "camera_id", "lat", "lon",
                F.array(F.lit(MAIN)).alias("contributing_logics"),
                F.lit(float(c.MAIN_LOGIC_SCORE)).alias("final_score"),
                F.lit(True).alias("is_best_location"),
                F.lit("primary").alias("convergence_group"),
                F.lit(1).alias("num_contributing_logics"),
                "total_logics_for_camera",
                F.lit("high").alias("confidence_flag"),
                F.array(F.col("_detail")).alias("logic_details"),
                F.coalesce(F.col("_other_names"), F.array().cast("array<string>"))
                    .alias("other_locations"),
            )
        )

        # Alternative rows for cameras with mainLogic
        main_alt_rows = (
            main_others
            .join(main_totals, "camera_id", "left")
            .select(
                "camera_id", "lat", "lon",
                F.array(F.col("logic_name")).alias("contributing_logics"),
                F.lit(float(c.BASE_SCORE_SINGLE_LOGIC)).alias("final_score"),
                F.lit(False).alias("is_best_location"),
                F.lit("alternative").alias("convergence_group"),
                F.lit(1).alias("num_contributing_logics"),
                "total_logics_for_camera",
                F.lit("low_confidence").alias("confidence_flag"),
                F.array(F.col("_detail")).alias("logic_details"),
                F.array(F.lit(MAIN)).alias("other_locations"),
            )
        )

        # =============================================================
        # STEP 2: For non-main cameras — Grid-accelerated clustering
        # =============================================================
        # If no non-main candidates, skip clustering
        if non_main_candidates.head(1) is None:
            return main_rows.unionByName(main_alt_rows)

        totals = (
            non_main_candidates.groupBy("camera_id")
            .agg(F.count("*").alias("total_logics_for_camera"))
        )

        # Grid cell assignment (approximate meter projection)
        cand = (
            non_main_candidates
            .withColumn("_cos_lat", F.cos(F.radians(F.col("lat"))))
            .withColumn("cell_x", F.floor(F.col("lon") * 111320.0 * F.col("_cos_lat") / R).cast("long"))
            .withColumn("cell_y", F.floor(F.col("lat") * 111320.0 / R).cast("long"))
        )

        # =============================================================
        # STEP 3: Candidate pairs from adjacent cells only
        # =============================================================
        # Self-join on (camera_id, cell proximity), then exact Haversine
        offsets = spark.createDataFrame(
            [(-1,-1),(-1,0),(-1,1),(0,-1),(0,0),(0,1),(1,-1),(1,0),(1,1)],
            ["dx", "dy"]
        ).hint("broadcast")

        left = cand.alias("a")
        right = (
            cand
            .crossJoin(offsets)
            .withColumn("ncell_x", F.col("cell_x") + F.col("dx"))
            .withColumn("ncell_y", F.col("cell_y") + F.col("dy"))
            .alias("b")
        )

        # Join: a's cell matches b's neighbor cell, same camera, different logic
        edges = (
            left.join(
                right,
                (F.col("a.camera_id") == F.col("b.camera_id"))
                & (F.col("a.cell_x") == F.col("b.ncell_x"))
                & (F.col("a.cell_y") == F.col("b.ncell_y"))
                & (F.col("a.logic_name") < F.col("b.logic_name")),
                "inner"
            )
            .withColumn(
                "dist",
                haversine_meters(
                    F.col("a.lat"), F.col("a.lon"),
                    F.col("b.lat"), F.col("b.lon")
                )
            )
            .filter(F.col("dist") <= R)
            .select(
                F.col("a.camera_id").alias("camera_id"),
                F.col("a.logic_name").alias("a"),
                F.col("b.logic_name").alias("b"),
            )
            .distinct()
        )

        # =============================================================
        # STEP 4: Connected components via Union-Find per camera
        # =============================================================
        # Collect edges + all node names per camera
        edges_per_cam = (
            edges.groupBy("camera_id")
            .agg(F.collect_list(F.struct("a", "b")).alias("edges"))
        )

        nodes_per_cam = (
            non_main_candidates.groupBy("camera_id")
            .agg(F.collect_set("logic_name").alias("all_nodes"))
        )

        cc_input = nodes_per_cam.join(edges_per_cam, "camera_id", "left")

        # Apply union-find
        cc_result = (
            cc_input
            .withColumn("_cc", union_find_components(
                F.coalesce(F.col("edges"), F.array().cast("array<struct<a:string,b:string>>")),
                F.col("all_nodes")
            ))
            .select("camera_id", F.explode("_cc").alias("_cc"))
            .select(
                "camera_id",
                F.col("_cc.node").alias("logic_name"),
                F.col("_cc.cluster_id").alias("cluster_id"),
            )
        )

        # =============================================================
        # STEP 5: Fuse clusters — one row per cluster
        # =============================================================
        clustered = (
            non_main_candidates
            .join(cc_result, ["camera_id", "logic_name"], "inner")
            .join(totals, "camera_id", "left")
        )

        fused = (
            clustered
            .groupBy("camera_id", "cluster_id", "total_logics_for_camera")
            .agg(
                (F.sum(F.col("lat") * F.col("w")) / F.sum("w")).alias("lat"),
                (F.sum(F.col("lon") * F.col("w")) / F.sum("w")).alias("lon"),
                F.sort_array(F.collect_set("logic_name")).alias("contributing_logics"),
                F.count("*").alias("num_contributing_logics"),
                F.collect_list("_detail").alias("logic_details"),
            )
        )

        # =============================================================
        # STEP 6: Score + roles
        # =============================================================
        # Proportional score
        fused = fused.withColumn(
            "final_score",
            F.when(
                F.col("num_contributing_logics") == F.col("total_logics_for_camera"),
                # All logics converge (even if just 1) — use ratio
                F.when(
                    F.col("total_logics_for_camera") == 1,
                    F.lit(float(c.BASE_SCORE_SINGLE_LOGIC))  # isolated
                ).otherwise(
                    F.least(
                        F.lit(float(c.MAX_SCORE_WITHOUT_MAIN)),
                        (F.col("num_contributing_logics") / F.col("total_logics_for_camera"))
                        * F.lit(float(c.MAX_SCORE_WITHOUT_MAIN))
                    )
                )
            ).otherwise(
                F.least(
                    F.lit(float(c.MAX_SCORE_WITHOUT_MAIN)),
                    (F.col("num_contributing_logics") / F.col("total_logics_for_camera"))
                    * F.lit(float(c.MAX_SCORE_WITHOUT_MAIN))
                )
            )
        )

        # Confidence
        fused = fused.withColumn(
            "confidence_flag",
            F.when(F.col("num_contributing_logics") >= 3, F.lit("high"))
             .when(F.col("num_contributing_logics") >= 2, F.lit("medium"))
             .otherwise(F.lit("low_confidence"))
        )

        # Convergence group: clusters with 2+ logics = primary, singletons = isolated
        fused = fused.withColumn(
            "convergence_group",
            F.when(F.col("num_contributing_logics") >= 2, F.lit("primary"))
             .otherwise(F.lit("isolated"))
        )

        # is_best_location: primary rows are best; isolated only if no primary exists
        has_primary = (
            fused.filter(F.col("convergence_group") == "primary")
            .select("camera_id").distinct()
            .withColumn("_has_primary", F.lit(True))
        )

        fused = (
            fused.join(has_primary, "camera_id", "left")
            .withColumn("_has_primary", F.coalesce(F.col("_has_primary"), F.lit(False)))
            .withColumn(
                "is_best_location",
                F.when(F.col("convergence_group") == "primary", F.lit(True))
                 .when(~F.col("_has_primary"), F.lit(True))  # isolated, no primary
                 .otherwise(F.lit(False))
            )
        )

        # =============================================================
        # STEP 7: Cross-references — other_locations per row
        # =============================================================
        all_logics_per_cam = (
            non_main_candidates.groupBy("camera_id")
            .agg(F.collect_set("logic_name").alias("_all_logics"))
        )

        fused = (
            fused.join(all_logics_per_cam, "camera_id", "left")
            .withColumn(
                "other_locations",
                F.array_except(F.col("_all_logics"), F.col("contributing_logics"))
            )
        )

        # =============================================================
        # STEP 8: Select + union
        # =============================================================
        cluster_rows = fused.select(
            "camera_id", "lat", "lon",
            "contributing_logics",
            "final_score",
            "is_best_location",
            "convergence_group",
            "num_contributing_logics",
            "total_logics_for_camera",
            "confidence_flag",
            "logic_details",
            "other_locations",
        )

        # Union: main + main_alternatives + clustered
        result = (
            main_rows
            .unionByName(main_alt_rows)
            .unionByName(cluster_rows)
        )

        # Convert arrays to strings for readability
        result = (
            result
            .withColumn("contributing_logics", F.concat_ws(",", F.col("contributing_logics")))
            .withColumn("other_locations", F.concat_ws(",", F.col("other_locations")))
        )

        return result
