"""
Multi-Logic Camera Geolocation Framework
==========================================
Extensible framework where N independent localization logics each produce
candidate locations for cameras. A convergence engine scores results based
on how many logics agree.

Rules:
  - mainLogic fires → score = 100, all other logics for that camera are DROPPED
  - Otherwise: all logics kept, convergence within a radius boosts score
  - Non-converging cameras flagged as 'low_confidence'

Usage:
  spark-submit multi_logic_geolocation.py --date 2025-06-15

Adding a new logic:
  1. Create a class that extends BaseLocalizationLogic
  2. Implement the `run()` method → returns DataFrame[camera_id, lat, lon, logic_name, ...]
  3. Register it in the LOGIC_REGISTRY
"""

from abc import ABC, abstractmethod
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, FloatType, DateType, TimestampType
)
import math
from datetime import datetime
import argparse


# =============================================================================
# CONFIGURATION
# =============================================================================

class Config:
    # Convergence radius in meters — two logics agreeing within this = convergence
    CONVERGENCE_RADIUS_METERS = 100

    # Scoring
    MAIN_LOGIC_SCORE = 100.0          # mainLogic always gets this score
    BASE_SCORE_SINGLE_LOGIC = 20.0    # isolated logic (no comparison) or alternative
    MAX_SCORE_WITHOUT_MAIN = 85.0     # cap — only mainLogic can reach 100
    # Proportional: score = (num_contributing / total_logics) × MAX_SCORE_WITHOUT_MAIN
    # Isolated (1 logic, no verification) stays at BASE_SCORE_SINGLE_LOGIC

    # GPS linking
    GPS_WINDOW_SECONDS = 900
    TEMPORAL_DECAY_HALF_LIFE = 300

    # CGNAT
    CGNAT_USER_THRESHOLD = 5

    # Spatial quality
    MIN_SESSIONS = 3
    MAX_SPATIAL_IQR_METERS = 200

    # Paths
    CAMERA_LOGS_PATH = "/data/raw/camera_logs"
    PHONE_WIFI_PATH = "/data/raw/phone_wifi_logs"
    PHONE_GPS_PATH = "/data/raw/phone_gps"
    SILVER_PATH = "/data/silver/camera_locations"
    GOLD_PATH = "/data/gold/camera_locations"

    # Main logic name — this is the authority
    MAIN_LOGIC_NAME = "mainLogic"


# =============================================================================
# STANDARD OUTPUT SCHEMA — All logics MUST produce this
# =============================================================================

LOGIC_OUTPUT_SCHEMA = StructType([
    StructField("camera_id", StringType(), False),
    StructField("lat", DoubleType(), False),
    StructField("lon", DoubleType(), False),
    StructField("logic_name", StringType(), False),
    StructField("logic_confidence", DoubleType(), True),   # 0.0 - 1.0, logic's self-assessed confidence
    StructField("num_evidence_points", IntegerType(), True),
    StructField("metadata", StringType(), True),            # JSON string for logic-specific info
])


# =============================================================================
# UDF — Haversine
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
# BASE CLASS — All logics extend this
# =============================================================================

class BaseLocalizationLogic(ABC):
    """
    Abstract base class for a localization logic.
    Each logic independently attempts to locate cameras.

    Subclasses must implement:
      - name: str property
      - run(spark, run_date, config) -> DataFrame matching LOGIC_OUTPUT_SCHEMA
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name for this logic. 'mainLogic' has special authority."""
        pass

    @abstractmethod
    def run(self, spark: SparkSession, run_date: datetime, config: Config) -> DataFrame:
        """
        Execute this logic and return candidate locations.

        Must return a DataFrame with columns:
          camera_id, lat, lon, logic_name, logic_confidence,
          num_evidence_points, metadata
        """
        pass

    def validate_output(self, df: DataFrame) -> DataFrame:
        """Ensure the output conforms to the expected schema."""
        required_cols = {"camera_id", "lat", "lon", "logic_name"}
        actual_cols = set(df.columns)
        missing = required_cols - actual_cols
        if missing:
            raise ValueError(f"Logic '{self.name}' output missing columns: {missing}")
        return df


# =============================================================================
# LOGIC 1: mainLogic — ISP Radius ID Co-location (Highest Authority)
# =============================================================================

class MainLogic(BaseLocalizationLogic):
    """
    The authoritative logic. Matches cameras to phones via User_Radius_ID
    (same ISP account = same household). Links to GPS with temporal proximity.

    When this logic fires for a camera, its score is 100 and all other
    logics for that camera are discarded.
    """

    @property
    def name(self) -> str:
        return "mainLogic"

    def run(self, spark, run_date, config):
        date_str = run_date.strftime("%Y-%m-%d")

        camera_df = spark.read.parquet(config.CAMERA_LOGS_PATH)
        phone_wifi_df = spark.read.parquet(config.PHONE_WIFI_PATH)
        phone_gps_df = spark.read.parquet(config.PHONE_GPS_PATH)

        # Camera has Radius ID → match to phone with same Radius ID on same IP
        cam_day = (
            camera_df
            .filter(F.to_date("timestamp") == F.lit(date_str))
            .filter(F.col("User_Radius_ID").isNotNull())
        )

        phone_day = phone_wifi_df.filter(F.to_date("timestamp") == F.lit(date_str))
        gps_day = phone_gps_df.filter(F.to_date("timestamp") == F.lit(date_str))

        # Network match on Radius ID + IP
        matches = (
            cam_day.alias("c")
            .join(
                phone_day.alias("p"),
                (F.col("c.User_Radius_ID") == F.col("p.User_Radius_ID"))
                & (F.col("c.ip") == F.col("p.ip")),
                "inner"
            )
            .select(
                F.col("c.ip").alias("camera_id"),
                F.col("p.timestamp").alias("phone_timestamp"),
                F.col("p.User_Radius_ID").alias("phone_radius_id"),
            )
        )

        # Link to GPS within time window
        matches_ts = matches.withColumn("phone_epoch", F.col("phone_timestamp").cast("long"))
        gps_ts = gps_day.withColumn("gps_epoch", F.col("timestamp").cast("long"))

        gps_linked = (
            matches_ts.alias("m")
            .join(
                gps_ts.alias("g"),
                (F.col("m.phone_radius_id") == F.col("g.User_Radius_ID"))
                & (F.abs(F.col("g.gps_epoch") - F.col("m.phone_epoch")) <= config.GPS_WINDOW_SECONDS),
                "inner"
            )
            .withColumn("time_delta", F.abs(F.col("g.gps_epoch") - F.col("m.phone_epoch")))
        )

        # Keep closest GPS per match
        w = Window.partitionBy("camera_id", "phone_timestamp").orderBy("time_delta")
        best = (
            gps_linked
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
        )

        # Aggregate per camera — median centroid
        result = (
            best
            .groupBy("camera_id")
            .agg(
                F.percentile_approx("g.lat", 0.5).alias("lat"),
                F.percentile_approx("g.lon", 0.5).alias("lon"),
                F.count("*").alias("num_evidence_points"),
                F.avg(
                    F.exp(-F.log(F.lit(2.0)) * F.col("time_delta") / F.lit(config.TEMPORAL_DECAY_HALF_LIFE))
                ).alias("logic_confidence"),
            )
            .filter(F.col("num_evidence_points") >= config.MIN_SESSIONS)
            .withColumn("logic_name", F.lit(self.name))
            .withColumn("logic_confidence", F.least(F.lit(1.0), F.col("logic_confidence")))
            .withColumn("metadata", F.lit('{"method": "radius_id_colocation"}'))
            .select("camera_id", "lat", "lon", "logic_name",
                    "logic_confidence", "num_evidence_points", "metadata")
        )

        return self.validate_output(result)


# =============================================================================
# LOGIC 2: Residential IP Co-location (Tier 2)
# =============================================================================

class ResidentialIPLogic(BaseLocalizationLogic):
    """
    For cameras WITHOUT a Radius ID: match by IP, but only on IPs
    classified as residential (not CGNAT).
    """

    @property
    def name(self) -> str:
        return "residentialIPLogic"

    def run(self, spark, run_date, config):
        date_str = run_date.strftime("%Y-%m-%d")

        camera_df = spark.read.parquet(config.CAMERA_LOGS_PATH)
        phone_wifi_df = spark.read.parquet(config.PHONE_WIFI_PATH)
        phone_gps_df = spark.read.parquet(config.PHONE_GPS_PATH)

        # Profile IPs
        ip_profile = (
            phone_wifi_df
            .filter(F.col("User_Radius_ID").isNotNull())
            .filter(F.to_date("timestamp") == F.lit(date_str))
            .groupBy("ip")
            .agg(F.countDistinct("User_Radius_ID").alias("distinct_users"))
            .filter(F.col("distinct_users") <= config.CGNAT_USER_THRESHOLD)
            .select("ip")
        )

        cam_day = (
            camera_df
            .filter(F.to_date("timestamp") == F.lit(date_str))
            .filter(F.col("User_Radius_ID").isNull())
        )

        phone_day = phone_wifi_df.filter(F.to_date("timestamp") == F.lit(date_str))
        gps_day = phone_gps_df.filter(F.to_date("timestamp") == F.lit(date_str))

        # IP match on residential IPs only
        matches = (
            cam_day.alias("c")
            .join(ip_profile.alias("ipr"), F.col("c.ip") == F.col("ipr.ip"), "inner")
            .join(phone_day.alias("p"), F.col("c.ip") == F.col("p.ip"), "inner")
            .select(
                F.col("c.ip").alias("camera_id"),
                F.col("p.timestamp").alias("phone_timestamp"),
                F.col("p.User_Radius_ID").alias("phone_radius_id"),
            )
        )

        # GPS linking (same pattern as mainLogic)
        matches_ts = matches.withColumn("phone_epoch", F.col("phone_timestamp").cast("long"))
        gps_ts = gps_day.withColumn("gps_epoch", F.col("timestamp").cast("long"))

        gps_linked = (
            matches_ts.alias("m")
            .join(
                gps_ts.alias("g"),
                (F.col("m.phone_radius_id") == F.col("g.User_Radius_ID"))
                & (F.abs(F.col("g.gps_epoch") - F.col("m.phone_epoch")) <= config.GPS_WINDOW_SECONDS),
                "inner"
            )
            .withColumn("time_delta", F.abs(F.col("g.gps_epoch") - F.col("m.phone_epoch")))
        )

        w = Window.partitionBy("camera_id", "phone_timestamp").orderBy("time_delta")
        best = gps_linked.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)

        result = (
            best
            .groupBy("camera_id")
            .agg(
                F.percentile_approx("g.lat", 0.5).alias("lat"),
                F.percentile_approx("g.lon", 0.5).alias("lon"),
                F.count("*").alias("num_evidence_points"),
                F.avg(
                    F.exp(-F.log(F.lit(2.0)) * F.col("time_delta") / F.lit(config.TEMPORAL_DECAY_HALF_LIFE))
                ).alias("logic_confidence"),
            )
            .filter(F.col("num_evidence_points") >= config.MIN_SESSIONS)
            .withColumn("logic_name", F.lit(self.name))
            .withColumn("logic_confidence",
                        F.least(F.lit(1.0), F.col("logic_confidence") * 0.7))  # penalize vs mainLogic
            .withColumn("metadata", F.lit('{"method": "residential_ip_colocation"}'))
            .select("camera_id", "lat", "lon", "logic_name",
                    "logic_confidence", "num_evidence_points", "metadata")
        )

        return self.validate_output(result)


# =============================================================================
# LOGIC 3: Recurring Co-location Pattern (Multi-day)
# =============================================================================

class RecurringColocationLogic(BaseLocalizationLogic):
    """
    Looks at the past 7 days of IP co-location data (any tier).
    If a camera repeatedly appears on the same network as phones that
    cluster around the same GPS point across multiple days, that's a
    strong signal even if each individual day is weak.
    """

    @property
    def name(self) -> str:
        return "recurringColocationLogic"

    def run(self, spark, run_date, config):
        from datetime import timedelta

        camera_df = spark.read.parquet(config.CAMERA_LOGS_PATH)
        phone_wifi_df = spark.read.parquet(config.PHONE_WIFI_PATH)
        phone_gps_df = spark.read.parquet(config.PHONE_GPS_PATH)

        start_date = (run_date - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = run_date.strftime("%Y-%m-%d")

        # 7-day window
        cam_window = camera_df.filter(
            (F.to_date("timestamp") >= F.lit(start_date))
            & (F.to_date("timestamp") <= F.lit(end_date))
        )
        phone_window = phone_wifi_df.filter(
            (F.to_date("timestamp") >= F.lit(start_date))
            & (F.to_date("timestamp") <= F.lit(end_date))
        )
        gps_window = phone_gps_df.filter(
            (F.to_date("timestamp") >= F.lit(start_date))
            & (F.to_date("timestamp") <= F.lit(end_date))
        )

        # Match on IP (both with and without Radius ID)
        matches = (
            cam_window.alias("c")
            .join(phone_window.alias("p"), F.col("c.ip") == F.col("p.ip"), "inner")
            .filter(F.col("p.User_Radius_ID").isNotNull())
            .select(
                F.col("c.ip").alias("camera_id"),
                F.col("p.timestamp").alias("phone_timestamp"),
                F.col("p.User_Radius_ID").alias("phone_radius_id"),
                F.to_date("c.timestamp").alias("match_date"),
            )
        )

        # GPS link
        matches_ts = matches.withColumn("phone_epoch", F.col("phone_timestamp").cast("long"))
        gps_ts = gps_window.withColumn("gps_epoch", F.col("timestamp").cast("long"))

        gps_linked = (
            matches_ts.alias("m")
            .join(
                gps_ts.alias("g"),
                (F.col("m.phone_radius_id") == F.col("g.User_Radius_ID"))
                & (F.abs(F.col("g.gps_epoch") - F.col("m.phone_epoch")) <= config.GPS_WINDOW_SECONDS),
                "inner"
            )
            .withColumn("time_delta", F.abs(F.col("g.gps_epoch") - F.col("m.phone_epoch")))
        )

        w = Window.partitionBy("camera_id", "phone_timestamp").orderBy("time_delta")
        best = gps_linked.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)

        # Require data from at least 3 distinct days
        result = (
            best
            .groupBy("camera_id")
            .agg(
                F.percentile_approx("g.lat", 0.5).alias("lat"),
                F.percentile_approx("g.lon", 0.5).alias("lon"),
                F.count("*").alias("num_evidence_points"),
                F.countDistinct("match_date").alias("distinct_days"),
                F.avg(
                    F.exp(-F.log(F.lit(2.0)) * F.col("time_delta") / F.lit(config.TEMPORAL_DECAY_HALF_LIFE))
                ).alias("logic_confidence"),
            )
            .filter(F.col("distinct_days") >= 3)
            .withColumn("logic_name", F.lit(self.name))
            .withColumn("logic_confidence",
                        F.least(F.lit(1.0), F.col("logic_confidence") * 0.8))
            .withColumn("metadata",
                        F.concat(F.lit('{"method":"recurring_7day","distinct_days":'),
                                 F.col("distinct_days").cast(StringType()),
                                 F.lit('}')))
            .select("camera_id", "lat", "lon", "logic_name",
                    "logic_confidence", "num_evidence_points", "metadata")
        )

        return self.validate_output(result)


# =============================================================================
# LOGIC 4: Stub — Add your own logic here
# =============================================================================

class ExampleCustomLogic(BaseLocalizationLogic):
    """
    TEMPLATE: Copy this class to add a new localization logic.
    Examples of what you could implement:
      - Reverse DNS / hostname-based geolocation
      - IP geolocation database lookup (MaxMind, etc.)
      - Camera MAC OUI + known deployment databases
      - Network topology inference from traceroute-like data
    """

    @property
    def name(self) -> str:
        return "customLogicExample"

    def run(self, spark, run_date, config):
        # Return an empty DataFrame with the correct schema
        return spark.createDataFrame([], LOGIC_OUTPUT_SCHEMA)


# =============================================================================
# LOGIC REGISTRY — Add new logics here
# =============================================================================

LOGIC_REGISTRY = [
    MainLogic(),
    ResidentialIPLogic(),
    RecurringColocationLogic(),
    # ExampleCustomLogic(),       # <-- uncomment or add yours here
]


# =============================================================================
# CONVERGENCE ENGINE
# =============================================================================

class ConvergenceEngine:
    """
    Takes outputs from all logics and MERGES converging logics into a single row.

    Output: one row per DISTINCT LOCATION per camera (not per logic).
      - Converging logics → fused into 1 row (weighted centroid)
      - Non-converging logics → 1 row each
      - mainLogic → 1 row, score=100

    Each row carries bidirectional cross-references:
      - alternative_locations: names of logics proposing a different location
      - best_location_logics:  names of logics forming the best location

    Rules:
      - mainLogic → score=100, is_best=True. Others kept as alternative rows.
      - No mainLogic → converging logics fused into one primary row.
        Non-converging logics = separate alternative rows.
      - Single logic → isolated row, low confidence.
    """

    def __init__(self, config: Config):
        self.config = config

    def score_and_merge(self, all_candidates: DataFrame, spark: SparkSession) -> DataFrame:
        config = self.config

        # =================================================================
        # PHASE 1: Assign unique row_id
        # =================================================================
        candidates = all_candidates.withColumn(
            "row_id", F.monotonically_increasing_id()
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
        # PHASE 3: Build convergence sets per logic
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
        )

        # =================================================================
        # PHASE 4: Enrich candidates
        # =================================================================
        enriched = (
            candidates
            .join(conv_agg, ["camera_id", "logic_name"], "left")
            .withColumn("converges_with",
                        F.coalesce(F.col("converges_with"), F.array()))
            .withColumn("num_converging",
                        F.coalesce(F.col("num_converging"), F.lit(0)))
        )

        # Count logics per camera
        logic_count = (
            candidates.groupBy("camera_id")
            .agg(F.count("*").alias("total_logics_for_camera"))
        )
        enriched = enriched.join(logic_count, "camera_id", "left")

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
        # PHASE 5: Connected components — find ALL clusters per camera
        # =================================================================
        # A cluster = a connected component in the convergence graph.
        # A↔B and C↔D = two separate clusters. E alone = isolated.
        #
        # Algorithm: iterative label propagation on convergence pairs.
        # Each logic starts with its own name as label. On each iteration,
        # every logic takes the MIN label among itself and its neighbors.
        # Converges in at most N iterations (N = number of logics per camera).
        # In practice 5-10 iterations is plenty.

        # Start: every logic that converges with someone
        converging_logics = (
            all_convergences
            .select("camera_id", F.col("logic_name").alias("node"))
            .distinct()
        )

        # Initialize label = logic_name (alphabetical min will propagate)
        labels = converging_logics.withColumn("cluster_id", F.col("node"))

        # Build edge list (bidirectional, already done in all_convergences)
        edges = (
            all_convergences
            .select(
                "camera_id",
                F.col("logic_name").alias("node"),
                F.col("converges_with_logic").alias("neighbor")
            )
        )

        # Iterate: propagate min label through edges
        MAX_ITERATIONS = 10
        for _ in range(MAX_ITERATIONS):
            # Join labels to neighbors
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

            # For each node: min of own label and all neighbor labels
            new_labels = (
                labels
                .unionByName(
                    neighbor_labels.select(
                        "camera_id",
                        "node",
                        F.col("neighbor_label").alias("cluster_id")
                    )
                )
                .groupBy("camera_id", "node")
                .agg(F.min("cluster_id").alias("cluster_id"))
            )

            labels = new_labels

        # Now labels has: (camera_id, node=logic_name, cluster_id)
        # cluster_id is the same for all logics in the same connected component

        # =================================================================
        # PHASE 5b: Assign roles
        # =================================================================

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

        # Assign role
        enriched = enriched.withColumn(
            "role",
            F.when(
                F.col("has_main_logic") & (F.col("logic_name") == config.MAIN_LOGIC_NAME),
                F.lit("main")
            ).when(
                F.col("has_main_logic") & (F.col("logic_name") != config.MAIN_LOGIC_NAME),
                F.lit("alternative")
            ).when(
                F.col("cluster_id").isNotNull(),
                F.lit("cluster_member")   # belongs to some convergence cluster
            ).otherwise(
                F.lit("isolated")         # no convergence with anyone
            )
        )

        # =================================================================
        # PHASE 6: Build output rows
        # =================================================================

        def _logic_detail_struct():
            return F.struct(
                F.col("logic_name").alias("logic"),
                "lat", "lon", "logic_confidence",
                "num_evidence_points", "metadata"
            )

        enriched = enriched.withColumn("_detail", _logic_detail_struct())

        # ── A) mainLogic rows ───────────────────────────────────────────
        main_rows = (
            enriched.filter(F.col("role") == "main")
            .select(
                "camera_id", "lat", "lon",
                F.col("logic_name").alias("contributing_logics"),
                F.lit(config.MAIN_LOGIC_SCORE).alias("final_score"),
                F.lit(True).alias("is_best_location"),
                F.lit("primary").alias("convergence_group"),
                F.lit(1).alias("num_contributing_logics"),
                "total_logics_for_camera",
                F.lit("high").alias("confidence_flag"),
                F.array(F.col("_detail")).alias("logic_details"),
            )
        )

        # ── B) Cluster members → one fused row PER CLUSTER ─────────────
        fused_clusters = (
            enriched.filter(F.col("role") == "cluster_member")
            .groupBy("camera_id", "cluster_id", "total_logics_for_camera")
            .agg(
                (F.sum(F.col("lat") * F.col("logic_confidence"))
                 / F.sum("logic_confidence")).alias("lat"),
                (F.sum(F.col("lon") * F.col("logic_confidence"))
                 / F.sum("logic_confidence")).alias("lon"),
                F.concat_ws(",", F.sort_array(F.collect_set("logic_name")))
                    .alias("contributing_logics"),
                F.count("*").alias("num_contributing_logics"),
                F.collect_list("_detail").alias("logic_details"),
            )
            .withColumn(
                "final_score",
                F.least(
                    F.lit(config.MAX_SCORE_WITHOUT_MAIN),
                    (F.col("num_contributing_logics") / F.col("total_logics_for_camera"))
                    * F.lit(config.MAX_SCORE_WITHOUT_MAIN)
                )
            )
            .withColumn("is_best_location", F.lit(True))
            .withColumn("convergence_group", F.lit("primary"))
            .withColumn(
                "confidence_flag",
                F.when(F.col("num_contributing_logics") >= 3, F.lit("high"))
                 .when(F.col("num_contributing_logics") == 2, F.lit("medium"))
                 .otherwise(F.lit("low_confidence"))
            )
            .select(
                "camera_id", "lat", "lon", "contributing_logics",
                "final_score", "is_best_location", "convergence_group",
                "num_contributing_logics", "total_logics_for_camera",
                "confidence_flag", "logic_details",
            )
        )

        # ── C) Alternative rows (has mainLogic, not mainLogic itself) ───
        alternative_rows = (
            enriched.filter(F.col("role") == "alternative")
            .select(
                "camera_id", "lat", "lon",
                F.col("logic_name").alias("contributing_logics"),
                F.lit(config.BASE_SCORE_SINGLE_LOGIC).alias("final_score"),
                F.lit(False).alias("is_best_location"),
                F.lit("alternative").alias("convergence_group"),
                F.lit(1).alias("num_contributing_logics"),
                "total_logics_for_camera",
                F.lit("low_confidence").alias("confidence_flag"),
                F.array(F.col("_detail")).alias("logic_details"),
            )
        )

        # ── D) Isolated rows ───────────────────────────────────────────
        isolated_rows = (
            enriched.filter(F.col("role") == "isolated")
            .select(
                "camera_id", "lat", "lon",
                F.col("logic_name").alias("contributing_logics"),
                F.lit(config.BASE_SCORE_SINGLE_LOGIC).alias("final_score"),
                F.lit(True).alias("is_best_location"),
                F.lit("isolated").alias("convergence_group"),
                F.lit(1).alias("num_contributing_logics"),
                "total_logics_for_camera",
                F.lit("low_confidence").alias("confidence_flag"),
                F.array(F.col("_detail")).alias("logic_details"),
            )
        )

        # =================================================================
        # PHASE 7: Union all rows
        # =================================================================
        all_rows = (
            main_rows
            .unionByName(fused_clusters)
            .unionByName(alternative_rows)
            .unionByName(isolated_rows)
        )

        # =================================================================
        # PHASE 8: Cross-references (bidirectional)
        # =================================================================
        # Each row lists ALL other rows for the same camera as
        # "other_locations". Primary rows see other primaries + alternatives.
        # Alternatives/isolated see all primaries.

        # Collect all contributing_logics per camera
        all_logics_per_camera = (
            all_rows
            .groupBy("camera_id")
            .agg(F.collect_set("contributing_logics").alias("_all_logics"))
        )

        all_rows = all_rows.join(all_logics_per_camera, "camera_id", "left")

        # other_locations = all logics for this camera MINUS this row's own logics
        all_rows = all_rows.withColumn(
            "other_locations",
            F.concat_ws(",",
                F.array_except(F.col("_all_logics"),
                               F.array(F.col("contributing_logics")))
            )
        )

        # is_best_location: all primary rows are True
        # isolated = True ONLY if no primary exists for this camera
        has_primary = (
            all_rows.filter(F.col("convergence_group") == "primary")
            .select("camera_id").distinct()
            .withColumn("_has_primary", F.lit(True))
        )
        all_rows = (
            all_rows.join(has_primary, "camera_id", "left")
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
        # PHASE 9: Clean select
        # =================================================================
        final = all_rows.select(
            "camera_id",
            "lat",
            "lon",
            "contributing_logics",
            "final_score",
            "is_best_location",
            "convergence_group",
            "other_locations",
            "num_contributing_logics",
            "total_logics_for_camera",
            "confidence_flag",
            "logic_details",
        )

        return final


# =============================================================================
# GOLD TABLE MERGE
# =============================================================================

def merge_to_gold(silver_df, spark, config=Config):
    """
    Gold table keeps ALL rows (every logic for every camera).
    Merge strategy: for each camera, compare the BEST score in silver
    vs the BEST score in gold. If silver is better (or camera is new),
    replace ALL rows for that camera with silver's rows.
    Otherwise keep gold's rows.

    This ensures the gold table always has the complete picture
    (all logics + cross-references) from the best-scoring run.
    """
    try:
        gold_df = spark.read.parquet(config.GOLD_PATH)
    except Exception:
        silver_df.write.mode("overwrite").parquet(config.GOLD_PATH)
        print("[GOLD] Initialized Gold table from Silver.")
        return silver_df

    # Best score per camera in each table
    gold_best = (
        gold_df
        .groupBy("camera_id")
        .agg(F.max("final_score").alias("gold_best_score"))
    )
    silver_best = (
        silver_df
        .groupBy("camera_id")
        .agg(F.max("final_score").alias("silver_best_score"))
    )

    # Decide per camera: take silver or keep gold?
    comparison = (
        gold_best.alias("g")
        .join(silver_best.alias("s"), "camera_id", "full_outer")
        .withColumn(
            "winner",
            F.when(F.col("gold_best_score").isNull(), F.lit("silver"))  # new camera
             .when(F.col("silver_best_score").isNull(), F.lit("gold"))  # no new data
             .when(F.col("silver_best_score") > F.col("gold_best_score"), F.lit("silver"))
             .otherwise(F.lit("gold"))
        )
        .select("camera_id", "winner")
    )

    # Cameras where silver wins → take all silver rows
    silver_winners = comparison.filter(F.col("winner") == "silver").select("camera_id")
    silver_rows = silver_df.join(silver_winners, "camera_id", "inner")

    # Cameras where gold wins → keep all gold rows
    gold_winners = comparison.filter(F.col("winner") == "gold").select("camera_id")
    gold_rows = gold_df.join(gold_winners, "camera_id", "inner")

    # Union
    final = gold_rows.unionByName(silver_rows, allowMissingColumns=True)

    final.write.mode("overwrite").parquet(config.GOLD_PATH)
    distinct_cameras = final.select("camera_id").distinct().count()
    total_rows = final.count()
    print(f"[GOLD] Merged. {distinct_cameras} cameras, {total_rows} total rows.")

    return final


# =============================================================================
# MAIN PIPELINE
# =============================================================================

def run_pipeline(run_date, config=Config):
    spark = (
        SparkSession.builder
        .appName(f"MultiLogicGeolocation_{run_date.strftime('%Y%m%d')}")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    print(f"\n{'='*70}")
    print(f"  Multi-Logic Camera Geolocation — {run_date.strftime('%Y-%m-%d')}")
    print(f"  Registered logics: {[l.name for l in LOGIC_REGISTRY]}")
    print(f"{'='*70}\n")

    # ── Run all logics ──────────────────────────────────────────────────
    all_results = []
    for logic in LOGIC_REGISTRY:
        print(f"  [RUNNING] {logic.name}...")
        try:
            result = logic.run(spark, run_date, config)
            count = result.count()
            print(f"  [DONE]    {logic.name} → {count} cameras located")
            if count > 0:
                all_results.append(result)
        except Exception as e:
            print(f"  [ERROR]   {logic.name} failed: {e}")

    if not all_results:
        print("\n  No logics produced results. Exiting.")
        spark.stop()
        return

    # Union all logic outputs
    all_candidates = all_results[0]
    for df in all_results[1:]:
        all_candidates = all_candidates.unionByName(df, allowMissingColumns=True)

    print(f"\n  Total candidates across all logics: {all_candidates.count()}")

    # ── Convergence scoring ─────────────────────────────────────────────
    print("\n  [CONVERGENCE] Scoring...")
    engine = ConvergenceEngine(config)
    silver = engine.score_and_merge(all_candidates, spark)
    silver = silver.withColumn("run_date", F.lit(run_date.strftime("%Y-%m-%d")).cast(DateType()))

    # Print summary
    print("\n  ── Results Summary ──")
    distinct_cameras = silver.select("camera_id").distinct().count()
    total_rows = silver.count()
    print(f"  Cameras: {distinct_cameras}, Total rows: {total_rows}")

    print("\n  By convergence group:")
    silver.groupBy("convergence_group").agg(
        F.count("*").alias("rows"),
        F.countDistinct("camera_id").alias("cameras"),
        F.round(F.avg("final_score"), 1).alias("avg_score"),
    ).show()

    # Show cameras that have alternative locations
    cameras_with_alternatives = (
        silver
        .filter(F.col("alternative_locations") != "")
        .select("camera_id")
        .distinct()
        .count()
    )
    print(f"  Cameras with alternative locations flagged: {cameras_with_alternatives}")

    # ── Write Silver ────────────────────────────────────────────────────
    silver.write.mode("overwrite").parquet(
        f"{config.SILVER_PATH}/run_date={run_date.strftime('%Y-%m-%d')}"
    )

    # ── Merge to Gold ───────────────────────────────────────────────────
    print("  [GOLD] Merging...")
    merge_to_gold(silver, spark, config)

    print(f"\n{'='*70}")
    print(f"  Pipeline complete.")
    print(f"{'='*70}\n")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=datetime.now().strftime("%Y-%m-%d"))
    args = parser.parse_args()
    run_pipeline(datetime.strptime(args.date, "%Y-%m-%d"))
