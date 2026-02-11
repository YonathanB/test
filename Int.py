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
    BASE_SCORE_SINGLE_LOGIC = 20.0    # one logic alone, no convergence
    CONVERGENCE_BONUS_PER_LOGIC = 15.0 # bonus per additional converging logic
    MAX_SCORE_WITHOUT_MAIN = 85.0     # cap — only mainLogic can reach 100

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
    Takes outputs from all logics and produces ONE ROW PER LOGIC PER CAMERA.
    Every logic result is KEPT. Nothing is dropped.

    Output columns per row:
      - camera_id, lat, lon, logic_name            → the logic's own result
      - final_score                                  → scored (100 for mainLogic)
      - is_best_location (bool)                      → True for the winning row
      - convergence_group                            → 'primary'|'alternative'|'isolated'
      - converges_with         (list of logic names) → which other logics agree
      - alternative_locations  (list of logic names) → which logics disagree
      - best_location_logics   (list of logic names) → what the "best" is, for reference
      - confidence_flag                              → high / medium / low_confidence

    Rules:
      - mainLogic → score=100, is_best=True. Other logics for that camera
        are KEPT but marked convergence_group='alternative' and point to
        mainLogic via best_location_logics. mainLogic also lists them in
        alternative_locations.
      - No mainLogic → convergence check. Converging logics form the
        'primary' group (best). Non-converging logics are 'alternative'.
        Both sides cross-reference each other.
      - Camera with only one logic → 'isolated', low confidence.
    """

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
        # PHASE 3: Build "converges_with" and "diverges_from" per logic
        # =================================================================

        # Converging pairs (bidirectional)
        conv_from_a = (
            pairs.filter(F.col("within_radius"))
            .select(
                "camera_id",
                F.col("logic_a").alias("logic_name"),
                F.col("logic_b").alias("converges_with_logic"),
            )
        )
        conv_from_b = (
            pairs.filter(F.col("within_radius"))
            .select(
                "camera_id",
                F.col("logic_b").alias("logic_name"),
                F.col("logic_a").alias("converges_with_logic"),
            )
        )
        all_convergences = conv_from_a.unionByName(conv_from_b)

        # Diverging pairs (bidirectional)
        div_from_a = (
            pairs.filter(~F.col("within_radius"))
            .select(
                "camera_id",
                F.col("logic_a").alias("logic_name"),
                F.col("logic_b").alias("diverges_from_logic"),
            )
        )
        div_from_b = (
            pairs.filter(~F.col("within_radius"))
            .select(
                "camera_id",
                F.col("logic_b").alias("logic_name"),
                F.col("logic_a").alias("diverges_from_logic"),
            )
        )
        all_divergences = div_from_a.unionByName(div_from_b)

        # Aggregate per (camera_id, logic_name)
        conv_agg = (
            all_convergences
            .groupBy("camera_id", "logic_name")
            .agg(
                F.collect_set("converges_with_logic").alias("converges_with"),
                F.count("*").alias("num_converging"),
            )
        )

        div_agg = (
            all_divergences
            .groupBy("camera_id", "logic_name")
            .agg(
                F.collect_set("diverges_from_logic").alias("diverges_from"),
            )
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

        # =================================================================
        # PHASE 5: Determine best location group per camera
        # =================================================================
        # Count logics per camera
        logic_count_per_camera = (
            candidates
            .groupBy("camera_id")
            .agg(F.count("*").alias("total_logics_for_camera"))
        )
        enriched = enriched.join(logic_count_per_camera, "camera_id", "left")

        # Has mainLogic?
        has_main = (
            candidates
            .filter(F.col("logic_name") == config.MAIN_LOGIC_NAME)
            .select("camera_id")
            .distinct()
            .withColumn("has_main_logic", F.lit(True))
        )
        enriched = (
            enriched
            .join(has_main, "camera_id", "left")
            .withColumn("has_main_logic",
                        F.coalesce(F.col("has_main_logic"), F.lit(False)))
        )

        # For cameras WITHOUT mainLogic: find the "primary" group
        # = the largest set of mutually converging logics
        # Approximation: the logic with the most convergences anchors the group
        best_convergence_per_camera = (
            enriched
            .filter(~F.col("has_main_logic"))
            .groupBy("camera_id")
            .agg(F.max("num_converging").alias("max_converging"))
        )

        # All logics in the "primary" cluster = those that converge with
        # the best-connected logic (or are the best-connected logic)
        primary_anchor = (
            enriched
            .filter(~F.col("has_main_logic"))
            .join(best_convergence_per_camera, "camera_id", "inner")
            .filter(
                (F.col("num_converging") == F.col("max_converging"))
                & (F.col("num_converging") > 0)
            )
            .groupBy("camera_id")
            .agg(
                # Collect all logics that are part of the best convergence cluster
                F.array_distinct(
                    F.flatten(
                        F.array(
                            F.collect_set("logic_name"),
                            F.flatten(F.collect_list("converges_with"))
                        )
                    )
                ).alias("primary_logics")
            )
        )

        enriched = enriched.join(primary_anchor, "camera_id", "left")

        # =================================================================
        # PHASE 6: Assign convergence_group, score, cross-references
        # =================================================================
        scored = (
            enriched
            .withColumn(
                "convergence_group",
                F.when(
                    # mainLogic cameras: mainLogic is primary, others are alternative
                    F.col("has_main_logic") & (F.col("logic_name") == config.MAIN_LOGIC_NAME),
                    F.lit("primary")
                ).when(
                    F.col("has_main_logic") & (F.col("logic_name") != config.MAIN_LOGIC_NAME),
                    F.lit("alternative")
                ).when(
                    # No mainLogic: check if this logic is in the primary cluster
                    F.col("primary_logics").isNotNull()
                    & F.array_contains(F.col("primary_logics"), F.col("logic_name")),
                    F.lit("primary")
                ).when(
                    # No mainLogic, not in primary cluster, but primary cluster exists
                    F.col("primary_logics").isNotNull(),
                    F.lit("alternative")
                ).otherwise(
                    # No convergence at all (solo logic or all logics diverge)
                    F.lit("isolated")
                )
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
                F.lit(config.BASE_SCORE_SINGLE_LOGIC)  # kept but low score
            ).otherwise(
                F.lit(config.BASE_SCORE_SINGLE_LOGIC)  # isolated
            )
        )

        # is_best_location: True for the highest-scored row per camera
        best_w = Window.partitionBy("camera_id").orderBy(
            F.desc("final_score"), F.desc("logic_confidence"), F.desc("num_evidence_points")
        )
        scored = (
            scored
            .withColumn("_rank", F.row_number().over(best_w))
            .withColumn("is_best_location", F.col("_rank") == 1)
        )

        # =================================================================
        # PHASE 7: Cross-references (bidirectional)
        # =================================================================

        # For each row, collect the names of logics in the OTHER group(s)
        # "alternative_locations" = logics that located this camera elsewhere
        # "best_location_logics" = which logics form the best location

        # Build best-location logics per camera
        best_logics_per_camera = (
            scored
            .filter(F.col("convergence_group") == "primary")
            .groupBy("camera_id")
            .agg(F.collect_set("logic_name").alias("best_location_logics"))
        )

        # Build alternative logics per camera
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

        # Now assign the cross-reference columns depending on which group this row is in
        scored = (
            scored
            .withColumn(
                "alternative_locations",
                F.when(
                    F.col("convergence_group") == "primary",
                    F.coalesce(F.col("alternative_location_logics"), F.array())
                ).otherwise(
                    # For alternative/isolated rows: "the best location is from these logics"
                    F.array()  # they don't point to other alternatives, they point to best
                )
            )
            .withColumn(
                "best_location_logics",
                F.when(
                    F.col("convergence_group").isin("alternative", "isolated"),
                    F.coalesce(F.col("best_location_logics"), F.array())
                ).otherwise(
                    F.array()  # primary rows don't need to reference themselves
                )
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
        # PHASE 8: Final select — clean output
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
    print(f"  Cameras: {distinct_cameras}, Total logic rows: {total_rows}")

    print("\n  By convergence group:")
    silver.groupBy("convergence_group").agg(
        F.count("*").alias("rows"),
        F.countDistinct("camera_id").alias("cameras"),
        F.round(F.avg("final_score"), 1).alias("avg_score"),
    ).show()

    print("  By confidence flag:")
    silver.groupBy("confidence_flag").agg(
        F.count("*").alias("rows"),
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
