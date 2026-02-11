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
    Takes outputs from all logics and computes final scored locations.

    Algorithm:
    1. If mainLogic exists for a camera → score=100, drop all others
    2. For remaining cameras:
       a. Cross-compare all logic pairs for that camera
       b. Count how many logics converge (within CONVERGENCE_RADIUS_METERS)
       c. Score = base + bonus × (num_converging - 1), capped at MAX_SCORE
       d. Final location = weighted centroid of converging logics
       e. Non-converging logics kept but flagged
    """

    def __init__(self, config: Config):
        self.config = config

    def score_and_merge(self, all_candidates: DataFrame, spark: SparkSession) -> DataFrame:
        """
        Main entry point.

        Input:  Union of all logic outputs (LOGIC_OUTPUT_SCHEMA)
        Output: Scored, deduplicated locations per camera
                Columns: camera_id, lat, lon, final_score, confidence_flag,
                         contributing_logics, num_converging_logics, run_date
        """
        config = self.config

        # ── STEP A: Separate mainLogic cameras ──────────────────────────
        main_cameras = (
            all_candidates
            .filter(F.col("logic_name") == config.MAIN_LOGIC_NAME)
            .withColumn("final_score", F.lit(config.MAIN_LOGIC_SCORE))
            .withColumn("confidence_flag", F.lit("high"))
            .withColumn("contributing_logics", F.lit(config.MAIN_LOGIC_NAME))
            .withColumn("num_converging_logics", F.lit(1))
            .select("camera_id", "lat", "lon", "final_score",
                    "confidence_flag", "contributing_logics",
                    "num_converging_logics")
        )

        main_camera_ids = main_cameras.select("camera_id").distinct()

        # ── STEP B: All other cameras (no mainLogic) ───────────────────
        other_candidates = (
            all_candidates
            .join(main_camera_ids, "camera_id", "left_anti")
        )

        # ── STEP C: Pairwise convergence check ─────────────────────────
        # Self-join: for each camera, compare every pair of logics
        left = other_candidates.alias("a")
        right = other_candidates.alias("b")

        pairs = (
            left.join(
                right,
                (F.col("a.camera_id") == F.col("b.camera_id"))
                & (F.col("a.logic_name") < F.col("b.logic_name")),  # avoid duplicates
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
                "is_converging",
                (F.col("distance_m") <= config.CONVERGENCE_RADIUS_METERS).cast(IntegerType())
            )
        )

        # ── STEP D: For each logic per camera, count convergences ──────
        # A logic "converges" if it's within radius of at least one other logic

        convergence_a = (
            pairs
            .filter(F.col("is_converging") == 1)
            .select(
                F.col("a.camera_id").alias("camera_id"),
                F.col("a.logic_name").alias("logic_name"),
            )
        )
        convergence_b = (
            pairs
            .filter(F.col("is_converging") == 1)
            .select(
                F.col("b.camera_id").alias("camera_id"),
                F.col("b.logic_name").alias("logic_name"),
            )
        )
        converging_logics = convergence_a.unionByName(convergence_b).distinct()

        # Tag each candidate as converging or not
        other_tagged = (
            other_candidates.alias("oc")
            .join(
                converging_logics.alias("cv"),
                (F.col("oc.camera_id") == F.col("cv.camera_id"))
                & (F.col("oc.logic_name") == F.col("cv.logic_name")),
                "left"
            )
            .withColumn(
                "is_converging",
                F.when(F.col("cv.logic_name").isNotNull(), F.lit(True))
                 .otherwise(F.lit(False))
            )
            .select(
                F.col("oc.camera_id"),
                F.col("oc.lat"),
                F.col("oc.lon"),
                F.col("oc.logic_name"),
                F.col("oc.logic_confidence"),
                F.col("oc.num_evidence_points"),
                "is_converging",
            )
        )

        # ── STEP E: Per-camera scoring ─────────────────────────────────
        # For cameras WITH convergence: weighted centroid of converging logics
        converging_only = other_tagged.filter(F.col("is_converging") == True)

        scored_converging = (
            converging_only
            .groupBy("camera_id")
            .agg(
                # Weighted centroid (weight = logic_confidence × evidence)
                (F.sum(F.col("lat") * F.col("logic_confidence"))
                 / F.sum("logic_confidence")).alias("lat"),
                (F.sum(F.col("lon") * F.col("logic_confidence"))
                 / F.sum("logic_confidence")).alias("lon"),
                F.count("*").alias("num_converging_logics"),
                F.concat_ws(",", F.collect_set("logic_name")).alias("contributing_logics"),
            )
            .withColumn(
                "final_score",
                F.least(
                    F.lit(config.MAX_SCORE_WITHOUT_MAIN),
                    F.lit(config.BASE_SCORE_SINGLE_LOGIC)
                    + F.lit(config.CONVERGENCE_BONUS_PER_LOGIC)
                      * (F.col("num_converging_logics") - 1)
                )
            )
            .withColumn("confidence_flag",
                F.when(F.col("num_converging_logics") >= 3, F.lit("high"))
                 .when(F.col("num_converging_logics") == 2, F.lit("medium"))
                 .otherwise(F.lit("low"))
            )
            .select("camera_id", "lat", "lon", "final_score",
                    "confidence_flag", "contributing_logics",
                    "num_converging_logics")
        )

        # For cameras with NO convergence: keep best single logic, flag low
        cameras_with_convergence = scored_converging.select("camera_id").distinct()

        non_converging_cameras = (
            other_tagged
            .join(cameras_with_convergence, "camera_id", "left_anti")
        )

        # Pick the logic with the highest confidence for non-converging cameras
        best_single_w = Window.partitionBy("camera_id").orderBy(
            F.desc("logic_confidence"), F.desc("num_evidence_points")
        )

        scored_non_converging = (
            non_converging_cameras
            .withColumn("rn", F.row_number().over(best_single_w))
            .filter(F.col("rn") == 1)
            .withColumn("final_score", F.lit(config.BASE_SCORE_SINGLE_LOGIC))
            .withColumn("confidence_flag", F.lit("low_confidence"))
            .withColumn("contributing_logics", F.col("logic_name"))
            .withColumn("num_converging_logics", F.lit(1))
            .select("camera_id", "lat", "lon", "final_score",
                    "confidence_flag", "contributing_logics",
                    "num_converging_logics")
        )

        # ── STEP F: Union all ──────────────────────────────────────────
        final = (
            main_cameras
            .unionByName(scored_converging)
            .unionByName(scored_non_converging)
        )

        return final


# =============================================================================
# GOLD TABLE MERGE
# =============================================================================

def merge_to_gold(silver_df, spark, config=Config):
    """Upsert: keep the higher final_score per camera."""
    try:
        gold_df = spark.read.parquet(config.GOLD_PATH)
    except Exception:
        silver_df.write.mode("overwrite").parquet(config.GOLD_PATH)
        return silver_df

    merged = (
        gold_df.alias("g")
        .join(silver_df.alias("s"), F.col("g.camera_id") == F.col("s.camera_id"), "full_outer")
    )

    columns = silver_df.columns

    final = merged.select(
        *[
            F.when(F.col("g.camera_id").isNull(), F.col(f"s.{c}"))
             .when(F.col("s.camera_id").isNull(), F.col(f"g.{c}"))
             .when(F.col("s.final_score") > F.col("g.final_score"), F.col(f"s.{c}"))
             .otherwise(F.col(f"g.{c}"))
             .alias(c)
            for c in columns
        ]
    )

    final.write.mode("overwrite").parquet(config.GOLD_PATH)
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
    silver.groupBy("confidence_flag").agg(
        F.count("*").alias("cameras"),
        F.round(F.avg("final_score"), 1).alias("avg_score"),
    ).show()

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
