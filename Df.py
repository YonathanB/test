"""
Multi-Logic Convergence Engine
===============================
One row per cluster per camera.
Each row contains:
  - score: proportional to convergence ratio (mainLogic = 100)
  - centroid_lat/lon: weighted centroid of the cluster
  - logiques_convergentes: logics in this cluster
  - points_bruts: individual lat/lon/confidence from each logic in this cluster
  - groupes_divergents: all OTHER clusters for this camera with their details
"""

import json
import numpy as np
import pandas as pd
import networkx as nx

import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
from pyspark.sql import DataFrame, SparkSession


# =============================================================================
# CONFIG
# =============================================================================

class Config:
    CONVERGENCE_RADIUS_METERS = 100
    MAIN_LOGIC_NAME = "mainLogic"
    MAIN_LOGIC_SCORE = 100.0
    BASE_SCORE_SINGLE_LOGIC = 20.0
    MAX_SCORE_WITHOUT_MAIN = 85.0


# =============================================================================
# HAVERSINE (numpy)
# =============================================================================

def _haversine_np(lat1, lon1, lat2, lon2):
    R = 6_371_000.0
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return 2 * R * np.arcsin(np.sqrt(a))


# =============================================================================
# OUTPUT SCHEMA
# =============================================================================

OUTPUT_SCHEMA = StructType([
    StructField("camera_id", StringType(), False),
    StructField("centroid_lat", DoubleType(), True),
    StructField("centroid_lon", DoubleType(), True),
    StructField("score", DoubleType(), False),
    StructField("is_best_location", StringType(), False),
    StructField("confidence_flag", StringType(), False),
    StructField("num_logics_in_cluster", IntegerType(), False),
    StructField("total_logics_for_camera", IntegerType(), False),
    StructField("logiques_convergentes", StringType(), False),
    StructField("points_bruts", StringType(), False),
    StructField("groupes_divergents", StringType(), False),
])


# =============================================================================
# CORE: One camera → one row per cluster
# =============================================================================

def _process_one_camera(pdf: pd.DataFrame, config: Config) -> pd.DataFrame:

    camera_id = str(pdf["camera_id"].iloc[0])
    total = len(pdf)
    has_main = config.MAIN_LOGIC_NAME in pdf["logic_name"].values

    # ── Graph construction ──────────────────────────────────────────
    G = nx.Graph()
    for _, row in pdf.iterrows():
        G.add_node(row["logic_name"], **{
            "lat": row["lat"],
            "lon": row["lon"],
            "logic_confidence": row.get("logic_confidence", 1.0),
            "num_evidence_points": row.get("num_evidence_points", 0),
            "metadata": row.get("metadata", ""),
        })

    nodes = list(G.nodes(data=True))
    for i, (n1, d1) in enumerate(nodes):
        for n2, d2 in nodes[i + 1:]:
            if _haversine_np(d1["lat"], d1["lon"], d2["lat"], d2["lon"]) <= config.CONVERGENCE_RADIUS_METERS:
                G.add_edge(n1, n2)

    # ── Connected components = clusters ─────────────────────────────
    clusters = []
    for comp in nx.connected_components(G):
        members = sorted(comp)
        points = []
        sw_lat, sw_lon, sw = 0.0, 0.0, 0.0

        for name in members:
            d = G.nodes[name]
            w = d.get("logic_confidence", 1.0) or 1.0
            sw_lat += d["lat"] * w
            sw_lon += d["lon"] * w
            sw += w
            points.append({
                "logic": name,
                "lat": d["lat"],
                "lon": d["lon"],
                "logic_confidence": d.get("logic_confidence"),
                "num_evidence_points": d.get("num_evidence_points"),
                "metadata": d.get("metadata"),
            })

        clusters.append({
            "logiques": members,
            "points": points,
            "centroid_lat": sw_lat / sw if sw else points[0]["lat"],
            "centroid_lon": sw_lon / sw if sw else points[0]["lon"],
            "count": len(members),
            "has_main": config.MAIN_LOGIC_NAME in members,
        })

    # ── Score ───────────────────────────────────────────────────────
    for c in clusters:
        if c["has_main"]:
            c["score"] = config.MAIN_LOGIC_SCORE
        elif total == 1:
            c["score"] = config.BASE_SCORE_SINGLE_LOGIC
        else:
            c["score"] = min(
                config.MAX_SCORE_WITHOUT_MAIN,
                (c["count"] / total) * config.MAX_SCORE_WITHOUT_MAIN,
            )

    # ── is_best_location ───────────────────────────────────────────
    if has_main:
        for c in clusters:
            c["is_best"] = c["has_main"]
    else:
        has_primary = any(c["count"] >= 2 for c in clusters)
        for c in clusters:
            c["is_best"] = c["count"] >= 2 if has_primary else True

    # ── confidence_flag ────────────────────────────────────────────
    for c in clusters:
        if c["has_main"]:
            c["confidence_flag"] = "high"
        elif c["count"] >= 3:
            c["confidence_flag"] = "high"
        elif c["count"] >= 2:
            c["confidence_flag"] = "medium"
        else:
            c["confidence_flag"] = "low_confidence"

    # ── Build output with cross-references ─────────────────────────
    rows = []
    for i, cur in enumerate(clusters):
        divergents = [
            {
                "logiques": oth["logiques"],
                "centroid_lat": oth["centroid_lat"],
                "centroid_lon": oth["centroid_lon"],
                "score": oth["score"],
                "points": oth["points"],
            }
            for j, oth in enumerate(clusters) if j != i
        ]

        rows.append({
            "camera_id": camera_id,
            "centroid_lat": cur["centroid_lat"],
            "centroid_lon": cur["centroid_lon"],
            "score": cur["score"],
            "is_best_location": "true" if cur["is_best"] else "false",
            "confidence_flag": cur["confidence_flag"],
            "num_logics_in_cluster": cur["count"],
            "total_logics_for_camera": total,
            "logiques_convergentes": json.dumps(cur["logiques"]),
            "points_bruts": json.dumps(cur["points"]),
            "groupes_divergents": json.dumps(divergents),
        })

    return pd.DataFrame(rows)


# =============================================================================
# SPARK ENTRY POINT
# =============================================================================

def score_and_merge(all_candidates: DataFrame, spark: SparkSession, config: Config = Config()) -> DataFrame:

    clean = (
        all_candidates
        .filter(
            F.col("camera_id").isNotNull()
            & F.col("logic_name").isNotNull()
            & F.col("lat").isNotNull()
            & F.col("lon").isNotNull()
        )
        .withColumn("logic_confidence", F.coalesce(F.col("logic_confidence"), F.lit(1.0)))
        .withColumn("num_evidence_points", F.coalesce(F.col("num_evidence_points"), F.lit(0)))
        .withColumn("metadata", F.coalesce(F.col("metadata"), F.lit("")))
    )

    _config = config

    def _process(pdf: pd.DataFrame) -> pd.DataFrame:
        return _process_one_camera(pdf, _config)

    return clean.groupBy("camera_id").applyInPandas(_process, schema=OUTPUT_SCHEMA)
