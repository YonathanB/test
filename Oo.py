# ============================================================
# END-TO-END PySpark (INCRÉMENTAL, SANS DELTA)
# - Stockage Hadoop/Hive Parquet/ORC classique
# - Upsert via "bucket overwrite" (partition par bucket) pour éviter de réécrire toute la table
#
# Objectif:
#   1) Déduire une position robuste des routeurs à partir de points GPS de mobiles
#   2) Déduire une position des caméras via camera -> ip_best -> router_state
#   3) Maintenir ces résultats quotidiennement (incremental daily)
#
# Important:
# - Adapte les noms de tables/colonnes dans CONFIG à ton schéma réel.
# - Toutes les tables "gold state" sont partitionnées par 'bucket' (int) pour permettre l'overwrite ciblé.
# - Toutes les tables "gold daily" sont partitionnées par 'day' (string yyyy-MM-dd) en append-only.
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import Window

# ============================================================
# CONFIG — ADAPTE ICI
# ============================================================
CONFIG = {
    # RAW tables (partitionnées par 'day' idéalement)
    # COMM: énorme table communications caméras -> backend
    "COMM_RAW_TABLE": "raw.comm",                 # day, camera_id, ip, ts
    # GPS: positions GPS de mobiles (device) à différents timestamps
    "GPS_RAW_TABLE": "raw.gps",                   # day, device_id, ts, lat, lon
    # WIFI: évènements de connexion device -> routeur (via IP routeur)
    "WIFI_RAW_TABLE": "raw.wifi",                 # day, device_id, router_ip, wifi_ts

    # GOLD (daily append-only)
    "COMM_DAILY_TABLE": "gold.comm_daily",        # partition day
    "SEGMENT_DAILY_TABLE": "gold.segments_daily", # partition day
    "MATCHES_DAILY_TABLE": "gold.matches_daily",  # partition day
    "ROUTER_DAILY_TABLE": "gold.router_daily",    # partition day

    # GOLD (state upsert via bucket overwrite)
    "DEVICE_STATE_TABLE": "gold.device_state",    # partition bucket
    "ROUTER_STATE_TABLE": "gold.router_state",    # partition bucket
    "CAMERA_IP_STATE_TABLE": "gold.camera_ip_state",  # partition bucket
    "CAMERA_MASTER_TABLE": "gold.camera_master",      # partition bucket

    # Filtre géographique grossier (évite des points absurdes) - adapte à ton pays
    "COUNTRY_BBOX": {"min_lat": 29.0, "max_lat": 34.0, "min_lon": 34.0, "max_lon": 36.5},

    # Bucketing pour l'upsert (choisis 128/256/512 selon volume)
    "BUCKETS": 256,
}

# ============================================================
# PARAMS — Ajustables
# ============================================================
PARAMS = {
    # Segmentation GPS (détecter des "fragments stables")
    "SEG_RADIUS_M": 100.0,             # si distance entre points successifs > 100m => nouveau segment
    "SEG_MAX_GAP_MIN": 60,             # si trou temporel > 60min => nouveau segment
    "SEG_MIN_POINTS": 3,               # segment stable doit avoir >= 3 points
    "SEG_MIN_DURATION_MIN": 30,        # et durer >= 30min
    "SEG_WIFI_BUFFER_MIN": 10,         # on accepte wifi_ts dans [start-buffer, end+buffer]

    # Matching strict (join Wi-Fi -> point GPS)
    "STRICT_JOIN_MIN": 5,              # ±5min

    # Agrégation routeur robuste (outliers)
    "OUTLIER_PCTL": 0.95,              # on garde les points jusqu'au 95e percentile des distances
    "MAX_OUTLIER_M": 1500.0,           # plafond hard: > 1500m = rejet
    "TAU_RECENCY_DAYS": 14.0,          # décroissance récence dans les poids

    # Hystérésis (stabilité jour à jour)
    "HYST_DELTA": 8,                   # on switch si new_conf >= old_conf + 8
    "JUMP_SUSPECT_M": 15000.0,         # jump > 15km => très suspect
    "STALE_DAYS": 21,                  # si prev trop vieux => on accepte switch

    # Camera ip_best (fenêtre exacte sur comm_daily)
    "IP_WINDOW_DAYS": 30,
}

# ============================================================
# UTILS
# ============================================================
def in_bbox(lat_col, lon_col, bbox):
    """Filtre grossier pour garder les points dans le pays."""
    return (
        lat_col.isNotNull() & lon_col.isNotNull() &
        lat_col.between(bbox["min_lat"], bbox["max_lat"]) &
        lon_col.between(bbox["min_lon"], bbox["max_lon"])
    )

def haversine_m(lat1, lon1, lat2, lon2):
    """Distance Haversine en mètres (sans UDF)."""
    R = F.lit(6371000.0)
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dlat/2), 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon/2), 2)
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    return R * c

def exp_decay_days(age_days_col, tau):
    """Poids de récence : exp(-age/tau). Plus c'est récent, plus le poids est fort."""
    return F.exp(-age_days_col / F.lit(tau))

def add_bucket(df, key_col, buckets):
    """
    Ajoute une colonne 'bucket' stable à partir de la clé.
    On pourra overwrite uniquement ces partitions bucket.
    """
    return df.withColumn("bucket", F.pmod(F.xxhash64(F.col(key_col)), F.lit(buckets)).cast("int"))

# ============================================================
# UPSERT SANS DELTA: BUCKET OVERWRITE
# ============================================================
def ensure_bucketed_table_exists(spark, table_name, schema_df):
    """
    Optionnel: crée la table si elle n'existe pas (schema minimal).
    Dans beaucoup de setups, tu créeras les tables en amont (Hive DDL).
    """
    try:
        spark.table(table_name).limit(1).collect()
    except Exception:
        # créer une table vide partitionnée par bucket
        empty = schema_df.limit(0)
        (empty.write.mode("overwrite").format("parquet").partitionBy("bucket").saveAsTable(table_name))

def upsert_bucket_overwrite(spark, target_table, source_df, key_col, buckets):
    """
    Upsert sans Delta:
    - Les tables STATE sont partitionnées par bucket.
    - Chaque run: on ne met à jour que les buckets impactés par source_df.
    - On lit le contenu existant de ces buckets, union, puis on garde la dernière version par key.
    - Puis overwrite des partitions impactées uniquement.
    """
    src_b = add_bucket(source_df, key_col, buckets)

    # Buckets impactés aujourd'hui. En général: petit nombre.
    impacted = [r["bucket"] for r in src_b.select("bucket").distinct().collect()]

    # Lire les buckets impactés existants (si table existe)
    try:
        tgt = spark.table(target_table).where(F.col("bucket").isin(impacted))
    except Exception:
        tgt = None

    if tgt is None:
        final_b = src_b
    else:
        # Union existant + nouveau, puis dédoublonnage par key (dernière maj gagne)
        all_b = tgt.unionByName(src_b, allowMissingColumns=True)

        # IMPORTANT: il faut un champ "last_update_ts" pour choisir la version la plus récente
        w = Window.partitionBy(key_col).orderBy(F.col("last_update_ts").desc_nulls_last())
        final_b = all_b.withColumn("rn", F.row_number().over(w)).where(F.col("rn")==1).drop("rn")

    # Overwrite uniquement les partitions bucket impactées.
    # (Spark overwrite partition-by-partition quand on écrit avec partitionBy et mode overwrite)
    (final_b
     .write
     .mode("overwrite")
     .format("parquet")
     .partitionBy("bucket")
     .saveAsTable(target_table))

# ============================================================
# STEP 1 — COMM_DAILY (compression de la comm raw)
# Pourquoi?
# - La table raw comm est énorme.
# - On crée un résumé quotidien (beaucoup plus petit) pour calculer ip_best sur 30 jours
# ============================================================
def build_comm_daily(spark, day):
    comm = (spark.table(CONFIG["COMM_RAW_TABLE"])
            .where(F.col("day")==F.lit(day))
            .where(F.col("camera_id").isNotNull() & F.col("ip").isNotNull() & F.col("ts").isNotNull())
            .select("camera_id", "ip", "ts"))

    # Pour chaque camera_id/ip: combien de messages aujourd'hui + dernier timestamp
    comm_daily = (comm.groupBy("camera_id", "ip")
                  .agg(F.count("*").alias("obs_count"),
                       F.max("ts").alias("last_ts"))
                  .withColumn("day", F.lit(day)))

    (comm_daily.write.mode("append").format("parquet").partitionBy("day").saveAsTable(CONFIG["COMM_DAILY_TABLE"]))
    return comm_daily

# ============================================================
# STEP 2 — GPS SEGMENTS (incremental)
# Idée:
# - Les GPS sont "sparse". On regroupe en segments stables.
# - Incrémental: pour device_id, on réutilise le dernier point d'hier comme seed.
# - On met à jour device_state (last point + last segment_id) pour continuer proprement demain.
# ============================================================
def build_segments_incremental(spark, day):
    gps_d = (spark.table(CONFIG["GPS_RAW_TABLE"])
             .where(F.col("day")==F.lit(day))
             .where(F.col("device_id").isNotNull() & F.col("ts").isNotNull()
                    & F.col("lat").isNotNull() & F.col("lon").isNotNull())
             .select("device_id",
                     F.col("ts").alias("ts"),
                     F.col("lat").cast("double").alias("lat"),
                     F.col("lon").cast("double").alias("lon")))

    affected_devices = gps_d.select("device_id").distinct()

    # Etat précédent des devices: dernier point GPS connu + dernier segment_id
    try:
        device_state_prev = (spark.table(CONFIG["DEVICE_STATE_TABLE"])
                             .join(affected_devices, "device_id", "inner")
                             .select("device_id", "last_ts", "last_lat", "last_lon", "last_segment_id"))
    except Exception:
        device_state_prev = spark.createDataFrame([], """
            device_id string, last_ts timestamp, last_lat double, last_lon double, last_segment_id long
        """)

    # Seed: dernier point connu (pour ne pas casser un segment à minuit)
    seed = (device_state_prev
            .where(F.col("last_ts").isNotNull())
            .select(
                "device_id",
                F.col("last_ts").alias("ts"),
                F.col("last_lat").alias("lat"),
                F.col("last_lon").alias("lon"),
                F.lit(True).alias("is_seed"),
                F.coalesce(F.col("last_segment_id"), F.lit(0)).alias("seg_offset")
            ))

    gps_d2 = gps_d.withColumn("is_seed", F.lit(False)).withColumn("seg_offset", F.lit(0))
    gps_all = seed.unionByName(gps_d2, allowMissingColumns=True)

    # Ordonner les points par device/ts et décider quand un nouveau segment commence
    w = Window.partitionBy("device_id").orderBy("ts")
    seg = (gps_all
           .withColumn("prev_ts",  F.lag("ts").over(w))
           .withColumn("prev_lat", F.lag("lat").over(w))
           .withColumn("prev_lon", F.lag("lon").over(w))
           .withColumn("gap_min", (F.unix_timestamp("ts") - F.unix_timestamp("prev_ts"))/60.0)
           .withColumn("d_prev_m", haversine_m(F.col("prev_lat"), F.col("prev_lon"), F.col("lat"), F.col("lon")))
           .withColumn(
               "new_segment",
               F.when(F.col("prev_ts").isNull(), F.lit(1))  # premier point
                .when(F.col("gap_min") > F.lit(PARAMS["SEG_MAX_GAP_MIN"]), F.lit(1))  # trou temporel
                .when(F.col("d_prev_m") > F.lit(PARAMS["SEG_RADIUS_M"]), F.lit(1))    # saut spatial
                .otherwise(F.lit(0))
           )
           # segment local = cumul new_segment
           .withColumn("segment_local", F.sum("new_segment").over(w))
           # segment global = segment_local + offset (pour continuer après minuit)
           .withColumn("segment_id", F.col("segment_local") + F.max("seg_offset").over(Window.partitionBy("device_id")))
           .drop("segment_local")
          )

    # Agrégation segment: centroid + durée + nb points
    seg0 = (seg.groupBy("device_id", "segment_id")
            .agg(F.min("ts").alias("seg_start_ts"),
                 F.max("ts").alias("seg_end_ts"),
                 F.count("*").alias("seg_points"),
                 F.avg("lat").alias("seg_lat"),
                 F.avg("lon").alias("seg_lon"))
            .withColumn("seg_duration_min",
                        (F.unix_timestamp("seg_end_ts")-F.unix_timestamp("seg_start_ts"))/60.0))

    # Mesure dispersion du segment: distance max des points au centroid
    seg_with_centroid = (seg.join(seg0.select("device_id","segment_id","seg_lat","seg_lon"),
                                  ["device_id","segment_id"], "left")
                           .withColumn("d_to_centroid_m",
                                       haversine_m(F.col("seg_lat"),F.col("seg_lon"),F.col("lat"),F.col("lon"))))

    disp = (seg_with_centroid.groupBy("device_id","segment_id")
            .agg(F.max("d_to_centroid_m").alias("seg_max_dist_m"),
                 F.expr("percentile_approx(d_to_centroid_m, 0.95)").alias("seg_p95_dist_m")))

    segments = (seg0.join(disp, ["device_id","segment_id"], "left")
                .withColumn("is_stable",
                            (F.col("seg_points")>=F.lit(PARAMS["SEG_MIN_POINTS"])) &
                            (F.col("seg_duration_min")>=F.lit(PARAMS["SEG_MIN_DURATION_MIN"])) &
                            (F.col("seg_max_dist_m")<=F.lit(PARAMS["SEG_RADIUS_M"])))
                .withColumn("day", F.lit(day)))

    # On écrit les segments du jour en append (table daily)
    (segments.write.mode("append").format("parquet").partitionBy("day").saveAsTable(CONFIG["SEGMENT_DAILY_TABLE"]))

    # Mise à jour device_state: dernier point non-seed du jour
    last_point = (seg.where(F.col("is_seed")==F.lit(False))
                  .withColumn("rn", F.row_number().over(Window.partitionBy("device_id").orderBy(F.col("ts").desc())))
                  .where(F.col("rn")==1)
                  .select("device_id",
                          F.col("ts").alias("last_ts"),
                          F.col("lat").alias("last_lat"),
                          F.col("lon").alias("last_lon"),
                          F.col("segment_id").alias("last_segment_id"))
                 )

    device_state_new = last_point.withColumn("last_update_ts", F.current_timestamp())
    ensure_bucketed_table_exists(spark, CONFIG["DEVICE_STATE_TABLE"], add_bucket(device_state_new, "device_id", CONFIG["BUCKETS"]))
    upsert_bucket_overwrite(spark, CONFIG["DEVICE_STATE_TABLE"], device_state_new, "device_id", CONFIG["BUCKETS"])

    return segments

# ============================================================
# STEP 3 — MATCHES DAILY (wifi -> position)
# Priorité:
#   1) strict (wifi_ts proche d'un point gps)
#   2) sinon segment stable (wifi_ts dans l'intervalle d'un segment stable ± buffer)
# ============================================================
def build_matches_daily(spark, day):
    wifi_d = (spark.table(CONFIG["WIFI_RAW_TABLE"])
              .where(F.col("day")==F.lit(day))
              .where(F.col("device_id").isNotNull() & F.col("router_ip").isNotNull() & F.col("wifi_ts").isNotNull())
              .select("device_id", F.col("router_ip").alias("router_ip"), F.col("wifi_ts").alias("wifi_ts")))

    gps_d = (spark.table(CONFIG["GPS_RAW_TABLE"])
             .where(F.col("day")==F.lit(day))
             .where(F.col("device_id").isNotNull() & F.col("ts").isNotNull()
                    & F.col("lat").isNotNull() & F.col("lon").isNotNull())
             .select("device_id",
                     F.col("ts").alias("ts"),
                     F.col("lat").cast("double").alias("lat"),
                     F.col("lon").cast("double").alias("lon")))

    # ---- (1) strict join ±STRICT_JOIN_MIN
    strict_expr = f"INTERVAL {PARAMS['STRICT_JOIN_MIN']} MINUTES"
    strict = (wifi_d.alias("w")
              .join(gps_d.alias("g"),
                    (F.col("w.device_id")==F.col("g.device_id")) &
                    (F.col("g.ts").between(F.col("w.wifi_ts")-F.expr(strict_expr),
                                           F.col("w.wifi_ts")+F.expr(strict_expr))),
                    "left")
              .withColumn("dt_sec", F.abs(F.unix_timestamp("w.wifi_ts")-F.unix_timestamp("g.ts"))))

    w_pick = Window.partitionBy("w.device_id","w.router_ip","w.wifi_ts").orderBy(F.col("dt_sec").asc_nulls_last(), F.col("g.ts").desc_nulls_last())
    best_strict = (strict.withColumn("rn", F.row_number().over(w_pick))
                        .where(F.col("rn")==1).drop("rn")
                        .withColumn("match_type", F.when(F.col("g.ts").isNotNull(), F.lit("strict")).otherwise(F.lit(None)))
                        .select(
                            F.col("w.device_id").alias("device_id"),
                            F.col("w.router_ip").alias("router_ip"),
                            F.col("w.wifi_ts").alias("wifi_ts"),
                            F.col("g.lat").alias("lat"),
                            F.col("g.lon").alias("lon"),
                            "match_type",
                            F.col("dt_sec").alias("dt_sec")
                        ))

    # ---- (2) segment fallback (uniquement quand strict absent)
    no_strict = best_strict.where(F.col("match_type").isNull()).select("device_id","router_ip","wifi_ts")

    seg_d = (spark.table(CONFIG["SEGMENT_DAILY_TABLE"])
             .where(F.col("day")==F.lit(day))
             .where(F.col("is_stable")==F.lit(True))
             .select("device_id","segment_id","seg_lat","seg_lon","seg_start_ts","seg_end_ts","seg_points","seg_max_dist_m"))

    buffer_expr = f"INTERVAL {PARAMS['SEG_WIFI_BUFFER_MIN']} MINUTES"
    seg_join = (no_strict.alias("w")
                .join(seg_d.alias("s"),
                      (F.col("w.device_id")==F.col("s.device_id")) &
                      (F.col("w.wifi_ts").between(F.col("s.seg_start_ts")-F.expr(buffer_expr),
                                                  F.col("s.seg_end_ts")+F.expr(buffer_expr))),
                      "left")
                # dt_to_seg_sec = distance temporelle au segment (0 si à l'intérieur)
                .withColumn("dt_to_seg_sec",
                            F.when(F.col("s.seg_start_ts").isNull(), F.lit(None).cast("long"))
                             .when(F.col("w.wifi_ts") < F.col("s.seg_start_ts"), F.unix_timestamp("s.seg_start_ts")-F.unix_timestamp("w.wifi_ts"))
                             .when(F.col("w.wifi_ts") > F.col("s.seg_end_ts"),   F.unix_timestamp("w.wifi_ts")-F.unix_timestamp("s.seg_end_ts"))
                             .otherwise(F.lit(0))))

    w_pick_seg = Window.partitionBy("w.device_id","w.router_ip","w.wifi_ts").orderBy(
        F.col("dt_to_seg_sec").asc_nulls_last(),
        F.col("s.seg_max_dist_m").asc_nulls_last(),
        F.col("s.seg_end_ts").desc_nulls_last()
    )

    best_seg = (seg_join.withColumn("rn", F.row_number().over(w_pick_seg))
                      .where(F.col("rn")==1).drop("rn")
                      .withColumn("match_type", F.when(F.col("s.segment_id").isNotNull(), F.lit("segment")).otherwise(F.lit(None)))
                      .select(
                          F.col("w.device_id").alias("device_id"),
                          F.col("w.router_ip").alias("router_ip"),
                          F.col("w.wifi_ts").alias("wifi_ts"),
                          F.col("s.seg_lat").alias("lat"),
                          F.col("s.seg_lon").alias("lon"),
                          "match_type",
                          F.col("dt_to_seg_sec").alias("dt_sec"),
                          F.col("s.seg_points").alias("seg_points"),
                          F.col("s.seg_max_dist_m").alias("seg_max_dist_m")
                      ))

    matches = (best_strict.where(F.col("match_type").isNotNull())
               .unionByName(best_seg.where(F.col("match_type").isNotNull()), allowMissingColumns=True)
               .withColumn("day", F.lit(day)))

    # Confiance de l'observation: strict = plus haut, segment = dépend de dispersion et dt
    matches = (matches.withColumn(
        "conf_point",
        F.when(F.col("match_type")=="strict",
               F.round(90 - F.least(F.lit(30.0), F.col("dt_sec")/F.lit(10.0))).cast("int"))
         .otherwise(
               F.round(
                   80
                   - F.least(F.lit(25.0), F.col("dt_sec")/F.lit(60.0))
                   - F.least(F.lit(20.0), F.coalesce(F.col("seg_max_dist_m"),F.lit(0.0))/F.lit(10.0))
               ).cast("int")
         )
    ))

    # Filtre bbox sur matches (évite router hors pays)
    matches = matches.where(in_bbox(F.col("lat"),F.col("lon"), CONFIG["COUNTRY_BBOX"]))

    (matches.write.mode("append").format("parquet").partitionBy("day").saveAsTable(CONFIG["MATCHES_DAILY_TABLE"]))
    return matches

# ============================================================
# STEP 4 — ROUTER_DAILY (position robuste par routeur)
# - médiane lat/lon pour centre initial
# - outliers: distance au centre -> coupe au p95 (cap à MAX_OUTLIER_M)
# - recompute centre final en moyenne pondérée
# ============================================================
def build_router_daily(spark, day):
    m = (spark.table(CONFIG["MATCHES_DAILY_TABLE"])
         .where(F.col("day")==F.lit(day))
         .where(F.col("router_ip").isNotNull() & F.col("wifi_ts").isNotNull()
                & F.col("lat").isNotNull() & F.col("lon").isNotNull())
         .select("router_ip","device_id","wifi_ts","lat","lon","match_type",F.col("conf_point").cast("double").alias("conf_point")))

    # Poids:
    # - strict > segment
    # - récent > ancien
    # - conf_point renforce
    m = (m.withColumn("w_type", F.when(F.col("match_type")=="strict", F.lit(1.0)).otherwise(F.lit(0.65)))
          .withColumn("age_days", F.datediff(F.current_timestamp(), F.col("wifi_ts")).cast("double"))
          .withColumn("w_rec", exp_decay_days(F.col("age_days"), PARAMS["TAU_RECENCY_DAYS"]))
          .withColumn("w", F.col("w_type") * F.col("w_rec") * (F.col("conf_point")/F.lit(100.0))))

    center0 = (m.groupBy("router_ip")
               .agg(F.expr("percentile_approx(lat, 0.5)").alias("lat_med"),
                    F.expr("percentile_approx(lon, 0.5)").alias("lon_med"),
                    F.count("*").alias("obs_total"),
                    F.countDistinct("device_id").alias("devices_distinct"),
                    F.max("wifi_ts").alias("last_seen_ts")))

    m2 = (m.join(center0.select("router_ip","lat_med","lon_med"), "router_ip", "left")
            .withColumn("d_m", haversine_m(F.col("lat_med"),F.col("lon_med"),F.col("lat"),F.col("lon"))))

    dist_stats = (m2.groupBy("router_ip")
                  .agg(F.expr(f"percentile_approx(d_m, {PARAMS['OUTLIER_PCTL']})").alias("d_p95"))
                  .withColumn("d_cut", F.least(F.col("d_p95"), F.lit(PARAMS["MAX_OUTLIER_M"]))))

    # On garde uniquement les points proches du centre médian -> robuste aux outliers
    m3 = (m2.join(dist_stats.select("router_ip","d_cut"), "router_ip", "left")
            .where(F.col("d_m") <= F.col("d_cut")))

    router_daily = (m3.groupBy("router_ip")
                    .agg(
                        (F.sum(F.col("lat")*F.col("w"))/F.sum("w")).alias("router_lat"),
                        (F.sum(F.col("lon")*F.col("w"))/F.sum("w")).alias("router_lon"),
                        F.count("*").alias("obs_kept"),
                        F.countDistinct("device_id").alias("devices_kept"),
                        F.avg("conf_point").alias("avg_point_conf"),
                        F.max("wifi_ts").alias("last_seen_ts"),
                        F.sum("w").alias("w_sum")
                    )
                    .withColumn("day", F.lit(day)))

    # Dispersion autour du centre final (p95) -> sert au scoring
    m4 = (m3.join(router_daily.select("router_ip","router_lat","router_lon"), "router_ip", "left")
            .withColumn("d_final_m", haversine_m(F.col("router_lat"),F.col("router_lon"),F.col("lat"),F.col("lon"))))

    disp = (m4.groupBy("router_ip")
            .agg(F.expr("percentile_approx(d_final_m, 0.95)").alias("disp_p95_m"),
                 F.expr("percentile_approx(d_final_m, 0.5)").alias("disp_p50_m"),
                 F.max("d_final_m").alias("disp_max_m")))

    router_daily = router_daily.join(disp, "router_ip", "left").join(center0, "router_ip", "left")

    # Score routeur 0..100
    router_daily = (router_daily
        .withColumn("age_days_last", F.datediff(F.current_timestamp(), F.col("last_seen_ts")).cast("double"))
        .withColumn("S_rec", exp_decay_days(F.col("age_days_last"), PARAMS["TAU_RECENCY_DAYS"]))
        .withColumn("S_dev", F.least(F.lit(1.0), F.log1p(F.col("devices_kept"))/F.log1p(F.lit(20.0))))
        .withColumn("S_obs", F.least(F.lit(1.0), F.log1p(F.col("obs_kept"))/F.log1p(F.lit(200.0))))
        .withColumn("S_disp", F.exp(-F.col("disp_p95_m")/F.lit(250.0)))
        .withColumn("conf01", F.clamp(0.30*F.col("S_rec")+0.25*F.col("S_dev")+0.20*F.col("S_obs")+0.25*F.col("S_disp"), 0, 1))
        .withColumn("router_confidence", F.round(F.col("conf01")*100).cast("int"))
        .withColumn("flags", F.array_remove(F.array(
            F.when(F.col("devices_kept") < F.lit(2), F.lit("LOW_DEVICE_COUNT")),
            F.when(F.col("disp_p95_m") > F.lit(1000.0), F.lit("HIGH_DISPERSION")),
            F.when(F.col("obs_kept") < F.lit(5), F.lit("LOW_OBS"))
        ), F.lit(None)))
    )

    (router_daily.write.mode("append").format("parquet").partitionBy("day").saveAsTable(CONFIG["ROUTER_DAILY_TABLE"]))
    return router_daily

# ============================================================
# STEP 5 — ROUTER_STATE (hystérésis)
# - Evite que la position du routeur saute tous les jours
# - On "switch" vers la nouvelle position seulement si elle est nettement meilleure
# ============================================================
def update_router_state(spark, day):
    daily = (spark.table(CONFIG["ROUTER_DAILY_TABLE"])
             .where(F.col("day")==F.lit(day))
             .select("router_ip","router_lat","router_lon","router_confidence","last_seen_ts","flags"))

    # Etat précédent: dernière position acceptée + confiance
    try:
        prev = spark.table(CONFIG["ROUTER_STATE_TABLE"]).select(
            "router_ip",
            F.col("lat").alias("prev_lat"),
            F.col("lon").alias("prev_lon"),
            F.col("router_confidence").alias("prev_conf"),
            F.col("last_seen_ts").alias("prev_last_seen_ts"),
            F.col("flags").alias("prev_flags")
        )
    except Exception:
        prev = spark.createDataFrame([], "router_ip string, prev_lat double, prev_lon double, prev_conf int, prev_last_seen_ts timestamp, prev_flags array<string>")

    cand = (daily.join(prev, "router_ip", "left")
            # jump = distance entre ancienne et nouvelle position
            .withColumn("jump_m", haversine_m(F.col("prev_lat"),F.col("prev_lon"),F.col("router_lat"),F.col("router_lon")))
            .withColumn("jump_suspect", (F.col("jump_m") > F.lit(PARAMS["JUMP_SUSPECT_M"])))
            # si état précédent trop vieux => on accepte plus facilement le switch
            .withColumn("prev_age_days", F.datediff(F.current_timestamp(), F.col("prev_last_seen_ts")).cast("double"))
            .withColumn("prev_stale", F.col("prev_last_seen_ts").isNull() | (F.col("prev_age_days") > F.lit(PARAMS["STALE_DAYS"])))
            # hystérésis: new doit battre old d'au moins HYST_DELTA
            .withColumn("should_switch",
                        F.when(F.col("prev_conf").isNull(), F.lit(True))
                         .when(F.col("prev_stale"), F.lit(True))
                         .when(F.col("router_confidence") >= (F.col("prev_conf")+F.lit(PARAMS["HYST_DELTA"])), F.lit(True))
                         .otherwise(F.lit(False)))
           )

    # Si jump énorme, on est encore plus conservateur (évite téléportations)
    cand = cand.withColumn(
        "should_switch",
        F.when(F.col("jump_suspect") & (F.col("router_confidence") < (F.col("prev_conf")+F.lit(PARAMS["HYST_DELTA"]+10))), F.lit(False))
         .otherwise(F.col("should_switch"))
    )

    new_state = (cand.select(
        "router_ip",
        F.when(F.col("should_switch"), F.col("router_lat")).otherwise(F.col("prev_lat")).alias("lat"),
        F.when(F.col("should_switch"), F.col("router_lon")).otherwise(F.col("prev_lon")).alias("lon"),
        F.when(F.col("should_switch"), F.col("router_confidence")).otherwise(F.col("prev_conf")).alias("router_confidence"),
        # last_seen = max(prev, new)
        F.greatest(F.col("last_seen_ts"), F.col("prev_last_seen_ts")).alias("last_seen_ts"),
        F.current_timestamp().alias("last_update_ts"),
        # flags: union prev + daily + jump suspect
        F.array_distinct(F.concat(F.coalesce(F.col("prev_flags"),F.array()),
                                 F.coalesce(F.col("flags"),F.array()),
                                 F.when(F.col("jump_suspect"), F.array(F.lit("JUMP_SUSPECT"))).otherwise(F.array())
                                )).alias("flags")
    ))

    ensure_bucketed_table_exists(spark, CONFIG["ROUTER_STATE_TABLE"], add_bucket(new_state, "router_ip", CONFIG["BUCKETS"]))
    upsert_bucket_overwrite(spark, CONFIG["ROUTER_STATE_TABLE"], new_state, "router_ip", CONFIG["BUCKETS"])
    return new_state

# ============================================================
# STEP 6 — CAMERA_IP_STATE (ip_best sur 30 jours à partir de comm_daily)
# - But: associer chaque caméra à une IP "représentative"
# - On combine: share (fréquence) + récence + volume
# ============================================================
def update_camera_ip_state(spark, day):
    start_day = F.date_sub(F.lit(day).cast("date"), PARAMS["IP_WINDOW_DAYS"]-1)

    comm30 = (spark.table(CONFIG["COMM_DAILY_TABLE"])
              .where(F.col("day").between(F.date_format(start_day, "yyyy-MM-dd"), F.lit(day)))
              .select("camera_id","ip","obs_count","last_ts"))

    cam_total = comm30.groupBy("camera_id").agg(
        F.sum("obs_count").alias("obs_total"),
        F.max("last_ts").alias("cam_last_seen_ts")
    )

    cam_ip = comm30.groupBy("camera_id","ip").agg(
        F.sum("obs_count").alias("obs_ip"),
        F.max("last_ts").alias("last_seen_ip_ts")
    )

    scored = (cam_ip.join(cam_total, "camera_id", "left")
              .withColumn("share", F.col("obs_ip")/F.col("obs_total"))
              .withColumn("age_days_ip", F.datediff(F.current_timestamp(), F.col("last_seen_ip_ts")).cast("double"))
              .withColumn("S_rec", exp_decay_days(F.col("age_days_ip"), 14.0))
              .withColumn("S_vol", F.least(F.lit(1.0), F.log1p(F.col("obs_ip"))/F.log1p(F.lit(50.0))))
              .withColumn("score01", F.clamp(0.6*F.col("share")+0.25*F.col("S_rec")+0.15*F.col("S_vol"), 0, 1))
             )

    w = Window.partitionBy("camera_id").orderBy(F.col("score01").desc(), F.col("last_seen_ip_ts").desc())
    best = (scored.withColumn("rn", F.row_number().over(w)).where(F.col("rn")==1).drop("rn")
            .select(
                "camera_id",
                F.col("ip").alias("ip_best"),
                F.col("last_seen_ip_ts").alias("ip_best_last_seen_ts"),
                F.col("obs_ip").alias("ip_best_obs_count"),
                F.col("share").alias("ip_best_share"),
                F.round(F.col("score01")*100).cast("int").alias("ip_confidence"),
                F.col("cam_last_seen_ts").alias("last_seen_ts"),
                F.current_timestamp().alias("last_update_ts")
            ))

    ensure_bucketed_table_exists(spark, CONFIG["CAMERA_IP_STATE_TABLE"], add_bucket(best, "camera_id", CONFIG["BUCKETS"]))
    upsert_bucket_overwrite(spark, CONFIG["CAMERA_IP_STATE_TABLE"], best, "camera_id", CONFIG["BUCKETS"])
    return best

# ============================================================
# STEP 7 — CAMERA_MASTER (camera -> ip_best -> router_state)
# - On récupère lat/lon du routeur et un score camera_conf
# - Puis hystérésis pour éviter que la caméra saute trop facilement
# ============================================================
def update_camera_master(spark, day):
    # Caméras impactées: vues aujourd'hui (tu peux élargir: aussi si router_state change)
    comm_today = (spark.table(CONFIG["COMM_DAILY_TABLE"])
                  .where(F.col("day")==F.lit(day))
                  .select("camera_id").distinct())

    cam_ip = (spark.table(CONFIG["CAMERA_IP_STATE_TABLE"])
              .join(comm_today, "camera_id", "inner")
              .select("camera_id","ip_best","ip_best_share","ip_confidence","last_seen_ts"))

    router = (spark.table(CONFIG["ROUTER_STATE_TABLE"])
              .select(F.col("router_ip").alias("ip_best"),
                      F.col("lat").alias("r_lat"),
                      F.col("lon").alias("r_lon"),
                      F.col("router_confidence").alias("r_conf"),
                      F.col("last_seen_ts").alias("r_last_seen_ts")))

    cand = (cam_ip.join(router, "ip_best", "left")
            .withColumn("hard_reject", F.col("r_lat").isNull() | F.col("r_lon").isNull())
            .withColumn("is_valid_bbox", in_bbox(F.col("r_lat"),F.col("r_lon"), CONFIG["COUNTRY_BBOX"]))
            .withColumn("S_router", F.coalesce(F.col("r_conf")/100.0, F.lit(0.0)))
            .withColumn("S_ipshare", F.clamp(F.coalesce(F.col("ip_best_share"),F.lit(0.0)), 0, 1))
            # score caméra: routeur (65%) + stabilité IP (35%)
            .withColumn("score01",
                        F.when(F.col("hard_reject"), F.lit(0.0))
                         .otherwise(F.clamp(0.65*F.col("S_router")+0.35*F.col("S_ipshare"), 0, 1)))
            .withColumn("candidate_conf", F.round(F.col("score01")*100).cast("int"))
            # si hors bbox => on casse le score
            .withColumn("candidate_conf",
                        F.when(~F.col("is_valid_bbox"), F.round(F.col("candidate_conf")*F.lit(0.05)).cast("int"))
                         .otherwise(F.col("candidate_conf")))
            .withColumn("flags", F.array_remove(F.array(
                F.when(F.col("hard_reject"), F.lit("HARD_REJECT")),
                F.when(~F.col("is_valid_bbox"), F.lit("OUT_COUNTRY_BBOX")),
                F.when(F.col("ip_best_share") < 0.25, F.lit("LOW_IP_STABILITY"))
            ), F.lit(None)))
           )

    # état caméra précédent
    try:
        prev = (spark.table(CONFIG["CAMERA_MASTER_TABLE"])
                .join(comm_today, "camera_id", "inner")
                .select(
                    "camera_id",
                    F.col("lat").alias("prev_lat"),
                    F.col("lon").alias("prev_lon"),
                    F.col("confidence").alias("prev_conf"),
                    F.col("last_seen_ts").alias("prev_last_seen_ts"),
                    F.col("flags").alias("prev_flags")
                ))
    except Exception:
        prev = spark.createDataFrame([], "camera_id string, prev_lat double, prev_lon double, prev_conf int, prev_last_seen_ts timestamp, prev_flags array<string>")

    merged = (cand.join(prev, "camera_id", "left")
              .withColumn("jump_m", haversine_m(F.col("prev_lat"),F.col("prev_lon"),F.col("r_lat"),F.col("r_lon")))
              .withColumn("jump_suspect", (F.col("jump_m") > F.lit(PARAMS["JUMP_SUSPECT_M"])))
              .withColumn("prev_age_days", F.datediff(F.current_timestamp(), F.col("prev_last_seen_ts")).cast("double"))
              .withColumn("prev_stale", F.col("prev_last_seen_ts").isNull() | (F.col("prev_age_days") > F.lit(PARAMS["STALE_DAYS"])))
              .withColumn("should_switch",
                          F.when(F.col("prev_conf").isNull(), F.lit(True))
                           .when(F.col("prev_stale"), F.lit(True))
                           .when(F.col("candidate_conf") >= (F.col("prev_conf")+F.lit(PARAMS["HYST_DELTA"])), F.lit(True))
                           .otherwise(F.lit(False)))
             )

    merged = merged.withColumn(
        "should_switch",
        F.when(F.col("jump_suspect") & (F.col("candidate_conf") < (F.col("prev_conf")+F.lit(PARAMS["HYST_DELTA"]+10))), F.lit(False))
         .otherwise(F.col("should_switch"))
    )

    new_master = (merged.select(
        "camera_id",
        F.when(F.col("should_switch"), F.col("r_lat")).otherwise(F.col("prev_lat")).alias("lat"),
        F.when(F.col("should_switch"), F.col("r_lon")).otherwise(F.col("prev_lon")).alias("lon"),
        F.when(F.col("should_switch"), F.col("candidate_conf")).otherwise(F.col("prev_conf")).alias("confidence"),
        F.lit("router_state").alias("source"),
        F.greatest(F.col("last_seen_ts"), F.col("prev_last_seen_ts")).alias("last_seen_ts"),
        F.current_timestamp().alias("last_update_ts"),
        F.array_distinct(F.concat(
            F.coalesce(F.col("prev_flags"),F.array()),
            F.coalesce(F.col("flags"),F.array()),
            F.when(F.col("jump_suspect"), F.array(F.lit("JUMP_SUSPECT"))).otherwise(F.array())
        )).alias("flags")
    ))

    ensure_bucketed_table_exists(spark, CONFIG["CAMERA_MASTER_TABLE"], add_bucket(new_master, "camera_id", CONFIG["BUCKETS"]))
    upsert_bucket_overwrite(spark, CONFIG["CAMERA_MASTER_TABLE"], new_master, "camera_id", CONFIG["BUCKETS"])
    return new_master

# ============================================================
# ORCHESTRATION DAILY
# ============================================================
def run_daily(spark, day):
    # Conseil: fixe le timezone pour éviter des surprises dans datediff etc.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # 1) Résumé comm du jour
    build_comm_daily(spark, day)

    # 2) Segments GPS incremental
    build_segments_incremental(spark, day)

    # 3) Matches Wi-Fi -> position (strict puis segment)
    build_matches_daily(spark, day)

    # 4) RouterDaily (robuste)
    build_router_daily(spark, day)

    # 5) RouterState (hystérésis)
    update_router_state(spark, day)

    # 6) CameraIPState (ip_best 30j)
    update_camera_ip_state(spark, day)

    # 7) CameraMaster (position caméra)
    update_camera_master(spark, day)

# ============================================================
# RUN EXAMPLE
# ============================================================
# run_daily(spark, "2026-01-13")
