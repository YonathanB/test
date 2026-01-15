# ============================================================
# PYSPARK PIPELINE (Hive + Metastore, sans Delta)
# Objectif:
# 1) Localiser des ROUTEURS (ou "points d'accès réseau") à partir de (wifi_event + gps)
# 2) Créer un identifiant "router_id" STABLE basé sur la géographie (pas sur l'IP)
# 3) Associer les CAMERAS à un router_id (par proximité géographique des IP observées)
# 4) Localiser les CAMERAS et détecter les CAMERAS MOBILES (instabilité spatiale)
# 5) Tout faire INCREMENTAL quotidien, sans "read & overwrite same table"
#
# Hypothèses sur tes sources:
# - raw.wifi(day, device_id, router_ip, wifi_ts)
# - raw.gps (day, device_id, ts, lat, lon)
# - raw.comm(day, camera_id, ip, ts)  # "camera -> ip vue" (énorme, partitionnée par day)
#
# Notes:
# - On n'utilise PAS une "ip_best sur 30 jours" comme vérité.
#   L'IP est un tremplin: on calcule d'abord la position des IP (ip_state),
#   puis on mappe les IP vers un router_id géographique stable.
# - On écrit des tables DAILY (append) + tables STATE (upsert par bucket via staging+insert overwrite)
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql import Window
import uuid

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
CFG = {
    "COMM_RAW": "raw.comm",
    "GPS_RAW":  "raw.gps",
    "WIFI_RAW": "raw.wifi",

    # daily (append)
    "COMM_DAILY":    "gold.comm_daily",
    "MATCH_DAILY":   "gold.match_daily",     # wifi->pos
    "IP_DAILY":      "gold.ip_daily",        # ip->pos daily (agrégé)
    "ROUTER_DAILY":  "gold.router_daily",    # router_id->pos daily
    "CAM_DAILY":     "gold.camera_daily",    # camera->pos daily
    "MOB_DAILY":     "gold.camera_mobility_daily",

    # state (upsert by bucket)
    "DEVICE_STATE":  "gold.device_state",    # optionnel (seed segmentation)
    "IP_STATE":      "gold.ip_state",        # ip -> lat/lon stable + confidence + router_id
    "ROUTER_STATE":  "gold.router_state",    # router_id -> lat/lon stable + confidence
    "CAM_MASTER":    "gold.camera_master",   # camera_id -> lat/lon stable + confidence + router_id
    "CAM_MOB_STATE": "gold.camera_mobility_state",  # camera_id -> mobility score + flags

    "BUCKETS": 256,
}

P = {
    # Matching wifi<->gps
    "STRICT_MIN": 5,            # ±5 min
    "WIDE_MIN": 60,             # fallback window ±60 min (mais on exige K points cohérents)

    # "preuve" minimum si pas de point strict
    "MIN_POINTS_WIDE": 5,
    "MAX_RADIUS_M": 80.0,       # points cohérents dans un rayon ~80m
    "MIN_SPAN_MIN": 30,         # ces points doivent couvrir au moins 30 min (stabilité temporelle)

    # IP / Router aggregation
    "OUTLIER_PCTL": 0.95,
    "MAX_OUTLIER_M": 1500.0,
    "TAU_REC_DAYS": 14.0,

    # Router geo-id
    "ROUTER_CELL_M": 150.0,     # taille cellule (en mètres) pour créer un router_id stable
                                # (plus petit = plus précis mais plus de routeurs distincts)

    # Hysteresis
    "HYST_DELTA": 8,
    "JUMP_SUSPECT_M": 15000.0,
    "STALE_DAYS": 21,

    # Camera association
    "CAM_LINK_MAX_DIST_M": 250.0,   # si IP vue pour la caméra tombe près d'un router_id (<=250m)
    "CAM_LINK_MIN_HITS": 3,         # min d'observations pour accepter le lien camera->router_id

    # Mobility detection
    "MOB_WIN_DAYS": 14,             # fenêtre d'analyse mobilité
    "MOB_MIN_OBS": 6,
    "MOB_JUMP_M": 2000.0,           # sauts > 2km
    "MOB_SWITCH_ZONES": 4,          # nb de zones distinctes (router_id) élevé
    "MOB_SCORE_TH": 70,             # score >= 70 => mobile probable
}

# ------------------------------------------------------------
# UTILS (distance, weights, bucketing, safe upsert Hive)
# ------------------------------------------------------------
def haversine_m(lat1, lon1, lat2, lon2):
    R = F.lit(6371000.0)
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = F.pow(F.sin(dlat/2), 2) + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon/2), 2)
    c = 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))
    return R * c

def exp_decay_days(age_days_col, tau_days):
    return F.exp(-age_days_col / F.lit(float(tau_days)))

def add_bucket(df, key_col, buckets):
    h = F.xxhash64(F.col(key_col))
    h_pos = F.when(h < 0, -h - 1).otherwise(h)
    return df.withColumn("bucket", (h_pos % F.lit(int(buckets))).cast("int"))

def ensure_table_exists_like(spark, table_name, df_with_bucket):
    try:
        spark.table(table_name).limit(1).collect()
    except Exception:
        (df_with_bucket.limit(0)
         .write.mode("overwrite").format("parquet").partitionBy("bucket").saveAsTable(table_name))

def upsert_bucket_hive(spark, target_table, source_df, key_col, buckets):
    """
    Upsert Hive-safe:
    - calc buckets impactés
    - union existing buckets + source
    - latest-wins via last_update_ts
    - write staging table
    - INSERT OVERWRITE TABLE target PARTITION(bucket) for impacted buckets
    """
    spark.sql("SET hive.exec.dynamic.partition=true")
    spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    src_b = add_bucket(source_df, key_col, buckets).persist()

    impacted = [r["bucket"] for r in src_b.select("bucket").distinct().collect()]
    if not impacted:
        src_b.unpersist()
        return

    impacted_sql = ",".join(str(b) for b in impacted)

    try:
        tgt_b = spark.table(target_table).where(F.col("bucket").isin(impacted))
        all_b = tgt_b.unionByName(src_b, allowMissingColumns=True)
    except Exception:
        all_b = src_b

    if "last_update_ts" not in all_b.columns:
        raise ValueError("upsert_bucket_hive: colonne last_update_ts obligatoire.")

    w = Window.partitionBy(key_col).orderBy(F.col("last_update_ts").desc_nulls_last())
    final_b = (all_b.withColumn("rn", F.row_number().over(w))
                    .where(F.col("rn") == 1).drop("rn"))

    staging = f"{target_table}__stg__{uuid.uuid4().hex[:8]}"
    (final_b.write.mode("overwrite").format("parquet").partitionBy("bucket").saveAsTable(staging))

    non_part_cols = [c for c in final_b.columns if c != "bucket"]
    select_cols_sql = ", ".join(non_part_cols + ["bucket"])

    spark.sql(f"""
        INSERT OVERWRITE TABLE {target_table} PARTITION (bucket)
        SELECT {select_cols_sql}
        FROM {staging}
        WHERE bucket IN ({impacted_sql})
    """)
    spark.sql(f"DROP TABLE {staging}")
    src_b.unpersist()


# ------------------------------------------------------------
# STEP 1 — COMM_DAILY: compresser raw.comm (énorme)
# But:
# - réduire la volumétrie
# - garder camera_id <-> ip avec stats du jour
# ------------------------------------------------------------
def step1_comm_daily(spark, day):
    comm = (spark.table(CFG["COMM_RAW"])
            .where(F.col("day") == F.lit(day))
            .where("camera_id is not null and ip is not null and ts is not null")
            .select("camera_id", "ip", "ts"))

    comm_daily = (comm.groupBy("camera_id", "ip")
                  .agg(F.count("*").alias("obs_count"),
                       F.max("ts").alias("last_ts"))
                  .withColumn("day", F.lit(day)))

    (comm_daily.write.mode("append").format("parquet").partitionBy("day").saveAsTable(CFG["COMM_DAILY"]))
    return comm_daily


# ------------------------------------------------------------
# STEP 2 — MATCH_DAILY: wifi->position (logique "preuve")
# But:
# - Pour chaque event wifi(device_id, router_ip, wifi_ts), produire un point (lat,lon) SI fiable.
# Règle:
# - Si un point GPS existe à ±5min => OK (strict)
# - Sinon, on accepte UNIQUEMENT si on a >=K points GPS dans ±WIDE_MIN,
#   cohérents spatialement (rayon <= MAX_RADIUS_M) et couvrant >= MIN_SPAN_MIN.
# ------------------------------------------------------------
def step2_match_daily(spark, day):
    wifi = (spark.table(CFG["WIFI_RAW"])
            .where(F.col("day") == F.lit(day))
            .where("device_id is not null and router_ip is not null and wifi_ts is not null")
            .select("device_id", F.col("router_ip").alias("ip"), F.col("wifi_ts").alias("wifi_ts")))

    gps = (spark.table(CFG["GPS_RAW"])
           .where(F.col("day") == F.lit(day))
           .where("device_id is not null and ts is not null and lat is not null and lon is not null")
           .select("device_id", F.col("ts").alias("gps_ts"),
                   F.col("lat").cast("double").alias("lat"),
                   F.col("lon").cast("double").alias("lon")))

    # ---- STRICT join ±5 minutes => choisir le gps le plus proche
    strict_iv = f"INTERVAL {int(P['STRICT_MIN'])} MINUTES"
    strict = (wifi.alias("w")
              .join(gps.alias("g"),
                    (F.col("w.device_id") == F.col("g.device_id")) &
                    (F.col("g.gps_ts").between(F.col("w.wifi_ts") - F.expr(strict_iv),
                                               F.col("w.wifi_ts") + F.expr(strict_iv))),
                    "left")
              .withColumn("dt_sec", F.abs(F.unix_timestamp("w.wifi_ts") - F.unix_timestamp("g.gps_ts"))))

    w_pick = Window.partitionBy("w.device_id", "w.ip", "w.wifi_ts").orderBy(F.col("dt_sec").asc_nulls_last())
    best_strict = (strict.withColumn("rn", F.row_number().over(w_pick))
                        .where(F.col("rn") == 1).drop("rn")
                        .select(
                            F.col("w.device_id").alias("device_id"),
                            F.col("w.ip").alias("ip"),
                            F.col("w.wifi_ts").alias("wifi_ts"),
                            F.col("g.lat").alias("lat_strict"),
                            F.col("g.lon").alias("lon_strict"),
                            F.col("dt_sec").alias("strict_dt_sec")
                        ))

    # ---- WIDE window candidates ±WIDE_MIN (pour seulement ceux sans strict)
    wide_iv = f"INTERVAL {int(P['WIDE_MIN'])} MINUTES"
    wide = (best_strict.where(F.col("strict_dt_sec").isNull()).alias("w")
            .join(gps.alias("g"),
                  (F.col("w.device_id") == F.col("g.device_id")) &
                  (F.col("g.gps_ts").between(F.col("w.wifi_ts") - F.expr(wide_iv),
                                             F.col("w.wifi_ts") + F.expr(wide_iv))),
                  "left")
            .select("w.device_id", "w.ip", "w.wifi_ts", "g.gps_ts", "g.lat", "g.lon"))

    # S'il n'y a aucun gps dans la fenêtre large => on rejettera de toute façon
    # On va calculer une "cohérence": centre médian + distances => p95 <= MAX_RADIUS_M, K points, span >= MIN_SPAN_MIN
    grp = (wide.groupBy("device_id", "ip", "wifi_ts")
           .agg(
               F.count("*").alias("k_points"),
               F.min("gps_ts").alias("min_ts"),
               F.max("gps_ts").alias("max_ts"),
               F.expr("percentile_approx(lat, 0.5)").alias("lat_med"),
               F.expr("percentile_approx(lon, 0.5)").alias("lon_med"),
               F.collect_list(F.struct("lat", "lon")).alias("pts")  # pour calcul distances via explode
           ))

    # Distance p95 au centre (sans UDF)
    pts_expl = (grp.select("device_id", "ip", "wifi_ts", "k_points", "min_ts", "max_ts", "lat_med", "lon_med",
                           F.explode_outer("pts").alias("p"))
                .select("device_id", "ip", "wifi_ts", "k_points", "min_ts", "max_ts", "lat_med", "lon_med",
                        F.col("p.lat").alias("lat"), F.col("p.lon").alias("lon"))
                .withColumn("d_m", haversine_m(F.col("lat_med"), F.col("lon_med"), F.col("lat"), F.col("lon"))))

    coh = (pts_expl.groupBy("device_id", "ip", "wifi_ts", "k_points", "min_ts", "max_ts", "lat_med", "lon_med")
           .agg(F.expr("percentile_approx(d_m, 0.95)").alias("p95_m"),
                F.max("d_m").alias("max_m")))

    coh = (coh.withColumn("span_min", (F.unix_timestamp("max_ts") - F.unix_timestamp("min_ts"))/60.0)
              .withColumn("wide_ok",
                          (F.col("k_points") >= F.lit(int(P["MIN_POINTS_WIDE"]))) &
                          (F.col("p95_m") <= F.lit(float(P["MAX_RADIUS_M"]))) &
                          (F.col("span_min") >= F.lit(int(P["MIN_SPAN_MIN"])))
                         ))

    # ---- Fusion strict + wide_ok
    # Confiance (simple, ajustable):
    # - strict: très haut, dépend dt
    # - wide: dépend p95_m + k_points + span
    wide_scored = (coh.where(F.col("wide_ok"))
                   .withColumn("conf_wide",
                               F.round(
                                   70
                                   + F.least(F.lit(15.0), F.log1p(F.col("k_points")) * 5)    # + points
                                   + F.least(F.lit(10.0), F.col("span_min")/10.0)           # + span
                                   - F.least(F.lit(25.0), F.col("p95_m")/5.0)               # - dispersion
                               ).cast("int"))
                   .select(
                       "device_id", "ip", "wifi_ts",
                       F.col("lat_med").alias("lat"),
                       F.col("lon_med").alias("lon"),
                       F.lit("wide_stable").alias("match_type"),
                       F.col("conf_wide").alias("conf_point"),
                       F.lit(None).cast("long").alias("dt_sec"),
                       F.col("k_points").alias("k_points"),
                       F.col("p95_m").alias("p95_m"),
                       F.col("span_min").alias("span_min")
                   ))

    strict_scored = (best_strict.where(F.col("strict_dt_sec").isNotNull())
                     .withColumn("conf_point",
                                 F.round(90 - F.least(F.lit(30.0), F.col("strict_dt_sec")/10.0)).cast("int"))
                     .select(
                         "device_id", "ip", "wifi_ts",
                         F.col("lat_strict").alias("lat"),
                         F.col("lon_strict").alias("lon"),
                         F.lit("strict").alias("match_type"),
                         F.col("conf_point").alias("conf_point"),
                         F.col("strict_dt_sec").alias("dt_sec"),
                         F.lit(None).cast("int").alias("k_points"),
                         F.lit(None).cast("double").alias("p95_m"),
                         F.lit(None).cast("double").alias("span_min")
                     ))

    matches = (strict_scored.unionByName(wide_scored, allowMissingColumns=True)
               .withColumn("day", F.lit(day)))

    (matches.write.mode("append").format("parquet").partitionBy("day").saveAsTable(CFG["MATCH_DAILY"]))
    return matches


# ------------------------------------------------------------
# STEP 3 — IP_DAILY + IP_STATE: position stable par IP
# But:
# - À partir de match_daily: ip -> position robuste (médiane + filtre outliers)
# - Score ip_confidence
# - (Option) assigner router_id géographique (step4/5) ensuite
# ------------------------------------------------------------
def step3_ip_daily_and_state(spark, day):
    m = (spark.table(CFG["MATCH_DAILY"])
         .where(F.col("day") == F.lit(day))
         .where("ip is not null and lat is not null and lon is not null and wifi_ts is not null")
         .select("ip", "device_id", "wifi_ts", "lat", "lon", "match_type", F.col("conf_point").cast("double").alias("conf_point")))

    # Poids: type + récence + qualité du point
    m = (m.withColumn("w_type", F.when(F.col("match_type")=="strict", F.lit(1.0)).otherwise(F.lit(0.7)))
          .withColumn("age_days", F.datediff(F.current_timestamp(), F.col("wifi_ts")).cast("double"))
          .withColumn("w_rec", exp_decay_days(F.col("age_days"), P["TAU_REC_DAYS"]))
          .withColumn("w", F.col("w_type") * F.col("w_rec") * (F.col("conf_point")/100.0)))

    center0 = (m.groupBy("ip")
               .agg(F.expr("percentile_approx(lat, 0.5)").alias("lat_med"),
                    F.expr("percentile_approx(lon, 0.5)").alias("lon_med"),
                    F.count("*").alias("obs_total"),
                    F.countDistinct("device_id").alias("dev_cnt"),
                    F.max("wifi_ts").alias("last_seen_ts")))

    m2 = (m.join(center0.select("ip","lat_med","lon_med"), "ip", "left")
            .withColumn("d_m", haversine_m(F.col("lat_med"),F.col("lon_med"),F.col("lat"),F.col("lon"))))

    dist_stats = (m2.groupBy("ip")
                  .agg(F.expr(f"percentile_approx(d_m, {float(P['OUTLIER_PCTL'])})").alias("d_p95"))
                  .withColumn("d_cut", F.least(F.col("d_p95"), F.lit(float(P["MAX_OUTLIER_M"])))))

    m3 = (m2.join(dist_stats.select("ip","d_cut"), "ip", "left")
            .where(F.col("d_m") <= F.col("d_cut")))

    ip_daily = (m3.groupBy("ip")
                .agg(
                    (F.sum(F.col("lat")*F.col("w"))/F.sum("w")).alias("lat"),
                    (F.sum(F.col("lon")*F.col("w"))/F.sum("w")).alias("lon"),
                    F.count("*").alias("obs_kept"),
                    F.countDistinct("device_id").alias("dev_kept"),
                    F.avg("conf_point").alias("avg_point_conf"),
                    F.max("wifi_ts").alias("last_seen_ts"),
                    F.sum("w").alias("w_sum")
                )
                .withColumn("day", F.lit(day)))

    # dispersion autour centre final
    m4 = (m3.join(ip_daily.select("ip","lat","lon"), "ip", "left")
            .withColumn("d_final_m", haversine_m(F.col("lat"),F.col("lon"),F.col("lat"),F.col("lon"))) )  # dummy safe
    # => correction: d_final_m doit être distance point->centre (on recalc correctement)
    m4 = (m3.join(ip_daily.select("ip",F.col("lat").alias("c_lat"),F.col("lon").alias("c_lon")), "ip", "left")
            .withColumn("d_final_m", haversine_m(F.col("c_lat"),F.col("c_lon"),F.col("lat"),F.col("lon"))))

    disp = (m4.groupBy("ip")
            .agg(F.expr("percentile_approx(d_final_m, 0.95)").alias("disp_p95_m"),
                 F.expr("percentile_approx(d_final_m, 0.5)").alias("disp_p50_m")))

    ip_daily = (ip_daily.join(disp, "ip", "left")
                .join(center0, "ip", "left"))

    # score ip 0..100
    ip_daily = (ip_daily
        .withColumn("age_days_last", F.datediff(F.current_timestamp(), F.col("last_seen_ts")).cast("double"))
        .withColumn("S_rec", exp_decay_days(F.col("age_days_last"), P["TAU_REC_DAYS"]))
        .withColumn("S_dev", F.least(F.lit(1.0), F.log1p(F.col("dev_kept"))/F.log1p(F.lit(20.0))))
        .withColumn("S_obs", F.least(F.lit(1.0), F.log1p(F.col("obs_kept"))/F.log1p(F.lit(200.0))))
        .withColumn("S_disp", F.exp(-F.col("disp_p95_m")/F.lit(250.0)))
        .withColumn("conf01", F.clamp(0.30*F.col("S_rec")+0.25*F.col("S_dev")+0.20*F.col("S_obs")+0.25*F.col("S_disp"), 0, 1))
        .withColumn("ip_confidence", F.round(F.col("conf01")*100).cast("int"))
        .withColumn("flags", F.array_remove(F.array(
            F.when(F.col("dev_kept") < 2, F.lit("LOW_DEVICE_COUNT")),
            F.when(F.col("disp_p95_m") > 1000, F.lit("HIGH_DISPERSION")),
            F.when(F.col("obs_kept") < 5, F.lit("LOW_OBS"))
        ), F.lit(None)))
    )

    # write daily
    (ip_daily.write.mode("append").format("parquet").partitionBy("day").saveAsTable(CFG["IP_DAILY"]))

    # upsert ip_state (latest)
    ip_state = (ip_daily.select(
                    "ip","lat","lon","ip_confidence","last_seen_ts","flags"
                )
                .withColumn("router_id", F.lit(None).cast("string"))   # step4 assignera
                .withColumn("last_update_ts", F.current_timestamp()))

    ip_state_b = add_bucket(ip_state, "ip", CFG["BUCKETS"])
    ensure_table_exists_like(spark, CFG["IP_STATE"], ip_state_b)
    upsert_bucket_hive(spark, CFG["IP_STATE"], ip_state, "ip", CFG["BUCKETS"])

    return ip_daily


# ------------------------------------------------------------
# STEP 4 — Créer router_id géographique à partir de ip_state lat/lon
# But:
# - IP change tout le temps -> on veut un identifiant STABLE par zone géographique
# - router_id = hash(geo_cell)
#
# Implémentation simple, très efficace:
# - on convertit lat/lon -> cellule métrique approximative:
#   * cell_y = floor(lat * meters_per_degree_lat / cell_m)
#   * cell_x = floor(lon * meters_per_degree_lon(lat) / cell_m)
# - router_id = sha1(cell_x, cell_y) (ou xxhash64)
#
# Ensuite, on met à jour ip_state.router_id
# ------------------------------------------------------------
def _meters_per_deg_lon(lat_col):
    # approx: 111320 * cos(lat)
    return F.lit(111320.0) * F.cos(F.radians(lat_col))

def _meters_per_deg_lat():
    return F.lit(110540.0)

def step4_assign_router_id_to_ip(spark, day):
    # On prend les IP mises à jour aujourd'hui depuis ip_daily (moins coûteux)
    ip_today = (spark.table(CFG["IP_DAILY"])
                .where(F.col("day") == F.lit(day))
                .select("ip", "lat", "lon", "ip_confidence", "last_seen_ts", "flags")
                .where("ip is not null and lat is not null and lon is not null"))

    cell_m = float(P["ROUTER_CELL_M"])
    ip_with_cell = (ip_today
        .withColumn("cell_y", F.floor((F.col("lat") * _meters_per_deg_lat()) / F.lit(cell_m)).cast("long"))
        .withColumn("cell_x", F.floor((F.col("lon") * _meters_per_deg_lon(F.col("lat"))) / F.lit(cell_m)).cast("long"))
        .withColumn("router_id", F.sha1(F.concat_ws(":", F.col("cell_x").cast("string"), F.col("cell_y").cast("string"))))
    )

    # Upsert dans ip_state : router_id + lat/lon/score etc.
    ip_state_upd = (ip_with_cell
        .select("ip","lat","lon","router_id", F.col("ip_confidence").alias("ip_confidence"),
                "last_seen_ts","flags")
        .withColumn("last_update_ts", F.current_timestamp()))

    upsert_bucket_hive(spark, CFG["IP_STATE"], ip_state_upd, "ip", CFG["BUCKETS"])
    return ip_state_upd


# ------------------------------------------------------------
# STEP 5 — ROUTER_DAILY + ROUTER_STATE: agréger les IP d'un même router_id
# But:
# - Plusieurs IP peuvent tomber dans la même cellule => même router_id
# - On fusionne pour affiner la position du routeur avec le temps
# - Hystérésis: n'update que si amélioration suffisante
# ------------------------------------------------------------
def step5_router_daily_and_state(spark, day):
    # IP du jour avec router_id
    ip_state_today = (spark.table(CFG["IP_STATE"])
                      .join(spark.table(CFG["IP_DAILY"]).where(F.col("day")==F.lit(day)).select("ip").distinct(),
                            "ip", "inner")
                      .select("ip","router_id","lat","lon","ip_confidence","last_seen_ts"))

    # Router daily = agrégation pondérée par ip_confidence + récence
    df = (ip_state_today
          .withColumn("age_days", F.datediff(F.current_timestamp(), F.col("last_seen_ts")).cast("double"))
          .withColumn("w_rec", exp_decay_days(F.col("age_days"), P["TAU_REC_DAYS"]))
          .withColumn("w", (F.col("ip_confidence")/100.0) * F.col("w_rec"))
         )

    router_daily = (df.groupBy("router_id")
        .agg(
            (F.sum(F.col("lat")*F.col("w"))/F.sum("w")).alias("lat"),
            (F.sum(F.col("lon")*F.col("w"))/F.sum("w")).alias("lon"),
            F.count("*").alias("ips_used"),
            F.max("last_seen_ts").alias("last_seen_ts"),
            F.avg("ip_confidence").alias("avg_ip_conf")
        )
        .withColumn("day", F.lit(day))
    )

    # Router confidence (simple, ajustable)
    router_daily = (router_daily
        .withColumn("S_ip", F.least(F.lit(1.0), F.log1p(F.col("ips_used"))/F.log1p(F.lit(15.0))))
        .withColumn("S_conf", F.col("avg_ip_conf")/100.0)
        .withColumn("conf01", F.clamp(0.55*F.col("S_conf") + 0.45*F.col("S_ip"), 0, 1))
        .withColumn("router_confidence", F.round(F.col("conf01")*100).cast("int"))
        .withColumn("flags", F.array_remove(F.array(
            F.when(F.col("ips_used") < 2, F.lit("LOW_IP_COUNT"))
        ), F.lit(None)))
    )

    (router_daily.write.mode("append").format("parquet").partitionBy("day").saveAsTable(CFG["ROUTER_DAILY"]))

    # --- Router state with hysteresis
    try:
        prev = spark.table(CFG["ROUTER_STATE"]).select(
            "router_id",
            F.col("lat").alias("prev_lat"),
            F.col("lon").alias("prev_lon"),
            F.col("router_confidence").alias("prev_conf"),
            F.col("last_seen_ts").alias("prev_last_seen_ts"),
            F.col("flags").alias("prev_flags")
        )
    except Exception:
        prev = spark.createDataFrame([], "router_id string, prev_lat double, prev_lon double, prev_conf int, prev_last_seen_ts timestamp, prev_flags array<string>")

    cand = (router_daily.select("router_id","lat","lon","router_confidence","last_seen_ts","flags")
            .join(prev, "router_id", "left")
            .withColumn("jump_m", haversine_m(F.col("prev_lat"),F.col("prev_lon"),F.col("lat"),F.col("lon")))
            .withColumn("jump_suspect", F.col("jump_m") > F.lit(float(P["JUMP_SUSPECT_M"])))
            .withColumn("prev_age_days", F.datediff(F.current_timestamp(), F.col("prev_last_seen_ts")).cast("double"))
            .withColumn("prev_stale", F.col("prev_last_seen_ts").isNull() | (F.col("prev_age_days") > F.lit(int(P["STALE_DAYS"]))))
            .withColumn("should_switch",
                        F.when(F.col("prev_conf").isNull(), F.lit(True))
                         .when(F.col("prev_stale"), F.lit(True))
                         .when(F.col("router_confidence") >= (F.col("prev_conf")+F.lit(int(P["HYST_DELTA"]))), F.lit(True))
                         .otherwise(F.lit(False)))
           )

    cand = cand.withColumn(
        "should_switch",
        F.when(F.col("jump_suspect") & (F.col("router_confidence") < (F.col("prev_conf")+F.lit(int(P["HYST_DELTA"])+10))), F.lit(False))
         .otherwise(F.col("should_switch"))
    )

    router_state = (cand.select(
        "router_id",
        F.when(F.col("should_switch"), F.col("lat")).otherwise(F.col("prev_lat")).alias("lat"),
        F.when(F.col("should_switch"), F.col("lon")).otherwise(F.col("prev_lon")).alias("lon"),
        F.when(F.col("should_switch"), F.col("router_confidence")).otherwise(F.col("prev_conf")).alias("router_confidence"),
        F.greatest(F.col("last_seen_ts"), F.col("prev_last_seen_ts")).alias("last_seen_ts"),
        F.current_timestamp().alias("last_update_ts"),
        F.array_distinct(F.concat(
            F.coalesce(F.col("prev_flags"), F.array()),
            F.coalesce(F.col("flags"), F.array()),
            F.when(F.col("jump_suspect"), F.array(F.lit("JUMP_SUSPECT"))).otherwise(F.array())
        )).alias("flags")
    ))

    router_state_b = add_bucket(router_state, "router_id", CFG["BUCKETS"])
    ensure_table_exists_like(spark, CFG["ROUTER_STATE"], router_state_b)
    upsert_bucket_hive(spark, CFG["ROUTER_STATE"], router_state, "router_id", CFG["BUCKETS"])

    return router_daily


# ------------------------------------------------------------
# STEP 6 — CAMERA_DAILY: camera -> router_id (via IP -> router_id)
# But:
# - L'IP bouge => on ne cherche PAS ip_best.
# - Chaque jour, on observe des (camera_id, ip) dans comm_daily,
#   on mappe ces ip vers router_id via ip_state,
#   on garde le router_id qui reçoit assez de "hits" et est géographiquement cohérent.
# ------------------------------------------------------------
def step6_camera_daily(spark, day):
    comm = (spark.table(CFG["COMM_DAILY"])
            .where(F.col("day")==F.lit(day))
            .select("camera_id","ip","obs_count","last_ts"))

    ip_state = spark.table(CFG["IP_STATE"]).select("ip","router_id","lat","lon","ip_confidence","last_seen_ts")

    # camera_ip_observations enrichies
    cam_ip = (comm.join(ip_state, "ip", "left")
              .where("router_id is not null and lat is not null and lon is not null")
              .withColumn("w", (F.col("obs_count").cast("double") * (F.col("ip_confidence")/100.0)))
             )

    # Pour une camera_id: quel router_id domine aujourd'hui ?
    by_router = (cam_ip.groupBy("camera_id","router_id")
                 .agg(F.sum("obs_count").alias("hits"),
                      F.sum("w").alias("w_sum"),
                      F.max("last_ts").alias("last_ts"),
                      F.expr("percentile_approx(lat, 0.5)").alias("lat_med"),
                      F.expr("percentile_approx(lon, 0.5)").alias("lon_med")))

    # Exiger min hits
    by_router = by_router.where(F.col("hits") >= F.lit(int(P["CAM_LINK_MIN_HITS"])))

    # Choisir le meilleur router_id pour la camera (par w_sum puis last_ts)
    w_pick = Window.partitionBy("camera_id").orderBy(F.col("w_sum").desc(), F.col("last_ts").desc())
    best = (by_router.withColumn("rn", F.row_number().over(w_pick))
            .where(F.col("rn")==1).drop("rn")
            .withColumn("day", F.lit(day)))

    # Position camera du jour = position router_state (plus stable) si dispo, sinon médiane IP
    router_state = spark.table(CFG["ROUTER_STATE"]).select("router_id", F.col("lat").alias("r_lat"), F.col("lon").alias("r_lon"),
                                                          "router_confidence","last_seen_ts")

    cam_daily = (best.join(router_state, "router_id", "left")
                 .withColumn("lat",
                             F.when(F.col("r_lat").isNotNull(), F.col("r_lat")).otherwise(F.col("lat_med")))
                 .withColumn("lon",
                             F.when(F.col("r_lon").isNotNull(), F.col("r_lon")).otherwise(F.col("lon_med")))
                 .withColumn("camera_confidence",
                             F.round(F.clamp(
                                 0.7*(F.coalesce(F.col("router_confidence"),F.lit(0))/100.0) +
                                 0.3*F.least(F.lit(1.0), F.log1p(F.col("hits"))/F.log1p(F.lit(50.0))),
                                 0, 1
                             )*100).cast("int"))
                 .withColumn("flags", F.array_remove(F.array(
                     F.when(F.col("router_confidence").isNull(), F.lit("ROUTER_STATE_MISSING"))
                 ), F.lit(None)))
                 .select("day","camera_id","router_id","lat","lon","hits","last_ts",
                         "camera_confidence","flags")
                )

    (cam_daily.write.mode("append").format("parquet").partitionBy("day").saveAsTable(CFG["CAM_DAILY"]))
    return cam_daily


# ------------------------------------------------------------
# STEP 7 — CAMERA_MASTER: stabilité camera (hystérésis)
# But:
# - position stable finale de la camera
# - ne pas "sauter" si variation faible / bruit
# ------------------------------------------------------------
def step7_camera_master(spark, day):
    cam_daily = (spark.table(CFG["CAM_DAILY"])
                 .where(F.col("day")==F.lit(day))
                 .select("camera_id","router_id","lat","lon","camera_confidence", F.col("last_ts").alias("last_seen_ts"), "flags"))

    try:
        prev = spark.table(CFG["CAM_MASTER"]).select(
            "camera_id",
            F.col("router_id").alias("prev_router_id"),
            F.col("lat").alias("prev_lat"),
            F.col("lon").alias("prev_lon"),
            F.col("confidence").alias("prev_conf"),
            F.col("last_seen_ts").alias("prev_last_seen_ts"),
            F.col("flags").alias("prev_flags")
        )
    except Exception:
        prev = spark.createDataFrame([], """
            camera_id string, prev_router_id string, prev_lat double, prev_lon double,
            prev_conf int, prev_last_seen_ts timestamp, prev_flags array<string>
        """)

    cand = (cam_daily.join(prev, "camera_id", "left")
            .withColumn("jump_m", haversine_m(F.col("prev_lat"),F.col("prev_lon"),F.col("lat"),F.col("lon")))
            .withColumn("jump_suspect", F.col("jump_m") > F.lit(float(P["JUMP_SUSPECT_M"])))
            .withColumn("prev_age_days", F.datediff(F.current_timestamp(), F.col("prev_last_seen_ts")).cast("double"))
            .withColumn("prev_stale", F.col("prev_last_seen_ts").isNull() | (F.col("prev_age_days") > F.lit(int(P["STALE_DAYS"]))))
            .withColumn("should_switch",
                        F.when(F.col("prev_conf").isNull(), F.lit(True))
                         .when(F.col("prev_stale"), F.lit(True))
                         .when(F.col("camera_confidence") >= (F.col("prev_conf")+F.lit(int(P["HYST_DELTA"]))), F.lit(True))
                         .otherwise(F.lit(False)))
           )

    cand = cand.withColumn(
        "should_switch",
        F.when(F.col("jump_suspect") & (F.col("camera_confidence") < (F.col("prev_conf")+F.lit(int(P["HYST_DELTA"])+10))), F.lit(False))
         .otherwise(F.col("should_switch"))
    )

    master = (cand.select(
        "camera_id",
        F.when(F.col("should_switch"), F.col("router_id")).otherwise(F.col("prev_router_id")).alias("router_id"),
        F.when(F.col("should_switch"), F.col("lat")).otherwise(F.col("prev_lat")).alias("lat"),
        F.when(F.col("should_switch"), F.col("lon")).otherwise(F.col("prev_lon")).alias("lon"),
        F.when(F.col("should_switch"), F.col("camera_confidence")).otherwise(F.col("prev_conf")).alias("confidence"),
        F.greatest(F.col("last_seen_ts"), F.col("prev_last_seen_ts")).alias("last_seen_ts"),
        F.current_timestamp().alias("last_update_ts"),
        F.array_distinct(F.concat(
            F.coalesce(F.col("prev_flags"), F.array()),
            F.coalesce(F.col("flags"), F.array()),
            F.when(F.col("jump_suspect"), F.array(F.lit("JUMP_SUSPECT"))).otherwise(F.array())
        )).alias("flags")
    ))

    master_b = add_bucket(master, "camera_id", CFG["BUCKETS"])
    ensure_table_exists_like(spark, CFG["CAM_MASTER"], master_b)
    upsert_bucket_hive(spark, CFG["CAM_MASTER"], master, "camera_id", CFG["BUCKETS"])

    return master


# ------------------------------------------------------------
# STEP 8 — MOBILITY: détecter caméras mobiles par instabilité
# Idée:
# - Une camera FIXE doit converger vers 1 zone/router_id et avoir peu de jumps
# - Une camera MOBILE:
#   * change souvent de router_id / zones distinctes
#   * jumps fréquents (km) sur peu de temps
#
# Sorties:
# - gold.camera_mobility_daily: features window
# - gold.camera_mobility_state: score + label stable
# ------------------------------------------------------------
def step8_camera_mobility(spark, day):
    # Fenêtre de jours
    start_date = F.date_sub(F.lit(day).cast("date"), int(P["MOB_WIN_DAYS"])-1)
    cam_hist = (spark.table(CFG["CAM_DAILY"])
                .where(F.col("day").between(F.date_format(start_date, "yyyy-MM-dd"), F.lit(day)))
                .select("day","camera_id","router_id","lat","lon",F.col("last_ts").alias("ts"),
                        F.col("camera_confidence").alias("conf")))

    # Ordonner par ts et calculer distances successives
    w = Window.partitionBy("camera_id").orderBy(F.col("ts").asc())
    hist2 = (cam_hist
             .withColumn("prev_lat", F.lag("lat").over(w))
             .withColumn("prev_lon", F.lag("lon").over(w))
             .withColumn("prev_ts",  F.lag("ts").over(w))
             .withColumn("jump_m", haversine_m(F.col("prev_lat"),F.col("prev_lon"),F.col("lat"),F.col("lon")))
             .withColumn("dt_min", (F.unix_timestamp("ts") - F.unix_timestamp("prev_ts"))/60.0)
             .withColumn("jump_flag", F.when(F.col("jump_m") > F.lit(float(P["MOB_JUMP_M"])), F.lit(1)).otherwise(F.lit(0)))
            )

    # Features par camera sur la fenêtre
    feats = (hist2.groupBy("camera_id")
             .agg(
                 F.count("*").alias("obs"),
                 F.countDistinct("router_id").alias("zones"),
                 F.sum("jump_flag").alias("big_jumps"),
                 F.expr("percentile_approx(jump_m, 0.95)").alias("jump_p95_m"),
                 F.max("jump_m").alias("jump_max_m"),
                 F.avg("conf").alias("avg_conf"),
                 F.max("ts").alias("last_seen_ts")
             )
             .withColumn("day", F.lit(day))
            )

    # Score mobilité (0..100)
    # - zones nombreuses => mobile
    # - big_jumps => mobile
    # - jump_p95 élevé => mobile
    # - si peu d'obs => on baisse le score (incertitude)
    feats = (feats
        .withColumn("S_zones", F.clamp(F.col("zones")/F.lit(float(P["MOB_SWITCH_ZONES"])), 0, 1))
        .withColumn("S_jumps", F.clamp(F.col("big_jumps")/F.lit(5.0), 0, 1))
        .withColumn("S_p95",   F.clamp(F.col("jump_p95_m")/F.lit(5000.0), 0, 1))  # 5km p95 => 1
        .withColumn("S_obs",   F.clamp(F.log1p(F.col("obs"))/F.log1p(F.lit(50.0)), 0, 1))
        .withColumn("mob01",   F.clamp(0.45*F.col("S_zones") + 0.35*F.col("S_jumps") + 0.20*F.col("S_p95"), 0, 1))
        .withColumn("mob01",   F.col("mob01") * F.col("S_obs"))  # pénalise si peu d'obs
        .withColumn("mobility_score", F.round(F.col("mob01")*100).cast("int"))
        .withColumn("is_mobile_probable",
                    (F.col("obs") >= F.lit(int(P["MOB_MIN_OBS"]))) & (F.col("mobility_score") >= F.lit(int(P["MOB_SCORE_TH"]))))
        .withColumn("flags", F.array_remove(F.array(
            F.when(F.col("obs") < F.lit(int(P["MOB_MIN_OBS"])), F.lit("LOW_OBS")),
            F.when(F.col("zones") >= F.lit(int(P["MOB_SWITCH_ZONES"])), F.lit("MANY_ZONES")),
            F.when(F.col("big_jumps") > F.lit(0), F.lit("BIG_JUMPS"))
        ), F.lit(None)))
    )

    (feats.write.mode("append").format("parquet").partitionBy("day").saveAsTable(CFG["MOB_DAILY"]))

    # upsert mobility_state
    mob_state = (feats.select("camera_id","mobility_score","is_mobile_probable","last_seen_ts","flags")
                 .withColumn("last_update_ts", F.current_timestamp()))
    mob_state_b = add_bucket(mob_state, "camera_id", CFG["BUCKETS"])
    ensure_table_exists_like(spark, CFG["CAM_MOB_STATE"], mob_state_b)
    upsert_bucket_hive(spark, CFG["CAM_MOB_STATE"], mob_state, "camera_id", CFG["BUCKETS"])

    return feats


# ------------------------------------------------------------
# ORCHESTRATION
# ------------------------------------------------------------
def run_daily(spark, day):
    # Bon réflexe: timezone stable
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    # 1) compresser comm
    step1_comm_daily(spark, day)

    # 2) wifi -> gps (logique preuve)
    step2_match_daily(spark, day)

    # 3) ip position (daily + state)
    step3_ip_daily_and_state(spark, day)

    # 4) router_id geo assign
    step4_assign_router_id_to_ip(spark, day)

    # 5) router daily + state (hystérésis)
    step5_router_daily_and_state(spark, day)

    # 6) camera daily (camera -> router_id via ip_state) + position du jour
    step6_camera_daily(spark, day)

    # 7) camera master (hystérésis)
    step7_camera_master(spark, day)

    # 8) mobilité camera
    step8_camera_mobility(spark, day)


# Exemple:
# run_daily(spark, "2026-01-13")
