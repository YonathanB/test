from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from typing import List

# =============================================================================
# 1. Spark session
# =============================================================================

spark = (
    SparkSession.builder
    .appName("vehicle-unification-v1")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# 2. Config
# =============================================================================

ORACLE_CONFIG = {
    "url": "jdbc:oracle:thin:@//host:1521/SERVICE",
    "driver": "oracle.jdbc.OracleDriver",
    "user": "USER",
    "password": "PASSWORD",
    "batchsize": "5000",
}

SOURCE_PRIORITY = {
    "SIV": 1,
    "ASSURANCE": 2,
    "SINISTRE": 3,
    "AUTRE": 9,
}

VEHICLE_COLUMNS = [
    "vin", "plaque", "pays", "marque", "modele",
    "couleur", "carburant", "annee",
]

MANDATORY_COMMON_COLUMNS = [
    "source", "source_record_id", "date_source",
    "vin", "plaque", "pays", "marque", "modele",
    "couleur", "carburant", "annee",
]

# =============================================================================
# 3. Helpers
# =============================================================================

def add_missing_columns(df: DataFrame, expected_cols: List[str]) -> DataFrame:
    for c in expected_cols:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df

def clean_string(col_name: str):
    return F.upper(F.trim(F.col(col_name)))

def with_standard_vehicle_key(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("vin_clean", clean_string("vin"))
        .withColumn("plaque_clean", clean_string("plaque"))
        .withColumn("pays_clean", clean_string("pays"))
        .withColumn(
            "vehicle_match_key",
            F.when(
                F.col("vin_clean").isNotNull() & (F.length("vin_clean") == 17),
                F.concat(F.lit("VIN:"), F.col("vin_clean"))
            ).when(
                F.col("plaque_clean").isNotNull() & F.col("pays_clean").isNotNull(),
                F.concat(F.lit("PLQ:"), F.col("plaque_clean"), F.lit("|"), F.col("pays_clean"))
            ).otherwise(F.lit(None))
        )
        .withColumn(
            "match_level",
            F.when(
                F.col("vin_clean").isNotNull() & (F.length("vin_clean") == 17),
                F.lit("VIN")
            ).when(
                F.col("plaque_clean").isNotNull() & F.col("pays_clean").isNotNull(),
                F.lit("PLAQUE_PAYS")
            ).otherwise(F.lit("NO_KEY"))
        )
    )

def add_source_priority(df: DataFrame, spark_session: SparkSession) -> DataFrame:
    # Création du DataFrame de priorité à la volée (plus propre que la variable globale)
    prio_data = list(SOURCE_PRIORITY.items())
    prio_df = spark_session.createDataFrame(prio_data, ["source", "source_priority"])
    
    return (
        df
        .join(F.broadcast(prio_df), on="source", how="left")
        .fillna({"source_priority": 99})
    )

def stable_vehicle_id() -> F.Column:
    return F.sha2(F.col("vehicle_match_key"), 256)

def jdbc_write(df: DataFrame, table_name: str, mode: str = "append") -> None:
    (
        df.write
        .format("jdbc")
        .option("url", ORACLE_CONFIG["url"])
        .option("driver", ORACLE_CONFIG["driver"])
        .option("user", ORACLE_CONFIG["user"])
        .option("password", ORACLE_CONFIG["password"])
        .option("dbtable", table_name)
        .option("batchsize", ORACLE_CONFIG["batchsize"])
        .mode(mode)
        .save()
    )

# =============================================================================
# 4. Normalisation source par source
# =============================================================================

def normalize_siv(df: DataFrame) -> DataFrame:
    df = df.select(
        F.col("id").cast("string").alias("source_record_id"),
        F.col("date_maj").alias("date_source"),
        F.lit("SIV").alias("source"),
        F.col("vin").cast("string"),
        F.col("plaque").cast("string"),
        F.col("pays").cast("string"),
        F.col("marque").cast("string"),
        F.col("modele").cast("string"),
        F.col("couleur").cast("string"),
        F.col("carburant").cast("string"),
        F.col("annee_mise_en_circulation").cast("int").alias("annee"),
    )
    df = add_missing_columns(df, MANDATORY_COMMON_COLUMNS)
    return with_standard_vehicle_key(df)

def normalize_assurance_vehicle_part(df: DataFrame) -> DataFrame:
    df = df.select(
        F.col("contrat_id").cast("string").alias("source_record_id"),
        F.col("date_debut").alias("date_source"),
        F.lit("ASSURANCE").alias("source"),
        F.col("vin").cast("string"),
        F.col("plaque").cast("string"),
        F.col("pays").cast("string"),
        F.lit(None).cast("string").alias("marque"),
        F.col("modele").cast("string"),
        F.col("couleur").cast("string"),
        F.col("carburant").cast("string"),
        F.lit(None).cast("int").alias("annee"),
    )
    df = add_missing_columns(df, MANDATORY_COMMON_COLUMNS)
    return with_standard_vehicle_key(df)

def normalize_sinistre_vehicle_part(df: DataFrame) -> DataFrame:
    df = df.select(
        F.col("sinistre_id").cast("string").alias("source_record_id"),
        F.col("date_sinistre").alias("date_source"),
        F.lit("SINISTRE").alias("source"),
        F.col("vin").cast("string"),
        F.col("plaque").cast("string"),
        F.col("pays").cast("string"),
        F.lit(None).cast("string").alias("marque"),
        F.lit(None).cast("string").alias("modele"),
        F.lit(None).cast("string").alias("couleur"),
        F.col("carburant").cast("string"),
        F.lit(None).cast("int").alias("annee"),
    )
    df = add_missing_columns(df, MANDATORY_COMMON_COLUMNS)
    return with_standard_vehicle_key(df)

# =============================================================================
# 5. Facts
# =============================================================================

def build_fact_assurance(df_normalized: DataFrame, df_raw: DataFrame, dim_vehicle: DataFrame) -> DataFrame:
    df_metier = df_raw.select(
        F.col("contrat_id").cast("string").alias("source_record_id"),
        F.col("assureur").cast("string"),
        F.col("formule").cast("string"),
        F.col("date_debut"),
        F.col("date_fin"),
        F.col("prime_annuelle").cast("double"),
    )

    return (
        df_metier
        .join(
            df_normalized.select("source_record_id", "vehicle_match_key").distinct(),
            on="source_record_id",
            how="inner",
        )
        .join(
            dim_vehicle.select("vehicule_id", "vehicle_match_key"),
            on="vehicle_match_key",
            how="inner",
        )
        .withColumn("assurance_id", F.sha2(F.concat_ws("|", F.lit("ASS"), "source_record_id"), 256))
        .withColumn("date_chargement", F.current_timestamp())
        .select(
            "assurance_id", "vehicule_id", "vehicle_match_key", "source_record_id",
            "assureur", "formule", "date_debut", "date_fin", "prime_annuelle",
            "date_chargement",
        )
    )

def build_fact_accident(df_normalized: DataFrame, df_raw: DataFrame, dim_vehicle: DataFrame) -> DataFrame:
    df_metier = df_raw.select(
        F.col("sinistre_id").cast("string").alias("source_record_id"),
        F.col("date_sinistre"),
        F.col("type_sinistre").cast("string"),
        F.col("montant_dommages").cast("double"),
        F.col("gravite").cast("string"),
    )

    return (
        df_metier
        .join(
            df_normalized.select("source_record_id", "vehicle_match_key").distinct(),
            on="source_record_id",
            how="inner",
        )
        .join(
            dim_vehicle.select("vehicule_id", "vehicle_match_key"),
            on="vehicle_match_key",
            how="inner",
        )
        .withColumn("accident_id", F.sha2(F.concat_ws("|", F.lit("ACC"), "source_record_id"), 256))
        .withColumn("date_chargement", F.current_timestamp())
        .select(
            "accident_id", "vehicule_id", "vehicle_match_key", "source_record_id",
            "date_sinistre", "type_sinistre", "montant_dommages", "gravite",
            "date_chargement",
        )
    )

# =============================================================================
# 6. Dimension véhicule consolidée
# =============================================================================

def build_dim_vehicle(vehicle_dfs: List[DataFrame], spark_session: SparkSession) -> DataFrame:
    df = vehicle_dfs[0]
    for other in vehicle_dfs[1:]:
        df = df.unionByName(other, allowMissingColumns=True)

    df = df.filter(F.col("vehicle_match_key").isNotNull())
    df = add_source_priority(df, spark_session)

    # Déterminisme garanti par date_source en ordre décroissant
    w = (
        Window.partitionBy("vehicle_match_key")
        .orderBy("source_priority", F.col("date_source").desc())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    df = (
        df
        .withColumn("vin_final",       F.first("vin_clean",   ignorenulls=True).over(w))
        .withColumn("plaque_final",    F.first("plaque_clean", ignorenulls=True).over(w))
        .withColumn("pays_final",      F.first("pays_clean",  ignorenulls=True).over(w))
        .withColumn("marque_final",    F.first("marque",      ignorenulls=True).over(w))
        .withColumn("modele_final",    F.first("modele",      ignorenulls=True).over(w))
        .withColumn("couleur_final",   F.first("couleur",     ignorenulls=True).over(w))
        .withColumn("carburant_final", F.first("carburant",   ignorenulls=True).over(w))
        .withColumn("annee_final",     F.first("annee",       ignorenulls=True).over(w))
        .withColumn("sources",         F.collect_set("source").over(w))
    )

    rank_w = Window.partitionBy("vehicle_match_key").orderBy("source_priority", F.col("date_source").desc())

    return (
        df
        .withColumn("rn", F.row_number().over(rank_w))
        .filter(F.col("rn") == 1)
        .withColumn("vehicule_id", stable_vehicle_id())
        .withColumn("date_chargement", F.current_timestamp())
        .select(
            "vehicule_id",
            "vehicle_match_key",
            "match_level",
            F.col("vin_final").alias("vin"),
            F.col("plaque_final").alias("plaque"),
            F.col("pays_final").alias("pays"),
            F.col("marque_final").alias("marque"),
            F.col("modele_final").alias("modele"),
            F.col("couleur_final").alias("couleur"),
            F.col("carburant_final").alias("carburant"),
            F.col("annee_final").cast("int").alias("annee"),
            F.concat_ws(", ", "sources").alias("sources_contributives"),
            F.col("source").alias("source_principale"),
            "date_chargement",
        )
    )

# =============================================================================
# 7. Traçabilité
# =============================================================================

def build_fact_source_trace(vehicle_dfs: List[DataFrame], dim_vehicle: DataFrame) -> DataFrame:
    cols = ["vehicle_match_key", "source", "source_record_id"]
    df = vehicle_dfs[0].select(cols)
    for other in vehicle_dfs[1:]:
        df = df.unionByName(other.select(cols))

    return (
        df
        .distinct()
        .join(
            dim_vehicle.select("vehicule_id", "vehicle_match_key"),
            on="vehicle_match_key",
            how="inner",
        )
        .withColumn("trace_id", F.sha2(F.concat_ws("|", "source", "source_record_id"), 256))
        .withColumn("date_detection", F.current_timestamp())
        .select("trace_id", "vehicule_id", "vehicle_match_key",
                "source", "source_record_id", "date_detection")
    )

# =============================================================================
# 8. Contrôle qualité optimisé
# =============================================================================

def quality_checks(
    dim_vehicle: DataFrame,
    fact_assurance: DataFrame,
    fact_accident: DataFrame,
    fact_source_trace: DataFrame,
    vehicle_dfs: List[DataFrame],
) -> bool:
    """Contrôles bloquants minimaux optimisés en un seul job Spark sur la dimension."""
    ok = True
    print("\n" + "=" * 60)
    print("CONTRÔLES QUALITÉ")
    print("=" * 60)

    # 1. Agrégation globale pour la dimension (1 seule Action Spark !)
    dim_metrics_expr = [
        F.count("*").alias("total_veh"),
        F.countDistinct("vehicule_id").alias("distinct_veh_id"),
        F.sum(F.when(F.col("match_level") == "VIN", 1).otherwise(0)).alias("n_vin"),
        F.sum(F.when(F.col("match_level") == "PLAQUE_PAYS", 1).otherwise(0)).alias("n_plq"),
        F.sum(F.when(F.size(F.split("sources_contributives", ", ")) > 1, 1).otherwise(0)).alias("n_multi")
    ]
    
    fill_cols = ["vin", "plaque", "marque", "modele", "couleur", "carburant", "annee"]
    for col_name in fill_cols:
        dim_metrics_expr.append(
            F.sum(F.when(F.col(col_name).isNotNull(), 1).otherwise(0)).alias(f"filled_{col_name}")
        )

    # Collecte des résultats de la dimension
    dim_stats = dim_vehicle.select(*dim_metrics_expr).collect()[0].asDict()

    # Collecte des comptages basiques pour les faits
    n_ass = fact_assurance.count()
    n_acc = fact_accident.count()
    n_trc = fact_source_trace.count()

    print(f"  Véhicules          : {dim_stats['total_veh']} (VIN: {dim_stats['n_vin'] or 0}, PLQ: {dim_stats['n_plq'] or 0})")
    print(f"  Assurances         : {n_ass}")
    print(f"  Accidents          : {n_acc}")
    print(f"  Traces source      : {n_trc}")
    print(f"  Enrichis multi-src : {dim_stats['n_multi'] or 0}")

    # Vérification des doublons (Total ID - ID Distincts)
    dup_veh = dim_stats['total_veh'] - dim_stats['distinct_veh_id']
    if dup_veh > 0:
        print(f"  ✗ {dup_veh} vehicule_id en doublon dans DIM_VEHICLE")
        ok = False
    else:
        print("  ✓ Pas de doublon vehicule_id")

    # Records source sans clé
    all_keys = vehicle_dfs[0].select("vehicle_match_key")
    for other in vehicle_dfs[1:]:
        all_keys = all_keys.unionByName(other.select("vehicle_match_key"))

    n_no_key = all_keys.filter(F.col("vehicle_match_key").isNull()).count()
    if n_no_key > 0:
        print(f"  ⚠ {n_no_key} enregistrements source sans clé (exclus)")
    else:
        print("  ✓ Tous les enregistrements source ont une clé")

    # Taux de remplissage
    print("  -- Remplissage DIM_VEHICLE --")
    for col_name in fill_cols:
        n_filled = dim_stats[f"filled_{col_name}"] or 0
        pct = (n_filled / dim_stats['total_veh'] * 100) if dim_stats['total_veh'] > 0 else 0
        flag = "✓" if pct >= 80 else "⚠"
        print(f"    {flag} {col_name:12s} : {pct:5.1f}% ({n_filled}/{dim_stats['total_veh']})")

    print("=" * 60)
    return ok

# =============================================================================
# 9. Main
# =============================================================================

def main():
    # -- Lecture --
    df_siv_raw = spark.table("raw.siv_vehicles")
    df_assurance_raw = spark.table("raw.assurance_contracts")
    df_sinistre_raw = spark.table("raw.sinistres")

    # -- Normalisation --
    df_siv_norm = normalize_siv(df_siv_raw)
    df_ass_norm = normalize_assurance_vehicle_part(df_assurance_raw)
    df_sin_norm = normalize_sinistre_vehicle_part(df_sinistre_raw)

    vehicle_dfs = [df_siv_norm, df_ass_norm, df_sin_norm]

    # -- Dimension véhicule --
    dim_vehicle = build_dim_vehicle(vehicle_dfs, spark)
    dim_vehicle.cache()

    # -- Facts --
    fact_assurance = build_fact_assurance(df_ass_norm, df_assurance_raw, dim_vehicle)
    fact_accident = build_fact_accident(df_sin_norm, df_sinistre_raw, dim_vehicle)
    fact_source_trace = build_fact_source_trace(vehicle_dfs, dim_vehicle)

    # -- Contrôle qualité (bloquant) --
    checks_ok = quality_checks(
        dim_vehicle, fact_assurance, fact_accident, fact_source_trace, vehicle_dfs
    )
    if not checks_ok:
        print("ABANDON : contrôles qualité KO")
        spark.stop()
        return

    # -- Écriture Parquet / Hive --
    dim_vehicle.write.mode("overwrite").format("parquet").saveAsTable("gold.dim_vehicle")
    fact_assurance.write.mode("overwrite").format("parquet").saveAsTable("gold.fact_assurance")
    fact_accident.write.mode("overwrite").format("parquet").saveAsTable("gold.fact_accident")
    fact_source_trace.write.mode("overwrite").format("parquet").saveAsTable("gold.fact_source_trace")

    # -- Oracle (décommenter si besoin) --
    # jdbc_write(dim_vehicle, "DIM_VEHICLE", mode="overwrite")
    # jdbc_write(fact_assurance, "FACT_ASSURANCE", mode="overwrite")
    # jdbc_write(fact_accident, "FACT_ACCIDENT", mode="overwrite")
    # jdbc_write(fact_source_trace, "FACT_SOURCE_TRACE", mode="overwrite")

    dim_vehicle.unpersist()
    print("ETL terminé.")

if __name__ == "__main__":
    main()
