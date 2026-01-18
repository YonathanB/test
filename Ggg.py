from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType

# Supposons df_B (Mobiles-Routeurs) et df_C (Mobiles-Geo) chargés.
# df_A (Cameras-Routeurs) sera utilisé à la fin.

# ==============================================================================
# ÉTAPE 1 : JOINTURE MOBILES + GEO (Pour localiser les IPs)
# ==============================================================================

# On calcule le delta temps absolu
cond = [
    df_B.mobile_id == df_C.mobile_id,
    F.abs(df_B.timestamp - df_C.timestamp) <= 3600 # Fenêtre large 1h pour attraper des candidats
]

df_observations = df_B.join(df_C, cond) \
    .select(
        df_B.ip_router,
        df_B.timestamp.alias("ts_connection"),
        df_C.lat,
        df_C.lon,
        F.abs(df_B.timestamp - df_C.timestamp).cast(LongType()).alias("delta")
    )

# On ajoute une colonne temporelle "Bucket" pour l'IP (ex: l'IP est à un endroit fixe par heure)
df_observations = df_observations.withColumn(
    "time_bucket", (F.col("ts_connection") / 3600).cast(LongType())
)

# ==============================================================================
# ÉTAPE 2 : CLUSTERING SPATIAL (Grille de ~11m/20m)
# ==============================================================================
# On regroupe les observations d'une même IP (dans la même heure) par zone géographique.

df_grids = df_observations.withColumn("lat_grid", F.round("lat", 4)) \
                          .withColumn("lon_grid", F.round("lon", 4))

# Calcul des stats par Grille (Zone de 20m)
df_groups = df_grids.groupBy("ip_router", "time_bucket", "lat_grid", "lon_grid").agg(
    F.count("*").alias("group_size"),
    F.avg("lat").alias("group_lat"),
    F.avg("lon").alias("group_lon"),
    F.stddev("lat").alias("group_dispersion"), # Null si taille = 1
    F.min("delta").alias("group_min_delta")
)

# Remplacer null dispersion par une valeur neutre ou infini pour le tri
df_groups = df_groups.fillna(999, subset=["group_dispersion"])

# ==============================================================================
# ÉTAPE 3 : CLASSEMENT SELON VOTRE LOGIQUE (Le "Ranking")
# ==============================================================================

# Définition des priorités (Tiers)
# 1. PRIORITY_TIER : Les groupes contenant une synchro < 60s sont TOP priorité (Score 2).
#    Sinon Score 1.
df_scored = df_groups.withColumn(
    "priority_tier",
    F.when(F.col("group_min_delta") <= 60, 2).otherwise(1)
)

# Fenêtre pour trier les groupes d'une même IP
w_ip = Window.partitionBy("ip_router", "time_bucket")

# LA LOGIQUE DE TRI :
df_ranked = df_scored.withColumn(
    "rank",
    F.row_number().over(
        w_ip.orderBy(
            # Critère 1 : On prend d'abord ceux qui ont delta <= 60 (Tier 2 > Tier 1)
            F.col("priority_tier").desc(),

            # Critère 2 : On préfère les groupes > 1 aux points isolés
            F.when(F.col("group_size") > 1, 1).otherwise(0).desc(),

            # Critère 3 : Arbitrage final
            # Si c'est un groupe (size > 1), on veut la plus petite dispersion
            # Si c'est un point (size = 1), la dispersion est inutile, on veut le plus petit delta
            # Astuce mathématique pour trier les deux cas en une ligne :
            F.when(
                F.col("group_size") > 1, 
                F.col("group_dispersion") # Si groupe, tri par stabilité
            ).otherwise(
                F.col("group_min_delta")  # Si point seul, tri par temps
            ).asc()
        )
    )
)

# On garde LA meilleure position pour cette IP à cette heure
df_ip_location = df_ranked.filter(F.col("rank") == 1).select(
    "ip_router",
    "time_bucket",
    F.col("group_lat").alias("ip_lat"),
    F.col("group_lon").alias("ip_lon"),
    "priority_tier",   # Pour info utilisateur (2 = Très fiable < 60s)
    "group_dispersion" # Pour info utilisateur
)

# ==============================================================================
# ÉTAPE 4 : JOINTURE AVEC LES CAMÉRAS (Enfin !)
# ==============================================================================

# On prépare la table A avec le même time_bucket
df_cameras = spark.table("cameras_router_logs") \
    .withColumn("time_bucket", (F.col("timestamp") / 3600).cast(LongType()))

# Jointure finale : On attribue à la caméra la position calculée de son IP
df_final = df_cameras.join(
    df_ip_location, 
    on=["ip_router", "time_bucket"], 
    how="inner" # Inner car si on n'a pas localisé l'IP, on ne peut pas localiser la caméra
).select(
    "camera_id",
    "ip_lat",
    "ip_lon",
    "priority_tier",
    "group_dispersion"
)
