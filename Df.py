from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Supposons que df_final contient : [camera_id, ip_lat, ip_lon, priority_tier, group_dispersion, time_bucket]

# ==============================================================================
# ÉTAPE 1 : REGROUPEMENT PAR LIEU (Arrondi géographique)
# ==============================================================================
# Si la caméra donne 48.80001 et 48.80002, c'est le même endroit.
# On arrondit à 4 décimales (~11m) pour grouper.

df_places = df_final.withColumn("lat_round", F.round("ip_lat", 4)) \
                    .withColumn("lon_round", F.round("ip_lon", 4))

# On agrège toutes les observations de la journée pour CE lieu spécifique
df_candidates = df_places.groupBy("camera_id", "lat_round", "lon_round").agg(
    # Est-ce qu'on a déjà vu une synchro parfaite (<60s) à cet endroit ? (Max de 2 ou 1)
    F.max("priority_tier").alias("best_tier_seen"), 
    
    # Combien de fois la caméra a été vue ici (nombre d'heures)
    F.count("*").alias("frequency"),
    
    # La meilleure dispersion vue ici
    F.min("group_dispersion").alias("best_dispersion"),
    
    # On recalcule un centroïde précis basé sur toutes les occurrences de ce lieu
    F.avg("ip_lat").alias("final_lat"),
    F.avg("ip_lon").alias("final_lon")
)

# ==============================================================================
# ÉTAPE 2 : CALCUL DU SCORE DE DÉPARTAGE
# ==============================================================================

# Formule :
# 1. Le Tier est prédominant (x 1 000 000). Un lieu Tier 2 écrase un lieu Tier 1.
# 2. Ensuite la fréquence (x 100). Un lieu vu 5 heures bat un lieu vu 1 heure.
# 3. Ensuite la dispersion (inverse). Plus c'est petit, mieux c'est.

df_scored = df_candidates.withColumn(
    "sorting_score",
    (F.col("best_tier_seen") * 1000000) +  # La priorité absolue
    (F.col("frequency") * 100) -           # La récurrence
    F.coalesce(F.col("best_dispersion"), F.lit(100)) # Pénalité si dispersion élevée
)

# ==============================================================================
# ÉTAPE 3 : SÉLECTION DU VAINQUEUR (Top 1)
# ==============================================================================

w_final = Window.partitionBy("camera_id").orderBy(F.col("sorting_score").desc())

df_unique_camera = df_scored.withColumn("rank", F.row_number().over(w_final)) \
    .filter(F.col("rank") == 1) \
    .select(
        "camera_id",
        "final_lat",
        "final_lon",
        "best_tier_seen", # Pour savoir si c'est une position "Golden" (<60s)
        "frequency"       # Nombre d'heures de confirmation
    )

# df_unique_camera contient maintenant EXACTEMENT 1 ligne par caméra (la meilleure).
