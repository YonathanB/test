from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Supposons qu'on a déjà df_joined (Mobile + GPS + Delta calculé)
# df_joined schema: [camera_id, lat, lon, time_delta, mobile_id]

# ==============================================================================
# ÉTAPE 1 : CLUSTERING SPATIAL (La logique des 20m)
# ==============================================================================

# On crée un "Cluster ID" en arrondissant les coordonnées (approx 11-15m de précision)
# Cela regroupe naturellement les points proches géographiquement.
df_clustered = df_joined.withColumn(
    "lat_grid", F.round(F.col("lat"), 4)
).withColumn(
    "lon_grid", F.round(F.col("lon"), 4)
)

# On calcule les stats pour chaque "Cluster" (Groupe de points au même endroit)
df_groups = df_clustered.groupBy("camera_id", "lat_grid", "lon_grid").agg(
    F.count("*").alias("group_size"),             # Taille du groupe
    F.avg("lat").alias("group_lat"),              # Centroïde du groupe
    F.avg("lon").alias("group_lon"),
    F.stddev("lat").alias("group_dispersion"),    # Dispersion interne (stabilité)
    F.min("time_delta").alias("group_min_delta")  # Le meilleur delta de ce groupe
)

# Gestion des nulls sur la dispersion (si taille groupe = 1, stddev est null -> on met 0)
df_groups = df_groups.fillna(0, subset=["group_dispersion"])

# ==============================================================================
# ÉTAPE 2 : SÉLECTION DU MEILLEUR CANDIDAT (Ta logique conditionnelle)
# ==============================================================================

# On définit une fenêtre par caméra pour classer les groupes entre eux
w_camera = Window.partitionBy("camera_id")

# On définit ta logique de tri complexe ici :
# Ordre de préférence :
# 1. D'abord les groupes qui ont une taille > 1 (Validation par la densité)
# 2. Ensuite, parmi eux, celui qui a la plus petite dispersion (Le plus précis)
# 3. Si aucun groupe > 1 (que des points isolés), on prend celui avec le plus petit delta temporel.

df_ranked = df_groups.withColumn(
    "rank",
    F.row_number().over(
        w_camera.orderBy(
            # Critère A : On veut group_size > 1 en priorité. 
            # On trie par DESC (True avant False). Si size > 1 c'est 1, sinon 0.
            F.when(F.col("group_size") > 1, 1).otherwise(0).desc(),
            
            # Critère B : La dispersion minimale (pour les groupes > 1)
            F.col("group_dispersion").asc(),
            
            # Critère C : Le delta temporel (pour départager les singletons ou égalités)
            F.col("group_min_delta").asc()
        )
    )
)

# On ne garde que le "Gagnant" de la logique pour définir la position principale
df_best_location = df_ranked.filter(F.col("rank") == 1).select(
    "camera_id",
    F.col("group_lat").alias("final_lat"),
    F.col("group_lon").alias("final_lon"),
    F.col("group_min_delta").alias("best_delta"),
    F.col("group_dispersion").alias("dispersion"),
    F.col("group_size").alias("nb_points_support")
)

# ==============================================================================
# ÉTAPE 3 : PRÉPARATION POUR ELASTIC (Attributs pour filtrage)
# ==============================================================================

# On ajoute des flags booléens pour faciliter les requêtes utilisateurs
df_elastic_ready = df_best_location.withColumn(
    "is_high_precision_sync", F.col("best_delta") <= 60 # Tag "Synchro < 1min"
).withColumn(
    "is_dense_cluster", F.col("nb_points_support") > 1  # Tag "Confirmé par plusieurs points"
)

# Résultat à envoyer dans Elastic
# L'utilisateur pourra faire : "SELECT * FROM cameras WHERE is_high_precision_sync = true"
