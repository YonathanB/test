from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import math

# Initialisation Spark
spark = SparkSession.builder.appName("CameraGeolocationConsolidation").getOrCreate()

# ==============================================================================
# 1. CONFIGURATION ET CONSTANTES
# ==============================================================================
DISTANCE_THRESHOLD_METERS = 50.0  # Distance max pour grouper deux points dans le même cluster
EARTH_RADIUS_KM = 6371.0

# Schema des données d'entrée (toutes les logiques unifiées)
# source_logic: 'INSTALLATION', 'IP_TRACE', 'WIFI_SNIFFING', etc.
input_schema = StructType([
    StructField("camera_id", StringType(), True),
    StructField("source_logic", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# ==============================================================================
# 2. LOGIQUE MÉTIER (PYTHON UDF)
# C'est ici que se fait l'intelligence du Clustering et du Scoring
# ==============================================================================

def haversine(lat1, lon1, lat2, lon2):
    """Calcule la distance en mètres entre deux points GPS."""
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2) * math.sin(dLat/2) + \
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * \
        math.sin(dLon/2) * math.sin(dLon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return EARTH_RADIUS_KM * c * 1000  # Retourne en mètres

def compute_centroid_and_score(candidates):
    """
    Entrée: Liste de dictionnaires [{'source_logic', 'lat', 'lon'}] appartenant à UN cluster.
    Sortie: Dictionnaire avec lat/lon moyen, score, et détails.
    """
    n = len(candidates)
    if n == 0: return None

    # 1. Calcul du Centroid (Moyenne des positions)
    avg_lat = sum(c['lat'] for c in candidates) / n
    avg_lon = sum(c['lon'] for c in candidates) / n

    # 2. Calcul de la "Dispersion" (Écart moyen par rapport au centre)
    # Plus c'est grand, moins on est sûr.
    spreads = [haversine(avg_lat, avg_lon, c['lat'], c['lon']) for c in candidates]
    avg_spread = sum(spreads) / n if n > 0 else 0

    # 3. Comptage des logiques uniques (Convergence)
    unique_logics = set(c['source_logic'] for c in candidates)
    nb_unique_logics = len(unique_logics)

    # 4. ALGORITHME DE SCORING
    # Base: 50 points
    # Bonus par logique supplémentaire: +20 points (Convergence)
    # Pénalité de distance: -1 point par mètre d'écart moyen (Dispersion)
    score = 50 + (20 * (nb_unique_logics - 1)) - (0.5 * avg_spread)
    
    # Bornes du score (0 à 100)
    final_score = max(0, min(100, score))

    return {
        "centroid_lat": avg_lat,
        "centroid_lon": avg_lon,
        "score": final_score,
        "nb_logics": nb_unique_logics,
        "radius_spread": avg_spread,
        "contributing_logics": list(unique_logics),
        "candidates": candidates # On garde les détails bruts pour l'affichage UI si besoin
    }

def process_camera_candidates(candidates_list):
    """
    Fonction principale UDF.
    Reçoit TOUTES les positions candidates pour UNE caméra.
    Retourne une liste de CLUSTERS.
    """
    if not candidates_list:
        return []

    # On convertit les Row spark en dict python pour manipuler
    # (candidates_list est une liste de Row ou dict selon la version Spark)
    points = [row.asDict() if hasattr(row, 'asDict') else row for row in candidates_list]
    
    clusters = []
    
    # Algorithme glouton (Greedy Clustering)
    # On prend un point, on trouve tous ses voisins < 50m, on crée un cluster, on répète.
    while points:
        current_point = points.pop(0)
        current_cluster = [current_point]
        
        # Trouver les voisins dans la liste restante
        neighbors = []
        non_neighbors = []
        
        for p in points:
            dist = haversine(current_point['lat'], current_point['lon'], p['lat'], p['lon'])
            if dist <= DISTANCE_THRESHOLD_METERS:
                neighbors.append(p)
            else:
                non_neighbors.append(p)
        
        current_cluster.extend(neighbors)
        points = non_neighbors # On ne garde que ceux qui n'ont pas été clusterisés
        
        # Calculer les stats de ce cluster
        cluster_data = compute_centroid_and_score(current_cluster)
        clusters.append(cluster_data)

    # Trier les clusters par Score décroissant (Le meilleur en premier)
    clusters.sort(key=lambda x: x['score'], reverse=True)
    
    return clusters

# Définition du schéma de retour de l'UDF (Complexe)
output_cluster_schema = ArrayType(StructType([
    StructField("centroid_lat", DoubleType(), False),
    StructField("centroid_lon", DoubleType(), False),
    StructField("score", FloatType(), False),
    StructField("nb_logics", IntegerType(), False),
    StructField("radius_spread", FloatType(), False),
    StructField("contributing_logics", ArrayType(StringType()), False),
    # On peut inclure les détails bruts si on veut les afficher au clic
    StructField("candidates", ArrayType(StructType([
        StructField("source_logic", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType())
    ])))
]))

# Enregistrement UDF
process_camera_udf = F.udf(process_camera_candidates, output_cluster_schema)

# ==============================================================================
# 3. EXÉCUTION DU PIPELINE
# ==============================================================================

# A. Chargement des données (Exemple simulé)
# Dans la réalité : df = spark.read.parquet(".../daily_and_history_merged")
data = [
    # Caméra 1 : Cas parfait, 3 logiques convergent au même endroit
    ("cam_01", "INSTALLATION", 48.8566, 2.3522),
    ("cam_01", "IP_TRACE", 48.8567, 2.3523), # ~15m d'écart
    ("cam_01", "AI_INFERENCE", 48.8565, 2.3521),
    
    # Caméra 2 : Ambiguïté. Logique A dit Paris, Logique B dit Lyon (bruit)
    ("cam_02", "INSTALLATION", 48.8566, 2.3522),
    ("cam_02", "IP_TRACE", 45.7640, 4.8357), # Lyon, très loin
]

df = spark.createDataFrame(data, ["camera_id", "source_logic", "lat", "lon"])

# B. GroupBy Caméra et Collecte de tous les candidats
# On regroupe tout dans une liste pour passer à l'UDF
grouped_df = df.groupBy("camera_id").agg(
    F.collect_list(F.struct("source_logic", "lat", "lon")).alias("raw_candidates")
)

# C. Application du Clustering et Scoring
result_df = grouped_df.withColumn("clusters_info", process_camera_udf(F.col("raw_candidates")))

# D. Préparation pour Elasticsearch (Aplatissement intelligent)
# On veut extraire le "Meilleur Cluster" pour l'affichage principal (Top Level)
# Mais garder "Tous les clusters" pour les "Ghost Markers" ou le débug.

final_output = result_df.select(
    F.col("camera_id"),
    
    # --- Champs principaux pour l'affichage par défaut (Le Gagnant) ---
    F.col("clusters_info")[0]["centroid_lat"].alias("best_lat"),
    F.col("clusters_info")[0]["centroid_lon"].alias("best_lon"),
    F.col("clusters_info")[0]["score"].alias("confidence_score"),
    
    # --- Données riches pour le panneau de détail / zoom ---
    F.col("clusters_info").alias("all_location_clusters") 
)

# Affichage pour vérification
print("=== RÉSULTAT FINAL POUR ELASTICSEARCH ===")
final_output.show(truncate=False)
final_output.printSchema()

# ==============================================================================
# EXPLICATION DES RÉSULTATS (Simulation)
# ==============================================================================
"""
Pour 'cam_02' (Le cas ambigu Paris vs Lyon) :
- all_location_clusters contiendra 2 éléments (2 clusters).
- Cluster 1 (Paris) : Score élevé (ex: 50) car source fiable 'INSTALLATION'.
- Cluster 2 (Lyon) : Score plus faible ou équivalent selon la logique.
- best_lat/best_lon prendra les coordonnées du Cluster avec le meilleur score.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import math

spark = SparkSession.builder.appName("CameraGeolocation_MasterLogic").getOrCreate()

# ==============================================================================
# 1. PARAMÈTRES DE LOGIQUE MÉTIER
# ==============================================================================
CLUSTER_MAX_DIST_METERS = 50.0   # Si > 50m, on crée un nouveau point sur la carte (Ghost)
STRICT_CONVERGENCE_METERS = 10.0 # Si < 10m, bonus de score (très précis)
EARTH_RADIUS_KM = 6371.0

# ==============================================================================
# 2. UDF : LE CERVEAU (Clustering + Scoring)
# ==============================================================================

def haversine(lat1, lon1, lat2, lon2):
    """Distance en mètres"""
    if None in [lat1, lon1, lat2, lon2]: return 0.0
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2) * math.sin(dLat/2) + \
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * \
        math.sin(dLon/2) * math.sin(dLon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return EARTH_RADIUS_KM * c * 1000

def calculate_score(candidates, centroid_lat, centroid_lon):
    """
    Calcule le score (0-100) selon tes règles :
    - Convergence (plusieurs logiques) = Bonus
    - Proximité (< 10m) = Bonus fort
    - Dispersion = Malus
    """
    n = len(candidates)
    unique_logics = set(c['source_logic'] for c in candidates)
    nb_logics = len(unique_logics)
    
    # Calcul de la dispersion moyenne (écart au centre)
    dists = [haversine(centroid_lat, centroid_lon, c['lat'], c['lon']) for c in candidates]
    avg_spread = sum(dists) / n if n > 0 else 0
    
    # --- FORMULE DE SCORING ---
    score = 40 # Base
    
    # 1. Bonus Convergence (Plus on a de sources différentes, plus c'est fiable)
    score += (nb_logics - 1) * 20 
    
    # 2. Bonus/Malus Précision
    if avg_spread <= STRICT_CONVERGENCE_METERS:
        score += 20 # Très précis (< 10m)
    else:
        # Pénalité : on perd 1 point par mètre au dessus de 10m
        penalty = (avg_spread - STRICT_CONVERGENCE_METERS) * 0.5
        score -= penalty
        
    return max(0, min(100, int(score))), avg_spread

def master_logic(candidates_list):
    """
    Reçoit une liste de points bruts (Daily + History).
    Retourne une liste de CLUSTERS géographiques distincts.
    """
    if not candidates_list: return []
    
    # Conversion Row -> Dict
    points = [row.asDict() if hasattr(row, 'asDict') else row for row in candidates_list]
    clusters = []
    
    # --- ÉTAPE A : CLUSTERING GLOUTON (Séparer Paris de Lyon) ---
    while points:
        ref_point = points.pop(0)
        current_cluster = [ref_point]
        remaining_points = []
        
        for p in points:
            dist = haversine(ref_point['lat'], ref_point['lon'], p['lat'], p['lon'])
            if dist <= CLUSTER_MAX_DIST_METERS:
                current_cluster.append(p)
            else:
                remaining_points.append(p)
        
        points = remaining_points
        
        # --- ÉTAPE B : SCORING DU CLUSTER ---
        # Centre de gravité
        avg_lat = sum(x['lat'] for x in current_cluster) / len(current_cluster)
        avg_lon = sum(x['lon'] for x in current_cluster) / len(current_cluster)
        
        score, spread = calculate_score(current_cluster, avg_lat, avg_lon)
        
        clusters.append({
            "lat": avg_lat,
            "lon": avg_lon,
            "score": score,
            "spread_radius": spread,
            "nb_logics": len(set(x['source_logic'] for x in current_cluster)),
            "logics": list(set(x['source_logic'] for x in current_cluster)),
            "count_sources": len(current_cluster)
        })
        
    # On trie les clusters pour que le [0] soit toujours le "Best Choice" (plus haut score)
    clusters.sort(key=lambda x: x['score'], reverse=True)
    return clusters

# Schema de sortie complexe (Nested)
output_schema = ArrayType(StructType([
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("score", IntegerType()),
    StructField("spread_radius", FloatType()),
    StructField("nb_logics", IntegerType()),
    StructField("logics", ArrayType(StringType())),
    StructField("count_sources", IntegerType())
]))

udf_master_logic = F.udf(master_logic, output_schema)

# ==============================================================================
# 3. CHARGEMENT ET FUSION (MERGE)
# ==============================================================================

# A. Charger l'historique (J-1)
df_history = spark.read.parquet("/data/cameras/history") \
    .select("camera_id", "source_logic", "lat", "lon", "timestamp")

# B. Charger le daily (J)
df_daily = spark.read.parquet("/data/cameras/daily_processed") \
    .select("camera_id", "source_logic", "lat", "lon", "timestamp")

# C. UNION : On met tout dans le même sac
# C'est ici qu'on "mixe" les logiques.
df_merged = df_history.union(df_daily)

# ==============================================================================
# 4. AGRÉGATION ET FORMATAGE FINAL
# ==============================================================================

# On regroupe TOUT par caméra
df_grouped = df_merged.groupBy("camera_id").agg(
    F.collect_list(F.struct("source_logic", "lat", "lon", "timestamp")).alias("raw_data")
)

# On applique la logique maître
df_processed = df_grouped.withColumn("clusters", udf_master_logic(F.col("raw_data")))

# On prépare la table finale pour Elasticsearch / UI
df_final = df_processed.select(
    F.col("camera_id"),
    
    # --- POUR LA GRID (AG Grid) ---
    # On prend le cluster [0] car c'est le meilleur score (grâce au sort dans l'UDF)
    F.col("clusters")[0]["lat"].alias("grid_lat"),
    F.col("clusters")[0]["lon"].alias("grid_lon"),
    F.col("clusters")[0]["score"].alias("grid_confidence"),
    
    # Flag pour dire à l'utilisateur : "Attention, cette caméra est ambiguë"
    # Si la taille du tableau clusters > 1, ça veut dire qu'elle est vue à 2 endroits > 50m
    (F.size(F.col("clusters")) > 1).alias("has_location_conflict"),
    
    # --- POUR LA CARTE (Map & Zoom) ---
    # On garde toute la structure imbriquée pour afficher les points multiples au zoom
    F.col("clusters").alias("map_locations") 
)

# Sauvegarde
# df_final.write.format("es").save("cameras_index/_doc")


