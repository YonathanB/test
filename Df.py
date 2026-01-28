# ==============================================================================
# 0. IMPORTS ET CONFIGURATION
# ==============================================================================
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import math

# Initialisation de la session
spark = SparkSession.builder \
    .appName("Camera_Geolocation_Master_Pipeline") \
    .getOrCreate()

# --- CONSTANTES MÉTIER ---
EARTH_RADIUS_KM = 6371.0
CLUSTER_THRESHOLD_METERS = 50.0  # Distance max pour grouper des points (Fusion)
HIGH_PRECISION_METERS = 10.0     # En-dessous de 10m, on considère la précision excellente

# ==============================================================================
# 1. DÉFINITION DES FONCTIONS UDF (L'INTELLIGENCE GÉOGRAPHIQUE)
# ==============================================================================

def haversine(lat1, lon1, lat2, lon2):
    """Calcule la distance en mètres entre deux coordonnées GPS."""
    if None in [lat1, lon1, lat2, lon2]: return 0.0
    
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2) * math.sin(dLat/2) + \
        math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * \
        math.sin(dLon/2) * math.sin(dLon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return EARTH_RADIUS_KM * c * 1000

def process_geolocation_logic(candidates):
    """
    Cette fonction reçoit la liste de toutes les positions actives pour une caméra.
    Elle effectue:
    1. Le Clustering (séparer les positions éloignées de > 50m).
    2. Le Scoring (calculer la confiance basée sur la convergence et la précision).
    """
    if not candidates: return []
    
    # Conversion Row Spark -> Dictionnaire Python
    points = [row.asDict() if hasattr(row, 'asDict') else row for row in candidates]
    clusters = []
    
    # --- PHASE A : CLUSTERING GLOUTON (Distance < 50m) ---
    while points:
        ref_point = points.pop(0)
        current_cluster = [ref_point]
        remaining_points = []
        
        for p in points:
            dist = haversine(ref_point['lat'], ref_point['lon'], p['lat'], p['lon'])
            if dist <= CLUSTER_THRESHOLD_METERS:
                current_cluster.append(p)
            else:
                remaining_points.append(p)
        
        points = remaining_points # On continue avec ceux qui restent
        
        # --- PHASE B : CALCUL DU SCORE DU CLUSTER ---
        n = len(current_cluster)
        unique_logics = set(p['source_logic'] for p in current_cluster)
        nb_logics = len(unique_logics)
        
        # 1. Calcul du Centroid (Position Moyenne)
        avg_lat = sum(p['lat'] for p in current_cluster) / n
        avg_lon = sum(p['lon'] for p in current_cluster) / n
        
        # 2. Calcul de la Dispersion (Rayon moyen)
        spreads = [haversine(avg_lat, avg_lon, p['lat'], p['lon']) for p in current_cluster]
        avg_spread = sum(spreads) / n if n > 0 else 0
        
        # 3. Formule de Scoring (Sur 100)
        score = 40 # Socle de base
        
        # Bonus Convergence : +20 points par logique supplémentaire qui est d'accord
        score += (nb_logics - 1) * 20
        
        # Bonus/Malus Précision
        if avg_spread <= HIGH_PRECISION_METERS:
            score += 10 # Bonus précision
        else:
            # Pénalité : -0.5 point par mètre d'écart au-delà de 10m
            score -= (avg_spread - HIGH_PRECISION_METERS) * 0.5
            
        final_score = int(max(0, min(100, score)))
        
        # Construction de l'objet résultat pour ce cluster
        clusters.append({
            "lat": avg_lat,
            "lon": avg_lon,
            "score": final_score,
            "radius_spread": avg_spread,
            "nb_logics": nb_logics,
            "logics": list(unique_logics),
            "sources": current_cluster # On garde les détails pour le debug
        })
        
    # On trie pour que le cluster [0] soit le plus probable (Meilleur Score)
    clusters.sort(key=lambda x: x['score'], reverse=True)
    return clusters

# Définition du Schéma de sortie pour Spark (Complexe)
output_schema = ArrayType(StructType([
    StructField("lat", DoubleType(), False),
    StructField("lon", DoubleType(), False),
    StructField("score", IntegerType(), False),
    StructField("radius_spread", FloatType(), False),
    StructField("nb_logics", IntegerType(), False),
    StructField("logics", ArrayType(StringType()), False),
    StructField("sources", ArrayType(StructType([
        StructField("source_logic", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("timestamp", StringType()) # Simplifié en string pour l'exemple
    ])))
]))

# Enregistrement UDF
udf_process_geo = F.udf(process_geolocation_logic, output_schema)

# ==============================================================================
# 2. GÉNÉRATION DE DONNÉES DE TEST (Simulation)
# ==============================================================================
# Ici, on crée des faux DataFrames pour simuler tes tables.
# Dans la réalité, tu feras : df = spark.read.parquet("...")

# A. HISTORIQUE (Données d'hier)
history_data = [
    # Caméra 1 : Hier on pensait qu'elle était à Paris (Wifi)
    ("cam_01", "WIFI", 48.8566, 2.3522, "2023-10-25 10:00:00"),
    # Caméra 2 : Hier on avait une install manuelle à Lyon
    ("cam_02", "INSTALLATION", 45.7640, 4.8357, "2023-01-01 00:00:00"),
]
df_history = spark.createDataFrame(history_data, ["camera_id", "source_logic", "lat", "lon", "timestamp"])

# B. DAILY (Données reçues aujourd'hui)
daily_data = [
    # Caméra 1 : Le Wifi se met à jour aujourd'hui (légèrement décalé) -> DOIT REMPLACER L'HISTORIQUE
    ("cam_01", "WIFI", 48.8567, 2.3523, "2023-10-26 14:00:00"),
    # Caméra 1 : Une nouvelle logique (IP) arrive et confirme Paris -> DOIT S'AJOUTER
    ("cam_01", "IP_TRACE", 48.8565, 2.3521, "2023-10-26 14:05:00"),
    
    # Caméra 2 : Une logique IP arrive mais place la caméra à Marseille (Erreur/Ghost) -> DOIT CRÉER UN CONFLIT
    ("cam_02", "IP_TRACE", 43.2965, 5.3698, "2023-10-26 09:00:00"),
]
df_daily = spark.createDataFrame(daily_data, ["camera_id", "source_logic", "lat", "lon", "timestamp"])

print("--- DONNÉES BRUTES ---")
# df_history.show()
# df_daily.show()

# ==============================================================================
# 3. ÉTAPE DE FUSION (MERGE & DEDUPLICATION)
# ==============================================================================

# 1. Union de tout le monde
df_all = df_history.union(df_daily)

# 2. Logique "Champion vs Challenger"
# Pour chaque (camera, logic), on garde seulement la ligne la plus récente.
window_spec = Window.partitionBy("camera_id", "source_logic").orderBy(F.col("timestamp").desc())

df_active_logics = df_all.withColumn("rank", F.row_number().over(window_spec)) \
    .filter(F.col("rank") == 1) \
    .drop("rank")

# C'est ici que tu sauvegardes ton "Nouvel Historique" pour demain
# df_active_logics.write.mode("overwrite").parquet("/path/to/history/new")

print("--- DONNÉES APRÈS FUSION (Active Logics) ---")
df_active_logics.show(truncate=False)

# ==============================================================================
# 4. ÉTAPE D'AGRÉGATION & SCORING (CLUSTERING)
# ==============================================================================

# 1. On regroupe toutes les logiques actives dans une liste par caméra
df_grouped = df_active_logics.groupBy("camera_id").agg(
    F.collect_list(F.struct("source_logic", "lat", "lon", "timestamp")).alias("candidates_list")
)

# 2. Application de l'UDF (Clustering + Scoring)
df_processed = df_grouped.withColumn("geo_clusters", udf_process_geo(F.col("candidates_list")))

# ==============================================================================
# 5. PRÉPARATION DE LA SORTIE FINALE (ES / UI)
# ==============================================================================

df_final = df_processed.select(
    F.col("camera_id"),
    
    # --- A. CHAMPS POUR LA TABLE (AG GRID) ---
    # On prend le cluster à l'index [0] car c'est celui qui a le meilleur score.
    F.col("geo_clusters")[0]["lat"].alias("grid_lat"),
    F.col("geo_clusters")[0]["lon"].alias("grid_lon"),
    F.col("geo_clusters")[0]["score"].alias("grid_confidence"),
    
    # Indicateur de conflit : Si on a plus d'un cluster, c'est qu'il y a des positions > 50m
    (F.size(F.col("geo_clusters")) > 1).alias("has_location_conflict"),
    
    # --- B. CHAMPS POUR LA CARTE (MAP / ZOOM) ---
    # On garde toute la structure complexe. Le Front-end affichera tous les éléments de ce tableau.
    F.col("geo_clusters").alias("map_locations_details")
)

print("--- RÉSULTAT FINAL (À ENVOYER DANS ELASTICSEARCH) ---")
df_final.printSchema()
df_final.show(truncate=False)

# EXPLICATION DES RÉSULTATS AFFICHÉS :
# cam_01 : Aura un score élevé (~70) car WIFI et IP sont proches (<50m). Un seul cluster.
# cam_02 : Aura has_location_conflict=True. Deux clusters dans map_locations_details (Lyon et Marseille).
import math
from dataclasses import dataclass
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# =============================================================================
# 1. CONFIGURATION
# =============================================================================

@dataclass
class Config:
    # Sessionisation
    session_gap_minutes: int = 30        # Si trou > 30 min, nouvelle session
    
    # GPS Matching & Cleaning
    gps_time_window_sec: int = 3600      # On cherche des GPS jusqu'à 1h autour
    max_speed_kmh: int = 250             # Rejet des points "téléportés"
    gps_accuracy_threshold: int = 100    # Rejet des GPS imprécis (>100m)
    
    # Scoring & CGNAT
    min_gps_points: int = 2              # Minimum de points pour valider une session
    max_scatter_meters: int = 500        # Si les points sont trop éparpillés -> CGNAT/Bruit -> Rejet
    cgnat_device_threshold: int = 10     # Si IP a > 10 devices -> Blacklist
    
    # Poids pour le score final
    weight_radius: float = 1.0           # Confiance max
    weight_ip_only: float = 0.5          # Confiance moyenne
    decay_halflife_min: float = 10.0     # Le score baisse de moitié si écart de 10 min

# =============================================================================
# 2. OUTILS (Spark Native Optimization)
# =============================================================================

def native_haversine(lat1, lon1, lat2, lon2):
    """Calcul distance en mètres SANS UDF Python (Performance x100)"""
    R = 6371000.0
    rad = F.lit(math.pi / 180.0)
    dlat = (lat2 - lat1) * rad
    dlon = (lon2 - lon1) * rad
    a = F.pow(F.sin(dlat / 2), 2) + \
        F.cos(lat1 * rad) * F.cos(lat2 * rad) * \
        F.pow(F.sin(dlon / 2), 2)
    return R * 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))

def temporal_decay_score(diff_seconds, halflife_minutes):
    """Plus l'écart temporel est grand, plus le score baisse"""
    halflife_sec = halflife_minutes * 60
    decay_const = math.log(2) / halflife_sec
    return F.exp(-decay_const * F.abs(diff_seconds))

# =============================================================================
# 3. PIPELINE PRINCIPAL
# =============================================================================

class CameraInferencePipeline:
    def __init__(self, spark: SparkSession, conf: Config = None):
        self.spark = spark
        self.cfg = conf or Config()

    def run(self, paths: dict):
        """
        paths = {'camera': '...', 'wifi': '...', 'gps': '...', 'gold': '...'}
        """
        # --- A. CHARGEMENT & NETTOYAGE GPS ---
        df_gps = self._load_and_clean_gps(paths['gps'])
        
        # --- B. SESSIONIZATION WIFI (Clé de voûte) ---
        df_sessions = self._build_wifi_sessions(paths['wifi'])
        
        # --- C. ANCHORING (Lier Sessions <-> GPS) ---
        df_anchored = self._anchor_gps_to_sessions(df_sessions, df_gps)
        
        # --- D. INFERENCE CAMERA ---
        df_candidates = self._infer_cameras(paths['camera'], df_anchored)
        
        # --- E. GOLD MERGE (Champion vs Challenger) ---
        self._merge_gold(df_candidates, paths['gold'])

    # -------------------------------------------------------------------------
    # ÉTAPES DÉTAILLÉES
    # -------------------------------------------------------------------------

    def _load_and_clean_gps(self, path):
        """Nettoyage GPS: Vitesse impossible + Précision"""
        df = self.spark.read.parquet(path)
        
        # 1. Filtre précision de base
        df = df.filter(F.col("accuracy") <= self.cfg.gps_accuracy_threshold)
        
        # 2. Filtre Vitesse (Implémentation 1 logic)
        w = Window.partitionBy("phone_id").orderBy("timestamp")
        df = df.withColumn("prev_lat", F.lag("lat").over(w)) \
               .withColumn("prev_lon", F.lag("lon").over(w)) \
               .withColumn("prev_ts", F.lag("timestamp").over(w))
        
        dist = native_haversine(F.col("lat"), F.col("lon"), F.col("prev_lat"), F.col("prev_lon"))
        time_diff = F.col("timestamp").cast("long") - F.col("prev_ts").cast("long")
        speed_kmh = (dist / time_diff) * 3.6
        
        return df.filter((speed_kmh < self.cfg.max_speed_kmh) | (speed_kmh.isNull())) \
                 .select("phone_id", "lat", "lon", "timestamp")

    def _build_wifi_sessions(self, path):
        """Regroupement des logs WiFi en Sessions [Start, End]"""
        df = self.spark.read.parquet(path)
        
        # Détection changement IP ou trou temporel
        w = Window.partitionBy("phone_id", "ip", "user_radius").orderBy("timestamp")
        
        df = df.withColumn("prev_ts", F.lag("timestamp").over(w))
        df = df.withColumn("is_new_session", 
            F.when(
                (F.col("timestamp").cast("long") - F.col("prev_ts").cast("long")) > (self.cfg.session_gap_minutes * 60), 
                1
            ).otherwise(0)
        )
        
        # ID unique de session
        df = df.withColumn("session_id", F.sum("is_new_session").over(w))
        
        # Aggregation
        return df.groupBy("phone_id", "ip", "user_radius", "session_id") \
                 .agg(
                     F.min("timestamp").alias("s_start"),
                     F.max("timestamp").alias("s_end")
                 )

    def _anchor_gps_to_sessions(self, df_sessions, df_gps):
        """Attache le meilleur GPS à chaque session WiFi"""
        
        # 1. Jointure Range (Optimisée)
        # On ne garde que les GPS dans la fenêtre temporelle de la session (+/- buffer)
        cond = (df_sessions.phone_id == df_gps.phone_id) & \
               (df_gps.timestamp >= df_sessions.s_start - self.cfg.gps_time_window_sec) & \
               (df_gps.timestamp <= df_sessions.s_end + self.cfg.gps_time_window_sec)
        
        joined = df_sessions.join(df_gps, cond, "inner")
        
        # 2. Calcul des métriques de la session (Scatter & Decay)
        # On calcule l'écart temporel moyen pour cette session
        mid_session = (F.col("s_start").cast("long") + F.col("s_end").cast("long")) / 2
        joined = joined.withColumn("time_gap", F.abs(F.col("timestamp").cast("long") - mid_session))
        
        # 3. Aggregation par session (Utilisation de la MEDIANE pour robustesse)
        # On calcule le centroïde (médiane lat/lon) et la dispersion (stddev)
        session_locs = joined.groupBy("ip", "user_radius").agg(
            F.expr("percentile_approx(lat, 0.5)").alias("net_lat"),
            F.expr("percentile_approx(lon, 0.5)").alias("net_lon"),
            F.stddev("lat").alias("lat_std"), # Proxy simple pour le scatter
            F.min("time_gap").alias("min_time_gap"),
            F.count("*").alias("gps_count"),
            F.countDistinct("phone_id").alias("device_count")
        )
        
        # 4. Filtrage Qualité (CGNAT & Noise Filter)
        # Si trop de devices sur l'IP -> Blacklist
        # Si trop de dispersion géographique -> Mauvaise qualité
        # Si pas assez de points GPS -> Pas fiable
        valid_networks = session_locs.filter(
            (F.col("device_count") < self.cfg.cgnat_device_threshold) & 
            (F.col("lat_std") < 0.005) & # ~500m approx en degrés
            (F.col("gps_count") >= self.cfg.min_gps_points)
        )
        
        # Calcul du score final du réseau
        return valid_networks.withColumn(
            "net_score", 
            temporal_decay_score(F.col("min_time_gap"), self.cfg.decay_halflife_min)
        )

    def _infer_cameras(self, path_cam, df_networks):
        """Propagation aux caméras"""
        df_cam = self.spark.read.parquet(path_cam)
        
        # BRANCHE A: RADIUS (Prioritaire)
        safe_nets = df_networks.filter(F.col("user_radius").isNotNull())
        matches_safe = df_cam.filter(F.col("user_radius").isNotNull()) \
                             .join(safe_nets, ["ip", "user_radius"], "inner") \
                             .withColumn("final_score", F.col("net_score") * self.cfg.weight_radius) \
                             .withColumn("method", F.lit("radius"))

        # BRANCHE B: IP ONLY (Dégradée)
        # On prend les réseaux qui n'ont PAS de user_radius connu côté réseau
        risky_nets = df_networks.drop("user_radius")
        matches_risky = df_cam.filter(F.col("user_radius").isNull()) \
                              .join(risky_nets, "ip", "inner") \
                              .withColumn("final_score", F.col("net_score") * self.cfg.weight_ip_only) \
                              .withColumn("method", F.lit("ip_only"))
        
        return matches_safe.unionByName(matches_risky)

    def _merge_gold(self, df_candidates, path_gold):
        """Strategie Best-Record-Wins"""
        try:
            df_history = self.spark.read.parquet(path_gold)
        except:
            df_history = self.spark.createDataFrame([], df_candidates.schema)

        # On combine tout
        df_all = df_history.unionByName(df_candidates, allowMissingColumns=True)
        
        # TRI INTELLIGENT
        # 1. Méthode (Radius > IP)
        # 2. Score (Proximité temporelle)
        # 3. Récence (Si égalité, le plus récent gagne)
        w = Window.partitionBy("camera_id").orderBy(
            F.when(F.col("method") == "radius", 2).otherwise(1).desc(),
            F.col("final_score").desc(),
            F.col("timestamp").desc()
        )
        
        df_final = df_all.withColumn("rank", F.row_number().over(w)) \
                         .filter(F.col("rank") == 1) \
                         .drop("rank")
        
        # Sauvegarde
        df_final.write.mode("overwrite").parquet(path_gold)
        print("Gold Updated.")

# =============================================================================
# EXECUTION
# =============================================================================

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CameraGeolocFinal").getOrCreate()
    
    pipeline = CameraInferencePipeline(spark)
    
    paths = {
        "camera": "hdfs://.../raw/camera/",
        "wifi":   "hdfs://.../raw/phone_wifi/",
        "gps":    "hdfs://.../raw/phone_gps/",
        "gold":   "hdfs://.../gold/camera_master/"
    }
    
    pipeline.run(paths)
import math
from dataclasses import dataclass
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# =============================================================================
# 1. CONFIGURATION
# =============================================================================

@dataclass
class Config:
    # Sessionisation
    session_gap_minutes: int = 30        # Si trou > 30 min, nouvelle session
    
    # GPS Matching & Cleaning
    gps_time_window_sec: int = 3600      # On cherche des GPS jusqu'à 1h autour
    max_speed_kmh: int = 250             # Rejet des points "téléportés"
    gps_accuracy_threshold: int = 100    # Rejet des GPS imprécis (>100m)
    
    # Scoring & CGNAT
    min_gps_points: int = 2              # Minimum de points pour valider une session
    max_scatter_meters: int = 500        # Si les points sont trop éparpillés -> CGNAT/Bruit -> Rejet
    cgnat_device_threshold: int = 10     # Si IP a > 10 devices -> Blacklist
    
    # Poids pour le score final
    weight_radius: float = 1.0           # Confiance max
    weight_ip_only: float = 0.5          # Confiance moyenne
    decay_halflife_min: float = 10.0     # Le score baisse de moitié si écart de 10 min

# =============================================================================
# 2. OUTILS (Spark Native Optimization)
# =============================================================================

def native_haversine(lat1, lon1, lat2, lon2):
    """Calcul distance en mètres SANS UDF Python (Performance x100)"""
    R = 6371000.0
    rad = F.lit(math.pi / 180.0)
    dlat = (lat2 - lat1) * rad
    dlon = (lon2 - lon1) * rad
    a = F.pow(F.sin(dlat / 2), 2) + \
        F.cos(lat1 * rad) * F.cos(lat2 * rad) * \
        F.pow(F.sin(dlon / 2), 2)
    return R * 2 * F.atan2(F.sqrt(a), F.sqrt(1 - a))

def temporal_decay_score(diff_seconds, halflife_minutes):
    """Plus l'écart temporel est grand, plus le score baisse"""
    halflife_sec = halflife_minutes * 60
    decay_const = math.log(2) / halflife_sec
    return F.exp(-decay_const * F.abs(diff_seconds))

# =============================================================================
# 3. PIPELINE PRINCIPAL
# =============================================================================

class CameraInferencePipeline:
    def __init__(self, spark: SparkSession, conf: Config = None):
        self.spark = spark
        self.cfg = conf or Config()

    def run(self, paths: dict):
        """
        paths = {'camera': '...', 'wifi': '...', 'gps': '...', 'gold': '...'}
        """
        # --- A. CHARGEMENT & NETTOYAGE GPS ---
        df_gps = self._load_and_clean_gps(paths['gps'])
        
        # --- B. SESSIONIZATION WIFI (Clé de voûte) ---
        df_sessions = self._build_wifi_sessions(paths['wifi'])
        
        # --- C. ANCHORING (Lier Sessions <-> GPS) ---
        df_anchored = self._anchor_gps_to_sessions(df_sessions, df_gps)
        
        # --- D. INFERENCE CAMERA ---
        df_candidates = self._infer_cameras(paths['camera'], df_anchored)
        
        # --- E. GOLD MERGE (Champion vs Challenger) ---
        self._merge_gold(df_candidates, paths['gold'])

    # -------------------------------------------------------------------------
    # ÉTAPES DÉTAILLÉES
    # -------------------------------------------------------------------------

    def _load_and_clean_gps(self, path):
        """Nettoyage GPS: Vitesse impossible + Précision"""
        df = self.spark.read.parquet(path)
        
        # 1. Filtre précision de base
        df = df.filter(F.col("accuracy") <= self.cfg.gps_accuracy_threshold)
        
        # 2. Filtre Vitesse (Implémentation 1 logic)
        w = Window.partitionBy("phone_id").orderBy("timestamp")
        df = df.withColumn("prev_lat", F.lag("lat").over(w)) \
               .withColumn("prev_lon", F.lag("lon").over(w)) \
               .withColumn("prev_ts", F.lag("timestamp").over(w))
        
        dist = native_haversine(F.col("lat"), F.col("lon"), F.col("prev_lat"), F.col("prev_lon"))
        time_diff = F.col("timestamp").cast("long") - F.col("prev_ts").cast("long")
        speed_kmh = (dist / time_diff) * 3.6
        
        return df.filter((speed_kmh < self.cfg.max_speed_kmh) | (speed_kmh.isNull())) \
                 .select("phone_id", "lat", "lon", "timestamp")

    def _build_wifi_sessions(self, path):
        """Regroupement des logs WiFi en Sessions [Start, End]"""
        df = self.spark.read.parquet(path)
        
        # Détection changement IP ou trou temporel
        w = Window.partitionBy("phone_id", "ip", "user_radius").orderBy("timestamp")
        
        df = df.withColumn("prev_ts", F.lag("timestamp").over(w))
        df = df.withColumn("is_new_session", 
            F.when(
                (F.col("timestamp").cast("long") - F.col("prev_ts").cast("long")) > (self.cfg.session_gap_minutes * 60), 
                1
            ).otherwise(0)
        )
        
        # ID unique de session
        df = df.withColumn("session_id", F.sum("is_new_session").over(w))
        
        # Aggregation
        return df.groupBy("phone_id", "ip", "user_radius", "session_id") \
                 .agg(
                     F.min("timestamp").alias("s_start"),
                     F.max("timestamp").alias("s_end")
                 )

    def _anchor_gps_to_sessions(self, df_sessions, df_gps):
        """Attache le meilleur GPS à chaque session WiFi"""
        
        # 1. Jointure Range (Optimisée)
        # On ne garde que les GPS dans la fenêtre temporelle de la session (+/- buffer)
        cond = (df_sessions.phone_id == df_gps.phone_id) & \
               (df_gps.timestamp >= df_sessions.s_start - self.cfg.gps_time_window_sec) & \
               (df_gps.timestamp <= df_sessions.s_end + self.cfg.gps_time_window_sec)
        
        joined = df_sessions.join(df_gps, cond, "inner")
        
        # 2. Calcul des métriques de la session (Scatter & Decay)
        # On calcule l'écart temporel moyen pour cette session
        mid_session = (F.col("s_start").cast("long") + F.col("s_end").cast("long")) / 2
        joined = joined.withColumn("time_gap", F.abs(F.col("timestamp").cast("long") - mid_session))
        
        # 3. Aggregation par session (Utilisation de la MEDIANE pour robustesse)
        # On calcule le centroïde (médiane lat/lon) et la dispersion (stddev)
        session_locs = joined.groupBy("ip", "user_radius").agg(
            F.expr("percentile_approx(lat, 0.5)").alias("net_lat"),
            F.expr("percentile_approx(lon, 0.5)").alias("net_lon"),
            F.stddev("lat").alias("lat_std"), # Proxy simple pour le scatter
            F.min("time_gap").alias("min_time_gap"),
            F.count("*").alias("gps_count"),
            F.countDistinct("phone_id").alias("device_count")
        )
        
        # 4. Filtrage Qualité (CGNAT & Noise Filter)
        # Si trop de devices sur l'IP -> Blacklist
        # Si trop de dispersion géographique -> Mauvaise qualité
        # Si pas assez de points GPS -> Pas fiable
        valid_networks = session_locs.filter(
            (F.col("device_count") < self.cfg.cgnat_device_threshold) & 
            (F.col("lat_std") < 0.005) & # ~500m approx en degrés
            (F.col("gps_count") >= self.cfg.min_gps_points)
        )
        
        # Calcul du score final du réseau
        return valid_networks.withColumn(
            "net_score", 
            temporal_decay_score(F.col("min_time_gap"), self.cfg.decay_halflife_min)
        )

    def _infer_cameras(self, path_cam, df_networks):
        """Propagation aux caméras"""
        df_cam = self.spark.read.parquet(path_cam)
        
        # BRANCHE A: RADIUS (Prioritaire)
        safe_nets = df_networks.filter(F.col("user_radius").isNotNull())
        matches_safe = df_cam.filter(F.col("user_radius").isNotNull()) \
                             .join(safe_nets, ["ip", "user_radius"], "inner") \
                             .withColumn("final_score", F.col("net_score") * self.cfg.weight_radius) \
                             .withColumn("method", F.lit("radius"))

        # BRANCHE B: IP ONLY (Dégradée)
        # On prend les réseaux qui n'ont PAS de user_radius connu côté réseau
        risky_nets = df_networks.drop("user_radius")
        matches_risky = df_cam.filter(F.col("user_radius").isNull()) \
                              .join(risky_nets, "ip", "inner") \
                              .withColumn("final_score", F.col("net_score") * self.cfg.weight_ip_only) \
                              .withColumn("method", F.lit("ip_only"))
        
        return matches_safe.unionByName(matches_risky)

    def _merge_gold(self, df_candidates, path_gold):
        """Strategie Best-Record-Wins"""
        try:
            df_history = self.spark.read.parquet(path_gold)
        except:
            df_history = self.spark.createDataFrame([], df_candidates.schema)

        # On combine tout
        df_all = df_history.unionByName(df_candidates, allowMissingColumns=True)
        
        # TRI INTELLIGENT
        # 1. Méthode (Radius > IP)
        # 2. Score (Proximité temporelle)
        # 3. Récence (Si égalité, le plus récent gagne)
        w = Window.partitionBy("camera_id").orderBy(
            F.when(F.col("method") == "radius", 2).otherwise(1).desc(),
            F.col("final_score").desc(),
            F.col("timestamp").desc()
        )
        
        df_final = df_all.withColumn("rank", F.row_number().over(w)) \
                         .filter(F.col("rank") == 1) \
                         .drop("rank")
        
        # Sauvegarde
        df_final.write.mode("overwrite").parquet(path_gold)
        print("Gold Updated.")

# =============================================================================
# EXECUTION
# =============================================================================

if __name__ == "__main__":
    spark = SparkSession.builder.appName("CameraGeolocFinal").getOrCreate()
    
    pipeline = CameraInferencePipeline(spark)
    
    paths = {
        "camera": "hdfs://.../raw/camera/",
        "wifi":   "hdfs://.../raw/phone_wifi/",
        "gps":    "hdfs://.../raw/phone_gps/",
        "gold":   "hdfs://.../gold/camera_master/"
    }
    
    pipeline.run(paths)
