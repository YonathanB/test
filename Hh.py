# -*- coding: utf-8 -*-
"""
Script de Géolocalisation Indirecte des Caméras via Inférence IP.
Auteur: Gemini
Description:
    1. Croise les logs Mobiles-Routeurs avec les logs GPS Mobiles.
    2. Localise les IPs des routeurs dynamiquement par créneau horaire.
    3. Infère la position des caméras connectées à ces IPs.
    4. Met à jour un référentiel historique avec un score de confiance.
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, DoubleType, StringType
from pyspark.sql.utils import AnalysisException

# ==========================================
# 1. CONFIGURATION & CONSTANTES
# ==========================================

# Paramètres Temporels
TIME_WINDOW_SEC = 3600          # Fenêtre de tolérance pour matcher Mobile-IP et Mobile-GPS (ex: 1h)
IP_BUCKET_SIZE = 3600           # Granularité de la géolocalisation IP (ex: l'IP est fixe sur 1h)

# Paramètres de Qualité
GPS_ACCURACY_THRESHOLD = 100    # On ignore les points GPS avec une précision > 100m
DECAY_FACTOR = 0.001            # Facteur de décroissance pour le score temporel
MIN_WITNESSES_PER_IP = 2        # Minimum de mobiles différents pour valider une IP
CGNAT_STDDEV_THRESHOLD = 0.05   # Si l'écart-type lat/lon d'une IP est > ~5km, on considère que c'est du CGNAT (IP partagée)

# Chemins HDFS / Hive (A ADAPTER)
PATH_OUTPUT_HISTORY = "hdfs://cluster/data/camera_geolocation/master_history"
PATH_OUTPUT_ELASTIC = "hdfs://cluster/data/camera_geolocation/elastic_feed"

# ==========================================
# 2. INITIALISATION SPARK
# ==========================================

spark = SparkSession.builder \
    .appName("Camera_Geolocation_Inference_Batch") \
    .config("spark.sql.shuffle.partitions", "200") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

def main():
    print(">>> Démarrage du Job de Géolocalisation...")

    # ==========================================
    # 3. CHARGEMENT DES DONNÉES (J)
    # ==========================================
    
    # Table A: Caméras (camera_id, ip_router, timestamp)
    df_cameras = spark.table("db_telecom.logs_cameras_router")
    
    # Table B: Mobiles-Routeurs (mobile_id, ip_router, timestamp)
    df_mobiles_net = spark.table("db_telecom.logs_mobiles_router")
    
    # Table C: Géolocalisation (mobile_id, lat, lon, accuracy, timestamp)
    df_mobiles_geo = spark.table("db_telecom.logs_mobiles_geo")

    # ==========================================
    # 4. CRÉATION DU RÉFÉRENTIEL IP (Mobiles + Geo)
    # ==========================================
    print(">>> Étape 1 : Croisement Logs Réseau & Logs GPS...")

    # Filtrage initial des GPS de mauvaise qualité
    df_geo_clean = df_mobiles_geo.filter(F.col("accuracy") <= GPS_ACCURACY_THRESHOLD)

    # Condition de jointure optimisée : Même mobile + fenêtre temporelle glissante
    join_condition = [
        df_mobiles_net.mobile_id == df_geo_clean.mobile_id,
        df_mobiles_net.timestamp >= df_geo_clean.timestamp - TIME_WINDOW_SEC,
        df_mobiles_net.timestamp <= df_geo_clean.timestamp + TIME_WINDOW_SEC
    ]

    df_joined = df_mobiles_net.alias("net").join(df_geo_clean.alias("geo"), join_condition, "inner") \
        .select(
            F.col("net.mobile_id"),
            F.col("net.ip_router"),
            F.col("net.timestamp").alias("ts_net"),
            F.col("geo.lat"),
            F.col("geo.lon"),
            F.col("geo.timestamp").alias("ts_geo"),
            F.col("geo.accuracy")
        )

    # Calcul du Delta Temporel
    df_with_delta = df_joined.withColumn(
        "time_delta", 
        F.abs(F.col("ts_net") - F.col("ts_geo")).cast(LongType())
    )

    # DÉDOUBLONNAGE INTELLIGENT
    # Pour une connexion routeur donnée, on ne garde que le point GPS le plus proche temporellement
    w_dedup = Window.partitionBy("mobile_id", "ts_net", "ip_router").orderBy("time_delta")
    
    df_best_match = df_with_delta.withColumn("rank", F.row_number().over(w_dedup)) \
        .filter(F.col("rank") == 1) \
        .drop("rank")

    # CALCUL DU POIDS DE L'OBSERVATION
    # Formule : Poids = (Proximité Temporelle) * (Précision GPS)
    df_weighted = df_best_match.withColumn(
        "obs_weight",
        (1.0 / (1.0 + (F.col("time_delta") * DECAY_FACTOR))) * (100.0 / (F.col("accuracy") + 1.0))
    )

    # ==========================================
    # 5. LOCALISATION DES IP ROUTEURS (Aggregation)
    # ==========================================
    print(">>> Étape 2 : Localisation des IPs par TimeBucket...")

    # Création des buckets horaires pour gérer les IPs dynamiques
    df_weighted_bucket = df_weighted.withColumn(
        "time_bucket", (F.col("ts_net") / IP_BUCKET_SIZE).cast(LongType())
    )

    # Agrégation par IP et par Heure
    df_ip_candidates = df_weighted_bucket.groupBy("ip_router", "time_bucket").agg(
        F.sum(F.col("lat") * F.col("obs_weight")).alias("sum_w_lat"),
        F.sum(F.col("lon") * F.col("obs_weight")).alias("sum_w_lon"),
        F.sum("obs_weight").alias("total_weight"),
        F.countDistinct("mobile_id").alias("nb_witnesses"),
        F.stddev("lat").alias("lat_stddev") # Pour détecter le CGNAT
    )

    # Nettoyage des IPs :
    # 1. Il faut au moins X témoins distincts (évite qu'un seul mobile biaise tout)
    # 2. Il faut que l'écart type soit faible (sinon c'est une IP partagée à l'échelle de la ville)
    df_ip_reliable = df_ip_candidates \
        .filter(F.col("nb_witnesses") >= MIN_WITNESSES_PER_IP) \
        .filter((F.col("lat_stddev").isNull()) | (F.col("lat_stddev") < CGNAT_STDDEV_THRESHOLD)) \
        .withColumn("ip_lat", F.col("sum_w_lat") / F.col("total_weight")) \
        .withColumn("ip_lon", F.col("sum_w_lon") / F.col("total_weight"))

    # ==========================================
    # 6. MAPPING CAMÉRAS (Table A + IPs)
    # ==========================================
    print(">>> Étape 3 : Mapping Caméras...")

    df_cameras_bucket = df_cameras.withColumn(
        "time_bucket", (F.col("timestamp") / IP_BUCKET_SIZE).cast(LongType())
    )

    # On trouve où se trouvaient les IPs utilisées par les caméras
    df_cam_daily_loc = df_cameras_bucket.join(
        df_ip_reliable, 
        on=["ip_router", "time_bucket"], 
        how="inner"
    ).select(
        "camera_id",
        F.col("ip_lat").alias("daily_lat"),
        F.col("ip_lon").alias("daily_lon"),
        F.col("total_weight").alias("daily_confidence_score"),
        F.lit(1).alias("daily_obs_count")
    )

    # ==========================================
    # 7. CONVERGENCE (Merge avec Historique)
    # ==========================================
    print(">>> Étape 4 : Convergence avec l'historique (J-1)...")

    # Chargement Historique (Gestion du premier run)
    try:
        df_history = spark.read.parquet(PATH_OUTPUT_HISTORY)
        # Schema attendu: camera_id, acc_lat_w, acc_lon_w, acc_weight, total_obs, last_updated
    except AnalysisException:
        print("!!! Aucun historique trouvé. Création d'un nouveau référentiel.")
        # Création schema vide si premier run
        df_history = spark.createDataFrame([], schema="camera_id string, acc_lat_w double, acc_lon_w double, acc_weight double, total_obs long")

    # Préparation des données du jour pour l'union
    # On stocke la somme pondérée (lat * weight) pour faciliter les moyennes futures
    df_daily_prepped = df_cam_daily_loc.select(
        "camera_id",
        (F.col("daily_lat") * F.col("daily_confidence_score")).alias("acc_lat_w"),
        (F.col("daily_lon") * F.col("daily_confidence_score")).alias("acc_lon_w"),
        F.col("daily_confidence_score").alias("acc_weight"),
        F.col("daily_obs_count").alias("total_obs")
    )

    # Union et Agrégation Globale
    df_union = df_history.select("camera_id", "acc_lat_w", "acc_lon_w", "acc_weight", "total_obs") \
        .union(df_daily_prepped)

    df_refined_master = df_union.groupBy("camera_id").agg(
        F.sum("acc_lat_w").alias("acc_lat_w"),
        F.sum("acc_lon_w").alias("acc_lon_w"),
        F.sum("acc_weight").alias("acc_weight"),
        F.sum("total_obs").alias("total_obs")
    )

    # Calcul des coordonnées finales et du score utilisateur
    # Score utilisateur : Normalisation simple basée sur le poids accumulé (logarithmique pour lisser)
    df_final_output = df_refined_master.withColumn(
        "final_lat", F.col("acc_lat_w") / F.col("acc_weight")
    ).withColumn(
        "final_lon", F.col("acc_lon_w") / F.col("acc_weight")
    ).withColumn(
        "user_confidence", 
        F.least(F.lit(100.0), F.log1p("acc_weight") * 10) # Exemple de scoring: log(poids)*10 plafonné à 100
    )

    # ==========================================
    # 8. SAUVEGARDE
    # ==========================================
    print(f">>> Étape 5 : Sauvegarde...")

    # 1. Sauvegarde du Master (Overwrite pour le lendemain)
    # On garde les colonnes brutes (acc_*) pour les calculs futurs
    df_final_output.write.mode("overwrite").parquet(PATH_OUTPUT_HISTORY)

    # 2. Sauvegarde pour ElasticSearch (Format plat pour l'indexation)
    # On formate geo_point pour Elastic: "lat,lon"
    df_elastic = df_final_output.select(
        "camera_id",
        "final_lat",
        "final_lon",
        "user_confidence",
        "total_obs",
        F.concat_ws(",", "final_lat", "final_lon").alias("location"),
        F.current_timestamp().alias("last_updated")
    )
    
    df_elastic.write.mode("overwrite").parquet(PATH_OUTPUT_ELASTIC)
    
    print(">>> Job terminé avec succès.")

if __name__ == "__main__":
    main()



from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class GeoStrategy(ABC):
    """
    Classe abstraite pour toute logique de géolocalisation.
    """
    
    def __init__(self, spark, params):
        self.spark = spark
        self.params = params # Dictionnaire contenant les réglages utilisateur (seuils, poids...)
        self.logic_name = "GENERIC_STRATEGY"

    @abstractmethod
    def run(self, inputs: dict) -> DataFrame:
        """
        inputs: Dictionnaire de DataFrames (A, B, C...)
        Retourne: DataFrame standardisé [camera_id, lat, lon, confidence, metadata]
        """
        pass
from strategies.base import GeoStrategy
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class IpInferenceStrategy(GeoStrategy):
    
    def __init__(self, spark, params):
        super().__init__(spark, params)
        self.logic_name = "IP_INFERENCE"

    def run(self, inputs):
        # Récupération des paramètres utilisateur
        decay = float(self.params.get("decay_factor", 0.001))
        min_witnesses = int(self.params.get("min_witnesses", 2))
        
        df_cameras = inputs["cameras"]
        df_mobiles = inputs["mobiles_net"]
        df_geo = inputs["mobiles_geo"]
        
        # ... [ICI, TOUT LE CODE DE L'ALGO PRÉCÉDENT] ...
        # (Je ne remets pas tout pour la lisibilité, mais c'est la même logique)
        
        # Au moment de calculer le poids :
        df_weighted = df_best_match.withColumn(
            "obs_weight",
            (1.0 / (1.0 + (F.col("time_delta") * decay))) * (100.0 / (F.col("accuracy") + 1.0))
        )
        
        # Filtrage dynamique selon le choix utilisateur
        df_ip_reliable = df_ip_candidates.filter(F.col("nb_witnesses") >= min_witnesses)
        
        # Formatage de la sortie standardisée
        return df_final_output.select(
            F.col("camera_id"),
            F.col("final_lat").alias("lat"),
            F.col("final_lon").alias("lon"),
            F.col("user_confidence").alias("confidence"),
            F.lit(self.logic_name).alias("logic_name"),
            # On stocke les params utilisés en JSON pour traçabilité
            F.lit(str(self.params)).alias("params_used_json") 
        )




import sys
import json
from pyspark.sql import SparkSession
from strategies.ip_inference import IpInferenceStrategy
# from strategies.image_recognition import ImageStrategy (exemple futur)

def main():
    spark = SparkSession.builder.appName("GeoLocator_Engine").getOrCreate()

    # 1. Récupération de la Configuration Utilisateur
    # On imagine que l'UI passe un JSON en argument du job Spark
    # Ex: '{"active_strategies": ["IP_INFERENCE"], "params": {"IP_INFERENCE": {"decay_factor": 0.005}}}'
    raw_config = sys.argv[1] 
    user_config = json.loads(raw_config)

    # 2. Chargement des données brutes (Commun à toutes les stratégies)
    inputs = {
        "cameras": spark.table("db.cameras"),
        "mobiles_net": spark.table("db.mobiles_net"),
        "mobiles_geo": spark.table("db.mobiles_geo")
    }

    results_dfs = []

    # 3. Exécution dynamique des stratégies demandées
    if "IP_INFERENCE" in user_config["active_strategies"]:
        print(">>> Lancement stratégie IP...")
        # Injection des paramètres spécifiques à cette algo
        algo_params = user_config["params"].get("IP_INFERENCE", {})
        
        strategy = IpInferenceStrategy(spark, algo_params)
        results_dfs.append(strategy.run(inputs))

    # if "OTHER_LOGIC" in ...
    
    # 4. Unification des résultats
    if results_dfs:
        from functools import reduce
        df_final_all_strategies = reduce(DataFrame.unionByName, results_dfs)
        
        # Sauvegarde vers ElasticSearch (Format enrichi)
        # Chaque ligne contient : CameraID, Lat, Lon, LogicName, ParamsUsed
        df_final_all_strategies.write.format("es").save("cameras/locations")

if __name__ == "__main__":
    main()
