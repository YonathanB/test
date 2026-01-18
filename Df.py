from pyspark.sql import functions as F

# Constante de conversion Degrés -> Mètres (Approximation pour latitude moyenne)
DEG_TO_METERS = 111111.0 

# Paramètres de sensibilité
SENSITIVITY_SIZE = 4.0   # À 4 témoins, on a ~75% de confiance sur la taille
TOLERANCE_DISP = 20.0    # À 20 mètres de dispersion, on divise la confiance par 2

df_scored_final = df_unique_camera.withColumn(
    # 1. Conversion de la dispersion (degrés) en mètres
    # On gère le cas où best_dispersion est null (cas group_size=1) -> 0 mètre
    "dispersion_meters",
    F.coalesce(F.col("best_dispersion"), F.lit(0.0)) * DEG_TO_METERS
).withColumn(
    # 2. Calcul du Score de Taille (Courbe de saturation)
    "score_size",
    F.tanh(F.col("frequency") / SENSITIVITY_SIZE)
).withColumn(
    # 3. Calcul du Score de Dispersion (Courbe de décroissance en cloche)
    "score_disp",
    F.lit(1.0) / (F.lit(1.0) + F.pow(F.col("dispersion_meters") / TOLERANCE_DISP, 2))
).withColumn(
    # 4. Score Final Combiné (0 à 1)
    "confidence_score",
    F.col("score_size") * F.col("score_disp")
).withColumn(
    # BONUS : Si c'est un "Tier 2" (Delta < 60s), on booste le score minimum
    # On s'assure qu'une synchro < 60s a au moins 0.8 de score, sauf si la dispersion est catastrophique
    "confidence_score",
    F.when(
        (F.col("best_tier_seen") == 2) & (F.col("confidence_score") < 0.8),
        # On remonte le score à 0.8, mais on le laisse baisser si la dispersion est horrible (>50m)
        F.least(F.lit(0.95), F.greatest(F.col("confidence_score"), F.lit(0.8) * F.col("score_disp")))
    ).otherwise(F.col("confidence_score"))
)

# Nettoyage final pour l'utilisateur
df_result = df_scored_final.select(
    "camera_id",
    "final_lat",
    "final_lon",
    F.round("confidence_score", 4).alias("score"), # Score arrondi
    "frequency",
    F.round("dispersion_meters", 1).alias("precision_metres")
)
