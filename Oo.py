# =====================================================================
#  PIPELINE LOCALISATION — Bronze -> Gold  (incrémental, sans Elastic)
# =====================================================================
#
#  ARCHITECTURE À DEUX ÉTAGES
#  --------------------------
#  gold_jour        = LA VÉRITÉ, au grain JOUR.
#                     Répond à "où était X le jour D ?".
#                     Incrémental trivial : on réécrit seulement les
#                     partitions de date récentes. Pas de fenêtre longue,
#                     pas de fantôme.
#
#  gold_intervalles = PROJECTION d'affichage (futur index).
#                     Reconstruite depuis gold_jour, seulement pour les
#                     sujets qui ont bougé, en relisant TOUT leur
#                     historique jour (peu de sujets => c'est léger et exact).
#
#  FLUX :
#    Bronze --adapters--> Silver(jour) --clean--> daily(score) --> gold_jour
#                                                                      |
#                                          build_intervals (sujets touchés)
#                                                                      v
#                                                              gold_intervalles
# =====================================================================

from functools import reduce
from pyspark.sql import Window, functions as F
from pyspark.sql.utils import AnalysisException


# =====================================================================
#  PARAMÈTRES
# =====================================================================
#  Toutes les dates sont en 'yyyyMMdd' (format string, ton format).
#
#  BUFFER_JOURS : petit tampon de jours recalculés à chaque run.
#    Sert à deux choses :
#      - donner au filtre anti-spike ses voisins (J-1, J+1),
#      - "auto-guérir" : un jour dont le voisin arrive plus tard voit
#        son statut spike finalisé au run suivant.
#    2 à 3 jours suffisent.
BUFFER_JOURS = 3

GOLD_JOUR        = "gold_jour"
GOLD_INTERVALLES = "gold_intervalles"


# =====================================================================
#  OUTIL : test d'existence de table robuste (contourne tableExists)
# =====================================================================
def _table_existe(spark, table):
    try:
        spark.table(table).schema
        return True
    except AnalysisException:
        return False


# =====================================================================
#  ÉTAPE 1 — BRONZE -> schéma commun (un ADAPTATEUR par logique)
#  --------------------------------------------------------------
#  C'est la SEULE partie spécifique à chaque logique.
#  Chaque adaptateur ramène sa table brute au schéma canonique :
#    subject_id, logic, date, lieu, pays, lat, lon, raw_source_id
#  Ajouter une logique = ajouter un adaptateur + l'inscrire dans ADAPTERS.
# =====================================================================
def adapter_logique_A(spark, date_min, date_max):
    return (spark.table("bronze_logique_A")
        .where((F.col("jour") >= date_min) & (F.col("jour") <= date_max))
        .select(
            F.col("device_id").alias("subject_id"),
            F.lit("logique_A").alias("logic"),
            F.col("jour").alias("date"),
            F.col("ville").alias("lieu"),
            F.col("pays").alias("pays"),
            F.col("latitude").cast("double").alias("lat"),
            F.col("longitude").cast("double").alias("lon"),
            F.col("id").alias("raw_source_id")))


def adapter_logique_B(spark, date_min, date_max):
    return (spark.table("bronze_logique_B")
        .where((F.col("date_obs") >= date_min) & (F.col("date_obs") <= date_max))
        .select(
            F.col("user_ref").alias("subject_id"),
            F.lit("logique_B").alias("logic"),
            F.col("date_obs").alias("date"),
            F.col("city").alias("lieu"),
            F.col("country").alias("pays"),
            F.lit(None).cast("double").alias("lat"),   # cette logique n'a pas de coords
            F.lit(None).cast("double").alias("lon"),
            F.col("row_id").alias("raw_source_id")))


ADAPTERS = [adapter_logique_A, adapter_logique_B]


def build_silver_raw(spark, date_min, date_max):
    """Union de toutes les logiques ramenées au schéma commun, sur [date_min, date_max]."""
    dfs = [a(spark, date_min, date_max) for a in ADAPTERS]
    return reduce(lambda x, y: x.unionByName(y), dfs)


# =====================================================================
#  ÉTAPE 2 — MAPPING : person_id (nullable) + consensus_key (jamais null)
#  --------------------------------------------------------------
#  Le mapping a un `rank` (pas de temporalité). On garde le MEILLEUR
#  rang par subject_id => un seul person_id, pas de fan-out.
#  Jointure NORMALE (pas de broadcast).
# =====================================================================
def add_consensus_key(spark, df):
    w = Window.partitionBy("subject_id").orderBy(F.col("rank").asc())  # rank=1 = meilleur
    mapping_best = (spark.table("subject_person_map")
        .withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .select("subject_id", "person_id"))

    return (df.join(mapping_best, "subject_id", "left")   # left => on garde les non résolus
              .withColumn("consensus_key", F.coalesce("person_id", "subject_id")))


# =====================================================================
#  ÉTAPE 3 — NETTOYAGE : flaguer les sauts spatio-temporels (spikes)
#  --------------------------------------------------------------
#  Un lieu isolé un seul jour entre deux voisins concordants est suspect
#  (ex. Paris, Paris, [Tokyo], Paris, Paris).
#  On FLAGUE (quality), on ne supprime pas -> auditabilité.
# =====================================================================
def clean_spikes(df):
    w = Window.partitionBy("consensus_key", "logic").orderBy("d")
    return (df
        .withColumn("d", F.to_date("date", "yyyyMMdd"))     # date typée pour l'ordre
        .withColumn("prev_lieu", F.lag("lieu").over(w))
        .withColumn("next_lieu", F.lead("lieu").over(w))
        .withColumn("quality", F.when(
            (F.col("lieu") != F.col("prev_lieu")) &
            (F.col("lieu") != F.col("next_lieu")) &
            (F.col("prev_lieu") == F.col("next_lieu")),
            F.lit("spike")).otherwise(F.lit("ok"))))


# =====================================================================
#  ÉTAPE 4 — SCORING au grain JOUR (par-logique + consensus)
#  --------------------------------------------------------------
#  1) DÉDUP : plusieurs entrées (consensus_key, jour, lieu, logic) sont
#     possibles -> on collapse à UNE ligne par logique-jour-lieu, sinon
#     le noisy-OR compterait une logique plusieurs fois.
#  2) per-logic : score = fiabilité de la logique.
#  3) consensus : noisy-OR sur les logiques DISTINCTES d'accord ce jour
#     -> plus il y a convergence, plus le score monte.
#  On conserve `raw` : les entrées brutes qui composent chaque ligne.
# =====================================================================
def build_daily(spark, silver_clean):
    reg = spark.table("logic_registry").select("logic", "fiabilite")

    raw_struct = F.struct(
        "logic", "subject_id", "date", "lieu", "pays", "lat", "lon", "raw_source_id")

    # 1) dédup logique-jour-lieu, on collecte les bruts de cette logique
    logic_day = (silver_clean.where(F.col("quality") == "ok")
        .withColumn("raw", raw_struct)
        .join(reg, "logic")
        .groupBy("consensus_key", "d", "lieu", "pays", "logic")
        .agg(F.first("fiabilite").alias("w"),
             F.first("person_id", ignorenulls=True).alias("person_id"),
             F.collect_list("raw").alias("raw")))

    # 2) couche PAR-LOGIQUE
    per_logic = logic_day.select(
        "consensus_key", "d", "lieu", "pays", "person_id", "logic",
        F.col("w").alias("score"),
        F.array("logic").alias("contributing_logics"),
        F.lit(1).alias("n_logics"),
        "raw")

    # 3) couche CONSENSUS : noisy-OR = 1 - PRODUIT(1 - w)
    consensus = (logic_day
        .groupBy("consensus_key", "d", "lieu", "pays")
        .agg((F.lit(1.0) - F.exp(F.sum(F.log(F.lit(1.0) - F.col("w"))))).alias("score"),
             F.collect_set("logic").alias("contributing_logics"),
             F.countDistinct("logic").alias("n_logics"),
             F.first("person_id", ignorenulls=True).alias("person_id"),
             F.flatten(F.collect_list("raw")).alias("raw"))
        .withColumn("logic", F.lit("consensus")))

    # même schéma pour les deux couches, distinguées par `logic`
    return per_logic.unionByName(consensus)


# =====================================================================
#  ÉTAPE 5 — ÉCRITURE gold_jour  (VÉRITÉ, incrémental par partition date)
#  --------------------------------------------------------------
#  On réécrit UNIQUEMENT les partitions de date recalculées ce run
#  (le tampon [J-BUFFER, J]). Dynamic overwrite => les autres jours
#  restent intacts.
# =====================================================================
def write_gold_jour(spark, daily):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    out = (daily
        .withColumn("date", F.date_format("d", "yyyyMMdd"))
        .select("consensus_key", "person_id", "date", "lieu", "pays",
                "logic", "score", "contributing_logics", "n_logics", "raw"))

    if not _table_existe(spark, GOLD_JOUR):
        (out.write.format("parquet").mode("overwrite")
            .partitionBy("date").saveAsTable(GOLD_JOUR))
    else:
        cols = spark.table(GOLD_JOUR).columns          # aligner par nom (partition en dernier)
        out.select(*cols).write.mode("overwrite").insertInto(GOLD_JOUR)


# =====================================================================
#  ÉTAPE 6 — INTERVALLES : gaps & islands depuis gold_jour
#  --------------------------------------------------------------
#  On regroupe les jours CONSÉCUTIFS de même (consensus_key, logic, lieu).
#  Un trou (>1 jour) démarre un nouvel intervalle.
#  `daily_scope` = gold_jour restreint aux sujets touchés (historique complet).
# =====================================================================
def build_intervals(daily_scope):
    d = daily_scope.withColumn("d", F.to_date("date", "yyyyMMdd"))

    w = Window.partitionBy("consensus_key", "logic", "lieu").orderBy("d")
    islands = (d
        .withColumn("prev", F.lag("d").over(w))
        .withColumn("gap", F.when(
            F.col("prev").isNull() | (F.datediff("d", "prev") > 1), 1).otherwise(0))
        .withColumn("island_id", F.sum("gap").over(w)))

    intervals = (islands
        .groupBy("consensus_key", "logic", "lieu", "island_id")
        .agg(F.min("d").alias("gte_d"), F.max("d").alias("lte_d"),
             F.first("pays").alias("pays"),
             F.first("person_id", ignorenulls=True).alias("person_id"),
             F.avg("score").alias("score"),          # score moyen sur la période
             F.max("n_logics").alias("n_logics"),
             F.array_distinct(F.flatten(F.collect_list("contributing_logics")))
                 .alias("contributing_logics"),
             F.flatten(F.collect_list("raw")).alias("raw_data")))

    return (intervals
        .withColumn("periode", F.struct(
            F.date_format("gte_d", "yyyyMMdd").alias("gte"),
            F.date_format("lte_d", "yyyyMMdd").alias("lte")))
        .withColumn("doc_id", F.concat_ws("_",
            "logic", "consensus_key", "lieu", F.date_format("gte_d", "yyyyMMdd")))
        .select("doc_id", "consensus_key", "person_id", "logic", "lieu", "pays",
                "periode", "score", "contributing_logics", "n_logics", "raw_data"))


# =====================================================================
#  ÉTAPE 7 — ÉCRITURE gold_intervalles  (remplacement des sujets touchés)
#  --------------------------------------------------------------
#  On remplace en bloc TOUS les intervalles des sujets recalculés :
#  ancien - (sujets touchés) + nouveaux intervalles.
#  Le "supprime tout pour ce sujet puis réinsère" élimine les fantômes
#  (fusion / redécoupage d'intervalles) par construction.
# =====================================================================
def write_gold_intervalles(spark, new_intervals, cles_touchees):
    if not _table_existe(spark, GOLD_INTERVALLES):
        new_intervals.write.format("parquet").mode("overwrite").saveAsTable(GOLD_INTERVALLES)
        return

    ancien = spark.table(GOLD_INTERVALLES)
    # on retire les sujets touchés de l'ancien (anti-join), puis on réunit avec le neuf
    a_garder = ancien.join(cles_touchees, "consensus_key", "left_anti")
    nouveau  = a_garder.unionByName(new_intervals)

    nouveau.write.format("parquet").mode("overwrite").saveAsTable(GOLD_INTERVALLES)


# =====================================================================
#  ORCHESTRATEUR — un run quotidien complet
# =====================================================================
def run_quotidien(spark, J, buffer_jours=BUFFER_JOURS):
    """
    J : jour à traiter, 'yyyyMMdd'.
    Traite le tampon [J - buffer_jours, J] pour gold_jour (auto-guérison
    des spikes + données légèrement en retard), puis reprojette en
    intervalles les sujets touchés.
    Pour un backfill d'un vieux jour, appeler avec ce J : le tampon
    autour de lui sera recalculé.
    """
    from datetime import datetime, timedelta
    dJ = datetime.strptime(J, "%Y%m%d")
    proc_start = (dJ - timedelta(days=buffer_jours)).strftime("%Y%m%d")
    proc_end   = J

    # ---- ÉTAGE 1 : VÉRITÉ AU JOUR -----------------------------------
    silver_raw   = build_silver_raw(spark, proc_start, proc_end)   # Bronze -> commun
    silver_keyed = add_consensus_key(spark, silver_raw)            # + person_id / consensus_key
    silver_clean = clean_spikes(silver_keyed)                      # flag spikes
    daily        = build_daily(spark, silver_clean)                # scoring jour
    write_gold_jour(spark, daily)                                  # -> gold_jour (partitions récentes)

    # ---- ÉTAGE 2 : PROJECTION EN INTERVALLES ------------------------
    # sujets qui ont de la donnée sur le tampon traité
    cles_touchees = daily.select("consensus_key").distinct()

    # historique COMPLET de ces sujets depuis la vérité au jour
    daily_scope = (spark.table(GOLD_JOUR)
        .join(cles_touchees, "consensus_key", "inner"))

    new_intervals = build_intervals(daily_scope)
    write_gold_intervalles(spark, new_intervals, cles_touchees)

    return new_intervals


# ---------------------------------------------------------------------
# Exemple d'appel :
#   run_quotidien(spark, "20240320")
# ---------------------------------------------------------------------
