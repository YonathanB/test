

# =====================================================================
#  PIPELINE FINAL v2 — logiques pilotées par la CONFIG JSON (Oracle CLOB)
#  Changements vs v1 :
#   - adaptateur GÉNÉRIQUE construit depuis la config (plus de code par logique)
#   - fiabilité lue dans la config (défaut 1.0, clampé pour le noisy-OR)
#   - colonnes supplémentaires -> champ `extras` (JSON string) porté jusqu'au raw
#  Le reste (relink, purge, gold_personnes, compaction) est inchangé.
# =====================================================================
import json
from datetime import datetime, timedelta
from pyspark.sql import Window, functions as F
from pyspark.sql.utils import AnalysisException

# ---------------------------------------------------------------------
#  PARAMÈTRES
# ---------------------------------------------------------------------
TTL_MOIS       = 6
BUFFER_JOURS   = 3
N_BUCKETS      = 1024      # 8192 était surdimensionné : autant de fichiers à committer
AVEC_PAR_CLE   = False     # table d'accès par identifiant : désactivée (cf. ingest)
CLAMP_FIAB     = 0.999999          # évite log(0) quand fiabilite = 1.0

GOLD_JOUR      = "gold_jour"
GOLD_PAR_CLE   = "gold_jour_par_cle"
GOLD_PERSONNES = "gold_personnes"
T_LAST_SEEN    = "subject_last_seen"
T_DEJA_RELIES  = "subjects_deja_relies"
T_TOMBSTONES   = "cles_tombstones"
T_MAPPING      = "subject_person_map"      # subject_id, person_id (déjà rank=1)

FMT = "%Y%m%d"

def _table_existe(spark, t):
    try:
        spark.table(t).schema
        return True
    except AnalysisException:
        return False

def _jours_avant(J, n):
    return (datetime.strptime(J, FMT) - timedelta(days=n)).strftime(FMT)

def _mois_avant(J, n):
    d = datetime.strptime(J, FMT)
    m = d.month - n
    y = d.year + (m - 1) // 12
    m = (m - 1) % 12 + 1
    return datetime(y, m, min(d.day, 28)).strftime(FMT)

def _ecrire_partitions(spark, df, table, partition_col):
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if not _table_existe(spark, table):
        (df.write.format("parquet").mode("overwrite")
           .partitionBy(partition_col).saveAsTable(table))
    else:
        cols = spark.table(table).columns
        df.select(*cols).write.mode("overwrite").insertInto(table)


# =====================================================================
#  CONFIG DES LOGIQUES — lue depuis Oracle (JSON en CLOB)
# ---------------------------------------------------------------------
#  Format attendu du JSON (une entrée par logique) :
#  {
#    "logic_id":  "logique_A",
#    "libelle":   "GPS mobile",
#    "fiabilite": 0.9,                          # absent -> 1.0
#    "table_source": "bronze_logique_A",
#    "mapping": {                               # canonique -> colonne source
#      "subject_id": "device_id",
#      "date":       "jour",                    # yyyyMMdd
#      "lieu":       "ville",
#      "pays":       "pays",
#      "lat":        "latitude",                # optionnel
#      "lon":        "longitude",               # optionnel
#      "raw_source_id": "id"
#    },
#    "colonnes_supplementaires": [              # optionnel
#      { "colonne_source": "operateur", "cle": "operateur", "label": "Opérateur" },
#      { "colonne_source": "signal",    "cle": "signal",    "label": "Signal" }
#    ]
#  }
#  Les LABELS ne voyagent pas dans la donnée : le front les lit dans la
#  config (servie par l'API C#) et les applique aux clés de `extras`.
# =====================================================================
def charger_configs(spark, jdbc_url, jdbc_props,
                    table_cfg="LOGIC_CONFIG", col_clob="CONFIG_JSON"):
    """Lit la table Oracle des configs (CLOB JSON) et retourne list[dict].
       Nécessite le driver ojdbc dans le classpath Spark.
       Alternative si JDBC indisponible : l'API C# dépose le JSON sur HDFS
       et on le lit avec spark.read.text / open()."""
    rows = (spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_cfg)
        .options(**jdbc_props)
        .load()
        .select(F.col(col_clob).cast("string").alias("j"))
        .collect())
    configs = [json.loads(r["j"]) for r in rows]
    for c in configs:
        c["fiabilite"] = float(c.get("fiabilite") or 1.0)   # défaut = 1.0
        c.setdefault("colonnes_supplementaires", [])
    return configs

def registre_df(spark, configs):
    """Petit DF (logic, fiabilite) dérivé de la config — remplace logic_registry."""
    return spark.createDataFrame(
        [(c["logic_id"], c["fiabilite"]) for c in configs],
        "logic string, fiabilite double")


# =====================================================================
#  ADAPTATEUR GÉNÉRIQUE — construit le schéma canonique depuis la config
# =====================================================================
CANONIQUES_REQUIS = ["subject_id", "date", "lieu", "pays", "raw_source_id"]

def adapter_generique(spark, cfg, dmin, dmax):
    m = cfg["mapping"]
    manquants = [c for c in CANONIQUES_REQUIS if c not in m]
    if manquants:
        raise ValueError(f"[{cfg['logic_id']}] mapping incomplet, manquent: {manquants}")

    df = (spark.table(cfg["table_source"])
        .where((F.col(m["date"]) >= dmin) & (F.col(m["date"]) <= dmax)))

    cols = [
        F.col(m["subject_id"]).cast("string").alias("subject_id"),
        F.lit(cfg["logic_id"]).alias("logic"),
        F.col(m["date"]).cast("string").alias("date"),
        F.col(m["lieu"]).cast("string").alias("lieu"),
        F.col(m["pays"]).cast("string").alias("pays"),
        (F.col(m["lat"]).cast("double") if "lat" in m
            else F.lit(None).cast("double")).alias("lat"),
        (F.col(m["lon"]).cast("double") if "lon" in m
            else F.lit(None).cast("double")).alias("lon"),
        F.col(m["raw_source_id"]).cast("string").alias("raw_source_id"),
    ]

    # colonnes supplémentaires -> extras (JSON string, schéma stable et évolutif)
    supp = cfg["colonnes_supplementaires"]
    if supp:
        extras = F.to_json(F.struct(*[
            F.col(s["colonne_source"]).cast("string").alias(s["cle"]) for s in supp]))
    else:
        extras = F.lit(None).cast("string")
    cols.append(extras.alias("extras"))

    return df.select(*cols)

def build_silver_raw(spark, configs, dmin, dmax):
    from functools import reduce
    dfs = [adapter_generique(spark, c, dmin, dmax) for c in configs]
    return reduce(lambda x, y: x.unionByName(y), dfs)


# =====================================================================
#  MAPPING IDENTITÉ
#  Table volumineuse (~600M après filtre priority=1), taux de match faible.
# =====================================================================
def mapping_df(spark):
    return (spark.table(T_MAPPING)
        .where(F.col("priority") == 1)                       # EN PREMIER : réduit le volume
        .select("subject_id", "person_id",
                F.col("rank").alias("mapping_score"))        # note du lien, gardée comme attribut
        .dropDuplicates(["subject_id"]))

def add_consensus_key(spark, df):
    # Join gros x gros (~600M matchables, taux de match faible) :
    # 1) semi-join pour isoler les subjects du flux réellement présents dans le mapping,
    # 2) le résultat (petit, ~300k) est broadcasté sur le gros flux -> pas de shuffle massif.
    # rank conservé comme attribut `mapping_score` (n'influence PAS le score consensus).
    subj = df.select("subject_id").distinct()
    resolus = (mapping_df(spark)
        .join(subj, "subject_id", "left_semi"))              # seulement les ~300k matchés
    return (df.join(F.broadcast(resolus), "subject_id", "left")
              .withColumn("consensus_key", F.coalesce("person_id", "subject_id")))


# =====================================================================
#  NETTOYAGE : flag des spikes
# =====================================================================
def clean_spikes(subject_day):
    """Détection de spikes AU NIVEAU PERSONNE (consensus_key), toutes logiques
    et tous subjects confondus.

    3 temps, pour garder la window légère ET l'ordre non ambigu :
      1) réduire à UN lieu dominant par (consensus_key, jour)
      2) window lag/lead sur ce grain : 1 ligne par jour, ordre par date sans ambiguïté
      3) re-marquer les lignes d'origine par (consensus_key, jour)

    Effet de bord voulu : une logique qui pose souvent un lieu contredit par
    l'ensemble des autres ressort via les jours flagués -> permet d'identifier
    les logiques peu fiables (cf. diagnostic_logiques_suspectes).
    """
    # 1. lieu DOMINANT du jour au niveau personne (majorité des lignes ; départage stable)
    compte = (subject_day.groupBy("consensus_key", "d", "lieu")
        .agg(F.count(F.lit(1)).alias("n")))
    wmaj = Window.partitionBy("consensus_key", "d").orderBy(F.desc("n"), F.asc("lieu"))
    jour_cle = (compte
        .withColumn("rn", F.row_number().over(wmaj))
        .where(F.col("rn") == 1)
        .select("consensus_key", "d", "lieu"))

    # 2. spike = lieu isolé un jour, encadré par deux voisins concordants
    w = Window.partitionBy("consensus_key").orderBy("d")
    spikes = (jour_cle
        .withColumn("prev_lieu", F.lag("lieu").over(w))
        .withColumn("next_lieu", F.lead("lieu").over(w))
        .where((F.col("lieu") != F.col("prev_lieu")) &
               (F.col("lieu") != F.col("next_lieu")) &
               (F.col("prev_lieu") == F.col("next_lieu")))
        .select("consensus_key", "d")
        .withColumn("est_spike", F.lit(True)))

    # 3. re-marquage des lignes d'origine (jour entier de la personne flagué)
    return (subject_day
        .join(spikes, ["consensus_key", "d"], "left")
        .withColumn("quality", F.when(F.col("est_spike"), F.lit("spike"))
                                .otherwise(F.lit("ok")))
        .drop("est_spike"))


def diagnostic_logiques_suspectes(subject_day, clean):
    """Quelles logiques contredisent le plus le lieu dominant de la personne ?
    Sert à repérer les logiques peu fiables (à ajuster dans la config Oracle)."""
    dominant = (clean.where(F.col("quality") == "ok")
        .groupBy("consensus_key", "d")
        .agg(F.first("lieu").alias("lieu_dominant")))
    return (subject_day.join(dominant, ["consensus_key", "d"], "inner")
        .withColumn("contredit", F.col("lieu") != F.col("lieu_dominant"))
        .groupBy("logic")
        .agg(F.count(F.lit(1)).alias("n_jours"),
             F.sum(F.col("contredit").cast("int")).alias("n_contradictions"))
        .withColumn("taux_contradiction",
                    F.round(F.col("n_contradictions") / F.col("n_jours"), 4))
        .orderBy(F.desc("taux_contradiction")))


# =====================================================================
#  SCORING jour : dédup -> per-logic + consensus (noisy-OR clampé)
#  raw contient désormais `extras`.
# =====================================================================
def _w_eff():
    return F.least(F.col("w"), F.lit(CLAMP_FIAB))   # fiabilite=1 ne casse plus log()

def _consensus_depuis_logic_day(logic_day):
    return (logic_day
        .groupBy("consensus_key", "d", "lieu", "pays")
        .agg((F.lit(1.0) - F.exp(F.sum(F.log(F.lit(1.0) - _w_eff())))).alias("score"),
             F.collect_set("logic").alias("contributing_logics"),
             F.array_distinct(F.flatten(F.collect_list("subjects")))
                 .alias("contributing_subjects"),
             F.countDistinct("logic").alias("n_logics"),
             F.first("person_id", ignorenulls=True).alias("person_id"),
             F.flatten(F.collect_list("raw")).alias("raw"))
        .withColumn("logic", F.lit("consensus"))
        .withColumn("subject_id", F.lit(None).cast("string"))     # consensus = niveau personne
        # mapping_score = note du lien subject->person : n'a pas de sens au niveau
        # personne (plusieurs subjects possibles) -> null sur les lignes consensus.
        .withColumn("mapping_score", F.lit(None).cast("double")))

def build_subject_day(silver_keyed, reg):
    """RÉDUCTION en amont du nettoyage : une ligne par
       (clé, subject, jour, lieu, logique), avec le raw collecté et la fiabilité.
       La window de clean_spikes tournera sur CE grain réduit (léger)."""
    raw_struct = F.struct("logic", "subject_id", "date", "lieu", "pays",
                          "lat", "lon", "raw_source_id", "extras", "mapping_score")
    return (silver_keyed
        .withColumn("d", F.to_date("date", "yyyyMMdd"))
        .withColumn("raw", raw_struct)
        .join(reg, "logic")
        .groupBy("consensus_key", "subject_id", "d", "lieu", "pays", "logic")
        .agg(F.first("fiabilite").alias("w"),
             F.first("person_id", ignorenulls=True).alias("person_id"),
             F.first("mapping_score", ignorenulls=True).alias("mapping_score"),
             F.collect_list("raw").alias("raw")))

def build_daily(subject_day_clean):
    """Consomme le grain réduit ET nettoyé (quality == ok déjà appliqué en amont).
       Produit les lignes per-logic + les lignes consensus."""
    per_logic = subject_day_clean.select(
        "consensus_key", "subject_id", "d", "lieu", "pays", "person_id", "mapping_score", "logic",
        F.col("w").alias("score"),
        F.array("logic").alias("contributing_logics"),
        F.array("subject_id").alias("contributing_subjects"),
        F.lit(1).alias("n_logics"), "raw")

    # grain LOGIQUE (subjects fusionnés) pour le consensus : une logique comptée une fois
    logic_day = (subject_day_clean
        .groupBy("consensus_key", "d", "lieu", "pays", "logic")
        .agg(F.first("w").alias("w"),
             F.first("person_id", ignorenulls=True).alias("person_id"),
             F.collect_set("subject_id").alias("subjects"),
             F.flatten(F.collect_list("raw")).alias("raw")))

    return per_logic.unionByName(_consensus_depuis_logic_day(logic_day))

def _forme_gold_jour(daily):
    return (daily
        .withColumn("date", F.date_format("d", "yyyyMMdd"))
        .select("consensus_key", "person_id", "subject_id", "lieu", "pays", "logic",
                "score", "mapping_score", "contributing_logics", "contributing_subjects",
                "n_logics", "raw", "date"))


# =====================================================================
#  TEMPS 1 — RE-LIAISON RÉTROACTIVE (inchangée, reg passé en paramètre)
# =====================================================================
def relink(spark, J, reg):
    if not (_table_existe(spark, T_LAST_SEEN) and _table_existe(spark, GOLD_JOUR)):
        return
    deja = (spark.table(T_DEJA_RELIES) if _table_existe(spark, T_DEJA_RELIES)
            else spark.createDataFrame([], "subject_id string"))

    nouveaux = (mapping_df(spark)
        .join(deja, "subject_id", "left_anti")
        .join(spark.table(T_LAST_SEEN), "subject_id", "inner"))
    if nouveaux.rdd.isEmpty():
        return

    b = nouveaux.agg(F.min("premiere_activite").alias("dmin"),
                     F.max("derniere_activite").alias("dmax")).first()
    cles_subject = nouveaux.select(F.col("subject_id").alias("consensus_key"))
    remap = nouveaux.select(F.col("subject_id").alias("consensus_key"),
                            F.col("person_id").alias("new_key"))

    part = (spark.table(GOLD_JOUR)
        .where((F.col("date") >= b["dmin"]) & (F.col("date") <= b["dmax"])))

    migrees = (part.join(remap, "consensus_key", "inner")
        .where(F.col("logic") != "consensus")
        .withColumn("consensus_key", F.col("new_key"))
        .withColumn("person_id", F.col("new_key"))
        .drop("new_key"))

    touches = migrees.select("consensus_key", "date").distinct()

    reste = (part
        .join(cles_subject, "consensus_key", "left_anti")
        .join(touches.withColumn("logic", F.lit("consensus")),
              ["consensus_key", "date", "logic"], "left_anti"))

    base = (reste.unionByName(migrees)
        .join(touches, ["consensus_key", "date"], "left_semi")
        .where(F.col("logic") != "consensus")
        .withColumn("d", F.to_date("date", "yyyyMMdd")))
    logic_day = (base
        .join(reg, "logic")
        .withColumn("raw_one", F.explode("raw"))
        .groupBy("consensus_key", "d", "lieu", "pays", "logic")
        .agg(F.first("fiabilite").alias("w"),
             F.first("person_id", ignorenulls=True).alias("person_id"),
             F.collect_set("subject_id").alias("subjects"),
             F.collect_list("raw_one").alias("raw")))
    consensus_neuf = _forme_gold_jour(_consensus_depuis_logic_day(logic_day))

    _ecrire_partitions(spark, reste.unionByName(migrees).unionByName(consensus_neuf),
                       GOLD_JOUR, "date")

    ts = cles_subject.withColumn("motif", F.lit("relink"))
    (ts.write.mode("append").format("parquet").saveAsTable(T_TOMBSTONES)
     if _table_existe(spark, T_TOMBSTONES)
     else ts.write.format("parquet").saveAsTable(T_TOMBSTONES))

    (nouveaux.select("subject_id").write.mode("append").format("parquet")
        .saveAsTable(T_DEJA_RELIES)
     if _table_existe(spark, T_DEJA_RELIES)
     else nouveaux.select("subject_id").write.format("parquet").saveAsTable(T_DEJA_RELIES))


# =====================================================================
#  TEMPS 2 — INGESTION du tampon [J-BUFFER, J]
# =====================================================================
def ingest(spark, J, configs, reg):
    dmin = _jours_avant(J, BUFFER_JOURS)

    silver_keyed = add_consensus_key(spark, build_silver_raw(spark, configs, dmin, J))

    # 1. RÉDUIRE  2. NETTOYER (window légère)  3. écarter les spikes  4. build_daily
    subj_day = build_subject_day(silver_keyed, reg)
    subj_ok  = clean_spikes(subj_day).where(F.col("quality") == "ok")
    sortie   = _forme_gold_jour(build_daily(subj_ok))

    _ecrire_partitions(spark, sortie, GOLD_JOUR, "date")

    # Table d'accès par identifiant : MÊME donnée que gold_jour, seul le
    # partitionnement change (bucket au lieu de date). Désactivée par défaut :
    # coûte une 2e écriture complète/jour, dont un commit driver très long
    # (partitionBy sur N_BUCKETS -> multitude de petits fichiers).
    # À réactiver quand la recherche "identifiant sans plage de dates" sera un vrai besoin.
    if AVEC_PAR_CLE:
        par_cle = sortie.withColumn("bucket",
            F.pmod(F.hash("consensus_key"), F.lit(N_BUCKETS)))
        # repartition("bucket") : 1 tâche par bucket -> N_BUCKETS fichiers, pas 100k.
        par_cle = par_cle.repartition("bucket").sortWithinPartitions("consensus_key", "date")
        if not _table_existe(spark, GOLD_PAR_CLE):
            par_cle.write.format("parquet").partitionBy("bucket").saveAsTable(GOLD_PAR_CLE)
        else:
            par_cle.write.mode("append").format("parquet").insertInto(GOLD_PAR_CLE)

    # last_seen : calculé sur silver_keyed (date en yyyyMMdd, subjects non résolus)
    vus = (silver_keyed.where(F.col("person_id").isNull())
        .groupBy("subject_id")
        .agg(F.min("date").alias("premiere_activite"),
             F.max("date").alias("derniere_activite")))
    if not _table_existe(spark, T_LAST_SEEN):
        vus.write.format("parquet").saveAsTable(T_LAST_SEEN)
    else:
        fusion = (spark.table(T_LAST_SEEN).unionByName(vus)
            .groupBy("subject_id")
            .agg(F.min("premiere_activite").alias("premiere_activite"),
                 F.max("derniere_activite").alias("derniere_activite")))
        fusion.write.mode("overwrite").format("parquet").saveAsTable(T_LAST_SEEN + "_tmp")
        spark.sql(f"DROP TABLE {T_LAST_SEEN}")
        spark.sql(f"ALTER TABLE {T_LAST_SEEN}_tmp RENAME TO {T_LAST_SEEN}")


# =====================================================================
#  TEMPS 3 — PURGE (silence == TTL et non relié)
# =====================================================================
def purge(spark, J):
    if not _table_existe(spark, T_LAST_SEEN):
        return
    seuil = _mois_avant(J, TTL_MOIS)
    candidats = (spark.table(T_LAST_SEEN)
        .where(F.col("derniere_activite") == seuil)
        .join(mapping_df(spark), "subject_id", "left_anti"))
    if candidats.rdd.isEmpty():
        return

    b = candidats.agg(F.min("premiere_activite").alias("dmin"),
                      F.max("derniere_activite").alias("dmax")).first()
    cles = candidats.select(F.col("subject_id").alias("consensus_key"))

    part = (spark.table(GOLD_JOUR)
        .where((F.col("date") >= b["dmin"]) & (F.col("date") <= b["dmax"])))
    _ecrire_partitions(spark, part.join(cles, "consensus_key", "left_anti"),
                       GOLD_JOUR, "date")

    cles.withColumn("motif", F.lit("purge")) \
        .write.mode("append").format("parquet").saveAsTable(T_TOMBSTONES)
    reste = spark.table(T_LAST_SEEN).join(
        candidats.select("subject_id"), "subject_id", "left_anti")
    reste.write.mode("overwrite").format("parquet").saveAsTable(T_LAST_SEEN + "_tmp")
    spark.sql(f"DROP TABLE {T_LAST_SEEN}")
    spark.sql(f"ALTER TABLE {T_LAST_SEEN}_tmp RENAME TO {T_LAST_SEEN}")


# =====================================================================
#  TEMPS 4 — gold_personnes (recalcul complet, inchangé)
# =====================================================================
def _intervalles(jour):
    d = jour.withColumn("d", F.to_date("date", "yyyyMMdd"))
    w = Window.partitionBy("consensus_key", "logic", "subject_id", "lieu").orderBy("d")
    islands = (d
        .withColumn("prev", F.lag("d").over(w))
        .withColumn("gap", F.when(
            F.col("prev").isNull() | (F.datediff("d", "prev") > 1), 1).otherwise(0))
        .withColumn("island_id", F.sum("gap").over(w)))
    return (islands
        .groupBy("consensus_key", "logic", "subject_id", "lieu", "island_id")
        .agg(F.min("d").alias("gte_d"), F.max("d").alias("lte_d"),
             F.first("pays").alias("pays"),
             F.avg("score").alias("score"),
             F.max("n_logics").alias("n_logics"),
             F.array_distinct(F.flatten(F.collect_list("contributing_logics")))
                 .alias("contributing_logics"),
             F.array_distinct(F.flatten(F.collect_list("contributing_subjects")))
                 .alias("contributing_subjects"))
        .withColumn("nb_jours", F.datediff("lte_d", "gte_d") + 1)
        .withColumn("periode", F.struct(
            F.date_format("gte_d", "yyyyMMdd").alias("gte"),
            F.date_format("lte_d", "yyyyMMdd").alias("lte"))))

def build_gold_personnes(spark):
    jour = spark.table(GOLD_JOUR).where(F.col("person_id").isNotNull())

    itv = _intervalles(jour).withColumn("sejour", F.struct(
        "lieu", "pays", "periode", "nb_jours", "score",
        "contributing_logics", "contributing_subjects", "n_logics"))

    voyages = (itv.where(F.col("logic") == "consensus")
        .groupBy("consensus_key").agg(F.collect_list("sejour").alias("voyages")))

    # détail au grain (logique, subject) : un même person_id peut montrer
    # des séjours différents sur une même période selon le subject
    detail_sejours = (itv.where(F.col("logic") != "consensus")
        .groupBy("consensus_key", "logic", "subject_id")
        .agg(F.collect_list(F.struct("lieu", "pays", "periode", "nb_jours")).alias("sejours")))

    counts = (jour.where(F.col("logic") != "consensus")
        .groupBy("consensus_key", "logic", "subject_id", "date")
        .agg(F.sum(F.size("raw")).alias("nb_events")))
    detail_counts = (counts.groupBy("consensus_key", "logic", "subject_id")
        .agg(F.collect_list(F.struct("date", "nb_events")).alias("evenements_par_jour"),
             F.sum("nb_events").alias("nb_events")))
    detail = (detail_sejours.join(detail_counts,
            ["consensus_key", "logic", "subject_id"], "full")
        .withColumn("bloc", F.struct("logic", "subject_id",
            "sejours", "evenements_par_jour", "nb_events"))
        .groupBy("consensus_key").agg(F.collect_list("bloc").alias("detail_logiques")))

    evt = (counts.groupBy("consensus_key", "logic").agg(F.sum("nb_events").alias("n"))
        .groupBy("consensus_key")
        .agg(F.map_from_entries(F.collect_list(F.struct("logic", "n")))
                 .alias("evenements_par_logique")))

    resume = (jour.where(F.col("logic") == "consensus")
        .groupBy("consensus_key")
        .agg(F.first("person_id", ignorenulls=True).alias("person_id"),
             F.min("date").alias("premier_jour"),
             F.max("date").alias("dernier_jour"),
             F.countDistinct("date").alias("nb_jours_actifs"),
             F.array_distinct(F.collect_list("pays")).alias("pays_visites"))
        .join(evt, "consensus_key", "left")
        .withColumn("resume", F.struct("premier_jour", "dernier_jour",
            "nb_jours_actifs", "pays_visites", "evenements_par_logique"))
        .select("consensus_key", "person_id", "resume"))

    (resume.join(voyages, "consensus_key", "left")
           .join(detail,  "consensus_key", "left")
           .write.format("parquet").mode("overwrite").saveAsTable(GOLD_PERSONNES))


# =====================================================================
#  COMPACTION périodique (inchangée)
# =====================================================================
def compaction_par_cle(spark):
    if not _table_existe(spark, GOLD_PAR_CLE):
        return
    df = spark.table(GOLD_PAR_CLE)
    if _table_existe(spark, T_TOMBSTONES):
        morts = spark.table(T_TOMBSTONES).select("consensus_key").distinct()
        df = df.join(morts, "consensus_key", "left_anti")
    df = df.dropDuplicates(["consensus_key", "subject_id", "date", "lieu", "logic"])
    (df.repartition("bucket").sortWithinPartitions("consensus_key", "date")
       .write.format("parquet").mode("overwrite")
       .partitionBy("bucket").saveAsTable(GOLD_PAR_CLE + "_new"))
    spark.sql(f"DROP TABLE {GOLD_PAR_CLE}")
    spark.sql(f"ALTER TABLE {GOLD_PAR_CLE}_new RENAME TO {GOLD_PAR_CLE}")
    if _table_existe(spark, T_TOMBSTONES):
        spark.sql(f"TRUNCATE TABLE {T_TOMBSTONES}")


# =====================================================================
#  ORCHESTRATEUR QUOTIDIEN
# =====================================================================
def run_quotidien(spark, J, jdbc_url, jdbc_props, avec_relink=True):
    configs = charger_configs(spark, jdbc_url, jdbc_props)   # la config Oracle pilote tout
    reg     = registre_df(spark, configs)

    if avec_relink:                    # 1. sauver d'abord — ÉTAPE OPTIONNELLE
        relink(spark, J, reg)          #    (peut être planifiée moins souvent / à la demande)
    ingest(spark, J, configs, reg)     # 2. le jour (tampon)
    purge(spark, J)                    # 3. faucher en dernier
    # 4. la couche de service est désormais l'index Elastic :
    #    projection_elastic.projeter(spark, J, registre=reg.select("logic","logic_libelle"))
    #    (gold_personnes retirée du run : doublon avec la projection — code conservé
    #     plus bas si un besoin hors-Elastic réapparaît)

# run_quotidien(spark, "20240320",
#     jdbc_url="jdbc:oracle:thin:@//host:1521/service",
#     jdbc_props={"user": "...", "password": "...",
#                 "driver": "oracle.jdbc.OracleDriver"})



# =

def _base(spark, J, consensus):
    debut = F.add_months(F.to_date(F.lit(J), "yyyyMMdd"), -HISTORIQUE_MOIS)
    df = (spark.table(GOLD_JOUR)
        .withColumn("d", F.to_date("date", "yyyyMMdd"))
        .where(F.col("d") >= debut))
    return df.where(F.col("logic") == "consensus") if consensus \
        else df.where(F.col("logic") != "consensus")


def sejours_consensus(spark, J):
    """Séjours au niveau identité : le corps de chaque séjour affiché."""
    return (_ilots(_base(spark, J, True), ["consensus_key"])
        .groupBy("consensus_key", "lieu", "ilot")
        .agg(F.min("d").alias("gte_d"), F.max("d").alias("lte_d"),
             F.first("pays").alias("pays"),
             F.first("person_id", ignorenulls=True).alias("person_id"),
             F.avg("score").alias("score"),
             F.max("n_logics").alias("n_logics"),
             F.array_distinct(F.flatten(F.collect_list("contributing_logics")))
                 .alias("logiques"),
             F.array_distinct(F.flatten(F.collect_list("contributing_subjects")))
                 .alias("subjects"))
        .withColumn("nb_jours", F.datediff("lte_d", "gte_d") + 1)
        .drop("ilot"))


def sejours_contributions(spark, J):
    """Séjours au niveau logique x subject : le détail (niveau 2 de la maquette)."""
    return (_ilots(_base(spark, J, False), ["consensus_key", "logic", "subject_id"])
        .groupBy("consensus_key", "logic", "subject_id", "lieu", "ilot")
        .agg(F.min("d").alias("c_gte_d"), F.max("d").alias("c_lte_d"),
             F.avg("score").alias("c_score"),
             F.first("mapping_score", ignorenulls=True).alias("mapping_score"))
        .drop("ilot"))


# ---------------------------------------------------------------------
#  2. Divergence : deux séjours simultanés sur des lieux différents
# ---------------------------------------------------------------------
def marquer_divergence(sej):
    b = sej.select(
            F.col("consensus_key").alias("k2"),
            F.col("lieu").alias("b_lieu"),
            F.col("gte_d").alias("b_gte"),
            F.col("lte_d").alias("b_lte"))
    chevauche = (sej.join(b, sej["consensus_key"] == b["k2"], "inner")
        .where((F.col("lieu") != F.col("b_lieu")) &
               (F.col("gte_d") <= F.col("b_lte")) &
               (F.col("b_gte") <= F.col("lte_d")))
        .select("consensus_key", "lieu", "gte_d").distinct()
        .withColumn("divergence", F.lit(True)))
    return (sej.join(chevauche, ["consensus_key", "lieu", "gte_d"], "left")
               .withColumn("divergence", F.coalesce("divergence", F.lit(False))))


# ---------------------------------------------------------------------
#  3. Assemblage : 1 ligne = 1 identité
# ---------------------------------------------------------------------
def documents(spark, J, registre=None):
    """registre : DataFrame (logic, logic_libelle) issu de la config Oracle.
       Petit (quelques dizaines de lignes) -> broadcast sûr."""
    sej  = marquer_divergence(sejours_consensus(spark, J))
    ctrb = sejours_contributions(spark, J)

    if registre is not None:
        ctrb = ctrb.join(F.broadcast(registre), "logic", "left")
    else:
        ctrb = ctrb.withColumn("logic_libelle", F.col("logic"))

    # rattacher chaque contribution au séjour consensus qu'elle recoupe
    s = sej.select(
        F.col("consensus_key").alias("s_key"), F.col("lieu").alias("s_lieu"),
        F.col("gte_d").alias("s_gte"), F.col("lte_d").alias("s_lte"))
    contrib_par_sejour = (ctrb.join(s,
            (ctrb["consensus_key"] == s["s_key"]) & (ctrb["lieu"] == s["s_lieu"]) &
            (ctrb["c_gte_d"] <= s["s_lte"]) & (s["s_gte"] <= ctrb["c_lte_d"]), "inner")
        .groupBy("s_key", "s_lieu", "s_gte")
        .agg(F.collect_list(F.struct(



# =====================================================================
#  PROJECTION gold_jour -> index Elastic (1 doc = 1 IDENTITÉ)
#  Séjours en `nested`, contributions en objets simples dans chaque séjour.
#  Sert la maquette v2 : ligne identité -> séjours -> contributions.
#  Le NIVEAU BRUT n'est pas indexé (lu à la demande dans gold_jour).
#  Les NOMS ne sont pas indexés : le front les résout via la table de personnes.
# =====================================================================
from pyspark.sql import functions as F, Window
from pyspark import StorageLevel

GOLD_JOUR   = "gold_jour"
ES_INDEX    = "localisation_identites_v2_{J}"

HISTORIQUE_MOIS       = 24     # profondeur indexée (gold_jour reste la vérité)
MAX_SEJOURS           = 2000   # garde-fou par document
FENETRE_SUBJECT_JOURS = 90     # non rattachés : seuls les actifs récents


# ---------------------------------------------------------------------
#  1. Séjours = îlots de jours consécutifs sur un même lieu
# ---------------------------------------------------------------------
def _ilots(jour, cles):
    w = Window.partitionBy(*cles, "lieu").orderBy("d")
    return (jour
        .withColumn("prev", F.lag("d").over(w))
        .withColumn("gap", F.when(
            F.col("prev").isNull() | (F.datediff("d", "prev") > 1), 1).otherwise(0))
        .withColumn("ilot", F.sum("gap").over(w)))


def _base(spark, J, consensus):
    debut = F.add_months(F.to_date(F.lit(J), "yyyyMMdd"), -HISTORIQUE_MOIS)
    df = (spark.table(GOLD_JOUR)
        .withColumn("d", F.to_date("date", "yyyyMMdd"))
        .where(F.col("d") >= debut))
    return df.where(F.col("logic") == "consensus") if consensus \
        else df.where(F.col("logic") != "consensus")


def cles_perimetre(spark, J, fenetre_jours=FENETRE_SUBJECT_JOURS):
    """Clés à indexer : rattachées, OU actives dans la fenêtre récente.
    Calculé EN AMONT de tout : le reste de la projection ne traite que ces clés,
    au lieu de tout construire puis de jeter les dormantes à la fin."""
    seuil = F.date_sub(F.to_date(F.lit(J), "yyyyMMdd"), fenetre_jours)
    return (_base(spark, J, True)
        .groupBy("consensus_key")
        .agg(F.max(F.col("person_id").isNotNull()).alias("rattache"),
             F.max("d").alias("derniere_d"))
        .where(F.col("rattache") | (F.col("derniere_d") >= seuil))
        .select("consensus_key"))


def sejours_consensus(spark, J):
    """Séjours au niveau identité : le corps de chaque séjour affiché."""
    return (_ilots(_base(spark, J, True), ["consensus_key"])
        .groupBy("consensus_key", "lieu", "ilot")
        .agg(F.min("d").alias("gte_d"), F.max("d").alias("lte_d"),
             F.first("pays").alias("pays"),
             F.first("person_id", ignorenulls=True).alias("person_id"),
             F.avg("score").alias("score"),
             F.max("n_logics").alias("n_logics"),
             F.array_distinct(F.flatten(F.collect_list("contributing_logics")))
                 .alias("logiques"),
             F.array_distinct(F.flatten(F.collect_list("contributing_subjects")))
                 .alias("subjects"))
        .withColumn("nb_jours", F.datediff("lte_d", "gte_d") + 1)
        .drop("ilot"))


def sejours_contributions(spark, J):
    """Séjours au niveau logique x subject : le détail (niveau 2 de la maquette)."""
    return (_ilots(_base(spark, J, False), ["consensus_key", "logic", "subject_id"])
        .groupBy("consensus_key", "logic", "subject_id", "lieu", "ilot")
        .agg(F.min("d").alias("c_gte_d"), F.max("d").alias("c_lte_d"),
             F.avg("score").alias("c_score"),
             F.first("mapping_score", ignorenulls=True).alias("mapping_score"))
        .drop("ilot"))


def sejours_consensus_filtre(spark, J, cles):
    """Comme sejours_consensus, mais restreint aux clés du périmètre AVANT
    les windows/groupBy : le semi-join coûte un shuffle léger et évite de
    calculer des îlots pour des millions d'identités jetées ensuite."""
    base = _base(spark, J, True).join(cles, "consensus_key", "left_semi")
    return (_ilots(base, ["consensus_key"])
        .groupBy("consensus_key", "lieu", "ilot")
        .agg(F.min("d").alias("gte_d"), F.max("d").alias("lte_d"),
             F.first("pays").alias("pays"),
             F.first("person_id", ignorenulls=True).alias("person_id"),
             F.avg("score").alias("score"),
             F.max("n_logics").alias("n_logics"),
             F.array_distinct(F.flatten(F.collect_list("contributing_logics")))
                 .alias("logiques"),
             F.array_distinct(F.flatten(F.collect_list("contributing_subjects")))
                 .alias("subjects"))
        .withColumn("nb_jours", F.datediff("lte_d", "gte_d") + 1)
        .drop("ilot"))


def sejours_contributions_filtre(spark, J, cles):
    base = _base(spark, J, False).join(cles, "consensus_key", "left_semi")
    return (_ilots(base, ["consensus_key", "logic", "subject_id"])
        .groupBy("consensus_key", "logic", "subject_id", "lieu", "ilot")
        .agg(F.min("d").alias("c_gte_d"), F.max("d").alias("c_lte_d"),
             F.avg("score").alias("c_score"),
             F.first("mapping_score", ignorenulls=True).alias("mapping_score"))
        .drop("ilot"))


# ---------------------------------------------------------------------
#  2. Divergence : deux séjours simultanés sur des lieux différents
#     Par WINDOW (balayage), pas par self-join : le self-join sur
#     consensus_key était quadratique et dominait le temps de projection.
#     Deux séjours d'un même lieu ne peuvent pas se chevaucher (îlots),
#     donc chevauchement => lieux différents : pas besoin de tester le lieu.
# ---------------------------------------------------------------------
def marquer_divergence(sej):
    wk = Window.partitionBy("consensus_key").orderBy("gte_d", "lte_d")
    w_prev = wk.rowsBetween(Window.unboundedPreceding, -1)
    w_next = wk.rowsBetween(1, Window.unboundedFollowing)
    return (sej
        .withColumn("max_lte_avant", F.max("lte_d").over(w_prev))
        .withColumn("min_gte_apres", F.min("gte_d").over(w_next))
        .withColumn("divergence",
            (F.col("gte_d") <= F.col("max_lte_avant")) |          # recouvre un précédent
            (F.col("min_gte_apres") <= F.col("lte_d")))           # recouvert par un suivant
        .withColumn("divergence", F.coalesce("divergence", F.lit(False)))
        .drop("max_lte_avant", "min_gte_apres"))


# ---------------------------------------------------------------------
#  3. Assemblage : 1 ligne = 1 identité
# ---------------------------------------------------------------------
def documents(spark, J, registre=None):
    """registre : DataFrame (logic, logic_libelle) issu de la config Oracle.
       Petit (quelques dizaines de lignes) -> broadcast sûr."""
    # PÉRIMÈTRE D'ABORD : tout le calcul (îlots, divergence, contributions)
    # ne porte que sur les clés à indexer. Les subjects dormants sont écartés
    # ici, pas après avoir tout construit.
    cles = cles_perimetre(spark, J)
    sej  = marquer_divergence(
        sejours_consensus_filtre(spark, J, cles))
    ctrb = sejours_contributions_filtre(spark, J, cles)

    if registre is not None:
        ctrb = ctrb.join(F.broadcast(registre), "logic", "left")
    else:
        ctrb = ctrb.withColumn("logic_libelle", F.col("logic"))

    # rattacher chaque contribution au séjour consensus qu'elle recoupe
    s = sej.select(
        F.col("consensus_key").alias("s_key"), F.col("lieu").alias("s_lieu"),
        F.col("gte_d").alias("s_gte"), F.col("lte_d").alias("s_lte"))
    contrib_par_sejour = (ctrb.join(s,
            (ctrb["consensus_key"] == s["s_key"]) & (ctrb["lieu"] == s["s_lieu"]) &
            (ctrb["c_gte_d"] <= s["s_lte"]) & (s["s_gte"] <= ctrb["c_lte_d"]), "inner")
        .groupBy("s_key", "s_lieu", "s_gte")
        .agg(F.collect_list(F.struct(
                "logic", "logic_libelle", "subject_id", "mapping_score",
                F.date_format("c_gte_d", "yyyyMMdd").alias("gte"),
                F.date_format("c_lte_d", "yyyyMMdd").alias("lte"),
                F.col("c_score").alias("score"))).alias("contributions")))

    # 1 ligne par séjour, contributions incluses
    sejour_complet = (sej.join(contrib_par_sejour,
            (sej["consensus_key"] == contrib_par_sejour["s_key"]) &
            (sej["lieu"] == contrib_par_sejour["s_lieu"]) &
            (sej["gte_d"] == contrib_par_sejour["s_gte"]), "left")
        .drop("s_key", "s_lieu", "s_gte")
        .withColumn("gte", F.date_format("gte_d", "yyyyMMdd"))
        .withColumn("lte", F.date_format("lte_d", "yyyyMMdd")))

    # 1 ligne par IDENTITÉ : les séjours deviennent un tableau nested.
    # gte en tête du struct -> sort_array trie les séjours chronologiquement.
    sejour_struct = F.struct(
        F.col("gte"), F.col("lte"), "lieu", "pays",
        F.struct(F.col("gte").alias("gte"), F.col("lte").alias("lte")).alias("periode"),
        "nb_jours", "score", "n_logics", "divergence",
        F.coalesce("contributions", F.array()).alias("contributions"))

    return (sejour_complet
        .groupBy("consensus_key")
        .agg(F.first("person_id", ignorenulls=True).alias("person_id"),
             F.min("gte").alias("premiere_activite"),
             F.max("lte").alias("derniere_activite"),
             F.max("score").alias("note_max"),
             F.count(F.lit(1)).alias("nb_sejours"),
             F.array_distinct(F.collect_list("lieu")).alias("lieux"),
             F.array_distinct(F.flatten(F.collect_list("logiques"))).alias("logiques"),
             F.array_distinct(F.flatten(F.collect_list("subjects"))).alias("subjects"),
             F.sort_array(F.collect_list(sejour_struct)).alias("sejours"))
        .withColumn("est_rattache", F.col("person_id").isNotNull())
        .withColumn("subject_id",
                    F.when(F.col("person_id").isNull(), F.col("consensus_key")))
        .withColumn("periode_couverte",
                    F.struct(F.col("premiere_activite").alias("gte"),
                             F.col("derniere_activite").alias("lte")))
        # garde-fou : un document pathologique ne doit pas faire sauter l'indexation
        .withColumn("sejours", F.slice(F.col("sejours"), 1, MAX_SEJOURS))
        .select("consensus_key", "person_id", "subject_id", "est_rattache",
                "premiere_activite", "derniere_activite", "periode_couverte",
                "note_max", "nb_sejours", "lieux", "logiques", "subjects", "sejours"))


# ---------------------------------------------------------------------
#  4. Périmètre + écriture
# ---------------------------------------------------------------------
def restreindre_perimetre(docs, J, fenetre_jours=FENETRE_SUBJECT_JOURS):
    """Rattachés : tout. Non rattachés : seulement les actifs récents,
    sinon les subjects dormants font exploser le volume de l'index."""
    seuil = F.date_format(
        F.date_sub(F.to_date(F.lit(J), "yyyyMMdd"), fenetre_jours), "yyyyMMdd")
    return docs.where(F.col("est_rattache") | (F.col("derniere_activite") >= seuil))


def ecrire_elastic(df, index, noeuds="localhost", port="9200"):
    (df.write.format("org.elasticsearch.spark.sql")
       .option("es.nodes", noeuds)
       .option("es.port", port)
       .option("es.mapping.id", "consensus_key")
       .option("es.write.operation", "index")
       .option("es.nodes.wan.only", "true")
       .option("es.batch.size.bytes", "5mb")
       .option("es.batch.size.entries", "500")     # docs plus gros qu'au grain séjour
       .option("es.batch.write.retry.count", "6")
       .option("es.batch.write.retry.wait", "30s")
       .mode("overwrite")
       .save(index))


def controle_volumetrie(docs):
    """À lancer au 1er run : dimensionner l'index avant de le construire en entier."""
    return docs.select(
        F.count(F.lit(1)).alias("nb_documents"),
        F.sum(F.col("est_rattache").cast("int")).alias("nb_rattaches"),
        F.avg(F.size("sejours")).alias("sejours_moyen"),
        F.max(F.size("sejours")).alias("sejours_max"))


def projeter(spark, J, registre=None, noeuds="localhost"):
    # le périmètre est appliqué EN TÊTE de documents() (cles_perimetre) ;
    # plus de filtre tardif ici.
    docs = documents(spark, J, registre)
    docs.persist(StorageLevel.MEMORY_AND_DISK)
    controle_volumetrie(docs).show()
    ecrire_elastic(docs, ES_INDEX.format(J=J), noeuds)
    docs.unpersist()
    # puis, hors Spark : bascule d'alias
    #   POST /_aliases {"actions":[
    #     {"remove":{"index":"localisation_identites_v2_*","alias":"localisation"}},
    #     {"add":{"index":"localisation_identites_v2_<J>","alias":"localisation"}}]}


