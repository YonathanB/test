https://claude.ai/public/artifacts/5c1e49d4-26f2-4096-8331-63687a17fc60


https://claude.ai/public/artifacts/89104e95-001e-4ca5-b037-03ca6a4833bc
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
N_BUCKETS      = 8192
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
        .join(F.broadcast(spikes), ["consensus_key", "d"], "left")
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
        .withColumn("subject_id", F.lit(None).cast("string")))   # consensus = niveau personne

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
                "score", "contributing_logics", "contributing_subjects",
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

    par_cle = sortie.withColumn("bucket",
        F.pmod(F.hash("consensus_key"), F.lit(N_BUCKETS)))
    if not _table_existe(spark, GOLD_PAR_CLE):
        (par_cle.sortWithinPartitions("consensus_key", "date")
            .write.format("parquet").partitionBy("bucket").saveAsTable(GOLD_PAR_CLE))
    else:
        (par_cle.sortWithinPartitions("consensus_key", "date")
            .write.mode("append").format("parquet").insertInto(GOLD_PAR_CLE))

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
    build_gold_personnes(spark)        # 4. la vue personnes

# run_quotidien(spark, "20240320",
#     jdbc_url="jdbc:oracle:thin:@//host:1521/service",
#     jdbc_props={"user": "...", "password": "...",
#                 "driver": "oracle.jdbc.OracleDriver"})
