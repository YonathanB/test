# =====================================================================
#  PIPELINE FINAL — localisation multi-logiques
#  gold_jour (vérité, jour) / gold_jour_par_cle (par identifiant)
#  gold_personnes (doc/personne) + re-liaison rétroactive + purge TTL
# =====================================================================
from datetime import datetime, timedelta
from functools import reduce
from pyspark.sql import Window, functions as F
from pyspark.sql.utils import AnalysisException

# ---------------------------------------------------------------------
#  PARAMÈTRES
# ---------------------------------------------------------------------
TTL_MOIS       = 6
BUFFER_JOURS   = 3
N_BUCKETS      = 8192

GOLD_JOUR      = "gold_jour"
GOLD_PAR_CLE   = "gold_jour_par_cle"
GOLD_PERSONNES = "gold_personnes"
T_LAST_SEEN    = "subject_last_seen"       # subject_id, premiere_activite, derniere_activite
T_DEJA_RELIES  = "subjects_deja_relies"    # subject_id
T_TOMBSTONES   = "cles_tombstones"         # consensus_key, motif
T_MAPPING      = "subject_person_map"      # subject_id, person_id  (déjà rank=1)
T_REGISTRE     = "logic_registry"          # logic, fiabilite

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
    day = min(d.day, 28)                       # évite les fins de mois piégeuses
    return datetime(y, m, day).strftime(FMT)

def _ecrire_partitions(spark, df, table, partition_col):
    """Création ou remplacement des seules partitions présentes dans df."""
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    if not _table_existe(spark, table):
        (df.write.format("parquet").mode("overwrite")
           .partitionBy(partition_col).saveAsTable(table))
    else:
        cols = spark.table(table).columns
        df.select(*cols).write.mode("overwrite").insertInto(table)


# =====================================================================
#  ADAPTATEURS  (la seule partie spécifique par logique)
# =====================================================================
def adapter_logique_A(spark, dmin, dmax):
    return (spark.table("bronze_logique_A")
        .where((F.col("jour") >= dmin) & (F.col("jour") <= dmax))
        .select(F.col("device_id").alias("subject_id"),
                F.lit("logique_A").alias("logic"),
                F.col("jour").alias("date"),
                F.col("ville").alias("lieu"),
                F.col("pays").alias("pays"),
                F.col("latitude").cast("double").alias("lat"),
                F.col("longitude").cast("double").alias("lon"),
                F.col("id").alias("raw_source_id")))

def adapter_logique_B(spark, dmin, dmax):
    return (spark.table("bronze_logique_B")
        .where((F.col("date_obs") >= dmin) & (F.col("date_obs") <= dmax))
        .select(F.col("user_ref").alias("subject_id"),
                F.lit("logique_B").alias("logic"),
                F.col("date_obs").alias("date"),
                F.col("city").alias("lieu"),
                F.col("country").alias("pays"),
                F.lit(None).cast("double").alias("lat"),
                F.lit(None).cast("double").alias("lon"),
                F.col("row_id").alias("raw_source_id")))

ADAPTERS = [adapter_logique_A, adapter_logique_B]

def build_silver_raw(spark, dmin, dmax):
    dfs = [a(spark, dmin, dmax) for a in ADAPTERS]
    return reduce(lambda x, y: x.unionByName(y), dfs)


# =====================================================================
#  MAPPING  (la table contient déjà rank=1 : 1 ligne par subject_id)
# =====================================================================
def mapping_df(spark):
    return spark.table(T_MAPPING).select("subject_id", "person_id").dropDuplicates(["subject_id"])

def add_consensus_key(spark, df):
    return (df.join(mapping_df(spark), "subject_id", "left")
              .withColumn("consensus_key", F.coalesce("person_id", "subject_id")))


# =====================================================================
#  NETTOYAGE : flag des spikes spatio-temporels
# =====================================================================
def clean_spikes(df):
    w = Window.partitionBy("consensus_key", "logic").orderBy("d")
    return (df
        .withColumn("d", F.to_date("date", "yyyyMMdd"))
        .withColumn("prev_lieu", F.lag("lieu").over(w))
        .withColumn("next_lieu", F.lead("lieu").over(w))
        .withColumn("quality", F.when(
            (F.col("lieu") != F.col("prev_lieu")) &
            (F.col("lieu") != F.col("next_lieu")) &
            (F.col("prev_lieu") == F.col("next_lieu")),
            F.lit("spike")).otherwise(F.lit("ok"))))


# =====================================================================
#  SCORING au grain jour : dédup -> per-logic + consensus (noisy-OR)
#  Utilisé par l'ingestion ET par le recalcul de consensus en re-liaison.
# =====================================================================
def _consensus_depuis_logic_day(logic_day):
    return (logic_day
        .groupBy("consensus_key", "d", "lieu", "pays")
        .agg((F.lit(1.0) - F.exp(F.sum(F.log(F.lit(1.0) - F.col("w"))))).alias("score"),
             F.collect_set("logic").alias("contributing_logics"),
             F.countDistinct("logic").alias("n_logics"),
             F.first("person_id", ignorenulls=True).alias("person_id"),
             F.flatten(F.collect_list("raw")).alias("raw"))
        .withColumn("logic", F.lit("consensus")))

def build_daily(spark, silver_clean):
    reg = spark.table(T_REGISTRE).select("logic", "fiabilite")
    raw_struct = F.struct("logic", "subject_id", "date", "lieu", "pays",
                          "lat", "lon", "raw_source_id")

    logic_day = (silver_clean.where(F.col("quality") == "ok")
        .withColumn("raw", raw_struct)
        .join(reg, "logic")
        .groupBy("consensus_key", "d", "lieu", "pays", "logic")
        .agg(F.first("fiabilite").alias("w"),
             F.first("person_id", ignorenulls=True).alias("person_id"),
             F.collect_list("raw").alias("raw")))

    per_logic = logic_day.select(
        "consensus_key", "d", "lieu", "pays", "person_id", "logic",
        F.col("w").alias("score"),
        F.array("logic").alias("contributing_logics"),
        F.lit(1).alias("n_logics"), "raw")

    return per_logic.unionByName(_consensus_depuis_logic_day(logic_day))

def _forme_gold_jour(daily):
    return (daily
        .withColumn("date", F.date_format("d", "yyyyMMdd"))
        .select("consensus_key", "person_id", "lieu", "pays", "logic",
                "score", "contributing_logics", "n_logics", "raw", "date"))


# =====================================================================
#  TEMPS 1 — RE-LIAISON RÉTROACTIVE (option A)
# =====================================================================
def relink(spark, J):
    if not (_table_existe(spark, T_LAST_SEEN) and _table_existe(spark, GOLD_JOUR)):
        return
    deja = (spark.table(T_DEJA_RELIES) if _table_existe(spark, T_DEJA_RELIES)
            else spark.createDataFrame([], "subject_id string"))

    # nouveaux reliés = dans le mapping, jamais traités, et ayant des données
    nouveaux = (mapping_df(spark)
        .join(deja, "subject_id", "left_anti")
        .join(spark.table(T_LAST_SEEN), "subject_id", "inner"))   # + premiere/derniere_activite

    if nouveaux.rdd.isEmpty():
        return

    bornes = nouveaux.agg(F.min("premiere_activite").alias("dmin"),
                          F.max("derniere_activite").alias("dmax")).first()
    dmin, dmax = bornes["dmin"], bornes["dmax"]

    cles_subject = nouveaux.select(F.col("subject_id").alias("consensus_key"))
    remap        = nouveaux.select(F.col("subject_id").alias("consensus_key"),
                                   F.col("person_id").alias("new_key"))

    # partitions concernées
    part = (spark.table(GOLD_JOUR)
        .where((F.col("date") >= dmin) & (F.col("date") <= dmax)))

    # 1) lignes des subjects re-liés -> nouvelle clé (per-logic seulement,
    #    les anciennes lignes consensus du subject sont abandonnées)
    migrees = (part.join(remap, "consensus_key", "inner")
        .where(F.col("logic") != "consensus")
        .withColumn("consensus_key", F.col("new_key"))
        .withColumn("person_id", F.col("new_key"))
        .drop("new_key"))

    # 2) (personne, jour) dont le consensus doit être recalculé
    touches = migrees.select("consensus_key", "date").distinct()

    # 3) reste de la table sur ces partitions :
    #    - sans les subjects re-liés (toutes leurs lignes)
    #    - sans les anciens consensus des (personne, jour) touchés
    reste = (part
        .join(cles_subject, "consensus_key", "left_anti")
        .join(touches.withColumn("logic", F.lit("consensus")),
              ["consensus_key", "date", "logic"], "left_anti"))

    # 4) recalcul du consensus des (personne, jour) touchés
    reg = spark.table(T_REGISTRE).select("logic", "fiabilite")
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
             F.collect_list("raw_one").alias("raw")))
    consensus_neuf = _forme_gold_jour(_consensus_depuis_logic_day(logic_day))

    nouveau = reste.unionByName(migrees).unionByName(consensus_neuf)
    _ecrire_partitions(spark, nouveau, GOLD_JOUR, "date")

    # tombstones (nettoyage physique des buckets à la compaction)
    ts = cles_subject.withColumn("motif", F.lit("relink"))
    (ts.write.mode("append").format("parquet").saveAsTable(T_TOMBSTONES)
     if _table_existe(spark, T_TOMBSTONES)
     else ts.write.format("parquet").saveAsTable(T_TOMBSTONES))

    # état
    nouveaux.select("subject_id").write.mode("append").format("parquet") \
        .saveAsTable(T_DEJA_RELIES) if _table_existe(spark, T_DEJA_RELIES) \
        else nouveaux.select("subject_id").write.format("parquet").saveAsTable(T_DEJA_RELIES)


# =====================================================================
#  TEMPS 2 — INGESTION du tampon [J-BUFFER, J]
# =====================================================================
def ingest(spark, J):
    dmin = _jours_avant(J, BUFFER_JOURS)

    silver = clean_spikes(add_consensus_key(spark, build_silver_raw(spark, dmin, J)))
    daily  = build_daily(spark, silver)
    sortie = _forme_gold_jour(daily)

    # gold_jour : remplacement des partitions du tampon
    _ecrire_partitions(spark, sortie, GOLD_JOUR, "date")

    # gold_jour_par_cle : APPEND du tampon dans les buckets
    #  (les versions précédentes de ces jours seront dédupliquées à la compaction ;
    #   en lecture, on garde la ligne la plus récente si doublon de (clé,date,lieu,logic))
    par_cle = sortie.withColumn("bucket",
        F.pmod(F.hash("consensus_key"), F.lit(N_BUCKETS)))
    if not _table_existe(spark, GOLD_PAR_CLE):
        (par_cle.sortWithinPartitions("consensus_key", "date")
            .write.format("parquet").partitionBy("bucket").saveAsTable(GOLD_PAR_CLE))
    else:
        (par_cle.sortWithinPartitions("consensus_key", "date")
            .write.mode("append").format("parquet").insertInto(GOLD_PAR_CLE))

    # subject_last_seen : premiere/derniere activité (subjects seulement)
    vus = (silver.where(F.col("person_id").isNull())
        .groupBy("subject_id")
        .agg(F.min("date").alias("premiere_activite"),
             F.max("date").alias("derniere_activite")))
    if not _table_existe(spark, T_LAST_SEEN):
        vus.write.format("parquet").saveAsTable(T_LAST_SEEN)
    else:
        ancien = spark.table(T_LAST_SEEN)
        fusion = (ancien.unionByName(vus)
            .groupBy("subject_id")
            .agg(F.min("premiere_activite").alias("premiere_activite"),
                 F.max("derniere_activite").alias("derniere_activite")))
        fusion.write.mode("overwrite").format("parquet").saveAsTable(T_LAST_SEEN + "_tmp")
        spark.sql(f"DROP TABLE {T_LAST_SEEN}")
        spark.sql(f"ALTER TABLE {T_LAST_SEEN}_tmp RENAME TO {T_LAST_SEEN}")


# =====================================================================
#  TEMPS 3 — PURGE des subjects silencieux depuis TTL_MOIS et non reliés
# =====================================================================
def purge(spark, J):
    if not _table_existe(spark, T_LAST_SEEN):
        return
    seuil = _mois_avant(J, TTL_MOIS)

    candidats = (spark.table(T_LAST_SEEN)
        .where(F.col("derniere_activite") == seuil)           # examinés UNE fois, pile au seuil
        .join(mapping_df(spark), "subject_id", "left_anti"))  # toujours non reliés

    if candidats.rdd.isEmpty():
        return

    bornes = candidats.agg(F.min("premiere_activite").alias("dmin"),
                           F.max("derniere_activite").alias("dmax")).first()
    cles = candidats.select(F.col("subject_id").alias("consensus_key"))

    # retirer leurs lignes des partitions concernées de gold_jour
    part = (spark.table(GOLD_JOUR)
        .where((F.col("date") >= bornes["dmin"]) & (F.col("date") <= bornes["dmax"])))
    _ecrire_partitions(spark,
        part.join(cles, "consensus_key", "left_anti"),
        GOLD_JOUR, "date")

    # tombstones pour les buckets ; retrait de l'état
    cles.withColumn("motif", F.lit("purge")) \
        .write.mode("append").format("parquet").saveAsTable(T_TOMBSTONES)
    reste = spark.table(T_LAST_SEEN).join(
        candidats.select("subject_id"), "subject_id", "left_anti")
    reste.write.mode("overwrite").format("parquet").saveAsTable(T_LAST_SEEN + "_tmp")
    spark.sql(f"DROP TABLE {T_LAST_SEEN}")
    spark.sql(f"ALTER TABLE {T_LAST_SEEN}_tmp RENAME TO {T_LAST_SEEN}")


# =====================================================================
#  TEMPS 4 — gold_personnes : recalcul complet (personnes uniquement)
# =====================================================================
def _intervalles(jour):
    d = jour.withColumn("d", F.to_date("date", "yyyyMMdd"))
    w = Window.partitionBy("consensus_key", "logic", "lieu").orderBy("d")
    islands = (d
        .withColumn("prev", F.lag("d").over(w))
        .withColumn("gap", F.when(
            F.col("prev").isNull() | (F.datediff("d", "prev") > 1), 1).otherwise(0))
        .withColumn("island_id", F.sum("gap").over(w)))
    return (islands
        .groupBy("consensus_key", "logic", "lieu", "island_id")
        .agg(F.min("d").alias("gte_d"), F.max("d").alias("lte_d"),
             F.first("pays").alias("pays"),
             F.avg("score").alias("score"),
             F.max("n_logics").alias("n_logics"),
             F.array_distinct(F.flatten(F.collect_list("contributing_logics")))
                 .alias("contributing_logics"))
        .withColumn("nb_jours", F.datediff("lte_d", "gte_d") + 1)
        .withColumn("periode", F.struct(
            F.date_format("gte_d", "yyyyMMdd").alias("gte"),
            F.date_format("lte_d", "yyyyMMdd").alias("lte"))))

def build_gold_personnes(spark):
    jour = spark.table(GOLD_JOUR).where(F.col("person_id").isNotNull())

    itv = _intervalles(jour).withColumn("sejour", F.struct(
        "lieu", "pays", "periode", "nb_jours", "score", "contributing_logics", "n_logics"))

    voyages = (itv.where(F.col("logic") == "consensus")
        .groupBy("consensus_key").agg(F.collect_list("sejour").alias("voyages")))

    detail_sejours = (itv.where(F.col("logic") != "consensus")
        .groupBy("consensus_key", "logic")
        .agg(F.collect_list(F.struct("lieu", "pays", "periode", "nb_jours")).alias("sejours")))

    counts = (jour.where(F.col("logic") != "consensus")
        .groupBy("consensus_key", "logic", "date")
        .agg(F.sum(F.size("raw")).alias("nb_events")))
    detail_counts = (counts.groupBy("consensus_key", "logic")
        .agg(F.collect_list(F.struct("date", "nb_events")).alias("evenements_par_jour"),
             F.sum("nb_events").alias("nb_events")))
    detail = (detail_sejours.join(detail_counts, ["consensus_key", "logic"], "full")
        .withColumn("bloc", F.struct("logic", "sejours", "evenements_par_jour", "nb_events"))
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

    doc = (resume.join(voyages, "consensus_key", "left")
                 .join(detail,  "consensus_key", "left"))
    doc.write.format("parquet").mode("overwrite").saveAsTable(GOLD_PERSONNES)


# =====================================================================
#  COMPACTION périodique de gold_jour_par_cle (ex. hebdo)
# =====================================================================
def compaction_par_cle(spark):
    if not _table_existe(spark, GOLD_PAR_CLE):
        return
    df = spark.table(GOLD_PAR_CLE)
    if _table_existe(spark, T_TOMBSTONES):
        morts = spark.table(T_TOMBSTONES).select("consensus_key").distinct()
        df = df.join(morts, "consensus_key", "left_anti")
    # dédup des versions multiples d'un même (clé, date, lieu, logic) : on garde 1 ligne
    df = df.dropDuplicates(["consensus_key", "date", "lieu", "logic"])
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
def run_quotidien(spark, J):
    relink(spark, J)            # 1. sauver d'abord
    ingest(spark, J)            # 2. le jour (tampon 3 j)
    purge(spark, J)             # 3. faucher en dernier
    build_gold_personnes(spark) # 4. la vue personnes

# run_quotidien(spark, "20240320")
# compaction_par_cle(spark)      # à planifier séparément (hebdo)
