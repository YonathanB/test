"""
ÉTAPE ① — prod.gold.daily_by_logic
Cohérence de chaque logique avec elle-même.

Grain : (entity_key, logic_id, day, country_code)
        une ligne par pays candidat, y compris écarté (weight = 0)

Version corrigée après la session de calibrage :
  - plus de table d'adjacence (les deux cas recevaient le même traitement)
  - is_non_individual joint ICI, pas en Silver (flag rétroactif)
  - un seul code de bruit : MINORITY_NOISE
  - traçabilité de la version du référentiel des identifiants pollués
"""

from pyspark.sql import functions as F, Window

# ═════════════════════════════════════════════════════════════
# 1. DDL
# ═════════════════════════════════════════════════════════════
DDL = """
CREATE TABLE IF NOT EXISTS prod.gold.daily_by_logic (
  entity_key        STRING  NOT NULL,
  person_id         STRING,
  logic_id          STRING  NOT NULL,
  day               DATE    NOT NULL,
  country_code      STRING  NOT NULL,

  status            STRING  NOT NULL,   -- OK | CONFLICT_INTERNAL | OUTLIER | EXCLUDED
  reason_code       STRING,             -- MINORITY_NOISE | MULTI_COUNTRY | NON_INDIVIDUAL_ID

  evidence_count    BIGINT  NOT NULL,
  nb_identifiers    INT     NOT NULL,   -- signal le plus fort
  nb_countries      INT     NOT NULL,
  ratio             DECIMAL(5,4)  NOT NULL,

  weight            DECIMAL(4,3) NOT NULL,
  coherence_score   DECIMAL(4,3),       -- NULL : seconde passe, calcul sur période

  event_ids         ARRAY<STRING>,
  source_ids        ARRAY<STRING>,

  ruleset_version   STRING  NOT NULL,
  non_ind_ref_version STRING,
  computed_at       TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (days(day))
TBLPROPERTIES (
  'format-version'='2',
  'write.distribution-mode'='none',
  'write.target-file-size-bytes'='536870912',
  'write.parquet.compression-codec'='zstd',
  'write.merge.mode'='copy-on-write'
)
"""

# ═════════════════════════════════════════════════════════════
# 2. PARAMÈTRES — versionnés avec le ruleset
# ═════════════════════════════════════════════════════════════
RULESET_VERSION  = "v0.1"

NOISE_MAX_RATIO  = 0.15   # sous ce ratio, le pays minoritaire est du bruit
MIN_TOP_VOLUME   = 10     # sous ce volume du dominant, on ne conclut pas
VOLUME_REF       = 20     # volume à partir duquel le facteur volume sature
IDENTITY_BONUS   = 0.5    # bonus par identifiant distinct supplémentaire
IDENTITY_CAP     = 2.0
CONFLICT_PENALTY = 0.5


# ═════════════════════════════════════════════════════════════
# 3. CONSTRUCTION
# ═════════════════════════════════════════════════════════════
def build_daily_by_logic(spark, day_from, day_to):

    # ── a) lecture de Silver ────────────────────────────────
    ev = (
        spark.table("prod.silver.location_events")
        .where(
            (F.col("observation_date") >= F.lit(day_from))
            & (F.col("observation_date") <= F.lit(day_to))
            & (F.col("event_type") == "PRESENCE")
            & (F.col("country_code").isNotNull())
        )
    )

    # ── b) identifiants pollués : joint ici, pas en Silver ──
    poll = (
        spark.table("prod.ref.non_individual_ids")
        .select(
            "identifier_norm",
            F.col("ref_version").alias("non_ind_ref_version"),
        )
        .withColumn("is_non_individual", F.lit(True))
    )

    ev = (
        ev.join(F.broadcast(poll), "identifier_norm", "left")
          .withColumn("is_non_individual",
                      F.coalesce("is_non_individual", F.lit(False)))
    )

    # ── c) agrégation par pays candidat ─────────────────────
    agg = (
        ev.groupBy("entity_key", "logic_id", "observation_date", "country_code")
          .agg(
              F.sum("evidence_count").alias("evidence_count"),
              F.countDistinct("identifier_norm").alias("nb_identifiers"),
              F.max("source_confidence").alias("source_confidence"),
              F.max("person_id").alias("person_id"),
              F.max("is_non_individual").alias("is_non_individual"),
              F.max("non_ind_ref_version").alias("non_ind_ref_version"),
              F.collect_set("event_id").alias("event_ids"),
              F.collect_set("source_id").alias("source_ids"),
          )
          .withColumnRenamed("observation_date", "day")
    )

    # ── d) contexte du jour ─────────────────────────────────
    w_day = Window.partitionBy("entity_key", "logic_id", "day")

    # tri déterministe : country_code en dernier critère
    w_rank = w_day.orderBy(
        F.col("evidence_count").desc(),
        F.col("nb_identifiers").desc(),
        F.col("country_code").asc(),
    )

    ctx = (
        agg
        .withColumn("nb_countries", F.count("*").over(w_day))
        .withColumn("n_top",        F.max("evidence_count").over(w_day))
        .withColumn("is_top",       F.row_number().over(w_rank) == 1)
        .withColumn("ratio",
                    (F.col("evidence_count") / F.col("n_top")).cast("decimal(5,4)"))
    )

    # ── e) classification ───────────────────────────────────
    is_minority = (
        (~F.col("is_top"))
        & (F.col("ratio") < NOISE_MAX_RATIO)
        & (F.col("n_top") >= MIN_TOP_VOLUME)
    )

    ctx = (
        ctx
        .withColumn(
            "reason_code",
            F.when(F.col("is_non_individual"), F.lit("NON_INDIVIDUAL_ID"))
             .when(is_minority, F.lit("MINORITY_NOISE"))
             .when((F.col("nb_countries") > 1) & (~F.col("is_top")),
                   F.lit("MULTI_COUNTRY"))
             .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "status",
            F.when(F.col("is_non_individual"), F.lit("EXCLUDED"))
             .when(F.col("reason_code") == "MINORITY_NOISE", F.lit("OUTLIER"))
             .when(F.col("nb_countries") > 1, F.lit("CONFLICT_INTERNAL"))
             .otherwise(F.lit("OK")),
        )
    )

    # ── f) poids ────────────────────────────────────────────
    identity_factor = F.least(
        F.lit(1.0) + F.lit(IDENTITY_BONUS) * (F.col("nb_identifiers") - 1),
        F.lit(IDENTITY_CAP),
    )

    # racine carrée : amortit sans écraser, adapté aux volumes ~50
    volume_factor = F.lit(0.5) + F.lit(0.5) * F.least(
        F.sqrt(F.col("evidence_count")) / F.sqrt(F.lit(VOLUME_REF)), F.lit(1.0)
    )

    base_weight = F.least(
        F.col("source_confidence") * identity_factor * volume_factor, F.lit(1.0)
    )

    final = (
        ctx
        .withColumn(
            "weight",
            F.when(F.col("status").isin("EXCLUDED", "OUTLIER"), F.lit(0.0))
             .when(F.col("status") == "CONFLICT_INTERNAL",
                   base_weight * F.lit(CONFLICT_PENALTY))
             .otherwise(base_weight)
             .cast("decimal(4,3)"),
        )
        .withColumn("coherence_score", F.lit(None).cast("decimal(4,3)"))
        .withColumn("ruleset_version", F.lit(RULESET_VERSION))
        .withColumn("computed_at", F.current_timestamp())
        .select(
            "entity_key", "person_id", "logic_id", "day", "country_code",
            "status", "reason_code",
            "evidence_count", "nb_identifiers", "nb_countries", "ratio",
            "weight", "coherence_score",
            "event_ids", "source_ids",
            "ruleset_version", "non_ind_ref_version", "computed_at",
        )
    )

    return final


def write_daily_by_logic(df):
    (
        df.sortWithinPartitions("entity_key", "logic_id")
          .writeTo("prod.gold.daily_by_logic")
          .option("distribution-mode", "none")
          .overwritePartitions()      # rejouable : réécrit les jours traités
    )


# ═════════════════════════════════════════════════════════════
# 4. LANCEMENT
# ═════════════════════════════════════════════════════════════
if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("daily_by_logic").getOrCreate()
    spark.sql(DDL)

    df = build_daily_by_logic(spark, "2026-05-01", "2026-07-31")
    write_daily_by_logic(df)


# ═════════════════════════════════════════════════════════════
# 5. CONTRÔLES — à lancer dans cet ordre après le premier run
# ═════════════════════════════════════════════════════════════
CHECKS = """
-- ① Répartition des statuts. Si OUTLIER > 20 %, le seuil est trop agressif.
SELECT status, reason_code, COUNT(*) AS n
FROM prod.gold.daily_by_logic
GROUP BY 1,2 ORDER BY n DESC;

-- ② Distribution des ratios : cherchez le creux entre les deux modes.
--    C'est lui qui donne NOISE_MAX_RATIO, il ne se devine pas.
SELECT ROUND(ratio, 2) AS r, COUNT(*) AS n
FROM prod.gold.daily_by_logic
WHERE ratio < 1
GROUP BY 1 ORDER BY 1;

-- ③ Le signal identifiants : combien de jours en bénéficient réellement ?
--    Si presque tout est à 1, le facteur identité ne sert à rien.
SELECT nb_identifiers, COUNT(*) AS n
FROM prod.gold.daily_by_logic
GROUP BY 1 ORDER BY 1;

-- ④ Profils frontaliers persistants : deux pays ensemble presque tous les jours.
--    Ce ne sont PAS des outliers — le ratio ne les attrape pas.
SELECT entity_key, logic_id, COUNT(DISTINCT day) AS jours_multi
FROM prod.gold.daily_by_logic
WHERE nb_countries > 1
GROUP BY 1,2
HAVING COUNT(DISTINCT day) > 20
ORDER BY jours_multi DESC LIMIT 50;

-- ⑤ Distribution des poids : doit s'étaler, pas s'entasser sur une valeur.
SELECT ROUND(weight, 1) AS w, COUNT(*) AS n
FROM prod.gold.daily_by_logic
GROUP BY 1 ORDER BY 1;
"""
