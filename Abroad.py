"""
ÉTAPE ① — prod.gold.daily_by_logic

Rôle : agréger, par logique, ce que chaque logique affirme pour un jour donné.
       ① MARQUE. Elle ne décide pas. ② décide, et l'utilisateur choisit.

Grain : (entity_key, logic_id, day, country_code)

Principes appliqués :
  - AUCUN score n'est jamais nul. Un pays déprécié garde une note visible.
  - identity_confidence entre dans le poids : 3 identifiants à 0,4 ne valent
    pas 3 identifiants à 0,95.
  - weight_raw ET weight_filtered : ② peut réhabiliter un pays minoritaire
    corroboré par une autre logique. ① ne détruit rien.
  - Les bornes temporelles (valid_from/valid_to) remontent PAR IDENTIFIANT :
    ce sont les points de rupture dont ② a besoin pour découper en plages.
  - Pas de coherence_score (suspendu), pas de table d'adjacence (abandonnée).
"""

from pyspark.sql import functions as F, Window

# ═════════════════════════════════════════════════════════════════════
# 1. DDL
# ═════════════════════════════════════════════════════════════════════
DDL = """
CREATE TABLE IF NOT EXISTS prod.gold.daily_by_logic (
  entity_key        STRING  NOT NULL,
  person_id         STRING,
  is_resolved       BOOLEAN NOT NULL,
  logic_id          STRING  NOT NULL,
  day               DATE    NOT NULL,
  country_code      STRING  NOT NULL,

  -- étiquettes : descriptives, jamais destructrices
  status            STRING  NOT NULL,   -- OK | CONFLICT_INTERNAL | DEPRECIATED
  reason_code       STRING,             -- MINORITY_NOISE | MULTI_COUNTRY | NON_INDIVIDUAL

  -- volumétrie
  evidence_count    BIGINT  NOT NULL,
  nb_identifiers    INT     NOT NULL,
  nb_countries      INT     NOT NULL,
  ratio             DECIMAL(5,4) NOT NULL,   -- volume / volume du dominant

  -- identité
  min_identity_confidence DECIMAL(3,2),
  max_identity_confidence DECIMAL(3,2),
  avg_identity_confidence DECIMAL(3,2),      -- pondérée par le volume

  -- source
  source_confidence_w   DECIMAL(4,3),        -- moyenne pondérée par le volume
  source_confidence_max DECIMAL(4,3),        -- pour comparaison au calibrage

  -- bornes temporelles : matière première du découpage en plages de ②
  span_start        TIMESTAMP,
  span_end          TIMESTAMP,

  -- poids : deux valeurs, ② arbitre
  weight_raw        DECIMAL(6,4) NOT NULL,   -- sans pénalité de minorité
  weight_filtered   DECIMAL(6,4) NOT NULL,   -- après pénalités. JAMAIS 0.

  -- explicabilité : qui a dit quoi, avec quelle confiance, sur quelle plage
  identifiers       ARRAY<STRUCT<
                      identifier_norm: STRING,
                      identifier_type: STRING,
                      identity_confidence: DECIMAL(3,2),
                      evidence_count: BIGINT,
                      valid_from: TIMESTAMP,
                      valid_to: TIMESTAMP,
                      is_non_individual: BOOLEAN
                    >>,
  source_ids        ARRAY<STRING>,
  event_ids         ARRAY<STRING>,

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

# ═════════════════════════════════════════════════════════════════════
# 2. PARAMÈTRES — versionnés avec le ruleset
# ═════════════════════════════════════════════════════════════════════
RULESET_VERSION = "v0.2"

NOISE_MAX_RATIO   = 0.15   # sous ce ratio, un pays minoritaire est déprécié
MIN_TOP_VOLUME    = 10     # sous ce volume du dominant, on ne déprécie pas
VOLUME_REF        = 20     # volume à partir duquel le facteur volume sature
IDENTITY_BONUS    = 0.5    # bonus par identifiant supplémentaire, × sa confiance
IDENTITY_CAP      = 2.0
CONFLICT_PENALTY  = 0.5
MINORITY_FLOOR    = 0.10   # remplace l'annulation. Ne descend jamais à 0.
NON_INDIVIDUAL_FLOOR = 0.02  # dépréciation forte, pas exclusion


# ═════════════════════════════════════════════════════════════════════
# 3. CONSTRUCTION
# ═════════════════════════════════════════════════════════════════════
def build_daily_by_logic(spark, day_from, day_to):

    # ── a) Silver, sur la fenêtre demandée ───────────────────────────
    # NB : les vraies bornes de date évitent que l'élagage de partition
    #      soit perdu. Vérifier PartitionFilters dans explain("formatted").
    ev = (
        spark.table("prod.silver.location_events")
        .where(
            (F.col("observation_date") >= F.to_date(F.lit(day_from)))
            & (F.col("observation_date") <= F.to_date(F.lit(day_to)))
            & (F.col("event_type") == "PRESENCE")
            & (F.col("country_code").isNotNull())
        )
    )

    # ── b) identifiants pollués : joints ici (le flag est rétroactif) ─
    #      Dépréciation forte, PAS exclusion : l'utilisateur doit voir
    #      ce qui a été retiré et pourquoi.
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
                      F.coalesce(F.col("is_non_individual"), F.lit(False)))
    )

    # ── c) AGRÉGATION EN DEUX TEMPS ──────────────────────────────────
    # Indispensable : une même logique peut recevoir plusieurs sources,
    # donc un même identifiant peut avoir plusieurs lignes Silver dans
    # (entity, logic, day, country). Sommer identity_confidence en une
    # seule passe la compterait plusieurs fois.

    # c.1 — par identifiant
    per_id = (
        ev.groupBy(
            "entity_key", "logic_id", "observation_date",
            "country_code", "identifier_norm",
        )
        .agg(
            F.sum("evidence_count").alias("evidence_count"),
            F.max("identifier_type").alias("identifier_type"),
            F.max("identity_confidence").alias("identity_confidence"),
            F.min("valid_from").alias("valid_from"),
            F.max("valid_to").alias("valid_to"),
            F.max("person_id").alias("person_id"),
            F.max("is_non_individual").alias("is_non_individual"),
            F.max("non_ind_ref_version").alias("non_ind_ref_version"),
            F.max("source_confidence").alias("source_confidence"),
            F.collect_set("source_id").alias("source_ids"),
            F.collect_set("event_id").alias("event_ids"),
        )
        # une confiance d'identité manquante est traitée comme neutre-basse,
        # jamais comme parfaite : ne pas récompenser l'absence d'information
        .withColumn("identity_confidence",
                    F.coalesce(F.col("identity_confidence"), F.lit(0.50)))
    )

    # c.2 — par pays candidat
    agg = (
        per_id.groupBy("entity_key", "logic_id", "observation_date", "country_code")
        .agg(
            F.sum("evidence_count").alias("evidence_count"),
            F.count("*").alias("nb_identifiers"),

            F.min("identity_confidence").alias("min_identity_confidence"),
            F.max("identity_confidence").alias("max_identity_confidence"),
            # somme des confiances : sert au bonus d'identifiants pondéré
            F.sum("identity_confidence").alias("sum_identity_confidence"),
            # moyenne pondérée par le volume : un identifiant à 22 événements
            # pèse plus qu'un identifiant à 1 événement
            (F.sum(F.col("identity_confidence") * F.col("evidence_count"))
             / F.sum("evidence_count")).alias("avg_identity_confidence"),

            (F.sum(F.col("source_confidence") * F.col("evidence_count"))
             / F.sum("evidence_count")).alias("source_confidence_w"),
            F.max("source_confidence").alias("source_confidence_max"),

            F.min("valid_from").alias("span_start"),
            F.max("valid_to").alias("span_end"),

            F.max("person_id").alias("person_id"),
            F.max("is_non_individual").alias("is_non_individual"),
            F.max("non_ind_ref_version").alias("non_ind_ref_version"),

            F.collect_list(
                F.struct(
                    "identifier_norm", "identifier_type", "identity_confidence",
                    "evidence_count", "valid_from", "valid_to", "is_non_individual",
                )
            ).alias("identifiers"),
            F.flatten(F.collect_set("source_ids")).alias("source_ids"),
            F.flatten(F.collect_set("event_ids")).alias("event_ids"),
        )
        .withColumnRenamed("observation_date", "day")
    )

    # ── d) contexte du jour, au sein de la logique ───────────────────
    w_day = Window.partitionBy("entity_key", "logic_id", "day")

    # tri déterministe : country_code en dernier critère, sinon le résultat
    # bascule d'un run à l'autre sans que les données bougent
    w_rank = w_day.orderBy(
        F.col("evidence_count").desc(),
        F.col("nb_identifiers").desc(),
        F.col("country_code").asc(),
    )

    ctx = (
        agg
        .withColumn("nb_countries", F.count("*").over(w_day))
        .withColumn("n_top", F.max("evidence_count").over(w_day))
        .withColumn("is_top", F.row_number().over(w_rank) == 1)
        .withColumn("ratio",
                    (F.col("evidence_count") / F.col("n_top")).cast("decimal(5,4)"))
    )

    # ── e) étiquetage ────────────────────────────────────────────────
    is_minority = (
        (~F.col("is_top"))
        & (F.col("ratio") < NOISE_MAX_RATIO)
        & (F.col("n_top") >= MIN_TOP_VOLUME)
    )

    ctx = (
        ctx
        .withColumn(
            "reason_code",
            F.when(F.col("is_non_individual"), F.lit("NON_INDIVIDUAL"))
             .when(is_minority, F.lit("MINORITY_NOISE"))
             .when((F.col("nb_countries") > 1) & (~F.col("is_top")),
                   F.lit("MULTI_COUNTRY"))
             .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "status",
            F.when(F.col("is_non_individual"), F.lit("DEPRECIATED"))
             .when(F.col("reason_code") == "MINORITY_NOISE", F.lit("DEPRECIATED"))
             .when(F.col("nb_countries") > 1, F.lit("CONFLICT_INTERNAL"))
             .otherwise(F.lit("OK")),
        )
    )

    # ── f) poids ─────────────────────────────────────────────────────
    # Bonus d'identifiants PONDÉRÉ par la confiance : le 2e, 3e... identifiant
    # n'apporte que ce que vaut son rattachement.
    identifier_bonus = F.least(
        F.lit(1.0)
        + F.lit(IDENTITY_BONUS)
          * (F.col("sum_identity_confidence") - F.col("max_identity_confidence")),
        F.lit(IDENTITY_CAP),
    )

    # racine carrée : amortit sans écraser, adapté aux volumes observés
    volume_factor = F.lit(0.5) + F.lit(0.5) * F.least(
        F.sqrt(F.col("evidence_count")) / F.sqrt(F.lit(float(VOLUME_REF))),
        F.lit(1.0),
    )

    # PAS de plafond à 1,0 : il écraserait justement les cas les mieux étayés
    # et fausserait la comparaison. ② normalise, evidence_strength veut l'absolu.
    weight_raw = (
        F.col("source_confidence_w")
        * F.col("avg_identity_confidence")
        * identifier_bonus
        * volume_factor
    )

    final = (
        ctx
        .withColumn("weight_raw", weight_raw.cast("decimal(6,4)"))
        .withColumn(
            "weight_filtered",
            F.when(F.col("reason_code") == "NON_INDIVIDUAL",
                   weight_raw * F.lit(NON_INDIVIDUAL_FLOOR))
             .when(F.col("reason_code") == "MINORITY_NOISE",
                   weight_raw * F.lit(MINORITY_FLOOR))
             .when(F.col("status") == "CONFLICT_INTERNAL",
                   weight_raw * F.lit(CONFLICT_PENALTY))
             .otherwise(weight_raw)
             .cast("decimal(6,4)"),
        )
        .withColumn("is_resolved", F.col("person_id").isNotNull())
        .withColumn("ruleset_version", F.lit(RULESET_VERSION))
        .withColumn("computed_at", F.current_timestamp())
        .select(
            "entity_key", "person_id", "is_resolved", "logic_id", "day",
            "country_code",
            "status", "reason_code",
            "evidence_count", "nb_identifiers", "nb_countries", "ratio",
            F.col("min_identity_confidence").cast("decimal(3,2)")
             .alias("min_identity_confidence"),
            F.col("max_identity_confidence").cast("decimal(3,2)")
             .alias("max_identity_confidence"),
            F.col("avg_identity_confidence").cast("decimal(3,2)")
             .alias("avg_identity_confidence"),
            F.col("source_confidence_w").cast("decimal(4,3)")
             .alias("source_confidence_w"),
            F.col("source_confidence_max").cast("decimal(4,3)")
             .alias("source_confidence_max"),
            "span_start", "span_end",
            "weight_raw", "weight_filtered",
            "identifiers", "source_ids", "event_ids",
            "ruleset_version", "non_ind_ref_version", "computed_at",
        )
    )

    return final


def write_daily_by_logic(df):
    (
        df.sortWithinPartitions("entity_key", "logic_id")
          .writeTo("prod.gold.daily_by_logic")
          .option("distribution-mode", "none")
          .overwritePartitions()   # ne réécrit que les jours présents dans df
    )


# ═════════════════════════════════════════════════════════════════════
# 4. LANCEMENT
# ═════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("daily_by_logic")
        # sans cette extension : pas de rewrite_data_files ni expire_snapshots
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )
    spark.sql(DDL)

    # commencer par une SEMAINE DENSE : itération rapide, distribution lisible
    df = build_daily_by_logic(spark, "2026-07-01", "2026-07-07")
    write_daily_by_logic(df)


# ═════════════════════════════════════════════════════════════════════
# 5. CONTRÔLES — dans cet ordre, le calibrage n'est pas parallélisable
# ═════════════════════════════════════════════════════════════════════
CHECKS = """
-- ① Répartition des étiquettes. Si DEPRECIATED > 20 %, le seuil est trop dur.
SELECT status, reason_code, COUNT(*) AS n
FROM prod.gold.daily_by_logic
GROUP BY 1,2 ORDER BY n DESC;

-- ② LE contrôle qui compte. Cherchez le creux entre les deux modes :
--    c'est lui qui donne NOISE_MAX_RATIO. Il ne se devine pas.
SELECT ROUND(ratio, 2) AS r, COUNT(*) AS n
FROM prod.gold.daily_by_logic
WHERE ratio < 1
GROUP BY 1 ORDER BY 1;

-- ③ Saturation du facteur de volume. Si la part >= VOLUME_REF est élevée,
--    le volume ne discrimine plus rien : remonter VOLUME_REF vers 40-50.
SELECT
  SUM(CASE WHEN evidence_count >= 20 THEN 1 ELSE 0 END) / COUNT(*) AS part_saturee,
  PERCENTILE(evidence_count, 0.5)  AS p50,
  PERCENTILE(evidence_count, 0.95) AS p95,
  MAX(evidence_count)              AS max_vol
FROM prod.gold.daily_by_logic;

-- ④ Le signal identifiants sert-il vraiment ?
--    Si presque tout est à 1, le bonus d'identifiants ne change rien.
SELECT nb_identifiers, COUNT(*) AS n,
       AVG(avg_identity_confidence) AS conf_moy
FROM prod.gold.daily_by_logic
GROUP BY 1 ORDER BY 1;

-- ⑤ Profils multi-pays persistants. Signalement vers l'équipe identité,
--    ce n'est PAS une règle métier : le ratio ne les attrape pas.
SELECT entity_key, logic_id, COUNT(DISTINCT day) AS jours_multi
FROM prod.gold.daily_by_logic
WHERE nb_countries > 1
GROUP BY 1,2
HAVING COUNT(DISTINCT day) > 20
ORDER BY jours_multi DESC LIMIT 50;

-- ⑥ Distribution des poids : doit s'étaler, pas s'entasser.
--    Et vérifier qu'AUCUN n'est nul.
SELECT ROUND(weight_filtered, 1) AS w, COUNT(*) AS n
FROM prod.gold.daily_by_logic
GROUP BY 1 ORDER BY 1;

-- ⑦ Journées où les plages se recouvrent presque totalement.
--    ② devra les traiter comme contestées SANS les découper.
SELECT entity_key, day, COUNT(DISTINCT country_code) AS pays,
       MIN(span_start) AS debut, MAX(span_end) AS fin
FROM prod.gold.daily_by_logic
GROUP BY 1,2
HAVING COUNT(DISTINCT country_code) > 1
ORDER BY pays DESC LIMIT 50;
"""
