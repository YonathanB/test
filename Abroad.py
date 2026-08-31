def build_state_anchors(local_candidates, presence_anchors):

    # ---------------------------------------------------------
    # 1. RANGES : meilleur candidat local
    # ---------------------------------------------------------

    ranges = (
        local_candidates
        .where(F.col("candidate_rank") == 1)
        .withColumn(
            "strength_factor",
            F.lit(1.0) - F.exp(-F.col("evidence_score"))
        )
        .withColumn(
            "local_confidence_raw",
            F.col("relative_score") * F.col("strength_factor")
        )
        .select(
            "entity_key",

            F.col("range_from").alias("state_from"),
            F.col("range_to").alias("state_to"),

            "country_code",

            F.col("local_confidence_raw"),
            F.col("evidence_score"),

            F.lit("RANGE").alias("state_origin"),
            F.col("range_id").alias("origin_id"),
        )
    )

    # ---------------------------------------------------------
    # 2. POINTS : plusieurs preuves ponctuelles peuvent arriver
    #    exactement au même instant.
    # ---------------------------------------------------------

    point_scores = (
        presence_anchors
        .groupBy(
            "entity_key",
            "anchor_ts",
            "country_code",
        )
        .agg(
            F.sum("evidence_weight").alias("evidence_score"),
            F.countDistinct("logic_id").alias("logic_count"),
        )
    )

    w = Window.partitionBy(
        "entity_key",
        "anchor_ts",
    )

    w_rank = w.orderBy(
        F.col("evidence_score").desc(),
        F.col("logic_count").desc(),
        F.col("country_code").asc(),
    )

    points = (
        point_scores

        .withColumn(
            "total_score",
            F.sum("evidence_score").over(w)
        )

        .withColumn(
            "relative_score",
            F.col("evidence_score") / F.col("total_score")
        )

        .withColumn(
            "rk",
            F.row_number().over(w_rank)
        )

        .where(F.col("rk") == 1)

        .withColumn(
            "strength_factor",
            F.lit(1.0) - F.exp(-F.col("evidence_score"))
        )

        .withColumn(
            "local_confidence_raw",
            F.col("relative_score") * F.col("strength_factor")
        )

        .select(
            "entity_key",

            F.col("anchor_ts").alias("state_from"),
            F.col("anchor_ts").alias("state_to"),

            "country_code",
            "local_confidence_raw",
            "evidence_score",

            F.lit("POINT").alias("state_origin"),

            F.sha2(
                F.concat_ws(
                    "||",
                    "entity_key",
                    F.col("anchor_ts").cast("string"),
                    "country_code",
                ),
                256
            ).alias("origin_id")
        )
    )

    return ranges.unionByName(points)


def build_state_adjacencies(state_anchors):

    w = (
        Window
        .partitionBy("entity_key")
        .orderBy(
            "state_from",
            "state_to",
            "country_code",
        )
    )

    return (
        state_anchors

        .withColumn(
            "next_country",
            F.lead("country_code").over(w)
        )

        .withColumn(
            "next_from",
            F.lead("state_from").over(w)
        )

        .withColumn(
            "next_to",
            F.lead("state_to").over(w)
        )

        .withColumn(
            "next_confidence_raw",
            F.lead("local_confidence_raw").over(w)
        )

        .withColumn(
            "next_origin_id",
            F.lead("origin_id").over(w)
        )

        .where(F.col("next_country").isNotNull())

        .withColumn(
            "gap_sec",
            F.greatest(
                F.unix_timestamp("next_from")
                - F.unix_timestamp("state_to"),
                F.lit(0)
            )
        )
    )
GAP_DECAY_DAYS = 30.0


def build_same_country_gaps(adjacencies):

    gaps = (
        adjacencies

        .where(
            (F.col("country_code") == F.col("next_country"))
            & (F.col("next_from") > F.col("state_to"))
        )

        .withColumn(
            "gap_days",
            F.col("gap_sec") / F.lit(86400.0)
        )

        .withColumn(
            "boundary_confidence",
            F.least(
                F.col("local_confidence_raw"),
                F.col("next_confidence_raw"),
            )
        )

        # Plus le trou est long, plus on est prudent.
        .withColumn(
            "confidence_raw",
            F.col("boundary_confidence")
            * F.exp(
                -F.col("gap_days") / F.lit(GAP_DECAY_DAYS)
            )
        )

        .select(
            "entity_key",

            F.col("state_to").alias("fragment_from"),
            F.col("next_from").alias("fragment_to"),

            "country_code",

            F.lit("INFERRED").alias("fragment_type"),
            F.lit("SAME_COUNTRY_BETWEEN")
             .alias("inference_method"),

            "confidence_raw",

            F.col("origin_id").alias("left_origin_id"),
            F.col("next_origin_id").alias("right_origin_id"),
        )
    )

    return gaps


def build_observed_fragments(state_anchors):

    return (
        state_anchors
        .select(
            "entity_key",

            F.col("state_from").alias("fragment_from"),
            F.col("state_to").alias("fragment_to"),

            "country_code",

            F.lit("OBSERVED").alias("fragment_type"),
            F.lit("DIRECT").alias("inference_method"),

            F.col("local_confidence_raw").alias("confidence_raw"),

            F.col("origin_id").alias("left_origin_id"),
            F.col("origin_id").alias("right_origin_id"),
        )
    )





def build_inferred_transitions(adjacencies):

    return (
        adjacencies

        .where(
            F.col("country_code") != F.col("next_country")
        )

        .withColumn(
            "transition_confidence_raw",
            F.least(
                F.col("local_confidence_raw"),
                F.col("next_confidence_raw"),
            )
        )

        .withColumn(
            "window_duration_sec",
            F.greatest(
                F.unix_timestamp("next_from")
                - F.unix_timestamp("state_to"),
                F.lit(0),
            )
        )

        .withColumn(
            "window_days",
            F.col("window_duration_sec") / F.lit(86400.0)
        )

        # On peut être sûr qu'un changement a eu lieu,
        # sans savoir précisément quand.
        .withColumn(
            "timing_confidence_raw",
            F.exp(
                -F.col("window_days") / F.lit(7.0)
            )
        )

        .select(
            "entity_key",

            F.col("country_code").alias("country_from"),
            F.col("next_country").alias("country_to"),

            F.col("state_to")
             .alias("transition_window_from"),

            F.col("next_from")
             .alias("transition_window_to"),

            F.lit(None)
             .cast("timestamp")
             .alias("transition_ts"),

            F.lit("INFERRED").alias("transition_type"),

            F.lit("BETWEEN_DIFFERENT_COUNTRIES")
             .alias("inference_method"),

            "transition_confidence_raw",
            "timing_confidence_raw",

            "window_duration_sec",

            F.col("origin_id").alias("left_origin_id"),
            F.col("next_origin_id").alias("right_origin_id"),
        )
    )

def refine_transitions_with_explicit(
    inferred_transitions,
    transition_anchors
):

    i = inferred_transitions.alias("i")
    t = transition_anchors.alias("t")

    matches = (
        i.join(
            t,
            on=(
                (F.col("i.entity_key") == F.col("t.entity_key"))

                & (
                    F.col("i.country_from")
                    == F.col("t.country_from")
                )

                & (
                    F.col("i.country_to")
                    == F.col("t.country_to")
                )

                & (
                    F.col("t.transition_ts")
                    >= F.col("i.transition_window_from")
                )

                & (
                    F.col("t.transition_ts")
                    <= F.col("i.transition_window_to")
                )
            ),
            how="left"
        )
    )

    w = (
        Window
        .partitionBy(
            F.col("i.entity_key"),
            F.col("i.left_origin_id"),
            F.col("i.right_origin_id"),
        )
        .orderBy(
            F.col("t.evidence_weight").desc_nulls_last()
        )
    )

    best = (
        matches

        .withColumn(
            "explicit_rank",
            F.row_number().over(w)
        )

        .where(F.col("explicit_rank") == 1)

        .select(
            F.col("i.entity_key").alias("entity_key"),

            F.col("i.country_from").alias("country_from"),
            F.col("i.country_to").alias("country_to"),

            F.when(
                F.col("t.transition_ts").isNotNull(),
                F.col("t.transition_ts")
            )
            .otherwise(
                F.col("i.transition_window_from")
            )
            .alias("transition_window_from"),

            F.when(
                F.col("t.transition_ts").isNotNull(),
                F.col("t.transition_ts")
            )
            .otherwise(
                F.col("i.transition_window_to")
            )
            .alias("transition_window_to"),

            F.col("t.transition_ts").alias("transition_ts"),

            F.when(
                F.col("t.transition_ts").isNotNull(),
                F.lit("OBSERVED_REFINED")
            )
            .otherwise(
                F.lit("INFERRED")
            )
            .alias("transition_type"),

            F.when(
                F.col("t.transition_ts").isNotNull(),
                F.lit("SILVER_TRANSITION")
            )
            .otherwise(
                F.col("i.inference_method")
            )
            .alias("inference_method"),

            F.when(
                F.col("t.transition_ts").isNotNull(),
                F.greatest(
                    F.col("i.transition_confidence_raw"),
                    F.col("t.evidence_weight"),
                )
            )
            .otherwise(
                F.col("i.transition_confidence_raw")
            )
            .alias("confidence_raw"),

            F.when(
                F.col("t.transition_ts").isNotNull(),
                F.col("t.evidence_weight")
            )
            .otherwise(
                F.col("i.timing_confidence_raw")
            )
            .alias("timing_confidence_raw"),

            F.col("t.silver_event_id")
             .alias("explicit_silver_event_id"),

            F.col("i.left_origin_id").alias("left_origin_id"),
            F.col("i.right_origin_id").alias("right_origin_id"),
        )
    )

    return best


def build_temporal_fragments(
    state_anchors,
    adjacencies
):

    observed = build_observed_fragments(
        state_anchors
    )

    inferred = build_same_country_gaps(
        adjacencies
    )

    return observed.unionByName(inferred)



def merge_fragments_to_segments(fragments):

    w = (
        Window
        .partitionBy("entity_key")
        .orderBy(
            "fragment_from",
            "fragment_to",
        )
    )

    tmp = (
        fragments

        .withColumn(
            "prev_country",
            F.lag("country_code").over(w)
        )

        .withColumn(
            "prev_to",
            F.lag("fragment_to").over(w)
        )

        .withColumn(
            "new_segment",
            F.when(
                F.col("prev_country").isNull(),
                F.lit(1)
            )
            .when(
                F.col("country_code") != F.col("prev_country"),
                F.lit(1)
            )
            .when(
                F.col("fragment_from") > F.col("prev_to"),
                F.lit(1)
            )
            .otherwise(F.lit(0))
        )
    )

    w_running = (
        Window
        .partitionBy("entity_key")
        .orderBy(
            "fragment_from",
            "fragment_to"
        )
        .rowsBetween(
            Window.unboundedPreceding,
            Window.currentRow
        )
    )

    tmp = (
        tmp
        .withColumn(
            "segment_group",
            F.sum("new_segment").over(w_running)
        )
    )

    segments = (
        tmp
        .groupBy(
            "entity_key",
            "segment_group",
            "country_code",
        )
        .agg(
            F.min("fragment_from").alias("segment_from"),
            F.max("fragment_to").alias("segment_to"),

            F.min("confidence_raw").alias("confidence_raw"),

            F.max(
                F.when(
                    F.col("fragment_type") == "OBSERVED",
                    1
                ).otherwise(0)
            ).alias("has_observed"),

            F.max(
                F.when(
                    F.col("fragment_type") == "INFERRED",
                    1
                ).otherwise(0)
            ).alias("has_inferred"),
        )

        .withColumn(
            "result_type",
            F.when(
                (F.col("has_observed") == 1)
                & (F.col("has_inferred") == 1),
                F.lit("MIXED")
            )
            .when(
                F.col("has_observed") == 1,
                F.lit("OBSERVED")
            )
            .otherwise(
                F.lit("INFERRED")
            )
        )

        .withColumn(
            "segment_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    "entity_key",
                    "country_code",
                    F.col("segment_from").cast("string"),
                    F.col("segment_to").cast("string"),
                    F.lit(RULESET_VERSION),
                ),
                256
            )
        )
    )

    return segments


def temporal_engine(stage1):

    state_anchors = build_state_anchors(
        stage1["local_candidates"],
        stage1["presence_anchors"],
    )

    adjacencies = build_state_adjacencies(
        state_anchors
    )

    fragments = build_temporal_fragments(
        state_anchors,
        adjacencies,
    )

    segments = merge_fragments_to_segments(
        fragments
    )

    inferred_transitions = build_inferred_transitions(
        adjacencies
    )

    transitions = refine_transitions_with_explicit(
        inferred_transitions,
        stage1["transition_anchors"],
    )

    return {
        "state_anchors": state_anchors,
        "adjacencies": adjacencies,

        "segments": segments,
        "transitions": transitions,
    }






"""
ÉTAPE ② — de Silver aux tables de service, en une passe.

Il n'y a plus d'étape ①. ② lit Silver directement.

CHAÎNE :
    silver.location_events
        → 2b  poids PAR IDENTIFIANT      (la pollution ne contamine que sa part)
        → 2c  scores par (personne, jour, pays) + bornes + comptage de logiques
        → 2d  test de recouvrement       (journée à mouvement ou non ?)
        → 2e  découpage en plages        (points de rupture)
        → 2f  ancrage sur les transitions
        → 2g  timeline continue + trous  (heures et jours, même mécanisme)
        → 2h  projection sur les jours   (grille dense)
        → 2i  écriture des 4 tables de service

PRINCIPE : le système note, il ne tranche pas. Aucun score n'est jamais nul.
"""

from pyspark.sql import functions as F, Window

# ═════════════════════════════════════════════════════════════════════
# PARAMÈTRES
# ═════════════════════════════════════════════════════════════════════
RULESET_VERSION = "v0.2"

VOLUME_REF       = 20     # saturation du facteur de volume — à recalibrer
LOGIC_BONUS      = 0.6    # bonus par logique indépendante supplémentaire
LOGIC_CAP        = 2.5
POLLUTION_FLOOR  = 0.02   # dépréciation, PAS exclusion
MISSING_IDENT_CONF = 0.50 # confiance d'identité absente : ni récompensée ni nulle
OVERLAP_MAX      = 0.70   # au-dessus : journée sans mouvement, on ne découpe pas
TRANSITION_CONF  = 1.00   # une transition est rattachée à la personne : un seul doute
TRANSITION_WEIGHT = 3.0   # poids de l'ancre. Domine sans écraser.


# ═════════════════════════════════════════════════════════════════════
# 2a — LECTURE
# ═════════════════════════════════════════════════════════════════════
def read_silver(spark, day_from, day_to):
    """
    Filtre sur les personnes résolues : les traces orphelines n'ont pas de
    parcours. Elles restent consultables dans Silver, elles n'entrent pas ici.
    C'est ce filtre qui rend tout l'aval bon marché.
    """
    base = (
        spark.table("prod.silver.location_events")
        .where(
            (F.col("observation_date") >= F.to_date(F.lit(day_from)))
            & (F.col("observation_date") <= F.to_date(F.lit(day_to)))
            & F.col("person_id").isNotNull()
        )
        # is_non_individual est désormais porté par Silver.
        # non_ind_ref_version dit QUAND le flag a été rafraîchi : il est
        # rétroactif, donc une valeur périmée doit rester détectable.
        .withColumn("is_non_individual",
                    F.coalesce(F.col("is_non_individual"), F.lit(False)))
        .withColumn("identity_confidence",
                    F.coalesce(F.col("identity_confidence"),
                               F.lit(MISSING_IDENT_CONF)))
    )

    presences = base.where(
        (F.col("event_type") == "PRESENCE") & F.col("country_code").isNotNull()
    )
    transitions = base.where(F.col("event_type") == "TRANSITION")

    return presences, transitions


# ═════════════════════════════════════════════════════════════════════
# 2b — POIDS PAR IDENTIFIANT
# ═════════════════════════════════════════════════════════════════════
def weight_per_identifier(presences):
    """
    Le poids se calcule ligne par ligne, JAMAIS sur des moyennes.

    C'est ce qui empêche la contamination : un identifiant pollué parmi cinq
    ne déprécie que sa propre contribution. Les quatre autres gardent la leur.
    Une moyenne de confiance d'identité aurait dilué un 0,54 dans un 0,96 —
    ici chaque identifiant apporte exactement ce qu'il vaut.
    """
    volume_factor = F.lit(0.5) + F.lit(0.5) * F.least(
        F.sqrt(F.col("evidence_count")) / F.sqrt(F.lit(float(VOLUME_REF))),
        F.lit(1.0),
    )

    return (
        presences
        .withColumn("volume_factor", volume_factor)
        .withColumn(
            "w_raw",
            F.col("source_confidence")
            * F.col("identity_confidence")
            * F.col("volume_factor"),
        )
        .withColumn(
            "w",
            F.when(F.col("is_non_individual"),
                   F.col("w_raw") * F.lit(POLLUTION_FLOOR))
             .otherwise(F.col("w_raw")),
        )
        .withColumn(
            "reason_code",
            F.when(F.col("is_non_individual"), F.lit("NON_INDIVIDUAL"))
             .otherwise(F.lit(None).cast("string")),
        )
        .select(
            "person_id", "observation_date", "country_code",
            "identifier_norm", "identifier_type", "identity_confidence",
            "logic_id", "source_id", "source_confidence",
            "evidence_count", "valid_from", "valid_to",
            "is_non_individual", "reason_code",
            "w_raw", "w",
        )
    )


# ═════════════════════════════════════════════════════════════════════
# 2c — SCORE PAR (personne, jour, pays)
# ═════════════════════════════════════════════════════════════════════
def country_day_scores(wid):
    """
    Somme des contributions individuelles, puis bonus d'accord entre logiques.

    La hiérarchie de force exige DEUX compteurs distincts : plusieurs logiques
    indépendantes qui concordent valent bien plus que plusieurs identifiants
    d'une même logique. Le bonus porte donc sur nb_logics, pas sur nb_identifiers
    — ce dernier est déjà reflété dans la somme des poids.
    """
    agg = (
        wid.groupBy("person_id", "observation_date", "country_code")
        .agg(
            F.sum("w").alias("w_sum"),
            F.sum("w_raw").alias("w_sum_raw"),
            F.sum("evidence_count").alias("evidence_count"),
            F.countDistinct("identifier_norm").alias("nb_identifiers"),
            F.countDistinct("logic_id").alias("nb_logics"),
            F.min("identity_confidence").alias("min_identity_confidence"),
            F.min("valid_from").alias("span_start"),
            F.max("valid_to").alias("span_end"),
            F.sum(F.when(F.col("is_non_individual"), 1).otherwise(0))
             .alias("nb_polluted"),
        )
    )

    logic_factor = F.least(
        F.lit(1.0) + F.lit(LOGIC_BONUS) * (F.col("nb_logics") - 1),
        F.lit(LOGIC_CAP),
    )

    return agg.withColumn("score", F.col("w_sum") * logic_factor)


# ═════════════════════════════════════════════════════════════════════
# 2d — TEST DE RECOUVREMENT : la journée a-t-elle du mouvement ?
# ═════════════════════════════════════════════════════════════════════
def overlap_test(scores):
    """
    Jaccard temporel entre les bornes des pays candidats du jour.

    Si deux pays couvrent quasiment la même journée, DÉCOUPER SERAIT MENTIR :
    on fabriquerait une alternance A/B/A/B alors qu'on ignore quand chaque
    événement a eu lieu à l'intérieur de sa plage. C'est une journée contestée,
    pas une journée de déplacement.
    """
    w = Window.partitionBy("person_id", "observation_date")

    return (
        scores
        .withColumn("nb_countries", F.count("*").over(w))
        .withColumn("day_start", F.min("span_start").over(w))
        .withColumn("day_end", F.max("span_end").over(w))
        # intersection : le plus tardif des débuts, le plus précoce des fins
        .withColumn("inter_start", F.max("span_start").over(w))
        .withColumn("inter_end", F.min("span_end").over(w))
        .withColumn(
            "overlap",
            F.when(F.col("nb_countries") < 2, F.lit(0.0)).otherwise(
                F.greatest(
                    F.unix_timestamp("inter_end") - F.unix_timestamp("inter_start"),
                    F.lit(0),
                )
                / F.greatest(
                    F.unix_timestamp("day_end") - F.unix_timestamp("day_start"),
                    F.lit(1),
                )
            ),
        )
        .withColumn(
            "is_segmentable",
            (F.col("nb_countries") > 1) & (F.col("overlap") < OVERLAP_MAX),
        )
    )


# ═════════════════════════════════════════════════════════════════════
# 2e — DÉCOUPAGE EN PLAGES
# ═════════════════════════════════════════════════════════════════════
def build_ranges(wid, day_flags, transitions):
    """
    Points de rupture = toutes les bornes d'intervalle + tous les instants
    de transition. Entre deux ruptures consécutives, l'ensemble des preuves
    actives est constant : une seule ligne suffit à décrire la plage.

    On ne découpe QUE les journées jugées segmentables (2d).
    """
    segmentable = (
        day_flags.where(F.col("is_segmentable"))
        .select("person_id", "observation_date").distinct()
    )

    ev = wid.join(segmentable, ["person_id", "observation_date"], "inner")

    cuts_ev = (
        ev.select("person_id", "observation_date",
                  F.col("valid_from").alias("t"))
        .union(ev.select("person_id", "observation_date",
                         F.col("valid_to").alias("t")))
    )

    cuts_tr = (
        transitions.join(segmentable, ["person_id", "observation_date"], "inner")
        .select("person_id", "observation_date",
                F.col("valid_from").alias("t"))
    )

    cuts = cuts_ev.union(cuts_tr).distinct()

    w = Window.partitionBy("person_id", "observation_date").orderBy("t")

    ranges = (
        cuts
        .withColumn("range_end", F.lead("t").over(w))
        .withColumnRenamed("t", "range_start")
        .where(F.col("range_end").isNotNull())
        .withColumn("range_seq", F.row_number().over(w))
    )

    # une preuve est active sur une plage si son intervalle la recouvre.
    # ⚠️ HYPOTHÈSE : on suppose l'identifiant présent en continu entre ses
    #    propres bornes. Silver ne donne que première et dernière observation.
    #    Cette hypothèse est ÉTIQUETÉE plus bas, pas dissimulée.
    active = (
        ranges.join(ev, ["person_id", "observation_date"], "inner")
        .where(
            (F.col("valid_from") <= F.col("range_start"))
            & (F.col("valid_to") >= F.col("range_end"))
        )
    )

    return ranges, active


# ═════════════════════════════════════════════════════════════════════
# 2f — ANCRAGE SUR LES TRANSITIONS
# ═════════════════════════════════════════════════════════════════════
def transition_anchors(transitions):
    """
    Une transition N'EST PAS convertie en présence dans Silver.
    Elle reste une transition ; on lui donne ici, au moment du calcul,
    un poids d'ancre sur l'intervalle qui suit.

    Elle est rattachée DIRECTEMENT à la personne : pas d'identifiant
    intermédiaire, donc une seule incertitude au lieu de deux. C'est ce qui
    justifie TRANSITION_WEIGHT et identity_confidence = 1,0.

    Règle : après T, le pays est `country_to` jusqu'à la transition suivante.
            Avant T, il est `country_from`.
    """
    w = Window.partitionBy("person_id").orderBy("valid_from")

    return (
        transitions
        .select(
            "person_id", "observation_date",
            F.col("valid_from").alias("t"),
            "country_from", "country_to",
            "source_id", "source_confidence", "event_id",
        )
        .withColumn("next_t", F.lead("t").over(w))
        .withColumn("prev_t", F.lag("t").over(w))
        .withColumn("anchor_weight", F.lit(TRANSITION_WEIGHT))
        .withColumn("identity_confidence", F.lit(TRANSITION_CONF))
    )


def apply_anchors(active, anchors):
    """
    Le pays d'ancre reçoit un poids supplémentaire sur les plages couvertes.
    Les autres pays ne sont PAS annulés — ils restent visibles avec leur score.
    Le système note, l'utilisateur choisit.
    """
    a = anchors.select(
        "person_id",
        F.col("t").alias("anchor_t"),
        F.coalesce(F.col("next_t"), F.lit("2999-12-31").cast("timestamp"))
         .alias("anchor_until"),
        F.col("country_to").alias("country_code"),
        "anchor_weight",
    )

    return (
        active.join(a, ["person_id", "country_code"], "left")
        .withColumn(
            "w_anchored",
            F.when(
                (F.col("anchor_t").isNotNull())
                & (F.col("range_start") >= F.col("anchor_t"))
                & (F.col("range_start") < F.col("anchor_until")),
                F.col("w") + F.col("anchor_weight"),
            ).otherwise(F.col("w")),
        )
        .withColumn("is_anchored",
                    F.col("w_anchored") > F.col("w"))
    )


# ═════════════════════════════════════════════════════════════════════
# 2g — SCORES PAR PLAGE
# ═════════════════════════════════════════════════════════════════════
def range_scores(anchored):
    """
    top1 / top2 : le pays le mieux noté et l'autre possibilité.
    Deux colonnes plates, pas de MAP : lisible en HQL.
    nb_candidates prévient s'il y en a davantage.
    """
    agg = (
        anchored.groupBy(
            "person_id", "observation_date", "range_seq",
            "range_start", "range_end", "country_code",
        )
        .agg(
            F.sum("w_anchored").alias("score"),
            F.countDistinct("logic_id").alias("nb_logics"),
            F.countDistinct("identifier_norm").alias("nb_identifiers"),
            F.max("is_anchored").alias("is_anchored"),
            F.sum(F.when(F.col("is_non_individual"), 1).otherwise(0))
             .alias("nb_polluted"),
        )
    )

    w = Window.partitionBy("person_id", "observation_date", "range_seq")
    w_rank = w.orderBy(
        F.col("score").desc(),
        F.col("nb_logics").desc(),
        F.col("country_code").asc(),   # départage déterministe, sinon le
    )                                   # résultat bascule d'un run à l'autre

    ranked = (
        agg
        .withColumn("total_score", F.sum("score").over(w))
        .withColumn("nb_candidates", F.count("*").over(w))
        .withColumn("evidence_strength", F.col("total_score"))
        .withColumn("rk", F.row_number().over(w_rank))
        .withColumn("probability", F.col("score") / F.col("total_score"))
    )

    top1 = ranked.where(F.col("rk") == 1).select(
        "person_id", "observation_date", "range_seq", "range_start", "range_end",
        F.col("country_code").alias("country_top1"),
        F.col("probability").alias("score_top1"),
        "nb_logics", "nb_identifiers", "nb_candidates",
        "evidence_strength", "is_anchored", "nb_polluted",
    )
    top2 = ranked.where(F.col("rk") == 2).select(
        "person_id", "observation_date", "range_seq",
        F.col("country_code").alias("country_top2"),
        F.col("probability").alias("score_top2"),
    )

    return top1.join(top2, ["person_id", "observation_date", "range_seq"], "left")


# ═════════════════════════════════════════════════════════════════════
# 2h — TIMELINE CONTINUE ET TROUS
# ═════════════════════════════════════════════════════════════════════
def fill_gaps(segments):
    """
    Un trou de 12 heures et un trou de 20 jours sont le MÊME objet.
    Le jour n'est pas une unité de raisonnement, c'est une projection finale.

    On ne comble pas en silence : chaque intervalle inféré porte son étiquette
    et la durée exacte du trou de chaque côté. Le consommateur coupe où il veut,
    on ne fixe aucune borne à sa place.
    """
    w = Window.partitionBy("person_id").orderBy("range_start")

    return (
        segments
        .withColumn("prev_country", F.lag("country_top1").over(w))
        .withColumn("prev_end", F.lag("range_end").over(w))
        .withColumn("next_country", F.lead("country_top1").over(w))
        .withColumn("next_start", F.lead("range_start").over(w))
        .withColumn(
            "gap_before_sec",
            F.unix_timestamp("range_start") - F.unix_timestamp("prev_end"),
        )
        .withColumn(
            "gap_after_sec",
            F.unix_timestamp("next_start") - F.unix_timestamp("range_end"),
        )
        .withColumn(
            "inference_method",
            F.when(F.col("is_anchored"), F.lit("ANCHORED"))
             .when(F.col("prev_country") == F.col("next_country"),
                   F.lit("NEAREST_MATCH"))
             .when(F.col("prev_country") != F.col("next_country"),
                   F.lit("NEAREST_DIVERGENT"))
             .when(F.col("prev_country").isNotNull(),
                   F.lit("NEAREST_BEFORE_ONLY"))
             .when(F.col("next_country").isNotNull(),
                   F.lit("NEAREST_AFTER_ONLY"))
             .otherwise(F.lit("OBSERVED")),
        )
    )


# ═════════════════════════════════════════════════════════════════════
# 2i — PROJECTION SUR LES JOURS (grille dense)
# ═════════════════════════════════════════════════════════════════════
def project_to_days(spark, segments, day_from, day_to):
    """
    Grille dense : une ligne par personne et par jour, SANS TROU.
    L'analyste SQL qui requête un jour sans donnée doit obtenir une réponse
    et une explication, jamais zéro ligne.

    day_share est calculé sur les segments, PAS sur les intervalles
    d'observation : valid_from/valid_to sont des bornes d'OBSERVATION, pas
    de présence. Une personne observée de 12h à 14h était probablement là
    depuis minuit — le segment le sait, l'observation ne le sait pas.
    """
    persons = segments.select("person_id").distinct()
    days = spark.sql(
        f"SELECT explode(sequence(to_date('{day_from}'), "
        f"to_date('{day_to}'), interval 1 day)) AS day"
    )
    grid = persons.crossJoin(days)

    per_day = (
        segments
        .withColumn("day", F.to_date("range_start"))
        .withColumn(
            "duration_sec",
            F.unix_timestamp("range_end") - F.unix_timestamp("range_start"),
        )
        .groupBy("person_id", "day", "country_top1")
        .agg(
            F.sum("duration_sec").alias("country_sec"),
            F.max("score_top1").alias("score_top1"),
            F.max("evidence_strength").alias("evidence_strength"),
            F.max("nb_logics").alias("nb_logics"),
            F.max("nb_candidates").alias("nb_candidates"),
            F.min("inference_method").alias("worst_inference_method"),
            F.count("*").alias("nb_segments"),
        )
    )

    w = Window.partitionBy("person_id", "day")
    daily = (
        per_day
        .withColumn("day_total_sec", F.sum("country_sec").over(w))
        .withColumn("day_share", F.col("country_sec") / F.col("day_total_sec"))
        .withColumn("nb_countries_in_day", F.count("*").over(w))
        .withColumn(
            "rk",
            F.row_number().over(
                w.orderBy(F.col("country_sec").desc(),
                          F.col("country_top1").asc())
            ),
        )
        .where(F.col("rk") == 1)
        .drop("rk")
    )

    return (
        grid.join(daily, ["person_id", "day"], "left")
        .withColumn(
            "inference_method",
            F.coalesce(F.col("worst_inference_method"), F.lit("UNKNOWN")),
        )
        .withColumn(
            "explanation",
            F.when(F.col("country_top1").isNull(),
                   F.lit("Aucune donnée pour cette personne ce jour-là."))
             .when(F.col("nb_countries_in_day") > 1,
                   F.concat_ws(
                       "",
                       F.lit("Journée à plusieurs pays ("),
                       F.col("nb_countries_in_day").cast("string"),
                       F.lit("). Dominant : "), F.col("country_top1"),
                       F.lit(", part de la journée "),
                       F.round(F.col("day_share"), 2).cast("string"),
                       F.lit(". Voir le détail par plage."),
                   ))
             .otherwise(
                   F.concat_ws(
                       "",
                       F.col("country_top1"),
                       F.lit(" — "), F.col("nb_logics").cast("string"),
                       F.lit(" logique(s), score "),
                       F.round(F.col("score_top1"), 2).cast("string"),
                   )),
        )
        .withColumn("ruleset_version", F.lit(RULESET_VERSION))
    )


# ═════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ═════════════════════════════════════════════════════════════════════
def run(spark, day_from, day_to):
    presences, transitions = read_silver(spark, day_from, day_to)

    wid       = weight_per_identifier(presences)
    scores    = country_day_scores(wid)
    day_flags = overlap_test(scores)
    anchors   = transition_anchors(transitions)

    ranges, active = build_ranges(wid, day_flags, transitions)
    anchored       = apply_anchors(active, anchors)
    segments       = fill_gaps(range_scores(anchored))
    daily          = project_to_days(spark, segments, day_from, day_to)

    return {
        "person_segment":         segments,
        "person_day":             daily,
        "person_day_identifier":  wid,        # déjà plat : une ligne = un identifiant
        "person_transition":      anchors,
        "contested_days":         day_flags.where(~F.col("is_segmentable")
                                                  & (F.col("nb_countries") > 1)),
    }


# ═════════════════════════════════════════════════════════════════════
# CONTRÔLES
# ═════════════════════════════════════════════════════════════════════
CHECKS = """
-- ① Journées contestées SANS mouvement : le cas A/B.
--    Si cette part est forte, OVERLAP_MAX est mal réglé.
SELECT COUNT(*) FROM contested_days;

-- ② Distribution de score_top1. Doit s'étaler.
--    Un pic à 1,00 signale des journées à source unique : vérifier
--    evidence_strength, car une source faible et seule sort AUSSI à 1,00.
SELECT ROUND(score_top1, 1) AS s, COUNT(*) FROM person_segment GROUP BY 1 ORDER BY 1;

-- ③ Saturation du facteur de volume. Si la part est forte, remonter VOLUME_REF.
SELECT SUM(CASE WHEN evidence_count >= 20 THEN 1 ELSE 0 END)/COUNT(*)
FROM person_day_identifier;

-- ④ Part de la grille dense réellement observée vs inférée.
SELECT inference_method, COUNT(*) FROM person_day GROUP BY 1 ORDER BY 2 DESC;

-- ⑤ Vérifier qu'aucun score n'est nul.
SELECT MIN(w) FROM person_day_identifier;   -- doit être > 0
"""




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
                      source_ids: ARRAY<STRING>,
                      identity_confidence: DECIMAL(3,2),
                      evidence_count: BIGINT,
                      valid_from: TIMESTAMP,
                      valid_to: TIMESTAMP,
                      is_non_individual: BOOLEAN
                    >>,

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
            # une ligne par identifiant à ce stade : somme simple correcte
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
                    "identifier_norm", "identifier_type", "source_ids",
                    "identity_confidence", "evidence_count",
                    "valid_from", "valid_to", "is_non_individual",
                )
            ).alias("identifiers"),
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
            "identifiers",
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
