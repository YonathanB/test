"""Construction des hypotheses de sejour, avant scoring -- PySpark 3.3+.

Contrat d'entree (une ligne par evenement, event_id unique par entity_key):
  entity_key, is_resolved (boolean), event_id, event_type,
  raw_identifier, identifier_type, valid_from, valid_to,
  country_code (PRESENCE), country_from/country_to (TRANSITION).
Les autres colonnes sont conservees dans evidence_payload.

Temps: valid_from/valid_to sont des timestamps. Pour une presence, valid_to
est EXCLUSIF; si null/egal au debut, il s'agit d'une observation ponctuelle.
Un range couvre [start_ts, end_ts), avec aussi le point end_ts si
end_inclusive=true (derniere observation ponctuelle). is_point=true indique
un range reduit a un point. Les FROM soutiennent separement la borne gauche
de la transition, meme si cette borne est exclusive pour les autres preuves.
Pour une transition: valid_from = depart, valid_to = arrivee (null => depart).
Si les deux instants sont egaux, FROM precede la coupure et TO la suit,
sans ajouter une duree artificielle. Aucune inference apres la derniere
borne connue. Adapter les dates inclusives en amont (fin + 1 jour).

Regles:
* prolonger le dernier pays jusqu'au prochain pays observe;
* construire par identifiant, puis aussi par entity_key resolue;
* les transitions de l'identite coupent toutes les continuites;
* fusionner un meme pays seulement si les intervalles se touchent/chevauchent
  ET appartiennent a la meme periode entre coupures;
* conserver les alternatives simultanees et les preuves contradictoires;
* garder les inferences a part des preuves brutes, pour eviter double comptage.

Le module ne choisit pas le pays gagnant et n'invente aucune formule de score.
Il ne fait aucune ecriture Hive/Iceberg et ne modifie pas le SparkSession.
Pour un recalcul partiel, fournir toutes les observations et les transitions
necessaires au scope, y compris les bornes voisines. Un simple filtre au mois
ne suffit pas a reconstruire une continuite commencant avant ce mois.
"""

from dataclasses import dataclass, field
from typing import List

from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


KEYS = ["entity_key", "scope_type", "scope_key", "epoch_key"]


def _hash(*columns):
    return F.sha2(F.to_json(F.struct(*columns)), 256)


def prepare_events(silver):
    """Normaliser sans perdre les colonnes originales; retourner valides/rejets.

    Les identifiants d'evenement doivent etre uniques et stables dans une entite.
    is_resolved doit etre coherent pour toutes les lignes d'une entity_key.
    """
    required = {
        "entity_key", "is_resolved", "event_id", "event_type", "valid_from",
        "raw_identifier", "identifier_type",
    }
    missing = required.difference(silver.columns)
    if missing:
        raise ValueError("Colonnes manquantes: " + ", ".join(sorted(missing)))

    def optional(name, dtype="string"):
        return F.col(name).cast(dtype) if name in silver.columns else F.lit(None).cast(dtype)

    df = silver.select(
        F.col("entity_key").cast("string").alias("entity_key"),
        F.col("is_resolved").cast("boolean").alias("is_resolved"),
        F.col("event_id").cast("string").alias("event_id"),
        F.col("raw_identifier").cast("string").alias("raw_identifier"),
        F.col("identifier_type").cast("string").alias("identifier_type"),
        F.upper(F.col("event_type")).alias("event_type"),
        F.col("valid_from").cast("timestamp").alias("valid_from"),
        F.coalesce(optional("valid_to", "timestamp"), F.col("valid_from").cast("timestamp")).alias("valid_to"),
        F.upper(optional("country_code")).alias("country_code"),
        F.upper(optional("country_from")).alias("country_from"),
        F.upper(optional("country_to")).alias("country_to"),
        F.struct(*[F.col(c) for c in silver.columns]).alias("evidence_payload"),
    ).withColumn("event_type", F.when(F.col("event_type") == "TRANSITIONS", "TRANSITION").otherwise(F.col("event_type")))

    reason = (
        F.when(F.col("entity_key").isNull() | (F.length("entity_key") == 0), "MISSING_ENTITY_KEY")
        .when(F.col("event_id").isNull() | (F.length("event_id") == 0), "MISSING_EVENT_ID")
        .when(F.col("is_resolved").isNull(), "MISSING_RESOLUTION_STATUS")
        .when(F.col("event_type").isNull() | ~F.col("event_type").isin("PRESENCE", "TRANSITION"), "INVALID_EVENT_TYPE")
        .when(F.col("valid_from").isNull() | F.col("valid_to").isNull(), "INVALID_TIMESTAMP")
        .when(F.col("valid_to") < F.col("valid_from"), "END_BEFORE_START")
        .when(~F.col("is_resolved") & F.col("raw_identifier").isNull(), "UNRESOLVED_WITHOUT_IDENTIFIER")
        .when(F.col("raw_identifier").isNotNull() & F.col("identifier_type").isNull(), "MISSING_IDENTIFIER_TYPE")
        .when((F.col("event_type") == "PRESENCE") & F.col("country_code").isNull(), "MISSING_PRESENCE_COUNTRY")
        .when((F.col("event_type") == "TRANSITION") & (F.col("country_from").isNull() | F.col("country_to").isNull()), "MISSING_TRANSITION_COUNTRY")
    )
    checked = df.withColumn("rejection_reason", reason)
    return checked.filter("rejection_reason IS NULL").drop("rejection_reason"), checked.filter("rejection_reason IS NOT NULL")


def _event_anchors(events):
    """Presence = intervalle/point; transition = deux preuves ponctuelles."""
    identity = ["entity_key", "is_resolved", "event_id", "raw_identifier", "identifier_type"]
    presence = events.filter(F.col("event_type") == "PRESENCE").select(
        *identity, "country_code", F.lit("PRESENCE").alias("anchor_kind"),
        F.col("valid_from").alias("ts"), F.col("valid_to").alias("known_end"),
    )
    transition = events.filter(F.col("event_type") == "TRANSITION")
    departure = transition.select(
        *identity, F.col("country_from").alias("country_code"),
        F.lit("FROM").alias("anchor_kind"), F.col("valid_from").alias("ts"),
        F.col("valid_from").alias("known_end"),
    )
    arrival = transition.select(
        *identity, F.col("country_to").alias("country_code"),
        F.lit("TO").alias("anchor_kind"), F.col("valid_to").alias("ts"),
        F.col("valid_to").alias("known_end"),
    )
    return presence.unionByName(departure).unionByName(arrival).withColumn(
        "anchor_id", _hash(F.col("entity_key"), F.col("event_id"), F.col("anchor_kind"))
    )


def build_scoped_anchors(events):
    """Distribuer les coupures par balayage de l'entite, sans join ids x voyages.

    Ordre a T: FROM, coupure, puis PRESENCE/TO. Ainsi un FROM appartient
    au sejour se terminant a T et un TO au sejour commencant a T.
    """
    anchors = _event_anchors(events).withColumn(
        "endpoint", F.struct("anchor_id", "event_id", "anchor_kind", "raw_identifier", "identifier_type", "ts")
    )
    endpoint_type = anchors.schema["endpoint"].dataType
    cuts = anchors.filter(F.col("anchor_kind").isin("FROM", "TO")).groupBy("entity_key", "ts").agg(
        F.min("endpoint").alias("cut_endpoint")
    ).withColumn("cut_id", _hash(F.col("entity_key"), F.col("ts")))

    anchor_rows = anchors.withColumn("phase", F.when(F.col("anchor_kind") == "FROM", -1).otherwise(1)).withColumn(
        "cut_id", F.lit(None).cast("string")
    ).withColumn("cut_endpoint", F.lit(None).cast(endpoint_type))
    cut_rows = cuts.select(*[
        F.col(c) if c in cuts.columns else (
            F.lit(0).alias("phase") if c == "phase" else F.lit(None).cast(anchor_rows.schema[c].dataType).alias(c)
        )
        for c in anchor_rows.columns
    ])
    stream = anchor_rows.unionByName(cut_rows)
    order = Window.partitionBy("entity_key").orderBy("ts", "phase", "anchor_id")
    before = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    after = order.rowsBetween(1, Window.unboundedFollowing)
    tagged = (
        stream.withColumn("epoch_key", F.coalesce(F.last("cut_id", ignorenulls=True).over(before), F.lit("BEFORE_FIRST_TRANSITION")))
        .withColumn("epoch_end", F.min(F.when(F.col("phase") == 0, F.col("ts"))).over(after))
        .withColumn("next_cut_endpoint", F.first("cut_endpoint", ignorenulls=True).over(after))
        .filter(F.col("phase") != 0)
        .withColumn("known_end", F.least("known_end", "epoch_end"))
        .drop("phase", "cut_id", "cut_endpoint")
    )

    per_id = tagged.filter(F.col("raw_identifier").isNotNull()).withColumn("scope_type", F.lit("IDENTIFIER")).withColumn(
        "scope_key", _hash(F.col("identifier_type"), F.col("raw_identifier"))
    )
    per_entity = tagged.filter("is_resolved").withColumn("scope_type", F.lit("ENTITY")).withColumn("scope_key", F.col("entity_key"))
    return per_id.unionByName(per_entity)


def build_candidate_ranges(anchors):
    """Compresser les observations successives, en preservant les conflits a T.

    Les pays presents au meme instant forment un ensemble de candidats.
    Un changement de cet ensemble demarre un bloc; aucune preference alphabetique.
    """
    points = anchors.groupBy(*KEYS, "ts").agg(
        F.sort_array(F.collect_set("country_code")).alias("countries"),
        F.first("epoch_end", ignorenulls=True).alias("epoch_end"),
        F.first("next_cut_endpoint", ignorenulls=True).alias("next_cut_endpoint"),
        F.min("endpoint").alias("first_endpoint"),
    )
    order = Window.partitionBy(*KEYS).orderBy("ts")
    prefix = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    points = points.withColumn("previous_countries", F.lag("countries").over(order)).withColumn(
        "new_block", F.when(F.col("previous_countries").isNull() | (F.col("countries") != F.col("previous_countries")), 1).otherwise(0)
    ).withColumn("block_no", F.sum("new_block").over(prefix))
    block_keys = KEYS + ["block_no"]
    blocks = points.groupBy(*block_keys).agg(
        F.min("ts").alias("start_ts"), F.first("epoch_end", ignorenulls=True).alias("epoch_end"),
        F.first("next_cut_endpoint", ignorenulls=True).alias("next_cut_endpoint"),
        F.min(F.struct("ts", "first_endpoint")).alias("first_at"),
    )
    by_block = Window.partitionBy(*KEYS).orderBy("block_no")
    blocks = blocks.withColumn("next_start", F.lead("start_ts").over(by_block)).withColumn(
        "next_endpoint", F.lead("first_at").over(by_block)
    ).withColumn("boundary_endpoint", F.coalesce(F.col("next_endpoint.first_endpoint"), F.col("next_cut_endpoint")))
    located = anchors.join(points.select(*KEYS, "ts", "block_no"), KEYS + ["ts"], "inner")
    extents = located.groupBy(*block_keys, "country_code").agg(
        F.max("known_end").alias("observed_end"), F.first("is_resolved").alias("is_resolved"),
        F.max(F.when((F.col("anchor_kind") != "FROM") & (F.col("known_end") == F.col("ts")), F.col("ts"))).alias("last_point_ts"),
    )
    candidates = (
        extents.join(blocks, block_keys, "inner")
        .withColumn("end_ts", F.coalesce("next_start", "epoch_end", "observed_end"))
        .withColumn("end_reason", F.when(F.col("next_start").isNotNull(), "NEXT_OBSERVATION")
                    .when(F.col("epoch_end").isNotNull(), "TRANSITION").otherwise("OBSERVED_END"))
        .withColumn("is_point", F.col("start_ts") == F.col("end_ts"))
        .withColumn("end_inclusive", F.coalesce(F.col("last_point_ts") == F.col("end_ts"), F.lit(False)))
        .withColumn("candidate_id", _hash(*[F.col(c) for c in block_keys + ["country_code", "start_ts", "end_ts"]]))
        .select(*block_keys, "candidate_id", "country_code", "is_resolved", "start_ts", "end_ts", "is_point", "end_inclusive", "last_point_ts", "end_reason", "boundary_endpoint")
    )
    membership = located.join(
        candidates.select(*block_keys, "country_code", "candidate_id", "end_ts", "boundary_endpoint"),
        block_keys + ["country_code"], "inner",
    )
    # Une inference remplit seulement un trou non couvert par les preuves du bloc.
    # Le max cumule est necessaire quand plusieurs presences se chevauchent.
    order = Window.partitionBy("candidate_id").orderBy("ts", "anchor_id")
    prefix = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    coverage = membership.withColumn("coverage_owner", F.max(F.struct("known_end", "endpoint")).over(prefix)).withColumn(
        "covered_to", F.col("coverage_owner.known_end")
    ).withColumn(
        "next_anchor", F.lead("endpoint").over(order)
    )
    inferences = (
        coverage.withColumn("inference_start", F.greatest("ts", F.least("covered_to", "end_ts")))
        .withColumn("inference_end", F.coalesce(F.col("next_anchor.ts"), F.col("end_ts")))
        .withColumn("right_endpoint", F.coalesce("next_anchor", "boundary_endpoint"))
        .filter(F.col("inference_end") > F.col("inference_start"))
        .withColumn("inference_id", _hash(F.col("candidate_id"), F.col("inference_start"), F.col("inference_end")))
        .select("inference_id", "candidate_id", *KEYS, "country_code",
                F.col("inference_start").alias("start_ts"), F.col("inference_end").alias("end_ts"),
                F.col("coverage_owner.endpoint").alias("left_endpoint"), "right_endpoint")
    )
    candidate_anchors = membership.select("candidate_id", "anchor_id", "event_id", "anchor_kind", "raw_identifier", "identifier_type")
    return candidates, inferences, candidate_anchors


def merge_country_ranges(candidates):
    """Union des intervalles par pays ET epoch. Jamais min/max par pays seul."""
    keys = ["entity_key", "epoch_key", "country_code"]
    order = Window.partitionBy(*keys).orderBy("start_ts", "end_ts", "candidate_id")
    before = order.rowsBetween(Window.unboundedPreceding, -1)
    prefix = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    numbered = candidates.withColumn("previous_max_end", F.max("end_ts").over(before)).withColumn(
        "new_range", F.when(F.col("previous_max_end").isNull() | (F.col("start_ts") > F.col("previous_max_end")), 1).otherwise(0)
    ).withColumn("range_no", F.sum("new_range").over(prefix))
    group_keys = keys + ["range_no"]
    ranges = numbered.groupBy(*group_keys).agg(
        F.min("start_ts").alias("start_ts"), F.max("end_ts").alias("end_ts"),
        F.first("is_resolved").alias("is_resolved"),
        F.max("last_point_ts").alias("last_point_ts"),
    ).withColumn("is_point", F.col("start_ts") == F.col("end_ts")).withColumn(
        "end_inclusive", F.coalesce(F.col("last_point_ts") == F.col("end_ts"), F.lit(False))
    ).drop("last_point_ts").withColumn(
        "range_id", _hash(*[F.col(c) for c in keys + ["start_ts", "end_ts"]])
    )
    links = numbered.join(ranges.select(*group_keys, "range_id"), group_keys, "inner").select(
        "range_id", "candidate_id", "scope_type", "scope_key"
    )
    return ranges.drop("range_no"), links


def attach_evidences(ranges, events, scoped_anchors=None):
    """Rattacher les preuves brutes, y compris les pays contradictoires.

    Equi-join entity_key + condition temporelle; pas de cross join.
    Les dates de preuve sont originales, sans prolongation ni duplication par scope.
    FROM a T soutient le cote gauche de T; TO/PRESENCE a T le cote droit.
    """
    if scoped_anchors is None:
        scoped_anchors = build_scoped_anchors(events)
    # Le meme anchor_id apparait aux deux scopes, mais sa periode entre
    # transitions est unique. Les points sont rattaches a cette periode;
    # les intervalles bruts peuvent traverser une coupure et la contredire.
    epochs = scoped_anchors.select("entity_key", "anchor_id", F.col("epoch_key").alias("evidence_epoch")).dropDuplicates()
    raw = _event_anchors(events).select(
        "entity_key", "event_id", "anchor_id", "anchor_kind", "raw_identifier", "identifier_type",
        F.col("country_code").alias("evidence_country"),
        F.col("ts").alias("evidence_start"), F.col("known_end").alias("evidence_end"),
    ).join(events.select("entity_key", "event_id", "evidence_payload"), ["entity_key", "event_id"], "inner").join(
        epochs, ["entity_key", "anchor_id"], "inner"
    )
    r, e = ranges.alias("r"), raw.alias("e")
    evidence_point = F.col("e.evidence_start") == F.col("e.evidence_end")
    equal_point = F.col("r.is_point") & (F.col("r.start_ts") == F.col("e.evidence_start"))
    departure_match = ((F.col("r.start_ts") < F.col("e.evidence_start")) & (F.col("e.evidence_start") <= F.col("r.end_ts"))) | equal_point
    ordinary_point_match = ((F.col("r.start_ts") <= F.col("e.evidence_start")) & (
        (F.col("e.evidence_start") < F.col("r.end_ts"))
        | (F.col("r.end_inclusive") & (F.col("e.evidence_start") == F.col("r.end_ts")))
    )) | equal_point
    interval_match = (
        ((F.col("e.evidence_start") < F.col("r.end_ts")) & (F.col("e.evidence_end") > F.col("r.start_ts")))
        | (F.col("r.is_point") & (F.col("e.evidence_start") <= F.col("r.start_ts")) & (F.col("r.start_ts") < F.col("e.evidence_end")))
        | (F.col("r.end_inclusive") & (F.col("e.evidence_start") <= F.col("r.end_ts")) & (F.col("r.end_ts") < F.col("e.evidence_end")))
    )
    overlaps = F.when(evidence_point,
        (F.col("e.evidence_epoch") == F.col("r.epoch_key"))
        & F.when(F.col("e.anchor_kind") == "FROM", departure_match).otherwise(ordinary_point_match)
    ).otherwise(interval_match)
    return r.join(e, (F.col("r.entity_key") == F.col("e.entity_key")) & overlaps, "inner").select(
        F.col("r.range_id"), F.col("r.entity_key"), F.col("r.country_code").alias("range_country"),
        F.col("e.event_id"), F.col("e.anchor_id").alias("evidence_id"), F.col("e.anchor_kind"),
        F.col("e.raw_identifier"), F.col("e.identifier_type"), F.col("e.evidence_country"),
        F.col("e.evidence_start"), F.col("e.evidence_end"),
        F.greatest(F.col("r.start_ts"), F.col("e.evidence_start")).alias("overlap_start"),
        F.least(F.col("r.end_ts"), F.col("e.evidence_end")).alias("overlap_end"),
        F.when(F.col("r.country_code") == F.col("e.evidence_country"), "SUPPORT").otherwise("CONTRADICTION").alias("relation"),
        F.col("e.evidence_payload"),
    )


@dataclass
class TravelRangesResult:
    events: DataFrame
    rejected_events: DataFrame
    anchors: DataFrame
    candidates: DataFrame
    candidate_anchors: DataFrame
    inferences: DataFrame
    ranges: DataFrame
    range_candidates: DataFrame
    range_inferences: DataFrame
    range_evidences: DataFrame
    evidence_summary: DataFrame
    _persisted: List[DataFrame] = field(default_factory=list, repr=False)

    def unpersist(self):
        """Appeler apres materialisation des resultats dont on a besoin."""
        for df in reversed(self._persisted):
            df.unpersist()


def build_travel_ranges(silver, persist_intermediates=False):
    """Orchestration lazy. DISK_ONLY optionnel pour reutiliser les branches.

    evidence_summary compte des event_id DISTINCTS par range. Une inference
    n'y ajoute aucune preuve et les deux scopes ne doublent pas les evenements.
    Il s'agit de metriques preparatoires, pas d'une probabilite.
    """
    persisted = []

    def stage(df):
        if persist_intermediates:
            df = df.persist(StorageLevel.DISK_ONLY)
            persisted.append(df)
        return df

    events, rejected = prepare_events(silver)
    events = stage(events)
    anchors = stage(build_scoped_anchors(events))
    candidates, inferences, candidate_anchors = build_candidate_ranges(anchors)
    candidates = stage(candidates)
    ranges, links = merge_country_ranges(candidates)
    ranges = stage(ranges)
    range_inferences = links.select("range_id", "candidate_id").join(inferences, "candidate_id", "inner")
    evidences = stage(attach_evidences(ranges, events, anchors))
    summary = evidences.groupBy("range_id").agg(
        F.countDistinct("event_id").alias("evidence_event_count"),
        F.countDistinct(F.when(F.col("relation") == "SUPPORT", F.col("event_id"))).alias("support_event_count"),
        F.countDistinct(F.when(F.col("relation") == "CONTRADICTION", F.col("event_id"))).alias("contradiction_event_count"),
    )
    return TravelRangesResult(events, rejected, anchors, candidates, candidate_anchors,
                              inferences, ranges, links, range_inferences, evidences, summary, persisted)


def example(spark):
    """Demonstration executable dans Jupyter: France, Espagne, France."""
    schema = """entity_key string, is_resolved boolean, event_id string,
                raw_identifier string, identifier_type string, event_type string,
                country_code string, country_from string, country_to string,
                valid_from string, valid_to string, source_id string"""
    silver = spark.createDataFrame([
        ("P1", True, "e1", "A", "PHONE", "PRESENCE", "FRA", None, None, "2026-06-01", "2026-06-02", "S1"),
        ("P1", True, "e2", "A", "PHONE", "PRESENCE", "ESP", None, None, "2026-06-04", "2026-06-05", "S1"),
        ("P1", True, "e3", "A", "PHONE", "PRESENCE", "FRA", None, None, "2026-06-06", "2026-06-07", "S1"),
    ], schema)
    return build_travel_ranges(silver, persist_intermediates=True)
