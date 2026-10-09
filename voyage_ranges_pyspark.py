"""Ranges candidats, rapprochement des voyages et scoring local -- PySpark 3.3+.

Temps en fuseau commun; fins de presence exclusives, bornes egales = point.
TRANSITION: valid_from=depart, valid_to=arrivee; un seul pays connu est accepte.
Les deux scopes partent des observations brutes. Seuls les trous non couverts
sont inferes; une presence concurrente ne raccourcit pas une duree observee.
Un mois signifie ici 30 jours ecoules, configurable dans inference_levels.
Les scores sont des soutiens heuristiques, pas des probabilites calibrees.

Base = .7 * identifier_confidence/100 + .3 * source_confidence par defaut.
Inference: base * coefficient du trou complet (pas de la tranche locale).
La confiance d'identite d'un lien est plafonnee par celle des deux extremites.
Un maximum par dependency_group/source_name, pays et tranche; jamais un vote
supplementaire pour le scope ENTITY ou pour un deuxieme identifiant.
Convergence: m + lambda*(1-m)*(1-produit(1-s_autres)), lambda=.5 par defaut.
Une source ne corrobore pas un pays lorsqu'elle soutient sensiblement davantage
un autre pays sur la meme tranche (corroboration_margin=.02).
Abstention si score < .5 ou ecart entre les deux premiers < .02.

SOURCE a CONFIRMED et SOURCE b SCHEDULED sont un parametrage de l'exemple,
pas des noms reserves. Sans parametre, les transitions sont CONFIRMED.
Les periodes de transport sont exposees separement, sans inventer de pays.
Les points de presence sont separes des intervalles de duree positive.
Aucun pays n'est choisi pendant un transport confirme ou rapproche d'au moins
un passage confirme; les horaires seuls n'interdisent pas une observation.
Une arrivee prevue peut activer la tolerance si une presence independante du
meme pays la corrobore dans la fenetre et dans le meme epoch. Les deux scores
de base doivent atteindre corroborated_arrival_minimum_score (.8 par defaut).
Un passage confirme garde la priorite. Ce contexte ne cree aucune preuve ni
aucun bonus artificiel; les scores locaux restent visibles avec guard_bases.
range_evidences conserve evidence_start/end; effective_start/end montre le
rapprochement. JOURNEY_CONTEXT n'a pas de overlap artificiel: bornes nulles.
principal_segments expose score_min/max et la moyenne ponderee par la duree.
Aucune ecriture Hive/Iceberg. Le scoring requiert un repertoire de checkpoints
partage par les executors (HDFS en cluster); les checkpoints coupent les plans
reutilises. Le chemin est fourni par l'appelant ou deja configure sur SparkContext.

Performance: calcul distribue sans collect des donnees ni UDF Python. Les
fenetres de corroboration sont bornees a deux buckets temporels par arrivee.
Les autres jointures d'intervalles restent sensibles au nombre de lignes par
entite/epoch; beaucoup de chevauchements peuvent produire un cout quadratique
local. Les checkpoints ecrivent plusieurs intermediaires et demandent de l'I/O.
Filtrer les entites impactees en amont, conserver leurs observations voisines,
et regler les partitions/AQE dans le job. Ne pas saler aveuglement entity_key:
les calculs chronologiques ont besoin de l'ensemble de chaque groupe.
Pour Spark 3.x, activer spark.checkpoint.compress=true au demarrage du job
limite le volume des checkpoints (codec controle par spark.io.compression.codec).
"""
from dataclasses import dataclass, field
from typing import Dict, List, Tuple
from pyspark import StorageLevel
from pyspark.sql import DataFrame, Window, functions as F

KEYS = ["entity_key", "scope_type", "scope_key", "epoch_key"]


@dataclass
class ScoringConfig:
    identifier_weight: float = .70
    inference_levels: Tuple[Tuple[float, float], ...] = ((1., .9), (3., .8), (7., .7), (30., .6))
    convergence_strength: float = .50
    ambiguity_margin: float = .02
    corroboration_margin: float = .02
    minimum_score: float = .50
    reconciliation_minutes: float = 180.
    transition_tolerance_minutes: float = 60.
    confirmed_source_minimum: float = .90
    confirmed_transition_sources: Tuple[str, ...] = ()
    scheduled_transition_sources: Tuple[str, ...] = ()
    dependency_groups: Dict[str, str] = field(default_factory=dict)
    block_transit: bool = True
    corroborated_arrival_window: bool = True
    corroborated_arrival_minimum_score: float = .80

    def __post_init__(self):
        for name in ("identifier_weight", "convergence_strength", "ambiguity_margin",
                     "corroboration_margin", "minimum_score", "confirmed_source_minimum",
                     "corroborated_arrival_minimum_score"):
            if not 0 <= getattr(self, name) <= 1:
                raise ValueError(name + " doit etre dans [0,1]")
        if self.reconciliation_minutes < 0 or self.transition_tolerance_minutes < 0:
            raise ValueError("Tolerances negatives")
        if set(self.confirmed_transition_sources) & set(self.scheduled_transition_sources):
            raise ValueError("Une source ne peut avoir les deux roles")
        previous = 0.
        for days, weight in self.inference_levels:
            if days <= previous or not 0 < weight <= 1:
                raise ValueError("Paliers croissants et coefficients dans ]0,1]")
            previous = days
        if not self.inference_levels:
            raise ValueError("Paliers temporels manquants")


def _hash(*columns):
    return F.sha2(F.to_json(F.struct(*columns)), 256)


def _text(column):
    value = F.trim(column.cast("string"))
    return F.when(F.length(value) > 0, value)


def _seconds(column):
    return column.cast("double")


def _coefficient(days, config):
    expression = F.lit(None).cast("double")
    for limit, value in reversed(config.inference_levels):
        expression = F.when(days <= limit, F.lit(value)).otherwise(expression)
    return expression


def prepare_events(silver, config=None, require_scores=False):
    config = config or ScoringConfig()
    required = {"entity_key", "is_resolved", "event_id", "event_type",
                "valid_from", "raw_identifier", "identifier_type"}
    if require_scores:
        required |= {"identifier_confidence", "source_confidence"}
        if "source_name" not in silver.columns and "source_id" not in silver.columns:
            raise ValueError("Le scoring exige source_name ou source_id")
    missing = required.difference(silver.columns)
    if missing:
        raise ValueError("Colonnes manquantes: " + ", ".join(sorted(missing)))
    def optional(name, dtype="string"):
        return F.col(name).cast(dtype) if name in silver.columns else F.lit(None).cast(dtype)
    df = silver.select(
        _text(F.col("entity_key")).alias("entity_key"),
        F.col("is_resolved").cast("boolean").alias("is_resolved"),
        _text(F.col("event_id")).alias("event_id"),
        _text(F.col("raw_identifier")).alias("raw_identifier"),
        _text(F.col("identifier_type")).alias("identifier_type"),
        F.upper(_text(F.col("event_type"))).alias("event_type"),
        F.col("valid_from").cast("timestamp").alias("valid_from"),
        F.coalesce(optional("valid_to", "timestamp"), F.col("valid_from").cast("timestamp")).alias("valid_to"),
        F.upper(_text(optional("country_code"))).alias("country_code"),
        F.upper(_text(optional("country_from"))).alias("country_from"),
        F.upper(_text(optional("country_to"))).alias("country_to"),
        F.coalesce(_text(optional("source_name")), _text(optional("source_id")), F.lit("UNKNOWN")).alias("source_name"),
        _text(optional("dependency_group")).alias("dependency_group"),
        F.upper(_text(optional("transition_nature"))).alias("transition_nature"),
        optional("identifier_confidence", "double").alias("identifier_confidence"),
        optional("source_confidence", "double").alias("source_confidence"),
        F.struct(*[F.col(c) for c in silver.columns]).alias("evidence_payload"))
    df = df.withColumn("event_type", F.when(F.col("event_type") == "TRANSITIONS", "TRANSITION").otherwise(F.col("event_type")))
    role = F.lit("CONFIRMED")
    if config.scheduled_transition_sources:
        role = F.when(F.col("source_name").isin(*config.scheduled_transition_sources), "SCHEDULED").otherwise(role)
    df = df.withColumn("transition_nature", F.coalesce("transition_nature", role))
    group = F.col("dependency_group")
    for source, dependency in config.dependency_groups.items():
        group = F.when(F.col("source_name") == source, F.lit(dependency)).otherwise(group)
    df = df.withColumn("source_group", F.coalesce(group, "source_name")).withColumn(
        "base_score", config.identifier_weight * F.coalesce(F.col("identifier_confidence") / 100., F.lit(0.))
        + (1 - config.identifier_weight) * F.coalesce(F.col("source_confidence"), F.lit(0.)))
    reason = (
        F.when(F.col("entity_key").isNull(), "MISSING_ENTITY_KEY")
        .when(F.col("event_id").isNull(), "MISSING_EVENT_ID")
        .when(F.col("is_resolved").isNull(), "MISSING_RESOLUTION_STATUS")
        .when(F.col("event_type").isNull() | ~F.col("event_type").isin("PRESENCE", "TRANSITION"), "INVALID_EVENT_TYPE")
        .when(F.col("valid_from").isNull() | F.col("valid_to").isNull(), "INVALID_TIMESTAMP")
        .when(F.col("valid_to") < F.col("valid_from"), "END_BEFORE_START")
        .when(~F.col("is_resolved") & F.col("raw_identifier").isNull(), "UNRESOLVED_WITHOUT_IDENTIFIER")
        .when(F.col("raw_identifier").isNotNull() & F.col("identifier_type").isNull(), "MISSING_IDENTIFIER_TYPE")
        .when((F.col("event_type") == "PRESENCE") & F.col("country_code").isNull(), "MISSING_PRESENCE_COUNTRY")
        .when((F.col("event_type") == "TRANSITION") & F.col("country_from").isNull() & F.col("country_to").isNull(), "MISSING_TRANSITION_COUNTRY")
        .when((F.col("event_type") == "TRANSITION") & ~F.col("transition_nature").isin("CONFIRMED", "SCHEDULED"), "INVALID_TRANSITION_NATURE"))
    if require_scores:
        reason = reason.when(
            F.col("identifier_confidence").isNull() | F.isnan("identifier_confidence")
            | ~F.col("identifier_confidence").between(0., 100.)
            | F.col("source_confidence").isNull() | F.isnan("source_confidence")
            | ~F.col("source_confidence").between(0., 1.), "INVALID_CONFIDENCE")
    checked = df.withColumn("rejection_reason", reason)
    return checked.filter("rejection_reason IS NULL").drop("rejection_reason"), checked.filter("rejection_reason IS NOT NULL")


META = ["entity_key", "is_resolved", "event_id", "raw_identifier", "identifier_type",
        "source_name", "source_group", "identifier_confidence", "source_confidence",
        "base_score", "transition_nature"]


def _event_anchors(events):
    presence = events.filter(F.col("event_type") == "PRESENCE").select(
        *META, "country_code", F.lit("PRESENCE").alias("anchor_kind"),
        F.col("valid_from").alias("ts"), F.col("valid_to").alias("known_end"))
    tr = events.filter(F.col("event_type") == "TRANSITION")
    departure = tr.filter(F.col("country_from").isNotNull()).select(
        *META, F.col("country_from").alias("country_code"), F.lit("FROM").alias("anchor_kind"),
        F.col("valid_from").alias("ts"), F.col("valid_from").alias("known_end"))
    arrival = tr.filter(F.col("country_to").isNotNull()).select(
        *META, F.col("country_to").alias("country_code"), F.lit("TO").alias("anchor_kind"),
        F.col("valid_to").alias("ts"), F.col("valid_to").alias("known_end"))
    return presence.unionByName(departure).unionByName(arrival).withColumn(
        "anchor_id", _hash(F.col("entity_key"), F.col("event_id"), F.col("anchor_kind"))
    ).withColumn("original_anchor_id", F.col("anchor_id")).withColumn(
        "original_start", F.col("ts")).withColumn("original_end", F.col("known_end"))


def reconcile_transitions(events, config):
    raw = _event_anchors(events)
    p = raw.filter((F.col("anchor_kind") != "PRESENCE") & (F.col("transition_nature") == "SCHEDULED")).alias("p")
    a = raw.filter((F.col("anchor_kind") != "PRESENCE") & (F.col("transition_nature") == "CONFIRMED")).alias("a")
    distance = F.abs(_seconds(F.col("p.ts")) - _seconds(F.col("a.ts")))
    matches = p.join(a, (F.col("p.entity_key") == F.col("a.entity_key"))
        & (F.col("p.country_code") == F.col("a.country_code"))
        & (F.col("p.anchor_kind") == F.col("a.anchor_kind"))
        & (distance <= config.reconciliation_minutes * 60.)).select(
        F.col("p.anchor_id").alias("planned_anchor_id"), F.col("p.event_id").alias("planned_event_id"),
        F.col("p.ts").alias("planned_ts"), F.col("p.anchor_kind").alias("anchor_kind"),
        F.col("p.country_code").alias("country_code"), F.col("a.anchor_id").alias("confirmed_anchor_id"),
        F.col("a.event_id").alias("confirmed_event_id"), F.col("a.ts").alias("confirmed_ts"),
        F.col("a.base_score").alias("confirmed_score"), distance.alias("distance_seconds"))
    minimum = matches.groupBy("planned_anchor_id").agg(F.min("distance_seconds").alias("minimum"))
    nearest = matches.join(minimum, "planned_anchor_id").filter(F.col("distance_seconds") == F.col("minimum"))
    unique = nearest.groupBy("planned_anchor_id").agg(F.countDistinct("confirmed_ts").alias("matching_instants"))
    nearest = nearest.join(unique, "planned_anchor_id")
    order = Window.partitionBy("planned_anchor_id").orderBy(F.desc("confirmed_score"), "confirmed_anchor_id")
    chosen = nearest.filter("matching_instants = 1").withColumn("_n", F.row_number().over(order)).filter("_n = 1").select(
        F.col("planned_anchor_id").alias("anchor_id"), "confirmed_anchor_id", "confirmed_event_id", "confirmed_ts", "distance_seconds")
    aligned = raw.join(chosen, "anchor_id", "left").withColumn("ts", F.coalesce("confirmed_ts", "ts")).withColumn(
        "known_end", F.when(F.col("anchor_kind") != "PRESENCE", F.col("ts")).otherwise(F.col("known_end")))
    dep = aligned.filter(F.col("anchor_kind") == "FROM").select(
        "entity_key", "event_id", F.col("ts").alias("departure_ts"), F.col("confirmed_event_id").alias("departure_confirmation"))
    arr = aligned.filter(F.col("anchor_kind") == "TO").select(
        "entity_key", "event_id", F.col("ts").alias("arrival_ts"), F.col("confirmed_event_id").alias("arrival_confirmation"))
    journeys = events.filter((F.col("event_type") == "TRANSITION") & F.col("country_from").isNotNull() & F.col("country_to").isNotNull()).join(
        dep, ["entity_key", "event_id"]).join(arr, ["entity_key", "event_id"]).withColumn(
        "alignment_valid", F.col("arrival_ts") >= F.col("departure_ts")).withColumn(
        "journey_id", _hash(F.col("entity_key"), F.col("event_id"))).select(
        "entity_key", "event_id", "journey_id", "country_from", "country_to", "valid_from", "valid_to",
        "departure_ts", "arrival_ts", "departure_confirmation", "arrival_confirmation",
        "transition_nature", "source_name", "base_score", "alignment_valid")
    invalid = journeys.filter(~F.col("alignment_valid")).select("entity_key", "event_id").withColumn("_invalid", F.lit(True))
    aligned = aligned.join(invalid, ["entity_key", "event_id"], "left").withColumn(
        "ts", F.when(F.col("_invalid"), F.col("original_start")).otherwise(F.col("ts"))).withColumn(
        "known_end", F.when(F.col("_invalid"), F.col("original_end")).otherwise(F.col("known_end"))).drop("_invalid")
    journeys = journeys.withColumn("departure_ts", F.when(~F.col("alignment_valid"), F.col("valid_from")).otherwise(F.col("departure_ts"))).withColumn(
        "arrival_ts", F.when(~F.col("alignment_valid"), F.col("valid_to")).otherwise(F.col("arrival_ts")))
    return aligned, journeys, nearest


def _number_intervals(df, keys, id_column):
    order = Window.partitionBy(*keys).orderBy("start_ts", "end_ts", id_column)
    before = order.rowsBetween(Window.unboundedPreceding, -1)
    prefix = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return df.withColumn("_previous_end", F.max("end_ts").over(before)).withColumn(
        "_new", F.when(F.col("_previous_end").isNull() | (F.col("start_ts") > F.col("_previous_end")), 1).otherwise(0)
    ).withColumn("_number", F.sum("_new").over(prefix))


def build_candidate_ranges(anchors, config=None):
    """Remplir les trous de couverture, toutes observations du scope confondues."""
    config = config or ScoringConfig()
    points = anchors.groupBy(*KEYS, "ts").agg(
        F.max("known_end").alias("covered_end"), F.max(F.struct("base_score", "endpoint")).alias("_best"),
        F.first("epoch_end", ignorenulls=True).alias("epoch_end"),
        F.first("next_cut_endpoint", ignorenulls=True).alias("next_cut_endpoint"))
    order = Window.partitionBy(*KEYS).orderBy("ts").rowsBetween(Window.unboundedPreceding, -1)
    gaps = points.withColumn("previous_end", F.max("covered_end").over(order)).filter(
        F.col("previous_end").isNotNull() & (F.col("ts") > F.col("previous_end"))).select(
        *KEYS, F.col("previous_end").alias("start_ts"), F.col("ts").alias("end_ts"),
        F.col("_best.endpoint").alias("right_endpoint"))
    tails = points.groupBy(*KEYS).agg(
        F.max("covered_end").alias("start_ts"), F.first("epoch_end", ignorenulls=True).alias("end_ts"),
        F.first("next_cut_endpoint", ignorenulls=True).alias("right_endpoint")).filter(F.col("end_ts") > F.col("start_ts"))
    gaps = gaps.unionByName(tails).withColumn("gap_days",
        (_seconds(F.col("end_ts")) - _seconds(F.col("start_ts"))) / 86400.).withColumn(
        "temporal_coefficient", _coefficient(F.col("gap_days"), config)).filter(F.col("temporal_coefficient").isNotNull())
    owners = anchors.select(*KEYS, F.col("known_end").alias("start_ts"), F.col("endpoint").alias("left_endpoint"), "is_resolved")
    inf = gaps.join(owners, KEYS + ["start_ts"]).withColumn("country_code", F.col("left_endpoint.country_code")).withColumn(
        "inference_id", _hash(*[F.col(c) for c in KEYS + ["start_ts", "end_ts"]], F.col("left_endpoint.original_anchor_id"))).withColumn(
        "_identity", F.least(F.col("left_endpoint.identifier_confidence"), F.coalesce(
            F.col("right_endpoint.identifier_confidence"), F.col("left_endpoint.identifier_confidence")))).withColumn(
        "inference_score", F.col("temporal_coefficient") * (
            config.identifier_weight * F.coalesce(F.col("_identity") / 100., F.lit(0.))
            + (1 - config.identifier_weight) * F.coalesce(F.col("left_endpoint.source_confidence"), F.lit(0.)))).drop("_identity")
    observed = anchors.select(*KEYS, "country_code", "is_resolved", F.col("ts").alias("start_ts"),
        F.col("known_end").alias("end_ts"), F.col("anchor_id").alias("link_id"), F.lit("OBSERVED").alias("piece_kind"),
        F.when((F.col("anchor_kind") != "FROM") & (F.col("ts") == F.col("known_end")), F.col("ts")).alias("last_point_ts"))
    inferred = inf.select(*KEYS, "country_code", "is_resolved", "start_ts", "end_ts",
        F.col("inference_id").alias("link_id"), F.lit("INFERRED").alias("piece_kind"), F.lit(None).cast("timestamp").alias("last_point_ts"))
    keys = KEYS + ["country_code"]
    numbered = _number_intervals(observed.unionByName(inferred), keys, "link_id")
    groups = keys + ["_number"]
    candidates = numbered.groupBy(*groups).agg(
        F.min("start_ts").alias("start_ts"), F.max("end_ts").alias("end_ts"), F.first("is_resolved").alias("is_resolved"),
        F.max("last_point_ts").alias("last_point_ts")).withColumn(
        "candidate_id", _hash(*[F.col(c) for c in keys + ["start_ts", "end_ts"]])).withColumn(
        "is_point", F.col("start_ts") == F.col("end_ts")).withColumn(
        "end_inclusive", F.coalesce(F.col("last_point_ts") == F.col("end_ts"), F.lit(False)))
    links = numbered.join(candidates.select(*groups, "candidate_id"), groups).select(*KEYS, "link_id", "piece_kind", "candidate_id")
    inf = inf.join(links.filter(F.col("piece_kind") == "INFERRED").select(
        *KEYS, F.col("link_id").alias("inference_id"), "candidate_id"), KEYS + ["inference_id"])
    member = links.filter(F.col("piece_kind") == "OBSERVED").select(*KEYS, F.col("link_id").alias("anchor_id"), "candidate_id").join(
        anchors.select(*KEYS, "anchor_id", "event_id", "anchor_kind", "raw_identifier", "identifier_type"), KEYS + ["anchor_id"])
    return candidates.drop("_number"), inf, member.select("candidate_id", "anchor_id", "event_id", "anchor_kind", "raw_identifier", "identifier_type")


def build_scoped_anchors(events, aligned_anchors=None):
    """Distribuer les coupures par balayage de l'entite, sans join ids x voyages.

    Ordre a T: FROM, coupure, puis PRESENCE/TO. Ainsi un FROM appartient
    au sejour se terminant a T et un TO au sejour commencant a T.
    """
    base = aligned_anchors if aligned_anchors is not None else _event_anchors(events)
    # Une coupure termine l'ancien range. Si une presence DE 18:40-23:00
    # couvre une arrivee en DE a 19:00, sa partie DE 19:00-23:00 reste connue.
    # Reprendre cette partie avec le MEME event_id, sans nouvelle preuve brute.
    # Une presence dans l'origine incompatible avec la destination ne reprend
    # pas apres la coupure; elle reste disponible comme preuve contradictoire.
    arrivals = base.filter(F.col("anchor_kind") == "TO").select(
        "entity_key", "country_code", F.col("ts").alias("arrival_ts")
    ).dropDuplicates()
    presence = base.filter(
        (F.col("anchor_kind") == "PRESENCE") & (F.col("known_end") > F.col("ts"))
    )
    p, a = presence.alias("p"), arrivals.alias("a")
    tails = p.join(a,
        (F.col("p.entity_key") == F.col("a.entity_key"))
        & (F.col("p.country_code") == F.col("a.country_code"))
        & (F.col("p.ts") < F.col("a.arrival_ts"))
        & (F.col("a.arrival_ts") < F.col("p.known_end")),
        "inner",
    ).select(*[
        F.col("a.arrival_ts").alias("ts") if c == "ts" else (
            _hash(F.col("p.anchor_id"), F.col("a.arrival_ts")).alias("anchor_id")
            if c == "anchor_id" else F.col("p." + c).alias(c)
        )
        for c in base.columns
    ])
    anchors = base.unionByName(tails).withColumn(
        "endpoint", F.struct("anchor_id", "original_anchor_id", "event_id", "anchor_kind", "raw_identifier", "identifier_type", "country_code", "source_name", "source_group", "identifier_confidence", "source_confidence", "base_score", "ts")
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


def attach_evidences(ranges, events, scoped_anchors=None, aligned_anchors=None):
    raw = aligned_anchors if aligned_anchors is not None else _event_anchors(events)
    scoped = scoped_anchors if scoped_anchors is not None else build_scoped_anchors(events, raw)
    epochs = scoped.select("entity_key", "original_anchor_id", F.col("epoch_key").alias("evidence_epoch")).dropDuplicates()
    e = raw.join(events.select("entity_key", "event_id", "evidence_payload"), ["entity_key", "event_id"]).join(
        epochs, ["entity_key", "original_anchor_id"]).alias("e")
    r = ranges.alias("r")
    def point_match(value):
        equal = F.col("r.is_point") & (F.col("r.start_ts") == value)
        before = ((F.col("r.start_ts") < value) & (value <= F.col("r.end_ts"))) | equal
        after = ((F.col("r.start_ts") <= value) & (
            (value < F.col("r.end_ts")) | (F.col("r.end_inclusive") & (value == F.col("r.end_ts"))))) | equal
        return F.when(F.col("e.anchor_kind") == "FROM", before).otherwise(after)
    interval = ((F.col("e.original_start") < F.col("r.end_ts")) & (F.col("e.original_end") > F.col("r.start_ts"))) | (
        F.col("r.is_point") & (F.col("e.original_start") <= F.col("r.start_ts")) & (F.col("r.start_ts") < F.col("e.original_end")))
    original_match = F.when(F.col("e.original_start") == F.col("e.original_end"), point_match(F.col("e.original_start"))).otherwise(interval)
    effective_match = (F.col("e.anchor_kind") != "PRESENCE") & point_match(F.col("e.ts"))
    overlap = F.when(F.col("e.anchor_kind") != "PRESENCE",
        (F.col("e.evidence_epoch") == F.col("r.epoch_key")) & (original_match | effective_match)).otherwise(original_match)
    return r.join(e, (F.col("r.entity_key") == F.col("e.entity_key")) & overlap).select(
        F.col("r.range_id"), F.col("r.entity_key"), F.col("r.country_code").alias("range_country"),
        F.col("e.event_id"), F.col("e.original_anchor_id").alias("evidence_id"), F.col("e.anchor_kind"),
        F.col("e.raw_identifier"), F.col("e.identifier_type"), F.col("e.source_name"),
        F.col("e.country_code").alias("evidence_country"),
        F.col("e.original_start").alias("evidence_start"), F.col("e.original_end").alias("evidence_end"),
        F.col("e.ts").alias("effective_start"), F.col("e.known_end").alias("effective_end"),
        F.when(original_match, F.greatest(F.col("r.start_ts"), F.col("e.original_start"))).alias("overlap_start"),
        F.when(original_match, F.least(F.col("r.end_ts"), F.col("e.original_end"))).alias("overlap_end"),
        F.when(original_match, "TEMPORAL_OVERLAP").otherwise("JOURNEY_CONTEXT").alias("match_type"),
        F.when(F.col("r.country_code") == F.col("e.country_code"), "SUPPORT").otherwise("CONTRADICTION").alias("relation"),
        F.col("e.evidence_payload")).dropDuplicates(["range_id", "evidence_id"])


def build_scoring_supports(anchors, inferences):
    direct = anchors.select(
        "entity_key", "epoch_key", "country_code", F.col("ts").alias("start_ts"),
        F.col("known_end").alias("end_ts"), "source_name", "source_group", "event_id",
        "original_anchor_id", "anchor_kind", F.col("base_score").alias("support_score"),
        F.lit(1.).alias("temporal_coefficient"),
        F.when(F.col("anchor_kind") == "PRESENCE", "OBSERVED").otherwise(F.col("transition_nature")).alias("support_type"))
    inferred = inferences.select(
        "entity_key", "epoch_key", "country_code", "start_ts", "end_ts",
        F.col("left_endpoint.source_name").alias("source_name"), F.col("left_endpoint.source_group").alias("source_group"),
        F.col("left_endpoint.event_id").alias("event_id"), F.col("left_endpoint.original_anchor_id").alias("original_anchor_id"),
        F.lit("INFERENCE").alias("anchor_kind"), F.col("inference_score").alias("support_score"),
        "temporal_coefficient", F.lit("INFERRED").alias("support_type"))
    supports = direct.unionByName(inferred).withColumn("support_id", _hash(*[
        F.col(c) for c in ["entity_key", "epoch_key", "original_anchor_id", "country_code", "start_ts", "end_ts", "support_type"]]))
    keys = [name for name in supports.columns if name != "support_score"]
    return supports.groupBy(*keys).agg(F.max("support_score").alias("support_score"))


def build_transition_guards(anchors, config):
    """Tolerance confirmee ou arrivee prevue corroboree par une presence.

    anchors contient les epochs et les reprises de presences apres une arrivee.
    Chaque scope est deduplique avant la comparaison. La cle temporelle limite
    chaque arrivee a deux buckets, sans exploser les longues presences en jours.
    Une corroboration doit avoir une autre source_group et une identite resolue
    ou le meme identifiant/type. Aucun score n'est ajoute par cette operation.
    """
    fields = ["entity_key", "epoch_key", "epoch_end", "anchor_id", "event_id",
              "original_anchor_id", "anchor_kind", "country_code", "ts", "known_end",
              "source_name", "source_group", "source_confidence", "base_score",
              "transition_nature", "is_resolved", "raw_identifier", "identifier_type"]
    unique = anchors.select(*fields).dropDuplicates(["entity_key", "epoch_key", "anchor_id"])
    seconds = config.transition_tolerance_minutes * 60.
    bucket_seconds = max(seconds, 1.)
    planned = unique.filter(
        (F.col("anchor_kind") == "TO") & (F.col("transition_nature") == "SCHEDULED")
        & (F.col("base_score") >= config.corroborated_arrival_minimum_score)
        & F.lit(config.corroborated_arrival_window))
    observed = unique.filter(
        (F.col("anchor_kind") == "PRESENCE")
        & (F.col("base_score") >= config.corroborated_arrival_minimum_score))
    planned = planned.withColumn("_bucket", F.explode(F.array_distinct(F.array(
        F.floor(_seconds(F.col("ts")) / bucket_seconds),
        F.floor((_seconds(F.col("ts")) + seconds) / bucket_seconds)))))
    observed = observed.withColumn("_bucket", F.floor(_seconds(F.col("ts")) / bucket_seconds))
    p, o = planned.alias("p"), observed.alias("o")
    identity = (F.col("p.is_resolved") & F.col("o.is_resolved")) | (
        F.col("p.raw_identifier").isNotNull()
        & (F.col("p.raw_identifier") == F.col("o.raw_identifier"))
        & (F.col("p.identifier_type") == F.col("o.identifier_type")))
    matches = p.join(o,
        (F.col("p.entity_key") == F.col("o.entity_key"))
        & (F.col("p.epoch_key") == F.col("o.epoch_key"))
        & (F.col("p.country_code") == F.col("o.country_code"))
        & (F.col("p._bucket") == F.col("o._bucket"))
        & (F.col("p.source_group") != F.col("o.source_group")) & identity
        & (F.col("o.ts") >= F.col("p.ts"))
        & (_seconds(F.col("o.ts")) <= _seconds(F.col("p.ts")) + seconds)).select(
        F.col("p.entity_key"), F.col("p.epoch_key"), F.col("p.country_code"),
        F.col("p.anchor_id").alias("planned_anchor_id"),
        F.col("p.event_id").alias("planned_event_id"),
        F.col("p.ts").alias("arrival_ts"),
        F.col("p.source_group").alias("planned_source_group"),
        F.col("o.original_anchor_id").alias("observed_anchor_id"),
        F.col("o.event_id").alias("observed_event_id"),
        F.col("o.ts").alias("presence_ts"),
        F.col("o.known_end").alias("presence_end"),
        F.col("o.source_group").alias("observed_source_group"),
        F.col("o.base_score").alias("observed_score")).dropDuplicates(
            ["entity_key", "epoch_key", "planned_anchor_id", "observed_anchor_id"])
    corroborations = matches.groupBy("entity_key", "epoch_key", "planned_anchor_id").agg(
        F.collect_set("observed_event_id").alias("corroborating_event_ids"))
    scheduled = unique.join(corroborations.withColumnRenamed("planned_anchor_id", "anchor_id"),
        ["entity_key", "epoch_key", "anchor_id"]).withColumn(
        "guard_priority", F.lit(1)).withColumn("guard_basis", F.lit("CORROBORATED_SCHEDULED_ARRIVAL"))
    confirmed = unique.filter((F.col("anchor_kind") != "PRESENCE")
        & (F.col("transition_nature") == "CONFIRMED")
        & (F.col("source_confidence") >= config.confirmed_source_minimum)).withColumn(
        "corroborating_event_ids", F.array().cast("array<string>")).withColumn(
        "guard_priority", F.lit(2)).withColumn("guard_basis", F.lit("CONFIRMED_PASSAGE"))
    guards = confirmed.unionByName(scheduled).withColumn("window_start",
        (_seconds(F.col("ts")) + F.when(F.col("anchor_kind") == "FROM", -seconds).otherwise(0.)).cast("timestamp")
    ).withColumn("window_end", F.when(F.col("anchor_kind") == "FROM", F.col("ts")).otherwise(
        F.least((_seconds(F.col("ts")) + seconds).cast("timestamp"), F.col("epoch_end"))))
    return guards, matches


def build_cells(supports, aligned, journeys, config, guards=None):
    bounds = supports.select("entity_key", F.col("start_ts").alias("ts")).unionByName(
        supports.select("entity_key", F.col("end_ts").alias("ts"))).unionByName(
        journeys.select("entity_key", F.col("departure_ts").alias("ts"))).unionByName(
        journeys.select("entity_key", F.col("arrival_ts").alias("ts")))
    extent = bounds.groupBy("entity_key").agg(F.min("ts").alias("first_ts"), F.max("ts").alias("last_ts"))
    seconds = config.transition_tolerance_minutes * 60.
    if guards is None:
        guards = aligned.filter((F.col("anchor_kind") != "PRESENCE") & (F.col("transition_nature") == "CONFIRMED")
            & (F.col("source_confidence") >= config.confirmed_source_minimum)).withColumn(
            "guard_priority", F.lit(2)).withColumn("guard_basis", F.lit("CONFIRMED_PASSAGE")).withColumn(
            "window_start", (_seconds(F.col("ts")) + F.when(
                F.col("anchor_kind") == "FROM", -seconds).otherwise(0.)).cast("timestamp")).withColumn(
            "window_end", (_seconds(F.col("ts")) + F.when(
                F.col("anchor_kind") == "TO", seconds).otherwise(0.)).cast("timestamp"))
    guard_bounds = guards.select("entity_key", F.col("window_start").alias("ts")).unionByName(
        guards.select("entity_key", F.col("window_end").alias("ts"))).join(
        extent, "entity_key").select("entity_key", F.greatest("first_ts", F.least("ts", "last_ts")).alias("ts"))
    times = bounds.unionByName(guard_bounds).filter(F.col("ts").isNotNull()).dropDuplicates()
    order = Window.partitionBy("entity_key").orderBy("ts")
    intervals = times.withColumn("end_ts", F.lead("ts").over(order)).filter(F.col("end_ts") > F.col("ts")).select(
        "entity_key", F.col("ts").alias("start_ts"), "end_ts", F.lit(False).alias("is_point"), F.lit(1).alias("phase"))
    points = supports.filter(F.col("start_ts") == F.col("end_ts")).select(
        "entity_key", "start_ts", "end_ts", F.lit(True).alias("is_point"),
        F.when(F.col("anchor_kind") == "FROM", -1).otherwise(1).alias("phase")).dropDuplicates()
    cells = intervals.unionByName(points).withColumn(
        "cell_id", _hash(F.col("entity_key"), F.col("start_ts"), F.col("end_ts"), F.col("phase"), F.col("is_point")))
    cuts = aligned.filter(F.col("anchor_kind") != "PRESENCE").select(
        "entity_key", F.col("ts").alias("start_ts")).dropDuplicates().withColumn(
        "cut_id", _hash(F.col("entity_key"), F.col("start_ts").alias("ts")))
    rows = cells.withColumn("cut_id", F.lit(None).cast("string"))
    cut_rows = cuts.select(*[
        F.col(c) if c in cuts.columns else F.lit(0).alias(c) if c == "phase" else
        F.lit(None).cast(rows.schema[c].dataType).alias(c) for c in rows.columns])
    order = Window.partitionBy("entity_key").orderBy("start_ts", "phase", "cell_id").rowsBetween(
        Window.unboundedPreceding, Window.currentRow)
    return rows.unionByName(cut_rows).withColumn("epoch_key", F.coalesce(
        F.last("cut_id", ignorenulls=True).over(order), F.lit("BEFORE_FIRST_TRANSITION"))
    ).filter(F.col("cell_id").isNotNull()).drop("cut_id"), guards


def score_cells(cells, supports, journeys, guards, config, stage=lambda df: df):
    c, s = cells.alias("c"), supports.alias("s")
    interval = (F.col("s.start_ts") <= F.col("c.start_ts")) & (F.col("c.end_ts") <= F.col("s.end_ts")) & (F.col("s.end_ts") > F.col("s.start_ts"))
    before = (F.col("s.start_ts") < F.col("c.start_ts")) & (F.col("c.start_ts") <= F.col("s.end_ts"))
    after = (F.col("s.start_ts") <= F.col("c.start_ts")) & (F.col("c.start_ts") < F.col("s.end_ts"))
    equal = (F.col("s.start_ts") == F.col("s.end_ts")) & (F.col("s.start_ts") == F.col("c.start_ts")) & (
        F.col("c.phase") == F.when(F.col("s.anchor_kind") == "FROM", -1).otherwise(1))
    match = F.when(F.col("c.is_point"), equal | F.when(F.col("c.phase") == -1, before).otherwise(after)).otherwise(interval)
    active = stage(c.join(s, (F.col("c.entity_key") == F.col("s.entity_key")) & (F.col("c.epoch_key") == F.col("s.epoch_key")) & match).select(
        *[F.col("c." + name) for name in cells.columns], *[F.col("s." + name) for name in [
            "country_code", "source_name", "source_group", "event_id", "support_score", "support_type", "support_id"]]))
    source = active.groupBy("cell_id", "country_code", "source_group").agg(
        F.max("support_score").alias("source_score"), F.collect_set("source_name").alias("source_names"),
        F.collect_set("event_id").alias("event_ids"), F.collect_set("support_type").alias("support_types"))
    best = source.groupBy("cell_id", "source_group").agg(F.max("source_score").alias("source_best"))
    source = source.join(best, ["cell_id", "source_group"]).withColumn(
        "corroborates", F.col("source_score") >= F.col("source_best") - config.corroboration_margin)
    country = source.groupBy("cell_id", "country_code").agg(
        F.max("source_score").alias("base_country_score"),
        F.sort_array(F.collect_list(F.when(F.col("corroborates"), F.struct(
            F.col("source_score").alias("score"), F.col("source_group").alias("group")))), asc=False).alias("_qualified"),
        F.array_distinct(F.flatten(F.collect_list("source_names"))).alias("sources"),
        F.array_distinct(F.flatten(F.collect_list("event_ids"))).alias("event_ids"),
        F.array_distinct(F.flatten(F.collect_list("support_types"))).alias("support_types"),
        F.collect_list(F.struct("source_group", "source_score", "corroborates")).alias("source_scores"))
    country = country.withColumn("_m", F.when(F.size("_qualified") > 0, F.col("_qualified")[0]["score"])).withColumn("_product", F.expr(
        "aggregate(slice(_qualified, 2, size(_qualified)), cast(1.0 as double), (p, x) -> p * (1.0 - x.score))")
    ).withColumn("score", F.greatest("base_country_score", F.when(
        F.size("_qualified") >= 2, F.col("_m") + config.convergence_strength * (1 - F.col("_m")) * (1 - F.col("_product"))))
    ).withColumn("convergence_bonus", F.col("score") - F.col("base_country_score")).drop("_qualified", "_m", "_product")
    c = cells.alias("c")
    # Un horaire prevu seul ne doit pas interdire une presence observee.
    j = journeys.filter((F.col("arrival_ts") > F.col("departure_ts")) & (
        (F.col("transition_nature") == "CONFIRMED") | F.col("departure_confirmation").isNotNull()
        | F.col("arrival_confirmation").isNotNull())).alias("j")
    transit_match = (F.col("c.start_ts") >= F.col("j.departure_ts")) & (F.col("c.start_ts") < F.col("j.arrival_ts")) & ~(
        F.col("c.is_point") & (F.col("c.phase") == -1) & (F.col("c.start_ts") == F.col("j.departure_ts")))
    transit = c.join(j, (F.col("c.entity_key") == F.col("j.entity_key")) & transit_match).select(
        F.col("c.cell_id"), F.col("j.journey_id")).groupBy("cell_id").agg(F.collect_set("journey_id").alias("journey_ids"))
    bucket_seconds = max(config.transition_tolerance_minutes * 60., 1.)
    guard_cells = cells.withColumn("_bucket", F.floor(_seconds(F.col("start_ts")) / bucket_seconds)).alias("c")
    guard_buckets = guards.withColumn("_bucket", F.explode(F.array_distinct(F.array(
        F.floor(_seconds(F.col("window_start")) / bucket_seconds),
        F.floor(_seconds(F.col("window_end")) / bucket_seconds)))))
    g = guard_buckets.alias("g")
    distance = F.abs(_seconds(F.col("c.start_ts")) - _seconds(F.col("g.ts")))
    before = (F.col("c.start_ts") < F.col("g.ts")) | (
        F.col("c.is_point") & (F.col("c.phase") == -1) & (F.col("c.start_ts") == F.col("g.ts")))
    after = (F.col("c.start_ts") > F.col("g.ts")) | (
        (F.col("c.phase") != -1) & (F.col("c.start_ts") == F.col("g.ts")))
    in_window = (F.col("c.start_ts") >= F.col("g.window_start")) & F.when(
        F.col("c.is_point"), F.col("c.start_ts") <= F.col("g.window_end")).otherwise(
        (F.col("c.start_ts") < F.col("g.window_end")) & (F.col("c.end_ts") <= F.col("g.window_end")))
    guard_match = in_window & F.when(F.col("g.anchor_kind") == "FROM", before).otherwise(after)
    epoch_match = (F.col("c.epoch_key") == F.col("g.epoch_key")) if "epoch_key" in guards.columns else F.lit(True)
    gm = guard_cells.join(g, (F.col("c.entity_key") == F.col("g.entity_key"))
        & epoch_match & (F.col("c._bucket") == F.col("g._bucket")) & guard_match).select(
        F.col("c.cell_id"), F.col("g.country_code").alias("guard_country"), F.col("g.guard_basis"),
        F.struct((-F.col("g.guard_priority")).alias("priority"), distance.alias("distance")).alias("_order"))
    minimum = gm.groupBy("cell_id").agg(F.min("_order").alias("_minimum"))
    guard_info = gm.join(minimum, "cell_id").filter(F.col("_order") == F.col("_minimum")).groupBy("cell_id").agg(
        F.collect_set("guard_country").alias("guard_countries"), F.collect_set("guard_basis").alias("guard_bases"))
    context = stage(cells.join(transit, "cell_id", "left").join(guard_info, "cell_id", "left").withColumn(
        "in_transit", F.coalesce((F.size("journey_ids") > 0) & F.lit(config.block_transit), F.lit(False))).withColumn(
        "guard_conflict", F.coalesce(F.size("guard_countries") > 1, F.lit(False))).withColumn(
        "guard_country", F.when(F.size("guard_countries") == 1, F.col("guard_countries")[0])))
    scored = stage(country.join(context, "cell_id").withColumn("eligible",
        ~F.col("in_transit") & ~F.col("guard_conflict") & (
            F.col("guard_country").isNull() | (F.col("guard_country") == F.col("country_code")))))
    ranked = scored.filter("eligible").withColumn("rank", F.row_number().over(
        Window.partitionBy("cell_id").orderBy(F.desc("score"), "country_code")))
    winners = ranked.groupBy("cell_id").agg(
        F.max(F.when(F.col("rank") == 1, F.col("score"))).alias("best_score"),
        F.max(F.when(F.col("rank") == 2, F.col("score"))).alias("second_score"),
        F.first(F.when(F.col("rank") == 1, F.col("country_code")), ignorenulls=True).alias("best_country"),
        F.first(F.when(F.col("rank") == 1, F.col("sources")), ignorenulls=True).alias("winning_sources"))
    decisions = stage(context.join(winners, "cell_id", "left").withColumn(
        "margin", F.col("best_score") - F.coalesce("second_score", F.lit(0.))).withColumn(
        "decision", F.when(F.col("in_transit"), "IN_TRANSIT").when(F.col("guard_conflict"), "CONFLICTING_TRANSITIONS")
        .when(F.col("best_country").isNull(), "NO_COMPATIBLE_SUPPORT")
        .when(F.col("best_score") < config.minimum_score, "INSUFFICIENT_SCORE")
        .when(F.col("second_score").isNotNull() & (F.col("margin") < config.ambiguity_margin - 1e-12), "AMBIGUOUS").otherwise("PRINCIPAL")
    ).withColumn("selected_country", F.when(F.col("decision") == "PRINCIPAL", F.col("best_country"))))
    local = scored.join(decisions.select("cell_id", "decision", "selected_country", "margin"), "cell_id").withColumn(
        "status", F.when(F.col("in_transit"), "ALTERNATIVE_IN_TRANSIT")
        .when(~F.col("eligible"), "TRANSITION_INCOMPATIBLE")
        .when(F.col("decision") == "AMBIGUOUS", "AMBIGUOUS")
        .when(F.col("country_code") == F.col("selected_country"), "PRINCIPAL").otherwise("ALTERNATIVE"))
    return local, decisions, source


def summarize_periods(decisions, principal=True):
    df = decisions.filter(~F.col("is_point"))
    if principal:
        df = df.filter(F.col("decision") == "PRINCIPAL").withColumn("country_code", F.col("selected_country"))
        keys = ["entity_key", "epoch_key", "country_code"]
    else:
        df = df.filter(F.col("decision") != "PRINCIPAL")
        keys = ["entity_key", "epoch_key", "decision"]
    numbered = _number_intervals(df, keys, "cell_id").withColumn("_duration", _seconds(F.col("end_ts")) - _seconds(F.col("start_ts")))
    groups = keys + ["_number"]
    if principal:
        out = numbered.groupBy(*groups).agg(
            F.min("start_ts").alias("start_ts"), F.max("end_ts").alias("end_ts"),
            F.min("best_score").alias("score_min"), F.max("best_score").alias("score_max"),
            (F.sum(F.col("best_score") * F.col("_duration")) / F.sum("_duration")).alias("score_mean"),
            F.array_distinct(F.flatten(F.collect_list("winning_sources"))).alias("sources"), F.collect_set("cell_id").alias("cell_ids")
        ).withColumn("segment_id", _hash(*[F.col(c) for c in keys + ["start_ts", "end_ts"]]))
    else:
        out = numbered.groupBy(*groups).agg(
            F.min("start_ts").alias("start_ts"), F.max("end_ts").alias("end_ts"), F.collect_set("cell_id").alias("cell_ids"))
    return out.drop("_number")


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
    journeys: DataFrame
    transition_matches: DataFrame
    scoring_supports: DataFrame = None
    local_scores: DataFrame = None
    source_scores: DataFrame = None
    decisions: DataFrame = None
    principal_segments: DataFrame = None
    principal_points: DataFrame = None
    unresolved_periods: DataFrame = None
    unresolved_points: DataFrame = None
    transition_guards: DataFrame = None
    arrival_corroborations: DataFrame = None
    _persisted: List[DataFrame] = field(default_factory=list, repr=False)

    def unpersist(self):
        for df in reversed(self._persisted):
            df.unpersist()


def build_travel_ranges(silver, persist_intermediates=False, config=None,
                        score=True, materialize_intermediates=False, checkpoint_dir=None):
    """Retourner candidats, preuves, scoring par tranche et parcours principal.

    score=False autorise un schema sans scores (construction seule).
    Les cadres reutilises peuvent etre conserves sur DISK_ONLY. Avec
    materialize_intermediates=True, count() materialise chaque cache successif.
    Scoring: checkpoint_dir doit etre fourni ou deja configure sur SparkContext.
    En cluster, utiliser HDFS/S3 accessible a tous les executors. Les fichiers
    appartiennent au run; l'appelant les supprime apres materialisation des sorties.
    Aucun collect()/UDF Python; jointures temporelles avec cle d'egalite.
    event_id doit etre unique/stable par entity_key. En incremental, fournir
    les observations voisines necessaires au scope, pas uniquement le mois.
    transition_guards/arrival_corroborations expliquent la priorite temporelle;
    local_scores conserve les scores bruts et les observations concurrentes.
    """
    config = config or ScoringConfig()
    context = silver.sparkSession.sparkContext
    if checkpoint_dir is not None:
        context.setCheckpointDir(checkpoint_dir)
    use_checkpoint = context.getCheckpointDir() is not None
    if score and not use_checkpoint:
        raise ValueError("Le scoring exige checkpoint_dir ou SparkContext.setCheckpointDir(...); "
                         "utiliser un chemin partage par les executors pour limiter la taille des plans.")
    persisted = []
    def stage(df):
        if use_checkpoint:
            df = df.checkpoint(eager=True)
        if persist_intermediates or materialize_intermediates:
            df = df.persist(StorageLevel.DISK_ONLY)
            persisted.append(df)
            if materialize_intermediates:
                df.count()
        return df
    events, rejected = prepare_events(silver, config, require_scores=score)
    events = stage(events)
    aligned, journeys, matches = reconcile_transitions(events, config)
    aligned, journeys = stage(aligned), stage(journeys)
    anchors = stage(build_scoped_anchors(events, aligned))
    candidates, inferences, members = build_candidate_ranges(anchors, config)
    candidates, inferences = stage(candidates), stage(inferences)
    ranges, links = merge_country_ranges(candidates)
    ranges = stage(ranges)
    evidences = attach_evidences(ranges, events, anchors, aligned)
    summary = evidences.groupBy("range_id").agg(
        F.countDistinct("event_id").alias("evidence_event_count"),
        F.countDistinct(F.when(F.col("relation") == "SUPPORT", F.col("event_id"))).alias("support_event_count"),
        F.countDistinct(F.when(F.col("relation") == "CONTRADICTION", F.col("event_id"))).alias("contradiction_event_count"))
    result = TravelRangesResult(events, rejected, anchors, candidates, members, inferences, ranges, links,
        links.select("range_id", "candidate_id").join(inferences, "candidate_id"),
        evidences, summary, journeys, matches, _persisted=persisted)
    if score:
        supports = stage(build_scoring_supports(anchors, inferences))
        guards, corroborations = build_transition_guards(anchors, config)
        guards = stage(guards)
        result.transition_guards, result.arrival_corroborations = guards, corroborations
        cells, guards = build_cells(supports, aligned, journeys, config, guards=guards)
        cells = stage(cells)
        local, decisions, source = score_cells(cells, supports, journeys, guards, config, stage)
        result.scoring_supports, result.local_scores, result.source_scores, result.decisions = supports, local, source, decisions
        result.principal_segments = summarize_periods(decisions)
        result.principal_points = decisions.filter(F.col("is_point") & (F.col("decision") == "PRINCIPAL")).select(
            "entity_key", "epoch_key", F.col("selected_country").alias("country_code"), F.col("start_ts").alias("ts"),
            "phase", F.col("best_score").alias("score"), F.col("winning_sources").alias("sources"), "cell_id")
        result.unresolved_periods = summarize_periods(decisions, principal=False)
        result.unresolved_points = decisions.filter(F.col("is_point") & (F.col("decision") != "PRINCIPAL"))
    return result




def example_user_16(spark, checkpoint_dir):
    """Reproduire les 16 lignes et les parametres retenus dans la conversation.

    event_id generes uniquement pour cet exemple fixe. En production, reutiliser
    les event_id stables de Silver. is_resolved=True est l'hypothese de l'exemple.
    Les heures sont interpretees dans spark.sql.session.timeZone, sans dependre
    du fuseau Python de la machine du driver.
    """
    data = [('a', 'pid', 100.0, 1.0, '07/05/2026 11:31', '07/05/2026 11:31', None, 'FR', None, 'a', 'TRANSITION'),
     ('a', 'pid', 100.0, 0.8, '07/05/2026 13:00', '07/05/2026 15:10', None, 'FR', 'ES', 'b', 'TRANSITION'),
     ('a', 'pid', 100.0, 0.8, '09/05/2026 00:40', '09/05/2026 02:45', None, 'es', 'fr', 'b', 'TRANSITION'),
     ('a', 'pid', 100.0, 1.0, '09/05/2026 02:38', '09/05/2026 02:38', None, None, 'fr', 'a', 'TRANSITION'),
     ('a', 'pid', 100.0, 1.0, '02/06/2026 14:24', '02/06/2026 14:24', None, 'FR', None, 'a', 'TRANSITION'),
     ('a', 'pid', 100.0, 0.8, '02/06/2026 15:55', '02/06/2026 16:55', None, 'FR', 'DE', 'b', 'TRANSITION'),
     ('b', 'qw', 94.0, 0.7, '02/06/2026 17:20', '02/06/2026 23:58', 'DK', None, None, 'c', 'PRESENCE'),
     ('c', 'qw', 94.0, 0.7, '02/06/2026 17:21', '02/06/2026 20:15', 'DK', None, None, 'c', 'PRESENCE'),
     ('b', 'qw', 94.0, 0.7, '02/06/2026 17:28', '02/06/2026 18:27', 'DE', None, None, 'c', 'PRESENCE'),
     ('b', 'qw', 94.0, 0.7, '03/06/2026 00:10', '03/06/2026 20:49', 'DK', None, None, 'c', 'PRESENCE'),
     ('c', 'qw', 94.0, 0.7, '03/06/2026 07:51', '03/06/2026 19:21', 'DK', None, None, 'c', 'PRESENCE'),
     ('c', 'qw', 94.0, 0.7, '04/06/2026 08:06', '04/06/2026 18:54', 'DK', None, None, 'c', 'PRESENCE'),
     ('b', 'qw', 94.0, 0.7, '04/06/2026 20:35', '04/06/2026 23:36', 'ch', None, None, 'c', 'PRESENCE'),
     ('a', 'pid', 100.0, 0.8, '04/06/2026 22:50', '05/06/2026 00:50', None, 'ch', 'fr', 'b', 'TRANSITION'),
     ('b', 'qw', 94.0, 0.7, '05/06/2026 00:52', '05/06/2026 00:52', 'ch', None, None, 'c', 'PRESENCE'),
     ('a', 'pid', 100.0, 1.0, '05/06/2026 01:14', '05/06/2026 01:14', None, None, 'FR', 'a', 'TRANSITION')]
    rows = [("1", True, f"example_{i:03d}", identifier, kind, id_score, source_score,
             start, end,
             country, origin, destination, source, event_type)
            for i, (identifier, kind, id_score, source_score, start, end,
                    country, origin, destination, source, event_type) in enumerate(data, 1)]
    schema = """entity_key string, is_resolved boolean, event_id string,
        raw_identifier string, identifier_type string, identifier_confidence double,
        source_confidence double, valid_from string, valid_to string,
        country_code string, country_from string, country_to string,
        source_name string, event_type string"""
    config = ScoringConfig(confirmed_transition_sources=("a",),
                           scheduled_transition_sources=("b",))
    silver = spark.createDataFrame(rows, schema).withColumn(
        "valid_from", F.to_timestamp("valid_from", "dd/MM/yyyy HH:mm")).withColumn(
        "valid_to", F.to_timestamp("valid_to", "dd/MM/yyyy HH:mm"))
    return build_travel_ranges(silver, config=config,
        checkpoint_dir=checkpoint_dir, persist_intermediates=True)
