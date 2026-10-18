"""Gold observations -> Iceberg -> Hive/ORC -> Elasticsearch Bulk.

Module autonome, PySpark 3.5.x / Iceberg 1.8.x+, Elasticsearch 8.x/9.x.
Pas d'inference, pas de calcul d'alternatives, pas de regroupement de sejours.
Un seul job de publication a la fois; voir GUIDE_GOLD_INDEXATION.md.
Les noms physiques Silver se configurent dans SilverColumns.
"""
from __future__ import annotations

# ISO 3166 alpha-2 -> alpha-3 (iso-codes/pycountry 24.6.1); les codes alpha-3 sont conserves.
ISO_COUNTRY_ALIASES = {'AD': 'AND', 'AE': 'ARE', 'AF': 'AFG', 'AG': 'ATG', 'AI': 'AIA', 'AL': 'ALB', 'AM': 'ARM', 'AO': 'AGO', 'AQ': 'ATA', 'AR': 'ARG', 'AS': 'ASM', 'AT': 'AUT', 'AU': 'AUS', 'AW': 'ABW', 'AX': 'ALA', 'AZ': 'AZE', 'BA': 'BIH', 'BB': 'BRB', 'BD': 'BGD', 'BE': 'BEL', 'BF': 'BFA', 'BG': 'BGR', 'BH': 'BHR', 'BI': 'BDI', 'BJ': 'BEN', 'BL': 'BLM', 'BM': 'BMU', 'BN': 'BRN', 'BO': 'BOL', 'BQ': 'BES', 'BR': 'BRA', 'BS': 'BHS', 'BT': 'BTN', 'BV': 'BVT', 'BW': 'BWA', 'BY': 'BLR', 'BZ': 'BLZ', 'CA': 'CAN', 'CC': 'CCK', 'CD': 'COD', 'CF': 'CAF', 'CG': 'COG', 'CH': 'CHE', 'CI': 'CIV', 'CK': 'COK', 'CL': 'CHL', 'CM': 'CMR', 'CN': 'CHN', 'CO': 'COL', 'CR': 'CRI', 'CU': 'CUB', 'CV': 'CPV', 'CW': 'CUW', 'CX': 'CXR', 'CY': 'CYP', 'CZ': 'CZE', 'DE': 'DEU', 'DJ': 'DJI', 'DK': 'DNK', 'DM': 'DMA', 'DO': 'DOM', 'DZ': 'DZA', 'EC': 'ECU', 'EE': 'EST', 'EG': 'EGY', 'EH': 'ESH', 'ER': 'ERI', 'ES': 'ESP', 'ET': 'ETH', 'FI': 'FIN', 'FJ': 'FJI', 'FK': 'FLK', 'FM': 'FSM', 'FO': 'FRO', 'FR': 'FRA', 'GA': 'GAB', 'GB': 'GBR', 'GD': 'GRD', 'GE': 'GEO', 'GF': 'GUF', 'GG': 'GGY', 'GH': 'GHA', 'GI': 'GIB', 'GL': 'GRL', 'GM': 'GMB', 'GN': 'GIN', 'GP': 'GLP', 'GQ': 'GNQ', 'GR': 'GRC', 'GS': 'SGS', 'GT': 'GTM', 'GU': 'GUM', 'GW': 'GNB', 'GY': 'GUY', 'HK': 'HKG', 'HM': 'HMD', 'HN': 'HND', 'HR': 'HRV', 'HT': 'HTI', 'HU': 'HUN', 'ID': 'IDN', 'IE': 'IRL', 'IL': 'ISR', 'IM': 'IMN', 'IN': 'IND', 'IO': 'IOT', 'IQ': 'IRQ', 'IR': 'IRN', 'IS': 'ISL', 'IT': 'ITA', 'JE': 'JEY', 'JM': 'JAM', 'JO': 'JOR', 'JP': 'JPN', 'KE': 'KEN', 'KG': 'KGZ', 'KH': 'KHM', 'KI': 'KIR', 'KM': 'COM', 'KN': 'KNA', 'KP': 'PRK', 'KR': 'KOR', 'KW': 'KWT', 'KY': 'CYM', 'KZ': 'KAZ', 'LA': 'LAO', 'LB': 'LBN', 'LC': 'LCA', 'LI': 'LIE', 'LK': 'LKA', 'LR': 'LBR', 'LS': 'LSO', 'LT': 'LTU', 'LU': 'LUX', 'LV': 'LVA', 'LY': 'LBY', 'MA': 'MAR', 'MC': 'MCO', 'MD': 'MDA', 'ME': 'MNE', 'MF': 'MAF', 'MG': 'MDG', 'MH': 'MHL', 'MK': 'MKD', 'ML': 'MLI', 'MM': 'MMR', 'MN': 'MNG', 'MO': 'MAC', 'MP': 'MNP', 'MQ': 'MTQ', 'MR': 'MRT', 'MS': 'MSR', 'MT': 'MLT', 'MU': 'MUS', 'MV': 'MDV', 'MW': 'MWI', 'MX': 'MEX', 'MY': 'MYS', 'MZ': 'MOZ', 'NA': 'NAM', 'NC': 'NCL', 'NE': 'NER', 'NF': 'NFK', 'NG': 'NGA', 'NI': 'NIC', 'NL': 'NLD', 'NO': 'NOR', 'NP': 'NPL', 'NR': 'NRU', 'NU': 'NIU', 'NZ': 'NZL', 'OM': 'OMN', 'PA': 'PAN', 'PE': 'PER', 'PF': 'PYF', 'PG': 'PNG', 'PH': 'PHL', 'PK': 'PAK', 'PL': 'POL', 'PM': 'SPM', 'PN': 'PCN', 'PR': 'PRI', 'PS': 'PSE', 'PT': 'PRT', 'PW': 'PLW', 'PY': 'PRY', 'QA': 'QAT', 'RE': 'REU', 'RO': 'ROU', 'RS': 'SRB', 'RU': 'RUS', 'RW': 'RWA', 'SA': 'SAU', 'SB': 'SLB', 'SC': 'SYC', 'SD': 'SDN', 'SE': 'SWE', 'SG': 'SGP', 'SH': 'SHN', 'SI': 'SVN', 'SJ': 'SJM', 'SK': 'SVK', 'SL': 'SLE', 'SM': 'SMR', 'SN': 'SEN', 'SO': 'SOM', 'SR': 'SUR', 'SS': 'SSD', 'ST': 'STP', 'SV': 'SLV', 'SX': 'SXM', 'SY': 'SYR', 'SZ': 'SWZ', 'TC': 'TCA', 'TD': 'TCD', 'TF': 'ATF', 'TG': 'TGO', 'TH': 'THA', 'TJ': 'TJK', 'TK': 'TKL', 'TL': 'TLS', 'TM': 'TKM', 'TN': 'TUN', 'TO': 'TON', 'TR': 'TUR', 'TT': 'TTO', 'TV': 'TUV', 'TW': 'TWN', 'TZ': 'TZA', 'UA': 'UKR', 'UG': 'UGA', 'UM': 'UMI', 'US': 'USA', 'UY': 'URY', 'UZ': 'UZB', 'VA': 'VAT', 'VC': 'VCT', 'VE': 'VEN', 'VG': 'VGB', 'VI': 'VIR', 'VN': 'VNM', 'VU': 'VUT', 'WF': 'WLF', 'WS': 'WSM', 'YE': 'YEM', 'YT': 'MYT', 'ZA': 'ZAF', 'ZM': 'ZMB', 'ZW': 'ZWE'}

from dataclasses import dataclass, field

from math import isfinite

from typing import Dict, Optional, Tuple

from pyspark import StorageLevel

from pyspark.sql import DataFrame, Window, functions as F

@dataclass(frozen=True)
class SilverColumns:
    entity_key: str = "entity_key"
    event_id: str = "event_id"
    event_type: str = "event_type"
    valid_from: str = "valid_from"
    valid_to: str = "valid_to"
    country: str = "country_code"
    country_from: str = "country_from"
    country_to: str = "country_to"
    raw_identifier: str = "raw_identifier"
    identifier_type: str = "identifier_type"
    identifier_namespace: Optional[str] = None
    identifier_confidence: str = "identifier_confidence"
    logic_confidence: str = "logic_confidence"
    occurrence_count: str = "occurrence_count"
    source: Optional[str] = None  # source_name, sinon source_id
    dependency_group: str = "dependency_group"
    source_confidence: str = "source_confidence"
    is_resolved: str = "is_resolved"
    timestamp_format: Optional[str] = None

@dataclass(frozen=True)
class ScoringConfig:
    logic_weight: float = .25
    identifier_weight: float = .35
    occurrence_weight: float = .10
    convergence_weight: float = .30
    identifier_scale: float = 100.
    occurrence_saturation: float = 10.
    convergence_saturation: float = 2.
    bonus_levels: Tuple[Tuple[float, float], ...] = ((24., .15), (72., .05))
    presence_cap: float = .85
    transition_cap: float = .90
    dependency_groups: Dict[str, str] = field(default_factory=dict)
    include_score_components: bool = False
    enable_temporal_bonus: bool = True
    country_aliases: Dict[str, str] = field(default_factory=lambda: dict(ISO_COUNTRY_ALIASES))

    def __post_init__(self):
        weights = (self.logic_weight, self.identifier_weight,
                   self.occurrence_weight, self.convergence_weight)
        if any(not isfinite(x) or x < 0 for x in weights) or abs(sum(weights)-1.) > 1e-9:
            raise ValueError("Les quatre poids doivent etre positifs et totaliser 1")
        for name in ("identifier_scale", "occurrence_saturation", "convergence_saturation"):
            value = getattr(self, name)
            if not isfinite(value) or value <= 0:
                raise ValueError(name + " doit etre strictement positif")
        for name in ("presence_cap", "transition_cap"):
            if not isfinite(getattr(self, name)) or not 0 < getattr(self, name) <= 1:
                raise ValueError(name + " doit etre dans ]0,1]")
        previous = 0.
        for hours, bonus in self.bonus_levels:
            if not isfinite(hours) or hours <= previous or not isfinite(bonus) or not 0 <= bonus <= 1:
                raise ValueError("Paliers de bonus invalides")
            previous = hours
        if not self.bonus_levels:
            raise ValueError("Paliers de bonus manquants")

@dataclass
class GoldResult:
    gold: DataFrame
    rejected_events: DataFrame
    _persisted: Tuple[DataFrame, ...] = field(default_factory=tuple, repr=False)

    def unpersist(self, blocking=False):
        for frame in self._persisted:
            frame.unpersist(blocking=blocking)

def _col(name):
    return F.col("`" + name.replace("`", "``") + "`")

def _text(column):
    value = F.trim(column.cast("string"))
    return F.when(F.length(value) > 0, value)

def _hash(*columns):
    return F.sha2(F.to_json(F.struct(*columns)), 256)

def _invalid_confidence(column, maximum=1.):
    return column.isNull() | F.isnan(column) | ~column.between(0., maximum)

def prepare_events(silver, columns=None, config=None):
    """Retourne (originals avec cles techniques, metadonnees acceptees, rejets).

    Aucune valeur d'occurrences/confiance manquante n'est inventee.
    Les doublons de (entity_key,event_id) sont tous rejetes avec leur raison.
    """
    columns, config = columns or SilverColumns(), config or ScoringConfig()
    generated = {"gold_id", "start_ts", "end_ts", "transition_id", "transition_role",
                 "nature", "score", "score_components"}
    collisions = generated.intersection(silver.columns)
    if collisions or any(name.startswith("_g_") for name in silver.columns):
        raise ValueError("Colonnes reservees Gold presentes dans Silver: " + ", ".join(sorted(collisions)))
    required = [columns.entity_key, columns.event_id, columns.event_type,
                columns.valid_from, columns.logic_confidence,
                columns.identifier_confidence, columns.occurrence_count]
    source = columns.source or next((name for name in ("source_name", "source_id") if name in silver.columns), None)
    if source:
        required.append(source)
    else:
        raise ValueError("Renseigner SilverColumns.source ou fournir source_name/source_id")
    missing = set(required).difference(silver.columns)
    if columns.identifier_namespace and columns.identifier_namespace not in silver.columns:
        missing.add(columns.identifier_namespace)
    if missing:
        raise ValueError("Colonnes manquantes; adapter SilverColumns: " + ", ".join(sorted(missing)))
    def optional(name, dtype="string"):
        return _col(name).cast(dtype) if name in silver.columns else F.lit(None).cast(dtype)
    def timestamp(name):
        value = _col(name)
        return F.to_timestamp(value, columns.timestamp_format) if columns.timestamp_format else value.cast("timestamp")
    originals = silver.withColumn("_g_entity", _text(_col(columns.entity_key))).withColumn(
        "_g_event", _text(_col(columns.event_id)))
    meta = originals.select(
        "_g_entity", "_g_event",
        F.upper(_text(_col(columns.event_type))).alias("_g_type"),
        timestamp(columns.valid_from).alias("_g_start"),
        (F.when(_col(columns.valid_to).isNull(), timestamp(columns.valid_from)).otherwise(timestamp(columns.valid_to))
         if columns.valid_to in silver.columns else timestamp(columns.valid_from)).alias("_g_end"),
        F.upper(_text(optional(columns.country))).alias("_g_country"),
        F.upper(_text(optional(columns.country_from))).alias("_g_from"),
        F.upper(_text(optional(columns.country_to))).alias("_g_to"),
        _text(optional(columns.raw_identifier)).alias("_g_raw"),
        _text(optional(columns.identifier_type)).alias("_g_identifier_type"),
        _text(optional(columns.identifier_namespace)).alias("_g_namespace") if columns.identifier_namespace else F.lit("").alias("_g_namespace"),
        _col(columns.logic_confidence).cast("double").alias("_g_logic"),
        _col(columns.identifier_confidence).cast("double").alias("_g_identity"),
        _col(columns.occurrence_count).cast("double").alias("_g_count"),
        _text(_col(source)).alias("_g_source"),
        _text(optional(columns.dependency_group)).alias("_g_dependency"),
        F.coalesce(optional(columns.source_confidence, "double"), _col(columns.logic_confidence).cast("double")).alias("_g_transition_score"),
        F.coalesce(optional(columns.is_resolved, "boolean"), F.lit(False)).alias("_g_resolved")
        if columns.is_resolved in silver.columns else F.lit(False).alias("_g_resolved")
    ).withColumn("_g_type", F.when(F.col("_g_type") == "TRANSITIONS", "TRANSITION").otherwise(F.col("_g_type")))
    aliases = F.create_map(*[F.lit(v) for pair in sorted(config.country_aliases.items()) for v in pair]) if config.country_aliases else None
    if aliases is not None:
        for name in ("_g_country", "_g_from", "_g_to"):
            meta = meta.withColumn(name, F.coalesce(F.element_at(aliases, F.col(name)), F.col(name)))
    group = F.col("_g_source")
    for source_name, dependency in config.dependency_groups.items():
        group = F.when(F.col("_g_source") == source_name, F.lit(dependency)).otherwise(group)
    meta = meta.withColumn("_g_group", F.coalesce("_g_dependency", group)).withColumn(
        "_g_identifier", F.when(F.col("_g_raw").isNotNull() & F.col("_g_identifier_type").isNotNull(),
        F.to_json(F.struct("_g_namespace", "_g_identifier_type", "_g_raw"))))
    duplicate_window = Window.partitionBy("_g_entity", "_g_event")
    meta = meta.withColumn("_g_duplicates", F.count(F.lit(1)).over(duplicate_window))
    presence = F.col("_g_type") == "PRESENCE"
    transition = F.col("_g_type") == "TRANSITION"
    reason = (F.when(F.col("_g_entity").isNull(), "MISSING_ENTITY_KEY")
        .when(F.col("_g_event").isNull(), "MISSING_EVENT_ID")
        .when(F.col("_g_type").isNull() | ~F.col("_g_type").isin("PRESENCE", "TRANSITION"), "INVALID_EVENT_TYPE")
        .when(F.col("_g_start").isNull() | F.col("_g_end").isNull(), "INVALID_TIMESTAMP")
        .when(F.col("_g_end") < F.col("_g_start"), "END_BEFORE_START")
        .when(F.col("_g_raw").isNotNull() & F.col("_g_identifier_type").isNull(), "MISSING_IDENTIFIER_TYPE")
        .when(presence & F.col("_g_country").isNull(), "MISSING_PRESENCE_COUNTRY")
        .when(presence & ~F.col("_g_country").rlike("^[A-Z]{3}$"), "INVALID_COUNTRY_CODE")
        .when(transition & ((F.col("_g_from").isNotNull() & ~F.col("_g_from").rlike("^[A-Z]{3}$")) | (F.col("_g_to").isNotNull() & ~F.col("_g_to").rlike("^[A-Z]{3}$"))), "INVALID_COUNTRY_CODE")
        .when(presence & F.col("_g_source").isNull(), "MISSING_SOURCE")
        .when(presence & (_invalid_confidence(F.col("_g_logic")) |
            _invalid_confidence(F.col("_g_identity"), config.identifier_scale)), "INVALID_CONFIDENCE")
        .when(presence & (F.col("_g_count").isNull() | F.isnan("_g_count") |
            ~F.col("_g_count").between(0., 1.7976931348623157e308) |
            (F.col("_g_count") != F.floor("_g_count"))), "INVALID_OCCURRENCE_COUNT")
        .when(transition & F.col("_g_from").isNull() & F.col("_g_to").isNull(), "MISSING_TRANSITION_COUNTRY")
        .when(transition & _invalid_confidence(F.col("_g_transition_score")), "INVALID_TRANSITION_CONFIDENCE")
        .when(F.col("_g_duplicates") > 1, "DUPLICATE_EVENT_ID"))
    checked = meta.withColumn("rejection_reason", reason)
    bad_keys = checked.filter(F.col("rejection_reason").isNotNull()).select(
        "_g_entity", "_g_event", "rejection_reason").dropDuplicates(["_g_entity", "_g_event"])
    original, bad = originals.alias("o"), bad_keys.alias("b")
    rejected = original.join(bad, F.col("o._g_entity").eqNullSafe(F.col("b._g_entity")) &
        F.col("o._g_event").eqNullSafe(F.col("b._g_event")), "inner").select(
        *[F.col("o.`" + name.replace("`", "``") + "`").alias(name) for name in silver.columns],
        F.col("b.rejection_reason"))
    accepted = checked.filter(F.col("rejection_reason").isNull()).drop("rejection_reason", "_g_duplicates")
    return originals, accepted, rejected

def _anchors(events):
    presence = events.filter(F.col("_g_type") == "PRESENCE").withColumn("_g_role", F.lit("OBSERVATION"))
    transitions = events.filter(F.col("_g_type") == "TRANSITION")
    departure = transitions.filter(F.col("_g_from").isNotNull()).withColumn("_g_country", F.col("_g_from")).withColumn(
        "_g_end", F.col("_g_start")).withColumn("_g_role", F.lit("DEPARTURE"))
    arrival = transitions.filter(F.col("_g_to").isNotNull()).withColumn("_g_country", F.col("_g_to")).withColumn(
        "_g_start", F.col("_g_end")).withColumn("_g_role", F.lit("ARRIVAL"))
    anchors = presence.unionByName(departure).unionByName(arrival).withColumn(
        "_g_id", _hash(F.col("_g_entity"), F.col("_g_event"), F.col("_g_role"))).withColumn(
        "_g_phase", F.when(F.col("_g_role") == "DEPARTURE", 0).when(F.col("_g_role") == "ARRIVAL", 1).otherwise(2))
    ordered = Window.partitionBy("_g_entity").orderBy("_g_start", "_g_phase", "_g_end", "_g_id")
    preceding = ordered.rowsBetween(Window.unboundedPreceding, -1)
    # Depart: contexte avant la coupure; arrivee: contexte apres la coupure.
    return anchors.withColumn("_g_epoch", F.coalesce(F.sum(F.when(F.col("_g_type") == "TRANSITION", 1).otherwise(0)).over(
        preceding), F.lit(0)) + F.when(F.col("_g_role") == "ARRIVAL", 1).otherwise(0))

def _neighbor():
    return F.struct(F.col("_g_start").alias("start"), F.col("_g_phase").alias("phase"),
        F.col("_g_end").alias("end"), F.col("_g_id").alias("id"),
        F.col("_g_event").alias("event_id"), F.col("_g_country").alias("country"),
        F.col("_g_epoch").alias("epoch"), F.col("_g_identifier").alias("identifier"))

def _bonus(neighbor_name, following, config):
    neighbor = F.col(neighbor_name)
    gap = F.greatest(F.lit(0.),
        (neighbor["start"].cast("double") - F.col("_g_end").cast("double")) if following else
        (F.col("_g_start").cast("double") - neighbor["end"].cast("double"))) / 3600.
    value = F.lit(0.)
    for hours, bonus in reversed(config.bonus_levels):
        value = F.when(gap <= hours, F.lit(bonus)).otherwise(value)
    supported = neighbor.isNotNull() & (neighbor["country"] == F.col("_g_country")) & (
        neighbor["epoch"] == F.col("_g_epoch")) & (F.col("_g_type") == "PRESENCE")
    return F.when(supported, value).otherwise(0.)

def _raw_bonus(anchors, config):
    identified = anchors.filter(F.col("_g_identifier").isNotNull())
    order = Window.partitionBy("_g_entity", "_g_identifier").orderBy("_g_start", "_g_phase", "_g_end", "_g_id")
    neighbors = identified.withColumn("_g_raw_previous", F.lag(_neighbor()).over(order)).withColumn(
        "_g_raw_next", F.lead(_neighbor()).over(order))
    neighbors = neighbors.withColumn("_g_raw_previous_bonus", _bonus("_g_raw_previous", False, config)).withColumn(
        "_g_raw_next_bonus", _bonus("_g_raw_next", True, config))
    return neighbors.select("_g_id", "_g_raw_previous", "_g_raw_next", "_g_raw_previous_bonus", "_g_raw_next_bonus",
        (F.col("_g_raw_previous_bonus") + F.col("_g_raw_next_bonus")).alias("_g_bonus_raw"))

def _entity_bonus(anchors, config, keep):
    identified = anchors.filter(F.col("_g_identifier").isNotNull() & F.col("_g_resolved"))
    order = Window.partitionBy("_g_entity", "_g_epoch").orderBy("_g_start", "_g_phase", "_g_end", "_g_id")
    runs = identified.withColumn("_g_previous_identifier", F.lag("_g_identifier").over(order)).withColumn(
        "_g_new_run", F.when(F.col("_g_previous_identifier").eqNullSafe(F.col("_g_identifier")), 0).otherwise(1)).withColumn(
        "_g_run", F.sum("_g_new_run").over(order.rowsBetween(Window.unboundedPreceding, Window.currentRow)))
    runs = keep(runs.drop("_g_previous_identifier", "_g_new_run"))
    # Blocs consecutifs du meme identifiant: voisin externe en temps lineaire,
    # sans liste historique non bornee ni self-join des observations.
    blocks = runs.groupBy("_g_entity", "_g_epoch", "_g_run").agg(
        F.min(_neighbor()).alias("_g_first"), F.max(_neighbor()).alias("_g_last"))
    block_order = Window.partitionBy("_g_entity", "_g_epoch").orderBy("_g_run")
    blocks = blocks.withColumn("_g_entity_previous", F.lag("_g_last").over(block_order)).withColumn(
        "_g_entity_next", F.lead("_g_first").over(block_order)).select(
        "_g_entity", "_g_epoch", "_g_run", "_g_entity_previous", "_g_entity_next")
    neighbors = runs.join(blocks, ["_g_entity", "_g_epoch", "_g_run"], "left")
    neighbors = neighbors.withColumn("_g_entity_previous_bonus", _bonus("_g_entity_previous", False, config)).withColumn(
        "_g_entity_next_bonus", _bonus("_g_entity_next", True, config))
    return neighbors.select("_g_id", "_g_entity_previous", "_g_entity_next", "_g_entity_previous_bonus", "_g_entity_next_bonus",
        (F.col("_g_entity_previous_bonus") + F.col("_g_entity_next_bonus")).alias("_g_bonus_entity"))

def build_travel_gold(silver, config=None, columns=None, persist_intermediates=True,
                      materialize_intermediates=False, checkpoint_dir=None):
    """GoldResult.gold contient toutes les lignes valides, sans filtre de score.

    materialize_intermediates=True coupe les plans avec des checkpoints fiables;
    fournir checkpoint_dir partage ou le configurer sur SparkContext auparavant.
    """
    config, columns = config or ScoringConfig(), columns or SilverColumns()
    persisted = []
    if checkpoint_dir:
        silver.sparkSession.sparkContext.setCheckpointDir(checkpoint_dir)
    if materialize_intermediates and not silver.sparkSession.sparkContext.getCheckpointDir():
        raise ValueError("Configurer un repertoire de checkpoints partage")
    def keep(frame):
        if materialize_intermediates:
            return frame.checkpoint(eager=True)
        if persist_intermediates:
            frame = frame.persist(StorageLevel.MEMORY_AND_DISK)
            persisted.append(frame)
        return frame
    originals, events, rejected = prepare_events(silver, columns, config)
    events = keep(events)
    anchors = keep(_anchors(events))
    daily = events.filter(F.col("_g_type") == "PRESENCE").withColumn("_g_day", F.to_date("_g_start")).groupBy(
        "_g_entity", "_g_country", "_g_day").agg((F.countDistinct("_g_group") - 1).alias("_g_convergence_count"))
    scored = anchors.withColumn("_g_day", F.to_date("_g_start")).join(daily, ["_g_entity", "_g_country", "_g_day"], "left").withColumn(
        "_g_convergence_count", F.coalesce("_g_convergence_count", F.lit(0))).withColumn(
        "_g_occurrence_factor", F.least(F.col("_g_count") / config.occurrence_saturation, F.lit(1.))).withColumn(
        "_g_convergence_factor", F.least(F.col("_g_convergence_count") / config.convergence_saturation, F.lit(1.))).withColumn(
        "_g_base", config.logic_weight*F.col("_g_logic") + config.identifier_weight*F.col("_g_identity")/config.identifier_scale
        + config.occurrence_weight*F.col("_g_occurrence_factor") + config.convergence_weight*F.col("_g_convergence_factor"))
    if config.enable_temporal_bonus:
        scored = scored.join(_raw_bonus(anchors, config), "_g_id", "left").join(_entity_bonus(anchors, config, keep), "_g_id", "left").withColumn(
            "_g_bonus_raw", F.coalesce("_g_bonus_raw", F.lit(0.))).withColumn("_g_bonus_entity", F.coalesce("_g_bonus_entity", F.lit(0.)))
    else:
        for prefix in ("raw_previous", "raw_next", "entity_previous", "entity_next"):
            scored = scored.withColumn("_g_" + prefix, F.lit(None).cast("struct<event_id:string>")).withColumn("_g_" + prefix + "_bonus", F.lit(0.))
        scored = scored.withColumn("_g_bonus_raw", F.lit(0.)).withColumn("_g_bonus_entity", F.lit(0.))
    scored = keep(scored.withColumn("score", F.when(F.col("_g_type") == "PRESENCE",
        F.least(F.lit(config.presence_cap), F.col("_g_base")*(1+F.col("_g_bonus_raw")+F.col("_g_bonus_entity")))).otherwise(
        F.least(F.lit(config.transition_cap), F.col("_g_transition_score")))))
    country_fields = {columns.country, columns.country_from, columns.country_to}
    original_fields = [name for name in silver.columns if name not in country_fields and name != "is_alternative"]
    joined = originals.alias("o").join(scored.alias("g"), ["_g_entity", "_g_event"], "inner")
    result_fields = [F.col("o.`" + name.replace("`", "``") + "`").alias(name) for name in original_fields]
    # Canonical entity_key/event_id ajoutés uniquement si les noms d'entree different.
    if "entity_key" not in original_fields:
        result_fields.append(F.col("g._g_entity").alias("entity_key"))
    if "event_id" not in original_fields:
        result_fields.append(F.col("g._g_event").alias("event_id"))
    result_fields += [F.col("g._g_id").alias("gold_id"), F.col("g._g_country").alias("country"),
        F.col("g._g_start").alias("start_ts"), F.col("g._g_end").alias("end_ts"),
        F.when(F.col("g._g_type") == "PRESENCE", F.lit("OBSERVATION")).otherwise(F.lit("TRANSITION")).alias("nature"),
        F.when(F.col("g._g_type") == "TRANSITION", F.col("g._g_event")).alias("transition_id"),
        F.when(F.col("g._g_type") == "TRANSITION", F.col("g._g_role")).alias("transition_role"),
        F.col("g.score")]
    if config.include_score_components:
        result_fields.append(F.struct(
            F.when(F.col("g._g_type") == "PRESENCE", F.col("g._g_base")).otherwise(F.col("g._g_transition_score")).alias("base_score"),
            F.when(F.col("g._g_type") == "PRESENCE", F.col("g._g_occurrence_factor")).alias("occurrence_factor"),
            F.when(F.col("g._g_type") == "PRESENCE", F.col("g._g_convergence_count")).alias("convergence_count"),
            F.when(F.col("g._g_type") == "PRESENCE", F.col("g._g_convergence_factor")).alias("convergence_factor"),
            F.col("g._g_bonus_raw").alias("bonus_raw"), F.col("g._g_bonus_entity").alias("bonus_entity"),
            F.when(F.col("g._g_raw_previous_bonus") > 0, F.col("g._g_raw_previous.event_id")).alias("raw_previous_event"),
            F.when(F.col("g._g_raw_next_bonus") > 0, F.col("g._g_raw_next.event_id")).alias("raw_next_event"),
            F.when(F.col("g._g_entity_previous_bonus") > 0, F.col("g._g_entity_previous.event_id")).alias("entity_previous_event"),
            F.when(F.col("g._g_entity_next_bonus") > 0, F.col("g._g_entity_next.event_id")).alias("entity_next_event")
        ).alias("score_components"))
    return GoldResult(joined.select(*result_fields), rejected, tuple(persisted))

def example_user_16(spark, checkpoint_dir=None, occurrence_count=1):
    """Les 16 entrees originales; occurrences ILLUSTRATIVES, absentes de l'exemple.

    source_confidence est recopiee en logic_confidence UNIQUEMENT pour ce jeu de
    demonstration. Fournir les vrais comptes et confiances logiques en production.
    """
    data = [
        ('a','pid',100.,1.,'07/05/2026 11:31','07/05/2026 11:31',None,'FR',None,'a','TRANSITION'),
        ('a','pid',100.,.8,'07/05/2026 13:00','07/05/2026 15:10',None,'FR','ES','b','TRANSITION'),
        ('a','pid',100.,.8,'09/05/2026 00:40','09/05/2026 02:45',None,'es','fr','b','TRANSITION'),
        ('a','pid',100.,1.,'09/05/2026 02:38','09/05/2026 02:38',None,None,'fr','a','TRANSITION'),
        ('a','pid',100.,1.,'02/06/2026 14:24','02/06/2026 14:24',None,'FR',None,'a','TRANSITION'),
        ('a','pid',100.,.8,'02/06/2026 15:55','02/06/2026 16:55',None,'FR','DE','b','TRANSITION'),
        ('b','qw',94.,.7,'02/06/2026 17:20','02/06/2026 23:58','DK',None,None,'c','PRESENCE'),
        ('c','qw',94.,.7,'02/06/2026 17:21','02/06/2026 20:15','DK',None,None,'c','PRESENCE'),
        ('b','qw',94.,.7,'02/06/2026 17:28','02/06/2026 18:27','DE',None,None,'c','PRESENCE'),
        ('b','qw',94.,.7,'03/06/2026 00:10','03/06/2026 20:49','DK',None,None,'c','PRESENCE'),
        ('c','qw',94.,.7,'03/06/2026 07:51','03/06/2026 19:21','DK',None,None,'c','PRESENCE'),
        ('c','qw',94.,.7,'04/06/2026 08:06','04/06/2026 18:54','DK',None,None,'c','PRESENCE'),
        ('b','qw',94.,.7,'04/06/2026 20:35','04/06/2026 23:36','ch',None,None,'c','PRESENCE'),
        ('a','pid',100.,.8,'04/06/2026 22:50','05/06/2026 00:50',None,'ch','fr','b','TRANSITION'),
        ('b','qw',94.,.7,'05/06/2026 00:52','05/06/2026 00:52','ch',None,None,'c','PRESENCE'),
        ('a','pid',100.,1.,'05/06/2026 01:14','05/06/2026 01:14',None,None,'FR','a','TRANSITION')]
    silver = _example_frame(spark, data, occurrence_count)
    return build_travel_gold(silver, config=ScoringConfig(include_score_components=True),
        checkpoint_dir=checkpoint_dir, materialize_intermediates=bool(checkpoint_dir))

def example_tha(spark, checkpoint_dir=None, occurrence_count=1):
    """Cas BE/THA: quatre entrees, sans prolongement du point THA dans Gold."""
    data = [
        ('b','qw',94.,.6,'14/06/2026 20:11','14/06/2026 20:13','BE',None,None,'c','PRESENCE'),
        ('a','pid',100.,1.,'28/08/2026 09:27','28/08/2026 09:27',None,'FR','BE','a','TRANSITION'),
        ('b','qw',94.,.6,'29/08/2026 15:09','29/08/2026 15:09','THA',None,None,'c','PRESENCE'),
        ('a','pid',100.,1.,'11/09/2026 08:41','11/09/2026 08:41',None,'BE','FR','a','TRANSITION')]
    return build_travel_gold(_example_frame(spark, data, occurrence_count),
        config=ScoringConfig(include_score_components=True), checkpoint_dir=checkpoint_dir,
        materialize_intermediates=bool(checkpoint_dir))

def _example_frame(spark, data, occurrence_count):
    rows = [("1", True, "example_%03d" % index, raw, identifier_type, identity, logic, logic,
             int(occurrence_count), start, end, country, origin, destination, source, event_type)
            for index, (raw, identifier_type, identity, logic, start, end, country, origin, destination, source, event_type)
            in enumerate(data, 1)]
    schema = """entity_key string, is_resolved boolean, event_id string, raw_identifier string,
        identifier_type string, identifier_confidence double, logic_confidence double,
        source_confidence double, occurrence_count long, valid_from string, valid_to string,
        country_code string, country_from string, country_to string, source_name string, event_type string"""
    return spark.createDataFrame(rows, schema).withColumn("valid_from", F.to_timestamp("valid_from", "dd/MM/yyyy HH:mm")).withColumn(
        "valid_to", F.to_timestamp("valid_to", "dd/MM/yyyy HH:mm"))

# ---------------------------------------------------------------------------
# Configuration, publication et indexation. Le moteur de score ci-dessus reste
# independant de l'orchestration et ne contient aucune ecriture.
# ---------------------------------------------------------------------------
import argparse
import base64
import hashlib
import http.client
import json
import logging
import math
import os
import random
import re
import ssl
import time
import uuid
from dataclasses import asdict, fields
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path
from urllib.parse import quote, urlsplit

from pyspark.sql import SparkSession, types as T

LOG = logging.getLogger("travel_gold")
SCHEMA_VERSION = 1
PARTITIONS = ("observation_month", "entity_bucket")
TECHNICAL = {"observation_month", "entity_bucket", "gold_content_hash",
             "gold_run_id", "gold_computed_at", "gold_publication_version"}
OP = "_publication_operation"
JSON_OPTIONS = {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
                "dateFormat": "yyyy-MM-dd", "ignoreNullFields": "false"}


@dataclass(frozen=True)
class ElasticsearchConfig:
    endpoints: Tuple[str, ...] = ("https://localhost:9200",)
    index: str = "travel-observations-v1"
    alias: str = "travel-observations"
    primary_shards: int = 1       # exemple seulement: dimensionner avec un pilote
    replicas: int = 1
    refresh_interval: str = "30s"
    auth: str = "api_key"         # api_key, basic, none
    api_key_env: str = "TRAVEL_ES_API_KEY"
    username_env: str = "TRAVEL_ES_USER"
    password_env: str = "TRAVEL_ES_PASSWORD"
    ca_file: Optional[str] = None
    request_timeout_seconds: float = 60.
    max_retries: int = 6
    backoff_seconds: float = 1.
    max_backoff_seconds: float = 30.
    max_bulk_documents: int = 1000
    max_bulk_bytes: int = 5 * 1024 * 1024
    max_document_bytes: int = 10 * 1024 * 1024
    writer_partitions: int = 8
    source_columns: Optional[Tuple[str, ...]] = None
    index_sort: bool = False
    initial_load_replicas: int = 0

    def __post_init__(self):
        for value in (self.index, self.alias):
            if not re.fullmatch(r"[a-z0-9][a-z0-9._-]{0,200}", value):
                raise ValueError("Nom d'index/alias invalide")
        if self.index == self.alias:
            raise ValueError("L'alias et l'index physique doivent avoir des noms differents")
        if not self.endpoints:
            raise ValueError("Renseigner au moins un endpoint Elasticsearch")
        for endpoint in self.endpoints:
            url = urlsplit(endpoint)
            if url.scheme not in ("http", "https") or not url.hostname or url.username or url.password or url.query or url.fragment:
                raise ValueError("Endpoint HTTP(S) sans identifiants dans l'URL requis")
        if self.auth not in ("api_key", "basic", "none"):
            raise ValueError("auth doit etre api_key, basic ou none")
        for name in ("primary_shards", "writer_partitions", "max_bulk_documents",
                     "max_bulk_bytes", "max_document_bytes"):
            if getattr(self, name) <= 0:
                raise ValueError(name + " doit etre positif")
        if min(self.replicas, self.initial_load_replicas, self.max_retries, self.backoff_seconds) < 0:
            raise ValueError("Repliques, essais et delai doivent etre positifs ou nuls")
        if self.request_timeout_seconds <= 0 or self.max_backoff_seconds < 0:
            raise ValueError("Delais HTTP invalides")


@dataclass(frozen=True)
class PipelineConfig:
    silver_table: str = "lake.silver.location_events"
    gold_table: str = "lake.gold.travel_observations"
    staging_root: str = "hdfs:///apps/travel-gold/publication"
    gold_buckets: int = 256
    shuffle_partitions: int = 400
    calculation_bucket_batch: int = 16
    silver_bucket_column: Optional[str] = None
    silver_bucket_count: int = 512
    materialize_scoring: bool = True
    fail_on_rejected: bool = True
    max_records_per_file: int = 1000000
    hive_table: Optional[str] = "prod_gold.travel_observations"
    hive_root: Optional[str] = "hdfs:///apps/travel-gold/hive"
    max_hive_partitions_per_run: int = 20000
    enable_additive_schema_evolution: bool = False
    cleanup_checkpoints: bool = True
    columns: SilverColumns = field(default_factory=SilverColumns)
    scoring: ScoringConfig = field(default_factory=ScoringConfig)
    elasticsearch: ElasticsearchConfig = field(default_factory=ElasticsearchConfig)

    def __post_init__(self):
        for name in ("silver_table", "gold_table"):
            _table_id(getattr(self, name))
        if self.hive_table:
            _table_id(self.hive_table)
            if not self.hive_root:
                raise ValueError("hive_root requis pour la copie Hive")
        if not re.match(r"^(hdfs|file)://", self.staging_root):
            raise ValueError("Le journal exige un chemin partage hdfs:// (file:// uniquement en local)")
        for name in ("gold_buckets", "shuffle_partitions", "calculation_bucket_batch", "silver_bucket_count",
                     "max_records_per_file", "max_hive_partitions_per_run"):
            if getattr(self, name) <= 0:
                raise ValueError(name + " doit etre positif")


def _q(name):
    return chr(96) + name.replace(chr(96), chr(96) * 2) + chr(96)


def _table_id(name):
    parts = name.split(".")
    if len(parts) > 4 or any(not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", p) for p in parts):
        raise ValueError("Identifiant de table invalide: " + name)
    return ".".join(_q(p) for p in parts)


def _literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def _utc():
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def load_config(path):
    raw = json.loads(Path(path).read_text())
    column_options = raw.pop("columns", {})
    score_options = raw.pop("scoring", {})
    es_options = raw.pop("elasticsearch", {})
    if "bonus_levels" in score_options:
        score_options["bonus_levels"] = tuple(tuple(p) for p in score_options["bonus_levels"])
    aliases = dict(ISO_COUNTRY_ALIASES)
    aliases.update({str(k).upper(): str(v).upper() for k, v in score_options.pop("country_aliases", {}).items()})
    score_options["country_aliases"] = aliases
    for key in ("endpoints", "source_columns"):
        if es_options.get(key) is not None:
            es_options[key] = tuple(es_options[key])
    return PipelineConfig(columns=SilverColumns(**column_options),
        scoring=ScoringConfig(**score_options),
        elasticsearch=ElasticsearchConfig(**es_options), **raw)


def _contract_hash(config):
    # Taille des bulks, delais et secrets peuvent changer en reprise.
    value = asdict(config)
    es = value.pop("elasticsearch")
    value["elasticsearch"] = {k: es[k] for k in
        ("index", "alias", "source_columns", "index_sort", "primary_shards")}
    for k in ("shuffle_partitions", "calculation_bucket_batch", "cleanup_checkpoints",
              "max_hive_partitions_per_run", "materialize_scoring", "max_records_per_file"):
        value.pop(k)
    value["module_schema"] = SCHEMA_VERSION
    return hashlib.sha256(json.dumps(_config_value(value), sort_keys=True).encode()).hexdigest()


def _config_value(value):
    if isinstance(value, dict):
        return {k: _config_value(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_config_value(v) for v in value]
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _rules_hash(config):
    value = {"schema": SCHEMA_VERSION, "columns": asdict(config.columns),
             "scoring": asdict(config.scoring), "gold_buckets": config.gold_buckets}
    return hashlib.sha256(json.dumps(_config_value(value), sort_keys=True).encode()).hexdigest()


def _indexed_properties():
    keywords = ("gold_id", "entity_key", "observation_id", "event_id", "silver_event_id",
                "person_id", "country", "nature", "event_type", "raw_identifier",
                "identifier_type", "identifier_namespace", "source_id", "source_name",
                "logic_id", "dependency_group", "transition_id", "transition_role",
                "bronze_table", "bronze_partition", "bronze_row_id")
    properties = {name: {"type": "keyword", "ignore_above": 1024} for name in keywords}
    for name in ("start_ts", "end_ts"):
        properties[name] = {"type": "date", "format": "strict_date_optional_time||epoch_millis"}
    for name in ("score", "identifier_confidence", "logic_confidence", "source_confidence"):
        properties[name] = {"type": "double"}
    properties.update({"occurrence_count": {"type": "long"},
                       "is_resolved": {"type": "boolean"},
                       "is_point": {"type": "boolean"},
                       "gold_publication_version": {"type": "long"}})
    return properties


def index_definition(config):
    es = config.elasticsearch
    settings = {"number_of_shards": es.primary_shards,
                "number_of_replicas": es.initial_load_replicas,
                "refresh_interval": "-1"}
    if es.index_sort:
        settings.update({"sort.field": ["entity_key", "start_ts"],
                         "sort.order": ["asc", "asc"]})
    return {"settings": settings, "mappings": {
        "dynamic": False, "_routing": {"required": True},
        "_meta": {"travel_gold_schema": SCHEMA_VERSION},
        "properties": _indexed_properties()}}


class ElasticsearchHTTP:
    """Connexion HTTP persistante par partition, sans dependance Python externe."""
    def __init__(self, config, worker=0):
        self.config = config
        self.worker = worker
        self.connection = None
        self.prefix = ""
        self.headers = {"Content-Type": "application/json"}
        if config.auth == "api_key":
            key = os.environ.get(config.api_key_env)
            if not key:
                raise ValueError("Secret absent: " + config.api_key_env + " (driver ET executeurs)")
            self.headers["Authorization"] = "ApiKey " + key
        elif config.auth == "basic":
            user, password = os.environ.get(config.username_env), os.environ.get(config.password_env)
            if user is None or password is None:
                raise ValueError("Identifiants Elasticsearch absents de l'environnement")
            self.headers["Authorization"] = "Basic " + base64.b64encode((user + ":" + password).encode()).decode()

    def close(self):
        if self.connection:
            self.connection.close()
        self.connection = None

    def _connect(self, attempt):
        self.close()
        url = urlsplit(self.config.endpoints[(self.worker + attempt) % len(self.config.endpoints)])
        self.prefix = url.path.rstrip("/")
        if url.scheme == "https":
            context = ssl.create_default_context(cafile=self.config.ca_file)
            self.connection = http.client.HTTPSConnection(url.hostname, url.port or 443,
                timeout=self.config.request_timeout_seconds, context=context)
        else:
            self.connection = http.client.HTTPConnection(url.hostname, url.port or 80,
                timeout=self.config.request_timeout_seconds)

    def pause(self, attempt):
        delay = min(self.config.max_backoff_seconds,
                    self.config.backoff_seconds * (2 ** attempt)) * random.uniform(.75, 1.25)
        if delay:
            time.sleep(delay)

    def request(self, method, path, body=None, ndjson=False, allowed=(200,)):
        payload = body if isinstance(body, bytes) else (json.dumps(body).encode() if body is not None else None)
        headers = dict(self.headers)
        if ndjson:
            headers["Content-Type"] = "application/x-ndjson"
        last = None
        for attempt in range(self.config.max_retries + 1):
            try:
                if self.connection is None:
                    self._connect(attempt)
                self.connection.request(method, self.prefix + path, body=payload, headers=headers)
                response = self.connection.getresponse()
                data = response.read()
                status = response.status
                try:
                    parsed = json.loads(data) if data else {}
                except (ValueError, UnicodeDecodeError):
                    if 200 <= status < 300:
                        raise RuntimeError("Reponse Elasticsearch JSON invalide")
                    parsed = {}
                if status in allowed:
                    return status, parsed
                if status not in (408, 429, 500, 502, 503, 504):
                    error = parsed.get("error", {})
                    kind = error.get("type", "HTTP_ERROR") if isinstance(error, dict) else "HTTP_ERROR"
                    raise RuntimeError("Elasticsearch HTTP %d (%s), %s %s" % (status, kind, method, path))
                last = "HTTP " + str(status)
            except (OSError, http.client.HTTPException) as exc:
                last = type(exc).__name__
            self.close()
            if attempt < self.config.max_retries:
                self.pause(attempt)
        raise RuntimeError("Elasticsearch indisponible apres reprises: " + str(last))


def _bulk_action(row, es):
    action = row["operation"].lower()
    metadata = {"_index": es.index, "_id": row["doc_id"],
                "routing": row["routing_key"], "version": int(row["version"]),
                "version_type": "external_gte"}
    header = json.dumps({action: metadata}, separators=(",", ":"), ensure_ascii=False).encode() + b"\n"
    if action == "delete":
        return header
    if action != "index":
        raise ValueError("Operation de publication inconnue")
    document = row["document"]
    if not document or len(document.encode()) > es.max_document_bytes:
        raise ValueError("Document absent ou trop volumineux: " + row["doc_id"])
    return header + document.encode() + b"\n"


def _send_bulk(client, records):
    if not records:
        return
    pending = list(records)
    for attempt in range(client.config.max_retries + 1):
        payload = b"".join(_bulk_action(r, client.config) for r in pending)
        status, response = client.request("POST", "/_bulk", payload, ndjson=True, allowed=(200, 413))
        if status == 413:
            if len(pending) == 1:
                raise RuntimeError("Un document depasse la limite HTTP du serveur")
            middle = len(pending) // 2
            _send_bulk(client, pending[:middle])
            _send_bulk(client, pending[middle:])
            return
        items = response.get("items", [])
        if len(items) != len(pending):
            raise RuntimeError("Reponse Bulk incomplete; publication non acquittee")
        retry = []
        for row, item in zip(pending, items):
            detail = item.get(row["operation"].lower(), {})
            code = detail.get("status", 0)
            error = detail.get("error") or {}
            kind = error.get("type", "") if isinstance(error, dict) else ""
            if 200 <= code < 300 or (code == 404 and row["operation"] == "DELETE"):
                continue
            if code == 409 and kind == "version_conflict_engine_exception":
                continue
            if code in (408, 429, 500, 502, 503, 504):
                retry.append(row)
            else:
                raise RuntimeError("Erreur Bulk HTTP %s (%s), id=%s" % (code, kind, row["doc_id"]))
        if not retry:
            return
        pending = retry
        if attempt < client.config.max_retries:
            client.pause(attempt)
    raise RuntimeError("Echec Bulk persistant sur %d documents" % len(pending))


def write_bulk_partition(rows, es):
    iterator = iter(rows)
    first = next(iterator, None)
    if first is None:
        return
    from itertools import chain
    from pyspark import TaskContext
    context = TaskContext.get()
    client = ElasticsearchHTTP(es, context.partitionId() if context else 0)
    try:
        batch, size = [], 0
        for raw in chain((first,), iterator):
            row = raw.asDict() if hasattr(raw, "asDict") else dict(raw)
            count = len(_bulk_action(row, es))
            if batch and (len(batch) >= es.max_bulk_documents or size + count > es.max_bulk_bytes):
                _send_bulk(client, batch)
                batch, size = [], 0
            batch.append(row)
            size += count
        _send_bulk(client, batch)
    finally:
        client.close()


def ensure_index(client, config, fresh_required=False):
    es = config.elasticsearch
    name = quote(es.index, safe="")
    status, data = client.request("GET", "/" + name, allowed=(200, 404))
    if status == 404:
        client.request("PUT", "/" + name, index_definition(config), allowed=(200,))
        return
    actual = data[es.index]
    if actual.get("mappings", {}).get("_meta", {}).get("travel_gold_schema") != SCHEMA_VERSION:
        raise ValueError("L'index existe mais ne correspond pas au schema de ce module")
    if not actual.get("mappings", {}).get("_routing", {}).get("required", False):
        raise ValueError("L'index existant n'exige pas le routing")
    settings = actual.get("settings", {})
    settings = settings.get("index", settings)
    if int(settings.get("number_of_shards", -1)) != es.primary_shards:
        raise ValueError("Nombre de shards different: creer un nouvel index et reindexer")
    actual_properties = actual.get("mappings", {}).get("properties", {})
    if any(actual_properties.get(n, {}).get("type") != definition["type"]
           for n, definition in _indexed_properties().items()):
        raise ValueError("Mapping existant incompatible avec le contrat des observations")
    if fresh_required:
        _, count = client.request("GET", "/" + name + "/_count")
        if count.get("count", 0):
            raise ValueError("Un chargement initial/reindex exige un index physique vide")


def promote_alias(client, config):
    es = config.elasticsearch
    _, existing = client.request("GET", "/_alias/" + quote(es.alias, safe=""), allowed=(200, 404))
    actions = [{"remove": {"index": name, "alias": es.alias}} for name in existing
               if isinstance(existing.get(name), dict) and "aliases" in existing[name]]
    actions.append({"add": {"index": es.index, "alias": es.alias, "is_write_index": True}})
    _, result = client.request("POST", "/_aliases", {"actions": actions})
    if result.get("errors") or result.get("acknowledged") is False:
        raise RuntimeError("La bascule d'alias n'est pas entierement acquittee")
    check_alias(client, config)


def check_alias(client, config):
    es = config.elasticsearch
    status, result = client.request("GET", "/_alias/" + quote(es.alias, safe=""), allowed=(200, 404))
    names = [name for name, value in result.items() if isinstance(value, dict) and "aliases" in value]
    if status != 200 or names != [es.index]:
        raise ValueError("L'alias de lecture ne pointe pas uniquement sur l'index configure")


def recommend_primary_shards(total_documents, measured_primary_bytes_per_document,
                             target_gib=30., target_documents=100000000):
    if min(total_documents, measured_primary_bytes_per_document, target_gib, target_documents) <= 0:
        raise ValueError("Mesures strictement positives requises")
    return max(1, math.ceil(total_documents * measured_primary_bytes_per_document / (target_gib * 1024**3)),
               math.ceil(total_documents / target_documents))


class RunFiles:
    """Journal et outbox durables sur HDFS; file:// pour les tests locaux."""
    def __init__(self, spark, root):
        self.spark = spark
        self.jvm = spark._jvm
        self.conf = spark._jsc.hadoopConfiguration()
        self.root = root.rstrip("/")
        self.path_class = self.jvm.org.apache.hadoop.fs.Path
        self.fs = self.path_class(self.root).getFileSystem(self.conf)
        self.fs.mkdirs(self.path_class(self.root))

    def path(self, *parts):
        return "/".join((self.root,) + tuple(str(p).strip("/") for p in parts))

    def exists(self, path):
        return self.fs.exists(self.path_class(path))

    def read(self, path):
        stream = self.fs.open(self.path_class(path))
        try:
            return json.loads(self.jvm.org.apache.commons.io.IOUtils.toString(stream, "UTF-8"))
        finally:
            stream.close()

    def create(self, path, value):
        stream = self.fs.create(self.path_class(path), False)
        try:
            stream.write(bytearray(json.dumps(value, ensure_ascii=False).encode()))
        finally:
            stream.close()

    def write(self, path, value):
        tmp = path + ".tmp-" + uuid.uuid4().hex
        self.create(tmp, value)
        try:
            fc = self.jvm.org.apache.hadoop.fs.FileContext.getFileContext(
                self.path_class(path).toUri(), self.conf)
            options = self.spark.sparkContext._gateway.new_array(
                self.jvm.org.apache.hadoop.fs.Options.Rename, 1)
            options[0] = self.jvm.org.apache.hadoop.fs.Options.Rename.OVERWRITE
            fc.rename(self.path_class(tmp), self.path_class(path), options)
        finally:
            if self.exists(tmp):
                self.fs.delete(self.path_class(tmp), False)

    def delete(self, path, recursive=False):
        if self.exists(path) and not self.fs.delete(self.path_class(path), recursive):
            raise RuntimeError("Suppression technique impossible: " + path)

    def reserve(self, run_id):
        lock = self.path("_active_run.json")
        if self.exists(lock):
            owner = self.read(lock).get("run_id")
            if owner != run_id:
                raise RuntimeError("Publication en attente: %s. Executer resume avant un nouveau run." % owner)
        else:
            try:
                self.create(lock, {"run_id": run_id, "created_at": _utc()})
            except Exception:
                if not self.exists(lock) or self.read(lock).get("run_id") != run_id:
                    raise RuntimeError("Un autre run a reserve la publication")

    def release(self, run_id):
        lock = self.path("_active_run.json")
        if self.exists(lock) and self.read(lock).get("run_id") == run_id:
            self.delete(lock)

    def allocate_version(self):
        path = self.path("_sequence.json")
        previous = int(self.read(path)["version"]) if self.exists(path) else 0
        value = max(previous + 1, time.time_ns() // 1000)
        self.write(path, {"version": value})
        return value


def _ensure_namespace(spark, table):
    parts = table.split(".")
    if len(parts) > 1:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS " + _table_id(".".join(parts[:-1])))


def _table_exists(spark, table):
    return spark.catalog.tableExists(table)


def latest_snapshot(spark, table):
    if not _table_exists(spark, table):
        return None
    row = spark.sql("SELECT snapshot_id FROM " + _table_id(table + ".snapshots") +
                    " ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1").first()
    return int(row.snapshot_id) if row else None


def read_snapshot(spark, table, snapshot_id):
    reader = spark.read.format("iceberg")
    if snapshot_id is not None:
        reader = reader.option("snapshot-id", str(snapshot_id))
    result = reader.load(table)
    # None est un instantane vide, pas une invitation a lire une version future.
    return result if snapshot_id is not None else result.limit(0)


def _canonical_value(column, dtype):
    if isinstance(dtype, T.StructType):
        value = F.struct(*[_canonical_value(column.getField(f.name), f.dataType).alias(f.name) for f in dtype])
        return F.when(column.isNull(), F.lit(None).cast(dtype)).otherwise(value)
    if isinstance(dtype, T.ArrayType):
        return F.transform(column, lambda v: _canonical_value(v, dtype.elementType))
    if isinstance(dtype, T.MapType):
        entries = F.transform(F.map_entries(column), lambda kv: F.struct(
            _canonical_value(kv["key"], dtype.keyType).alias("key"),
            _canonical_value(kv["value"], dtype.valueType).alias("value")))
        ordered = F.array_sort(entries, lambda a, b:
            F.when(a["key"].cast("string") < b["key"].cast("string"), F.lit(-1))
             .when(a["key"].cast("string") > b["key"].cast("string"), F.lit(1)).otherwise(F.lit(0)))
        return F.map_from_entries(ordered)
    return column


def canonical_hash(frame, names):
    by_name = {f.name: f.dataType for f in frame.schema}
    return F.sha2(F.to_json(F.struct(*[_canonical_value(_col(n), by_name[n]).alias(n)
        for n in sorted(names)]), JSON_OPTIONS), 256)


def decorate_gold(gold, config, run_id, version, computed_at):
    columns = config.columns
    required = ("entity_key", "event_id", "country", "start_ts", "end_ts", "gold_id", "score")
    if set(required).difference(gold.columns):
        raise ValueError("Sortie du moteur incomplete")
    collisions = TECHNICAL.intersection(gold.columns)
    if collisions:
        raise ValueError("Colonnes techniques deja presentes dans Silver: " + ", ".join(sorted(collisions)))
    output = gold.withColumn("entity_key", _text(_col("entity_key"))).withColumn(
        "observation_id", _col(columns.event_id).cast("string")).withColumn(
        "is_resolved", _col(columns.is_resolved).cast("boolean")).withColumn(
        "is_point", F.col("start_ts") == F.col("end_ts"))
    aliases = {
        "raw_identifier": columns.raw_identifier, "identifier_type": columns.identifier_type,
        "identifier_namespace": columns.identifier_namespace,
        "identifier_confidence": columns.identifier_confidence,
        "logic_confidence": columns.logic_confidence, "occurrence_count": columns.occurrence_count,
        "source_confidence": columns.source_confidence,
        "dependency_group": columns.dependency_group,
    }
    for target, source in aliases.items():
        if source and source in output.columns:
            dtype = "double" if target.endswith("confidence") else "long" if target == "occurrence_count" else "string"
            output = output.withColumn(target, _col(source).cast(dtype))
    if columns.source and columns.source in output.columns and "source_id" not in output.columns and "source_name" not in output.columns:
        output = output.withColumn("source_id", _col(columns.source).cast("string"))
    for name in ("person_id", "event_id", "silver_event_id", "source_id", "source_name", "logic_id"):
        if name in output.columns:
            output = output.withColumn(name, _col(name).cast("string"))
    output = output.withColumn("observation_month", F.trunc("start_ts", "month")).withColumn(
        "entity_bucket", F.pmod(F.xxhash64("entity_key"), F.lit(config.gold_buckets)).cast("int"))
    output = output.withColumn("gold_content_hash", canonical_hash(output, [n for n in output.columns if n not in TECHNICAL]))
    return output.withColumn("gold_run_id", F.lit(run_id)).withColumn(
        "gold_publication_version", F.lit(version).cast("long")).withColumn(
        "gold_computed_at", F.to_timestamp(F.lit(computed_at)))


def ensure_gold_table(spark, config, schema_frame):
    _ensure_namespace(spark, config.gold_table)
    if not _table_exists(spark, config.gold_table):
        (schema_frame.limit(0).writeTo(config.gold_table).using("iceberg")
            .partitionedBy(*[F.col(n) for n in PARTITIONS])
            .tableProperty("format-version", "2")
            .tableProperty("travel.gold.bucket-count", str(config.gold_buckets))
            .tableProperty("travel.gold.rules-hash", _rules_hash(config))
            .tableProperty("write.target-file-size-bytes", str(256 * 1024 * 1024)).create())
    properties = {r.key: r.value for r in spark.sql("SHOW TBLPROPERTIES " + _table_id(config.gold_table)).collect()}
    if properties.get("travel.gold.bucket-count") != str(config.gold_buckets):
        raise ValueError("Le nombre de buckets Gold est immuable: creer et recalculer une nouvelle table")
    if properties.get("travel.gold.rules-hash") != _rules_hash(config):
        raise ValueError("Regles de score/identite modifiees: recalculer toute Gold dans une nouvelle table")
    current = spark.table(config.gold_table).schema
    incoming = {f.name: f.dataType for f in schema_frame.schema}
    current_types = {f.name: f.dataType for f in current}
    removed = set(current_types).difference(incoming)
    changed = {n for n in set(current_types).intersection(incoming)
               if current_types[n].simpleString() != incoming[n].simpleString()}
    added = set(incoming).difference(current_types)
    if removed or changed or (added and not config.enable_additive_schema_evolution):
        raise ValueError("Schema Gold incompatible: absentes=%s types_modifies=%s ajoutees=%s" %
                         (sorted(removed), sorted(changed), sorted(added)))
    if added:
        definition = ", ".join(_q(n) + " " + incoming[n].simpleString() for n in sorted(added))
        spark.sql("ALTER TABLE " + _table_id(config.gold_table) + " ADD COLUMNS (" + definition + ")")


def align_frame(frame, schema):
    incoming = set(frame.columns)
    return frame.select(*[(_col(f.name).cast(f.dataType) if f.name in incoming else
        F.lit(None).cast(f.dataType)).alias(f.name) for f in schema])


def compute_changes(old_gold, new_gold):
    """Pas de seuil: les scores faibles restent des documents indexables."""
    old_metadata = old_gold.select("gold_id", F.col("gold_content_hash").alias("_previous_hash"))
    upserts = new_gold.join(old_metadata.hint("merge"), "gold_id", "left").where(
        F.col("_previous_hash").isNull() | (F.col("_previous_hash") != F.col("gold_content_hash"))
    ).drop("_previous_hash").withColumn(OP, F.lit("INDEX"))
    deletes = old_gold.join(new_gold.select("gold_id").hint("merge"), "gold_id", "left_anti").withColumn(OP, F.lit("DELETE"))
    return upserts.unionByName(deletes)


def prepare_for_index(changes, config, version):
    es = config.elasticsearch
    names = list(es.source_columns) if es.source_columns is not None else [n for n in changes.columns if n not in TECHNICAL and n != OP]
    required = {"gold_id", "entity_key", "observation_id", "country", "start_ts", "end_ts",
                "nature", "score", "is_point", "is_resolved", "transition_id", "transition_role"}
    if required.difference(names):
        raise ValueError("source_columns doit conserver les champs UI: " + ", ".join(sorted(required)))
    if set(names).difference(changes.columns):
        raise ValueError("source_columns contient des noms absents de Gold")
    payload = F.to_json(F.struct(*[_col(n).alias(n) for n in names]), JSON_OPTIONS)
    return changes.select(_col(OP).alias("operation"), F.col("gold_id").alias("doc_id"),
        F.col("entity_key").alias("routing_key"), F.lit(version).cast("long").alias("version"),
        F.when(_col(OP) == "INDEX", payload).otherwise(F.lit(None).cast("string")).alias("document"))


def merge_gold_changes(spark, config, changes):
    name = "_travel_changes_" + uuid.uuid4().hex
    changes.createOrReplaceTempView(name)
    columns = [n for n in changes.columns if n != OP]
    updates = ", ".join("t." + _q(n) + " = s." + _q(n) for n in columns)
    inserts = ", ".join(_q(n) for n in columns)
    values = ", ".join("s." + _q(n) for n in columns)
    try:
        spark.sql("MERGE INTO " + _table_id(config.gold_table) + " t USING " + _q(name) +
            " s ON t.gold_id = s.gold_id AND t.entity_key = s.entity_key "
            "WHEN MATCHED AND s." + _q(OP) + " = 'DELETE' THEN DELETE "
            "WHEN MATCHED AND s." + _q(OP) + " = 'INDEX' THEN UPDATE SET " + updates +
            " WHEN NOT MATCHED AND s." + _q(OP) + " = 'INDEX' THEN INSERT (" + inserts + ") VALUES (" + values + ")")
    finally:
        spark.catalog.dropTempView(name)


def _prepare_source(silver, config):
    names = silver.columns
    if len(set(n.lower() for n in names)) != len(names):
        raise ValueError("Colonnes Silver ambigues par leur casse")
    required = config.columns.is_resolved
    if required not in names:
        if "person_id" not in names:
            raise ValueError("Renseigner is_resolved dans SilverColumns, ou fournir person_id")
        silver = silver.withColumn(required, _text(F.col("person_id")).isNotNull())
    if config.silver_bucket_column and config.silver_bucket_column not in names:
        raise ValueError("silver_bucket_column absent: le mettre a null pour desactiver le decoupage")
    return silver


def _write_parquet(frame, path, config):
    frame.write.mode("overwrite").option("maxRecordsPerFile", config.max_records_per_file).parquet(path)


def _schema_gold(spark, config, silver, manifest):
    empty = build_travel_gold(silver.limit(0), config.scoring, config.columns,
        persist_intermediates=False, materialize_intermediates=False)
    return decorate_gold(empty.gold, config, manifest["run_id"], manifest["version"], manifest["started_at"])


def _save_manifest(files, manifest):
    manifest["updated_at"] = _utc()
    files.write(files.path(manifest["run_id"], "manifest.json"), manifest)


def _advance(files, manifest, state, **values):
    manifest.update(values)
    manifest["state"] = state
    _save_manifest(files, manifest)
    LOG.info("run=%s state=%s", manifest["run_id"], state)


def _check_stable_target(spark, config, manifest):
    current = latest_snapshot(spark, config.gold_table)
    if current != manifest["gold_before_snapshot"]:
        # CREATE TABLE AS schema vide peut produire son propre instantane vide.
        # C'est le bootstrap de ce premier chargement, pas une modification metier.
        if (manifest["mode"] == "initial" and manifest["gold_before_snapshot"] is None
                and not spark.table(config.gold_table).limit(1).count()):
            return
        raise RuntimeError("Gold a change hors de ce run avant sa publication; conserver le journal et verifier les writers")


def prepare_run(spark, config, files, manifest, impacted=None):
    """Prepare des fichiers immuables AVANT de modifier Gold ou Elasticsearch."""
    run_id, mode = manifest["run_id"], manifest["mode"]
    run_root = files.path(run_id)
    ready_path = run_root + "/prepared.json"
    if files.exists(ready_path):
        manifest.update(files.read(ready_path))
        _advance(files, manifest, "PREPARED")
        return
    _check_stable_target(spark, config, manifest)
    if mode == "reindex":
        if not _table_exists(spark, config.gold_table):
            raise ValueError("Gold doit exister pour reindexer")
        frame = read_snapshot(spark, config.gold_table, manifest["gold_before_snapshot"])
        changes = frame.withColumn(OP, F.lit("INDEX"))
        _write_parquet(changes, run_root + "/changes", config)
    else:
        silver = _prepare_source(read_snapshot(spark, config.silver_table, manifest["silver_snapshot"]), config)
        schema_frame = _schema_gold(spark, config, silver, manifest)
        ensure_gold_table(spark, config, schema_frame)
        if manifest["gold_before_snapshot"] is None:
            manifest["gold_before_snapshot"] = latest_snapshot(spark, config.gold_table)
            _save_manifest(files, manifest)
        if mode == "initial" and spark.table(config.gold_table).limit(1).count():
            raise ValueError("Le chargement initial exige une Gold vide; utiliser incremental ou reindex")
        if (mode == "initial" and config.hive_table and _table_exists(spark, config.hive_table)
                and spark.table(config.hive_table).limit(1).count()):
            raise ValueError("Le chargement initial exige une table Hive vide; choisir une nouvelle copie Hive")
        key_path = run_root + "/keys"
        if not files.exists(key_path + "/_SUCCESS"):
            if mode == "initial":
                keys = silver.select(_text(_col(config.columns.entity_key)).alias("entity_key"))
            elif impacted is not None:
                keys = impacted.select(_text(F.col("entity_key")).alias("entity_key"))
            elif manifest.get("impacted_table"):
                impacts = read_snapshot(spark, manifest["impacted_table"], manifest["impacted_snapshot"])
                if manifest.get("impact_run_id") is not None:
                    impacts = impacts.where(F.col("run_id") == F.lit(manifest["impact_run_id"]))
                keys = impacts.select(_text(F.col("entity_key")).alias("entity_key"))
            else:
                raise ValueError("Reprise PREPARING: fournir de nouveau impacted, ou une table d'impacts")
            if keys.where(F.col("entity_key").isNull()).limit(1).count():
                raise ValueError("entity_key absent/vide dans Silver ou les impacts")
            _write_parquet(keys.distinct(), key_path, config)
        keys = spark.read.parquet(key_path)
        if config.silver_bucket_column:
            group_expression = F.floor(F.pmod(F.xxhash64("entity_key"),
                F.lit(config.silver_bucket_count)) / config.calculation_bucket_batch).cast("int")
            groups = [int(r.group_id) for r in keys.select(group_expression.alias("group_id")).distinct().collect()]
        else:
            groups = [0]  # un seul scan distribue; ne pas rescanner Silver 16 fois
        groups.sort()
        batch_paths = []
        for group_id in groups:
            batch_path = run_root + "/batches/%05d" % group_id
            batch_paths.append(batch_path)
            if files.exists(batch_path + "/_SUCCESS"):
                continue
            if config.silver_bucket_column:
                lower = group_id * config.calculation_bucket_batch
                upper = min(lower + config.calculation_bucket_batch, config.silver_bucket_count)
                key_bucket = F.pmod(F.xxhash64("entity_key"), F.lit(config.silver_bucket_count))
                batch_keys = keys.where((key_bucket >= lower) & (key_bucket < upper))
                source = silver.where((_col(config.silver_bucket_column) >= lower) &
                                      (_col(config.silver_bucket_column) < upper))
            else:
                batch_keys, source = keys, silver
            source = source.withColumn("_publication_entity", _text(_col(config.columns.entity_key)))
            source = source.join(batch_keys.select(F.col("entity_key").alias("_publication_entity")).hint("merge"),
                                 "_publication_entity", "left_semi").drop("_publication_entity")
            if config.silver_bucket_column:
                expected = F.pmod(F.xxhash64(_text(_col(config.columns.entity_key))), F.lit(config.silver_bucket_count))
                if source.where(_col(config.silver_bucket_column) != expected).limit(1).count():
                    raise ValueError("Le bucket Silver ne correspond pas a pmod(xxhash64(entity_key), N)")
            checkpoint = run_root + "/checkpoints/%05d" % group_id
            result = build_travel_gold(source, config.scoring, config.columns,
                persist_intermediates=True, materialize_intermediates=config.materialize_scoring,
                checkpoint_dir=checkpoint if config.materialize_scoring else None)
            try:
                rejected = result.rejected_events
                if rejected.limit(1).count():
                    _write_parquet(rejected, run_root + "/rejected/%05d" % group_id, config)
                    if config.fail_on_rejected:
                        raise ValueError("Observations rejetees: consulter le repertoire rejected du run")
                new = decorate_gold(result.gold, config, run_id, manifest["version"], manifest["started_at"])
                schema = spark.table(config.gold_table).schema
                new = align_frame(new, schema)
                old = read_snapshot(spark, config.gold_table, manifest["gold_before_snapshot"])
                # Lire tous les mois de ces entites: pas de troncature des voisins
                # ni d'oubli des observations longues ou des anciennes identites.
                if config.silver_bucket_column and config.silver_bucket_count % config.gold_buckets == 0:
                    buckets = sorted({b % config.gold_buckets for b in range(lower, upper)})
                    old = old.where(F.col("entity_bucket").isin(buckets))
                old = old.join(batch_keys.hint("merge"), "entity_key", "left_semi")
                _write_parquet(compute_changes(old, new), batch_path, config)
            finally:
                result.unpersist()
            if config.cleanup_checkpoints and files.exists(checkpoint):
                files.delete(checkpoint, recursive=True)
        if batch_paths:
            combined = spark.read.parquet(*batch_paths)
        else:
            combined = align_frame(schema_frame.limit(0), spark.table(config.gold_table).schema).withColumn(OP, F.lit("INDEX"))
        _write_parquet(combined, run_root + "/changes", config)
    changes = spark.read.parquet(run_root + "/changes")
    # Ce sont des comptages agreges, jamais un collect des observations.
    counts = {r[OP]: int(r["count"]) for r in changes.groupBy(OP).count().collect()}
    documents = prepare_for_index(changes, config, manifest["version"])
    oversized = documents.where(F.length(F.encode("document", "UTF-8")) >
                               config.elasticsearch.max_document_bytes)
    if oversized.limit(1).count():
        raise ValueError("Document trop volumineux: reduire source_columns avant un nouveau run")
    _write_parquet(documents, run_root + "/outbox", config)
    if mode == "reindex":
        touched = changes.select(*PARTITIONS).distinct()
    else:
        old_parts = read_snapshot(spark, config.gold_table, manifest["gold_before_snapshot"]).join(
            changes.select("gold_id").hint("merge"), "gold_id", "left_semi").select(*PARTITIONS)
        touched = old_parts.unionByName(changes.select(*PARTITIONS)).distinct()
    _write_parquet(touched, run_root + "/partitions", config)
    summary = {"index_operations": counts.get("INDEX", 0),
               "delete_operations": counts.get("DELETE", 0),
               "change_count": sum(counts.values())}
    files.write(ready_path, summary)
    _advance(files, manifest, "PREPARED", **summary)


def _ensure_hive_table(spark, config, gold_schema):
    table = config.hive_table
    _ensure_namespace(spark, table)
    by_name = {f.name: f.dataType for f in gold_schema}
    data_names = [f.name for f in gold_schema if f.name not in PARTITIONS]
    definition = ", ".join(_q(n) + " " + by_name[n].simpleString() for n in data_names)
    partitions = ", ".join(_q(n) + " " + by_name[n].simpleString() for n in PARTITIONS)
    spark.sql("CREATE EXTERNAL TABLE IF NOT EXISTS " + _table_id(table) + " (" + definition +
        ") PARTITIONED BY (" + partitions + ") STORED AS ORC LOCATION " +
        _literal(config.hive_root.rstrip("/") + "/base") +
        " TBLPROPERTIES ('external.table.purge'='false')")
    current = {f.name: f.dataType for f in spark.table(table).schema}
    changed = {n for n in current if n not in by_name or current[n].simpleString() != by_name[n].simpleString()}
    added = set(by_name).difference(current)
    if changed or (added and not config.enable_additive_schema_evolution):
        raise ValueError("Schema Hive incompatible")
    if added:
        spark.sql("ALTER TABLE " + _table_id(table) + " ADD COLUMNS (" +
            ", ".join(_q(n) + " " + by_name[n].simpleString() for n in sorted(added)) + ")")


def publish_hive(spark, config, files, manifest):
    if not config.hive_table or manifest["mode"] == "reindex":
        return
    run_root = files.path(manifest["run_id"])
    partitions = spark.read.parquet(run_root + "/partitions")
    keys = partitions.limit(config.max_hive_partitions_per_run + 1).collect()
    if len(keys) > config.max_hive_partitions_per_run:
        raise ValueError("Trop de partitions Hive pour ce run; augmenter la limite ou fractionner le chargement")
    gold = read_snapshot(spark, config.gold_table, manifest["gold_after_snapshot"])
    _ensure_hive_table(spark, config, gold.schema)
    if not keys:
        return
    version_root = (config.hive_root.rstrip("/") + "/versions/" +
                    manifest["run_id"] + "-" + str(manifest["version"]))
    if not files.exists(version_root + "/_SUCCESS"):
        # Relire les partitions completes depuis Gold, pas seulement les entites
        # modifiees, pour conserver les autres entites de chaque partition Hive.
        months = sorted({r.observation_month for r in keys})
        buckets = sorted({int(r.entity_bucket) for r in keys})
        content = gold.where(F.col("observation_month").isin(months) &
                             F.col("entity_bucket").isin(buckets)).join(partitions.hint("merge"),
                             list(PARTITIONS), "left_semi")
        order = [n for n in spark.table(config.hive_table).columns if n not in PARTITIONS] + list(PARTITIONS)
        (content.select(*order).write.mode("overwrite")
            .option("maxRecordsPerFile", config.max_records_per_file)
            .partitionBy(*PARTITIONS).format("orc").save(version_root))
    # Le contenu est complet avant la modification des emplacements metastore.
    # Une reprise reutilise les fichiers deja termines, sans les ecraser.
    for row in keys:
        month, bucket = row.observation_month.isoformat(), int(row.entity_bucket)
        spec = "observation_month=" + _literal(month) + ", entity_bucket=" + str(bucket)
        location = version_root + "/observation_month=" + month + "/entity_bucket=" + str(bucket)
        if files.exists(location):
            spark.sql("ALTER TABLE " + _table_id(config.hive_table) +
                      " ADD IF NOT EXISTS PARTITION (" + spec + ") LOCATION " + _literal(location))
            spark.sql("ALTER TABLE " + _table_id(config.hive_table) +
                      " PARTITION (" + spec + ") SET LOCATION " + _literal(location))
        else:
            # Une partition devenue vide doit aussi disparaitre du metastore.
            spark.sql("ALTER TABLE " + _table_id(config.hive_table) +
                      " DROP IF EXISTS PARTITION (" + spec + ")")
    spark.catalog.refreshTable(config.hive_table)


def publish_outbox(spark, config, files, manifest):
    es = config.elasticsearch
    client = ElasticsearchHTTP(es)
    try:
        # L'index n'est exige vide qu'AVANT le premier envoi; les reprises doivent
        # accepter un index partiellement rempli par le meme run.
        ensure_index(client, config, fresh_required=False)
        if manifest["mode"] == "incremental":
            check_alias(client, config)
        data = spark.read.parquet(files.path(manifest["run_id"], "outbox"))
        for operation in ("DELETE", "INDEX"):
            amount = manifest["delete_operations" if operation == "DELETE" else "index_operations"]
            if amount:
                subset = data.where(F.col("operation") == operation)
                # N'equilibrer ni par entity_key ni par routing: une entite chargee
                # peut utiliser plusieurs workers et un meme shard.
                subset.repartition(es.writer_partitions).foreachPartition(
                    lambda rows: write_bulk_partition(rows, es))
        finalize_index(client, es)
    finally:
        client.close()


def finalize_index(client, es):
    _, settings = client.request("PUT", "/" + quote(es.index, safe="") + "/_settings",
        {"index": {"number_of_replicas": es.replicas, "refresh_interval": es.refresh_interval}})
    if settings.get("acknowledged") is False:
        raise RuntimeError("Les reglages finaux de l'index ne sont pas acquittes")
    _, refreshed = client.request("POST", "/" + quote(es.index, safe="") + "/_refresh")
    if refreshed.get("_shards", {}).get("failed", 0):
        raise RuntimeError("Le refresh a echoue sur certains shards")


def _finish_publication(spark, config, files, manifest):
    es = config.elasticsearch
    if manifest["state"] == "PREPARED":
        if manifest["mode"] != "reindex" and manifest["change_count"]:
            current = latest_snapshot(spark, config.gold_table)
            if current != manifest["gold_before_snapshot"]:
                # Crash apres MERGE et avant acquittement: verifier TOUT le resultat.
                staged = spark.read.parquet(files.path(manifest["run_id"], "changes"))
                expected = staged.where(_col(OP) == "INDEX").select("gold_id",
                    F.col("gold_content_hash").alias("_expected_hash"))
                actual = spark.table(config.gold_table).select("gold_id", "gold_content_hash", "gold_run_id")
                invalid = expected.join(actual, "gold_id", "left").where(
                    ~F.col("_expected_hash").eqNullSafe(F.col("gold_content_hash")) |
                    ~F.col("gold_run_id").eqNullSafe(F.lit(manifest["run_id"]))).limit(1).count()
                remaining = actual.join(staged.where(_col(OP) == "DELETE").select("gold_id"),
                                        "gold_id", "left_semi").limit(1).count()
                if invalid or remaining:
                    raise RuntimeError("Gold a change hors de ce run; ne pas rejouer ses anciennes donnees")
            else:
                changes = spark.read.parquet(files.path(manifest["run_id"], "changes"))
                merge_gold_changes(spark, config, changes)
        elif manifest["mode"] == "reindex":
            _check_stable_target(spark, config, manifest)
        snapshot = latest_snapshot(spark, config.gold_table)
        _advance(files, manifest, "GOLD_COMMITTED", gold_after_snapshot=snapshot)
    if manifest["state"] == "GOLD_COMMITTED":
        publish_hive(spark, config, files, manifest)
        _advance(files, manifest, "HIVE_PUBLISHED")
    if manifest["state"] == "HIVE_PUBLISHED":
        client = ElasticsearchHTTP(es)
        try:
            # Guard initial avant tout envoi. La phase INDEXING est persistee
            # avant foreachPartition pour distinguer premier envoi et reprise.
            ensure_index(client, config, fresh_required=manifest["mode"] in ("initial", "reindex"))
            if manifest["mode"] == "incremental":
                check_alias(client, config)
        finally:
            client.close()
        _advance(files, manifest, "INDEXING")
    if manifest["state"] == "INDEXING":
        publish_outbox(spark, config, files, manifest)
        _advance(files, manifest, "INDEXED")
    if manifest["state"] == "INDEXED":
        client = ElasticsearchHTTP(es)
        try:
            if manifest["mode"] in ("initial", "reindex"):
                promote_alias(client, config)
            else:
                check_alias(client, config)
        finally:
            client.close()
        _advance(files, manifest, "COMPLETED", completed_at=_utc())
    files.release(manifest["run_id"])
    return dict(manifest)


def run_pipeline(spark, config, mode="incremental", run_id=None, impacted=None,
                 impacted_table=None, impact_run_id=None):
    """Un seul writer a la fois. Une reprise conserve le meme run_id.

    impacted doit contenir entity_key, dont ANCIENNES et NOUVELLES cles lors
    d'un relink d'identite, ainsi que les cles dont les observations disparaissent.
    Les min/max fournis par l'amont ne sont pas necessaires a ce recalcul complet
    par entite. Les donnees restent distribuees.
    """
    if mode not in ("initial", "incremental", "reindex", "resume"):
        raise ValueError("Mode inconnu")
    if run_id is None:
        if mode == "resume":
            raise ValueError("run_id requis pour reprendre")
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,120}", run_id):
        raise ValueError("run_id invalide")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
    # Pas de conversion silencieuse d'une mauvaise date en observation disparue.
    spark.conf.set("spark.sql.ansi.enabled", "false")
    # Necessaire aussi quand ce fichier est importe depuis Jupyter.
    spark.sparkContext.addPyFile(str(Path(__file__).resolve()))
    if spark.sparkContext.getConf().get("spark.speculation", "false").lower() == "true":
        raise ValueError("Desactiver spark.speculation pour ce job de publication")
    files = RunFiles(spark, config.staging_root)
    manifest_path = files.path(run_id, "manifest.json")
    if files.exists(manifest_path):
        manifest = files.read(manifest_path)
        if manifest["contract_hash"] != _contract_hash(config):
            raise ValueError("Configuration de calcul/stockage modifiee pendant une reprise")
        if manifest["state"] == "COMPLETED":
            files.release(run_id)
            return manifest
        if manifest["state"] == "CANCELLED":
            raise ValueError("Run annule: choisir un nouveau run_id")
    else:
        if mode == "resume":
            raise ValueError("Run inconnu")
        if mode == "incremental" and impacted is None and not impacted_table:
            raise ValueError("Fournir impacted ou impacted_table")
        files.reserve(run_id)
        try:
            if mode == "incremental" and not _table_exists(spark, config.gold_table):
                raise ValueError("Initialiser Gold avant de lancer l'incremental")
            silver_snapshot = latest_snapshot(spark, config.silver_table) if mode != "reindex" else None
            impacted_snapshot = latest_snapshot(spark, impacted_table) if impacted_table else None
            manifest = {"run_id": run_id, "mode": mode, "state": "PREPARING",
                "started_at": _utc(), "version": files.allocate_version(),
                "contract_hash": _contract_hash(config), "silver_snapshot": silver_snapshot,
                "gold_before_snapshot": latest_snapshot(spark, config.gold_table),
                "impacted_table": impacted_table, "impacted_snapshot": impacted_snapshot,
                "impact_run_id": impact_run_id}
            _save_manifest(files, manifest)
        except Exception:
            if not files.exists(manifest_path):
                files.release(run_id)
            raise
    files.reserve(run_id)
    try:
        if manifest["state"] == "PREPARING":
            client = ElasticsearchHTTP(config.elasticsearch)
            try:
                ensure_index(client, config, fresh_required=manifest["mode"] in ("initial", "reindex"))
                if manifest["mode"] == "incremental":
                    check_alias(client, config)
            finally:
                client.close()
            prepare_run(spark, config, files, manifest, impacted=impacted)
        return _finish_publication(spark, config, files, manifest)
    except Exception as exc:
        # Ne jamais liberer un run partiellement publie. Le prochain lancement
        # doit reprendre celui-ci avant de traiter des donnees plus recentes.
        manifest["last_error_type"] = type(exc).__name__
        _save_manifest(files, manifest)
        LOG.error("run=%s reste en phase %s; reprendre avec le meme run_id", run_id, manifest["state"])
        raise


def cancel_preparation(spark, config, run_id):
    files = RunFiles(spark, config.staging_root)
    manifest = files.read(files.path(run_id, "manifest.json"))
    if manifest["state"] != "PREPARING":
        raise ValueError("Seule une preparation sans ecriture Gold peut etre annulee; sinon executer resume")
    _check_stable_target(spark, config, manifest)
    _advance(files, manifest, "CANCELLED")
    files.release(run_id)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("initial", "incremental", "resume", "reindex", "cancel", "mapping"))
    parser.add_argument("--config", required=True)
    parser.add_argument("--run-id")
    parser.add_argument("--impacted-table")
    parser.add_argument("--impact-run-id")
    args = parser.parse_args()
    config = load_config(args.config)
    if args.mode == "mapping":
        print(json.dumps(index_definition(config), ensure_ascii=False, indent=2))
        return
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    builder = SparkSession.builder.appName("travel-gold-" + args.mode)
    if config.hive_table:
        builder = builder.enableHiveSupport()
    spark = builder.getOrCreate()
    try:
        if args.mode == "cancel":
            if not args.run_id:
                parser.error("--run-id requis pour cancel")
            cancel_preparation(spark, config, args.run_id)
            print(json.dumps({"run_id": args.run_id, "state": "CANCELLED"}))
        else:
            result = run_pipeline(spark, config, mode=args.mode, run_id=args.run_id,
                impacted_table=args.impacted_table, impact_run_id=args.impact_run_id)
            print(json.dumps({k: result.get(k) for k in
                ("run_id", "state", "version", "index_operations", "delete_operations", "gold_after_snapshot")}))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
