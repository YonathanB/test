"""Construction Gold sans scoring, pour PySpark 3.5+.

Entree Silver (adapter les noms avec select/withColumnRenamed avant l'appel):
  entity_key, silver_event_id, event_type (PRESENCE / TRANSITION),
  valid_from, valid_to (timestamp), country_code, country_from, country_to,
  evidence_count (entier), transition_nature (confirmed / scheduled).

transition_nature vient de la configuration metier des sources. Elle n'est
deduite ni de la duree du trajet, ni d'une confiance. Elle peut etre null pour
une presence. silver_event_id doit etre stable et unique par entity_key.

Sortie: gold_id, entity_key, event_type, country, start_ts, end_ts,
occurrence_count, detail. detail est un array<struct> des lignes Silver
originales, y compris leur champ raw_data s'il existe. Les timestamps Silver
doivent deja representer les bons instants ; travailler dans une session UTC.

Le regroupement ne depend pas des transitions ni des autres pays.
L'ecart doit etre STRICTEMENT inferieur a gap_hours (12 par defaut).
Les bornes null valid_to sont traitees comme des points.

Pas d'ecriture de tables ni d'index ici : la fonction retourne un DataFrame.
Pour l'incremental, relire l'historique des entity_key impactees et remplacer
leurs anciens resultats, y compris les anciennes cles en cas de relink.
Les gold_id de PRESENCE dependent des bornes et peuvent changer apres fusion.
Ne pas faire un simple append, ni un MERGE ne supprimant jamais les anciens IDs.
"""

from pyspark.sql import DataFrame, Window, functions as F
from pyspark.sql.types import TimestampType


def build_gold(silver: DataFrame, gap_hours: int = 12,
               validate_input: bool = True) -> DataFrame:
    """Agrege les presences et transforme les transitions en marqueurs.

    validate_input effectue une verification distribuee avant construction.
    Le desactiver uniquement si ces controles ont deja ete faits en Silver.
    Aucun collect des observations, aucune UDF et aucun self-join.
    """
    required = {
        "entity_key", "silver_event_id", "event_type", "valid_from", "valid_to",
        "country_code", "country_from", "country_to", "evidence_count",
        "transition_nature",
    }
    missing = required.difference(silver.columns)
    if missing:
        raise ValueError("Colonnes manquantes: " + ", ".join(sorted(missing)))
    if type(gap_hours) is not int or gap_hours <= 0:
        raise ValueError("gap_hours doit etre un entier strictement positif")
    for name in ("valid_from", "valid_to"):
        if not isinstance(silver.schema[name].dataType, TimestampType):
            raise ValueError(name + ": convertir en timestamp avec to_timestamp avant l'appel")

    def text(name):
        value = F.trim(F.col(name).cast("string"))
        return F.when(F.length(value) > 0, value)

    def identifier(*values):
        return F.sha2(F.to_json(
            F.struct(*[value.alias("k" + str(i)) for i, value in enumerate(values)]),
            {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX", "timeZone": "UTC"},
        ), 256)

    data = silver.select(
        text("entity_key").alias("entity_key"),
        text("silver_event_id").alias("_id"),
        F.upper(text("event_type")).alias("_type"),
        F.col("valid_from").alias("_start"),
        F.coalesce("valid_to", "valid_from").alias("_end"),
        F.upper(text("country_code")).alias("country"),
        F.upper(text("country_from")).alias("_from"),
        F.upper(text("country_to")).alias("_to"),
        F.lower(text("transition_nature")).alias("_nature"),
        F.col("evidence_count").cast("long").alias("_count"),
        F.col("evidence_count").cast("double").alias("_provided_count"),
        F.struct(*[F.col("`" + c.replace("`", "``") + "`") for c in silver.columns]).alias("_raw"),
    )

    if validate_input:
        is_presence = F.col("_type") == "PRESENCE"
        is_transition = F.col("_type") == "TRANSITION"
        invalid = (
            F.col("entity_key").isNull() | F.col("_id").isNull()
            | F.col("_type").isNull() | ~F.col("_type").isin("PRESENCE", "TRANSITION")
            | F.col("_start").isNull() | F.col("_end").isNull()
            | (F.col("_end") < F.col("_start"))
            | (is_presence & (
                F.col("country").isNull() | F.col("_count").isNull()
                | F.isnan("_provided_count") | (F.col("_count") < 0)
                | (F.col("_provided_count") != F.col("_count").cast("double"))
            ))
            | (is_transition & (
                F.col("_nature").isNull() | ~F.col("_nature").isin("confirmed", "scheduled")
                | (F.col("_from").isNull() & F.col("_to").isNull())
            ))
        )
        if data.where(invalid).limit(1).count():
            raise ValueError("Silver invalide: verifier cles, types, dates, pays, evidence_count et transition_nature")

    # La fin maximale precedente gere aussi les intervalles imbriques.
    order = Window.partitionBy("entity_key", "country").orderBy("_start", "_end", "_id")
    previous = order.rowsBetween(Window.unboundedPreceding, -1)
    cumulative = order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    presences = (
        data.where(F.col("_type") == "PRESENCE")
        .withColumn("_previous_end", F.max("_end").over(previous))
        .withColumn("_new_group", F.when(
            F.col("_previous_end").isNull()
            | (F.col("_start") >= F.col("_previous_end") + F.expr(f"INTERVAL {gap_hours} HOURS")),
            1,
        ).otherwise(0))
        .withColumn("_group", F.sum("_new_group").over(cumulative))
    )

    # Comparateur sur les cles seulement: raw_data peut contenir des maps.
    packed = F.collect_list(F.struct(
        F.struct("_start", "_end", "_id").alias("key"), F.col("_raw").alias("raw_data"),
    ))
    sorted_detail = F.transform(F.array_sort(packed, lambda a, b:
        F.when(a["key"] < b["key"], -1).when(a["key"] > b["key"], 1).otherwise(0)
    ), lambda item: item["raw_data"])

    presences = (
        presences.groupBy("entity_key", "country", "_group")
        .agg(F.min("_start").alias("start_ts"), F.max("_end").alias("end_ts"),
             F.sum("_count").alias("occurrence_count"), sorted_detail.alias("detail"))
        .withColumn("event_type", F.lit("PRESENCE"))
        .withColumn("gold_id", identifier(F.col("entity_key"), F.lit("PRESENCE"),
            F.col("country"), F.col("start_ts"), F.col("end_ts")))
    )

    transitions = (
        data.where(F.col("_type") == "TRANSITION")
        .withColumn("_marker", F.explode(F.array(
            F.struct(F.concat("_nature", F.lit("_from")).alias("event_type"),
                     F.col("_from").alias("country"), F.col("_start").alias("ts")),
            F.struct(F.concat("_nature", F.lit("_to")).alias("event_type"),
                     F.col("_to").alias("country"), F.col("_end").alias("ts")),
        )))
        .where(F.col("_marker.country").isNotNull())
        .select(
            identifier(F.col("entity_key"), F.col("_id"), F.col("_marker.event_type")).alias("gold_id"),
            "entity_key", F.col("_marker.event_type").alias("event_type"),
            F.col("_marker.country").alias("country"),
            F.col("_marker.ts").alias("start_ts"), F.col("_marker.ts").alias("end_ts"),
            F.lit(None).cast("long").alias("occurrence_count"), F.array("_raw").alias("detail"),
        )
    )
    return presences.select(*transitions.columns).unionByName(transitions)


def user_projection(gold: DataFrame) -> DataFrame:
    """Colonnes simples pour la vue/table utilisateur."""
    return gold.select("entity_key", "event_type", "country", "start_ts", "end_ts", "occurrence_count")


def example_16(spark, evidence_counts=None) -> DataFrame:
    """Les 16 entrees validees. Compteurs ILLUSTRATIFS: 1 si non fournis.

    evidence_counts peut etre un dictionnaire {numero_de_ligne: compteur}.
    La nature des sources a/b est fixee explicitement pour ce jeu de donnees.
    """
    data = [
        ("a", "pid", "07/05/2026 11:31", "07/05/2026 11:31", None, "FRA", None, "a", "TRANSITION"),
        ("a", "pid", "07/05/2026 13:00", "07/05/2026 15:10", None, "FRA", "ESP", "b", "TRANSITION"),
        ("a", "pid", "09/05/2026 00:40", "09/05/2026 02:45", None, "ESP", "FRA", "b", "TRANSITION"),
        ("a", "pid", "09/05/2026 02:38", "09/05/2026 02:38", None, None, "FRA", "a", "TRANSITION"),
        ("a", "pid", "02/06/2026 14:24", "02/06/2026 14:24", None, "FRA", None, "a", "TRANSITION"),
        ("a", "pid", "02/06/2026 15:55", "02/06/2026 16:55", None, "FRA", "DEU", "b", "TRANSITION"),
        ("b", "qw", "02/06/2026 17:20", "02/06/2026 23:58", "DNK", None, None, "c", "PRESENCE"),
        ("c", "qw", "02/06/2026 17:21", "02/06/2026 20:15", "DNK", None, None, "c", "PRESENCE"),
        ("b", "qw", "02/06/2026 17:28", "02/06/2026 18:27", "DEU", None, None, "c", "PRESENCE"),
        ("b", "qw", "03/06/2026 00:10", "03/06/2026 20:49", "DNK", None, None, "c", "PRESENCE"),
        ("c", "qw", "03/06/2026 07:51", "03/06/2026 19:21", "DNK", None, None, "c", "PRESENCE"),
        ("c", "qw", "04/06/2026 08:06", "04/06/2026 18:54", "DNK", None, None, "c", "PRESENCE"),
        ("b", "qw", "04/06/2026 20:35", "04/06/2026 23:36", "CHE", None, None, "c", "PRESENCE"),
        ("a", "pid", "04/06/2026 22:50", "05/06/2026 00:50", None, "CHE", "FRA", "b", "TRANSITION"),
        ("b", "qw", "05/06/2026 00:52", "05/06/2026 00:52", "CHE", None, None, "c", "PRESENCE"),
        ("a", "pid", "05/06/2026 01:14", "05/06/2026 01:14", None, None, "FRA", "a", "TRANSITION"),
    ]
    counts = evidence_counts or {}
    rows = [("1", "s%02d" % i, *row, int(counts.get(i, 1)),
             {"a": "confirmed", "b": "scheduled"}.get(row[7]), {"input_row": str(i)})
            for i, row in enumerate(data, 1)]
    schema = """entity_key string, silver_event_id string, raw_identifier string,
        identifier_type string, valid_from string, valid_to string, country_code string,
        country_from string, country_to string, source_id string, event_type string,
        evidence_count long, transition_nature string, raw_data map<string,string>"""
    return (spark.createDataFrame(rows, schema)
        .withColumn("valid_from", F.to_timestamp("valid_from", "dd/MM/yyyy HH:mm"))
        .withColumn("valid_to", F.to_timestamp("valid_to", "dd/MM/yyyy HH:mm")))
