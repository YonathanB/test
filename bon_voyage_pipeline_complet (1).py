"""
Pipeline Bon Voyage complet : sources -> Bronze -> Silver -> Gold.

Ce fichier est volontairement autonome. Il contient :

* la creation des tables Iceberg internes et des tables Hive/ORC publiees ;
* la lecture de la configuration des sources dans Oracle ;
* l'ingestion idempotente Bronze ;
* la standardisation Silver et la resolution d'identite ;
* l'inference de presence par groupe, sans propagation recursive ;
* le scoring des presences et des transitions ;
* la construction de ranges, le traitement des trous temporels et des
  alternatives ;
* ``build_location_segments`` (le besoin metier est conserve) ;
* le premier chargement par lots de buckets ;
* l'incremental par entites impactees, avec les segments voisins ;
* l'orchestration, les checkpoints et les controles de coherence.

Architecture retenue
--------------------

1. Bronze, Silver et Gold interne sont en Iceberg. Les mises a jour de lignes
   et les suppressions de segments obsoletes utilisent ``MERGE``.
2. Les utilisateurs ne lisent pas Iceberg : les deux tables de segments sont
   materialisees en Hive/ORC. Une troisieme table technique Hive expose les
   trous temporels qui n'ont pas ete resolus, afin que l'inconnu reste visible.
3. La table resolue est partitionnee par ``entity_bucket`` (128 par defaut).
4. La table d'identites non resolues est partitionnee par mois ET bucket.
   Une requete applicative doit fournir ``entity_key``, une plage de dates et
   calculer le bucket avec la meme expression Spark.
5. ``computed_gold_impacted`` n'est pas "le resultat du jour" : ce sont tous
   les segments recalcules pour les entites touchees, dans une fenetre alignee
   sur leurs anciens segments voisins.

Important sur les scores
------------------------

La colonne ``probability`` est une part normalisee des supports concurrents.
Elle est utile pour comparer les pays d'une meme plage, mais elle n'est PAS une
probabilite statistiquement calibree. ``probability_is_calibrated`` vaut donc
toujours False tant qu'une calibration sur un jeu de verite terrain n'a pas
ete realisee.

Configuration Oracle attendue
------------------------------

La table ``SOURCE_CONFIG`` contient une ligne active par source et une colonne
``CONFIG_JSON``. Exemple minimal :

{
  "source_id": "lpr_01",
  "logic_id": "LPR",
  "dependency_group": "LPR_CAMERA",
  "enabled": true,
  "source_kind": "TABLE",
  "source_table": "raw.lpr_01",
  "bronze_table": "lake_bronze.lpr_01",
  "event_type": "PRESENCE",
  "source_confidence": 0.80,
  "identity_mode": "INDIRECT",
  "identifier_type": "PLATE",
  "observation_granularity": "TIMESTAMP",
  "columns": {
    "event_id": "id",
    "raw_identifier": "plate",
    "person_id": null,
    "observation_ts": "event_ts",
    "valid_from": null,
    "valid_to": null,
    "country_code": "country",
    "country_from": null,
    "country_to": null,
    "transition_ts": null,
    "evidence_count": null
  },
  "extra_columns": ["camera_id", "quality_code"]
}

Les noms de tables par defaut sont regroupes dans ``TableNames``. Ils doivent
etre adaptes a l'environnement avant le premier lancement.

Compatibilite cible : Python 3.10+, PySpark 3.4+ et Iceberg Spark extensions.

Commandes de lancement
----------------------

Les secrets ne sont jamais ecrits dans ce fichier :

    export BON_VOYAGE_JDBC_URL='jdbc:oracle:thin:@//host:1521/service'
    export BON_VOYAGE_JDBC_PROPERTIES_JSON='{"user":"...","password":"...","driver":"oracle.jdbc.OracleDriver"}'

Creation initiale des tables :

    spark-submit bon_voyage_pipeline_complet.py create-tables

Premier chargement (les deux dates sont incluses) :

    spark-submit bon_voyage_pipeline_complet.py first-fill \
      --from 2026-07-01 --to 2026-09-09

Run incremental normal :

    spark-submit bon_voyage_pipeline_complet.py incremental

Reconstruction manuelle des copies Hive depuis Iceberg :

    spark-submit bon_voyage_pipeline_complet.py publish-hive

Les noms de tables et les parametres peuvent etre surcharges avec les JSON
``BON_VOYAGE_TABLES_JSON`` et ``BON_VOYAGE_PARAMETERS_JSON``.
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from functools import reduce
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.utils import AnalysisException


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TableNames:
    source_config_oracle: str = "SOURCE_CONFIG"
    checkpoint_oracle: str = "PIPELINE_CHECKPOINT"
    run_oracle: str = "PIPELINE_RUN"

    identity_mapping: str = "prod_ref.identity_mapping"
    travel_groups: str = "prod_ref.travel_groups"

    silver_events: str = "prod_silver.location_events"
    impacted_entities: str = "prod_ops.gold_impacted_entities"

    gold_resolved_internal: str = "prod_gold_internal.location_segment_resolved"
    gold_unresolved_identity_internal: str = (
        "prod_gold_internal.location_segment_unresolved_identity"
    )
    gold_temporal_gaps_internal: str = (
        "prod_gold_internal.location_temporal_gap"
    )

    gold_resolved_hive: str = "prod_gold.location_segment_resolved"
    gold_unresolved_identity_hive: str = (
        "prod_gold.location_segment_unresolved_identity"
    )
    gold_temporal_gaps_hive: str = "prod_gold.location_temporal_gap"


@dataclass(frozen=True)
class PipelineParameters:
    algorithm_version: str = "bon-voyage-v3.0"
    session_timezone: str = "UTC"

    silver_buckets: int = 512
    resolved_gold_buckets: int = 128
    unresolved_gold_buckets: int = 256

    first_fill_source_chunk_days: int = 1
    first_fill_bucket_batch_size: int = 16
    incremental_bucket_batch_size: int = 128
    incremental_lookback_days: int = 3
    neighbour_segments_each_side: int = 1

    volume_reference: int = 20
    count_factor_floor: float = 0.75
    count_factor_amplitude: float = 0.25
    independent_logic_bonus: float = 0.20
    independent_logic_cap: float = 1.60
    recurrence_window_days: int = 7
    recurrent_evidence_bonus: float = 1.10
    isolated_evidence_factor: float = 0.80

    presence_weight: float = 1.00
    inferred_group_weight: float = 0.70
    transition_weight: float = 1.35
    transition_sequence_weight: float = 1.75
    transition_anchor_max_days: int = 30

    observed_min_share: float = 0.60
    observed_min_margin: float = 0.15
    gap_min_share: float = 0.65
    gap_min_margin: float = 0.20
    time_decay_days: float = 3.0
    max_inference_gap_days: float = 30.0

    direct_identity_confidence: float = 100.0
    unresolved_identity_confidence: float = 0.0
    group_confidence_factor: float = 0.80
    default_source_confidence: float = 0.50

    point_event_seconds: int = 1
    max_extra_json_columns: int = 100
    max_support_ids_per_silver_row: int = 500
    max_details_per_silver_row: int = 50
    enable_identity_relink: bool = True
    identity_mapping_updated_at_column: str = "updated_at"

    def validate(self) -> None:
        positive_ints = {
            "silver_buckets": self.silver_buckets,
            "resolved_gold_buckets": self.resolved_gold_buckets,
            "unresolved_gold_buckets": self.unresolved_gold_buckets,
            "first_fill_source_chunk_days": self.first_fill_source_chunk_days,
            "first_fill_bucket_batch_size": self.first_fill_bucket_batch_size,
            "incremental_bucket_batch_size": self.incremental_bucket_batch_size,
            "volume_reference": self.volume_reference,
            "recurrence_window_days": self.recurrence_window_days,
            "transition_anchor_max_days": self.transition_anchor_max_days,
            "point_event_seconds": self.point_event_seconds,
            "max_support_ids_per_silver_row": self.max_support_ids_per_silver_row,
            "max_details_per_silver_row": self.max_details_per_silver_row,
        }
        bad = [name for name, value in positive_ints.items() if value <= 0]
        if bad:
            raise ValueError(f"Parametres strictement positifs requis: {bad}")
        for name in (
            "observed_min_share",
            "observed_min_margin",
            "gap_min_share",
            "gap_min_margin",
        ):
            value = float(getattr(self, name))
            if value < 0.0 or value > 1.0:
                raise ValueError(f"{name} doit etre compris entre 0 et 1")
        if self.time_decay_days <= 0 or self.max_inference_gap_days <= 0:
            raise ValueError("Les durees d'inference doivent etre positives")
        if self.incremental_lookback_days < 0:
            raise ValueError("incremental_lookback_days ne peut pas etre negatif")
        if self.neighbour_segments_each_side < 0:
            raise ValueError("neighbour_segments_each_side ne peut pas etre negatif")
        if self.independent_logic_cap < 1.0:
            raise ValueError("independent_logic_cap doit etre au moins egal a 1")
        for name in (
            "direct_identity_confidence",
            "unresolved_identity_confidence",
        ):
            value = float(getattr(self, name))
            if value < 0.0 or value > 100.0:
                raise ValueError(f"{name} doit etre compris entre 0 et 100")
        if self.group_confidence_factor < 0.0 or self.group_confidence_factor > 1.0:
            raise ValueError("group_confidence_factor doit etre compris entre 0 et 1")


@dataclass(frozen=True)
class RuntimeConfig:
    jdbc_url: str
    jdbc_properties: Mapping[str, str]
    tables: TableNames = field(default_factory=TableNames)
    params: PipelineParameters = field(default_factory=PipelineParameters)


@dataclass(frozen=True)
class RunContext:
    run_id: str
    mode: str
    started_at: datetime
    requested_from: Optional[datetime]
    requested_to: Optional[datetime]
    input_cutoff: Optional[datetime] = None


# ---------------------------------------------------------------------------
# Schemas stables
# ---------------------------------------------------------------------------


SILVER_SCHEMA = T.StructType(
    [
        T.StructField("silver_event_id", T.StringType(), False),
        T.StructField("event_id", T.StringType(), True),
        T.StructField("source_id", T.StringType(), False),
        T.StructField("logic_id", T.StringType(), False),
        T.StructField("dependency_group", T.StringType(), False),
        T.StructField("run_id", T.StringType(), False),
        T.StructField("ingested_at", T.TimestampType(), False),
        T.StructField("bronze_table", T.StringType(), True),
        T.StructField("bronze_partition", T.StringType(), True),
        T.StructField("bronze_row_id", T.StringType(), True),
        T.StructField("raw_identifier", T.StringType(), True),
        T.StructField("identifier_type", T.StringType(), True),
        T.StructField("person_id", T.StringType(), True),
        T.StructField("identity_confidence", T.DoubleType(), False),
        T.StructField("identity_graph_version", T.StringType(), True),
        T.StructField("isresolved", T.BooleanType(), False),
        T.StructField("identity_resolution_method", T.StringType(), True),
        T.StructField("entity_key", T.StringType(), False),
        T.StructField("event_type", T.StringType(), False),
        T.StructField("country_code", T.StringType(), True),
        T.StructField("country_from", T.StringType(), True),
        T.StructField("country_to", T.StringType(), True),
        T.StructField("observation_ts", T.TimestampType(), False),
        T.StructField("observation_date", T.DateType(), False),
        T.StructField("valid_from", T.TimestampType(), False),
        T.StructField("valid_to", T.TimestampType(), False),
        T.StructField("transition_ts", T.TimestampType(), True),
        T.StructField("evidence_count", T.LongType(), False),
        T.StructField("source_confidence", T.DoubleType(), False),
        T.StructField("manual_score", T.DoubleType(), True),
        T.StructField("computed_score", T.DoubleType(), True),
        T.StructField("inference_method", T.StringType(), False),
        T.StructField("inferred_from_person_id", T.StringType(), True),
        T.StructField("support_event_ids", T.ArrayType(T.StringType()), False),
        T.StructField(
            "support_bronze_row_ids", T.ArrayType(T.StringType()), False
        ),
        T.StructField("details_json", T.StringType(), True),
        T.StructField("content_hash", T.StringType(), False),
        T.StructField("day", T.DateType(), False),
        T.StructField("silver_bucket", T.IntegerType(), False),
    ]
)


GOLD_SCHEMA = T.StructType(
    [
        T.StructField("segment_id", T.StringType(), False),
        T.StructField("entity_key", T.StringType(), False),
        T.StructField("person_id", T.StringType(), True),
        T.StructField("isresolved", T.BooleanType(), False),
        T.StructField("country_code", T.StringType(), False),
        T.StructField("segment_from", T.TimestampType(), False),
        T.StructField("segment_to", T.TimestampType(), False),
        T.StructField("probability", T.DoubleType(), False),
        T.StructField("probability_is_calibrated", T.BooleanType(), False),
        T.StructField("identity_score", T.DoubleType(), False),
        T.StructField("evidence_count", T.LongType(), False),
        T.StructField("source_ids", T.ArrayType(T.StringType()), False),
        T.StructField("logic_ids", T.ArrayType(T.StringType()), False),
        T.StructField("event_types", T.ArrayType(T.StringType()), False),
        T.StructField("range_ids", T.ArrayType(T.StringType()), False),
        T.StructField("has_temporal_inference", T.BooleanType(), False),
        T.StructField("has_unresolved_alternatives", T.BooleanType(), False),
        T.StructField("resolution_methods", T.ArrayType(T.StringType()), False),
        T.StructField("details_json", T.StringType(), True),
        T.StructField("algorithm_version", T.StringType(), False),
        T.StructField("inference_run_id", T.StringType(), False),
        T.StructField("input_cutoff", T.TimestampType(), False),
        T.StructField("segment_month", T.StringType(), False),
        T.StructField("entity_bucket", T.IntegerType(), False),
    ]
)


IMPACT_SCHEMA = T.StructType(
    [
        T.StructField("run_id", T.StringType(), False),
        T.StructField("entity_key", T.StringType(), False),
        T.StructField("person_id", T.StringType(), True),
        T.StructField("isresolved", T.BooleanType(), False),
        T.StructField("impacted_from", T.TimestampType(), False),
        T.StructField("impacted_to", T.TimestampType(), False),
        T.StructField("recompute_from", T.TimestampType(), False),
        T.StructField("recompute_to", T.TimestampType(), False),
        T.StructField("work_bucket", T.IntegerType(), False),
        T.StructField("impact_status", T.StringType(), False),
        T.StructField("created_at", T.TimestampType(), False),
    ]
)


TEMPORAL_GAP_SCHEMA = T.StructType(
    [
        T.StructField("gap_id", T.StringType(), False),
        T.StructField("entity_key", T.StringType(), False),
        T.StructField("person_id", T.StringType(), True),
        T.StructField("isresolved", T.BooleanType(), False),
        T.StructField("gap_from", T.TimestampType(), False),
        T.StructField("gap_to", T.TimestampType(), False),
        T.StructField("unresolved_reason", T.StringType(), False),
        T.StructField("candidates_json", T.StringType(), True),
        T.StructField("transitions_json", T.StringType(), True),
        T.StructField("algorithm_version", T.StringType(), False),
        T.StructField("inference_run_id", T.StringType(), False),
        T.StructField("input_cutoff", T.TimestampType(), False),
        T.StructField("gap_month", T.StringType(), False),
        T.StructField("entity_bucket", T.IntegerType(), False),
    ]
)


# ---------------------------------------------------------------------------
# Utilitaires generaux
# ---------------------------------------------------------------------------


def log_event(event: str, **values: Any) -> None:
    payload = {"event": event, "at": datetime.now(timezone.utc).isoformat()}
    payload.update(values)
    print(json.dumps(payload, ensure_ascii=False, default=str), flush=True)


def _table_exists(spark: SparkSession, table: str) -> bool:
    try:
        spark.table(table).schema
        return True
    except AnalysisException:
        return False


def _require_columns(df: DataFrame, required: Iterable[str], name: str) -> None:
    missing = sorted(set(required).difference(df.columns))
    if missing:
        raise ValueError(f"{name}: colonnes manquantes: {', '.join(missing)}")


def _stable_id(*columns: F.Column) -> F.Column:
    if not columns:
        raise ValueError("_stable_id exige au moins une colonne")
    return F.sha2(
        F.to_json(
            F.array(
                *[
                    F.coalesce(column.cast("string"), F.lit("<NULL>"))
                    for column in columns
                ]
            )
        ),
        256,
    )


def _entity_bucket(column: F.Column, bucket_count: int) -> F.Column:
    return F.pmod(F.xxhash64(column), F.lit(int(bucket_count))).cast("int")


def _column_or_null(df: DataFrame, name: str, data_type: str) -> F.Column:
    if name in df.columns:
        return F.col(name).cast(data_type)
    return F.lit(None).cast(data_type)


def _column_or_value(
    df: DataFrame,
    name: str,
    default: Any,
    data_type: str,
) -> F.Column:
    if name in df.columns:
        return F.coalesce(F.col(name).cast(data_type), F.lit(default).cast(data_type))
    return F.lit(default).cast(data_type)


def _array_or_empty(df: DataFrame, name: str) -> F.Column:
    if name in df.columns:
        return F.coalesce(F.col(name), F.expr("cast(array() as array<string>)"))
    return F.expr("cast(array() as array<string>)")


def _align_to_schema(df: DataFrame, schema: T.StructType) -> DataFrame:
    expressions: list[F.Column] = []
    for field in schema.fields:
        if field.name in df.columns:
            expressions.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            expressions.append(F.lit(None).cast(field.dataType).alias(field.name))
    return df.select(*expressions)


def _union_all(dataframes: Sequence[DataFrame]) -> DataFrame:
    if not dataframes:
        raise ValueError("_union_all exige au moins un DataFrame")
    return reduce(
        lambda left, right: left.unionByName(right, allowMissingColumns=True),
        dataframes[1:],
        dataframes[0],
    )


def _is_empty(df: DataFrame) -> bool:
    return df.limit(1).count() == 0


def _as_utc_naive(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _parse_datetime(value: str | datetime | date) -> datetime:
    if isinstance(value, datetime):
        return _as_utc_naive(value)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return _as_utc_naive(parsed)


def _date_chunks(
    start: datetime,
    end_exclusive: datetime,
    chunk_days: int,
) -> Iterator[tuple[datetime, datetime]]:
    cursor = start
    delta = timedelta(days=chunk_days)
    while cursor < end_exclusive:
        next_cursor = min(cursor + delta, end_exclusive)
        yield cursor, next_cursor
        cursor = next_cursor


def _ensure_namespace_for(spark: SparkSession, table: str) -> None:
    parts = table.split(".")
    if len(parts) == 2:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{parts[0]}`")
    elif len(parts) >= 3:
        namespace = ".".join(f"`{part}`" for part in parts[:-1])
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def _create_iceberg_table(
    spark: SparkSession,
    table: str,
    schema: T.StructType,
    partition_columns: Sequence[str],
) -> None:
    if _table_exists(spark, table):
        return
    _ensure_namespace_for(spark, table)
    empty = spark.createDataFrame([], schema)
    writer = empty.writeTo(table).using("iceberg")
    if partition_columns:
        writer = writer.partitionedBy(*[F.col(column) for column in partition_columns])
    writer.create()


def _create_iceberg_table_like(
    spark: SparkSession,
    table: str,
    dataframe: DataFrame,
    partition_columns: Sequence[str],
) -> None:
    if _table_exists(spark, table):
        return
    _ensure_namespace_for(spark, table)
    writer = dataframe.limit(0).writeTo(table).using("iceberg")
    if partition_columns:
        writer = writer.partitionedBy(*[F.col(column) for column in partition_columns])
    writer.create()


def _temporary_view_name(prefix: str) -> str:
    return f"_{prefix}_{uuid.uuid4().hex}"


def _merge_iceberg(
    spark: SparkSession,
    target_table: str,
    source: DataFrame,
    key_columns: Sequence[str],
    update_matched: bool = True,
    matched_condition: Optional[str] = None,
) -> None:
    if _is_empty(source):
        return
    target_columns = spark.table(target_table).columns
    missing = [column for column in target_columns if column not in source.columns]
    if missing:
        raise ValueError(
            f"MERGE {target_table}: colonnes source manquantes: {missing}"
        )
    source = source.select(*target_columns)
    view = _temporary_view_name("merge_source")
    source.createOrReplaceTempView(view)
    condition = " AND ".join(
        f"t.`{column}` <=> s.`{column}`" for column in key_columns
    )
    if update_matched:
        condition_sql = f" AND ({matched_condition})" if matched_condition else ""
        matched = f"WHEN MATCHED{condition_sql} THEN UPDATE SET *"
    else:
        matched = ""
    try:
        spark.sql(
            f"""
            MERGE INTO {target_table} t
            USING {view} s
            ON {condition}
            {matched}
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    finally:
        spark.catalog.dropTempView(view)


def _delete_gold_scope(
    spark: SparkSession,
    table: str,
    scopes: DataFrame,
) -> None:
    if not _table_exists(spark, table) or _is_empty(scopes):
        return
    view = _temporary_view_name("delete_scope")
    scopes.select(
        "entity_key", "recompute_from", "recompute_to"
    ).distinct().createOrReplaceTempView(view)
    try:
        spark.sql(
            f"""
            MERGE INTO {table} t
            USING {view} s
            ON  t.entity_key = s.entity_key
            AND t.segment_from < s.recompute_to
            AND t.segment_to   > s.recompute_from
            WHEN MATCHED THEN DELETE
            """
        )
    finally:
        spark.catalog.dropTempView(view)


def _delete_gap_scope(
    spark: SparkSession,
    table: str,
    scopes: DataFrame,
) -> None:
    if not _table_exists(spark, table) or _is_empty(scopes):
        return
    view = _temporary_view_name("delete_gap_scope")
    scopes.select(
        "entity_key", "recompute_from", "recompute_to"
    ).distinct().createOrReplaceTempView(view)
    try:
        spark.sql(
            f"""
            MERGE INTO {table} t
            USING {view} s
            ON  t.entity_key = s.entity_key
            AND t.gap_from < s.recompute_to
            AND t.gap_to   > s.recompute_from
            WHEN MATCHED THEN DELETE
            """
        )
    finally:
        spark.catalog.dropTempView(view)


# ---------------------------------------------------------------------------
# Tables de controle Oracle et checkpoints
# ---------------------------------------------------------------------------


def _jdbc_connection(spark: SparkSession, runtime: RuntimeConfig):
    props = dict(runtime.jdbc_properties)
    user = props.get("user")
    password = props.get("password")
    if not user or password is None:
        raise ValueError("jdbc_properties doit contenir user et password")
    driver = props.get("driver")
    if driver:
        spark._jvm.java.lang.Class.forName(driver)
    return spark._jvm.java.sql.DriverManager.getConnection(
        runtime.jdbc_url, user, password
    )


def _execute_jdbc(
    spark: SparkSession,
    runtime: RuntimeConfig,
    sql: str,
    parameters: Sequence[Any] = (),
) -> None:
    connection = _jdbc_connection(spark, runtime)
    statement = None
    try:
        connection.setAutoCommit(False)
        statement = connection.prepareStatement(sql)
        for index, value in enumerate(parameters, start=1):
            if value is None:
                statement.setNull(index, spark._jvm.java.sql.Types.NULL)
            elif isinstance(value, datetime):
                timestamp = spark._jvm.java.sql.Timestamp.valueOf(
                    _as_utc_naive(value).strftime("%Y-%m-%d %H:%M:%S.%f")
                )
                statement.setTimestamp(index, timestamp)
            elif isinstance(value, bool):
                statement.setBoolean(index, value)
            elif isinstance(value, int):
                statement.setLong(index, value)
            elif isinstance(value, float):
                statement.setDouble(index, value)
            else:
                statement.setString(index, str(value))
        statement.execute()
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        if statement is not None:
            statement.close()
        connection.close()


def ensure_oracle_control_tables(
    spark: SparkSession,
    runtime: RuntimeConfig,
) -> None:
    ddl_statements = [
        f"""
        CREATE TABLE {runtime.tables.checkpoint_oracle} (
            pipeline_name    VARCHAR2(100) NOT NULL,
            source_id        VARCHAR2(200) NOT NULL,
            processed_until  TIMESTAMP,
            processed_at     TIMESTAMP NOT NULL,
            run_id           VARCHAR2(64) NOT NULL,
            status           VARCHAR2(30) NOT NULL,
            CONSTRAINT pk_pipeline_checkpoint
                PRIMARY KEY (pipeline_name, source_id)
        )
        """,
        f"""
        CREATE TABLE {runtime.tables.run_oracle} (
            run_id           VARCHAR2(64) PRIMARY KEY,
            pipeline_name    VARCHAR2(100) NOT NULL,
            run_mode         VARCHAR2(30) NOT NULL,
            started_at       TIMESTAMP NOT NULL,
            input_cutoff     TIMESTAMP,
            finished_at      TIMESTAMP,
            status           VARCHAR2(30) NOT NULL,
            error_message    VARCHAR2(4000)
        )
        """,
    ]
    for ddl in ddl_statements:
        try:
            _execute_jdbc(spark, runtime, ddl)
        except Exception as exc:
            # ORA-00955 = l'objet existe deja. Toute autre erreur est bloquante.
            if "ORA-00955" not in str(exc):
                raise


def start_run(
    spark: SparkSession,
    runtime: RuntimeConfig,
    context: RunContext,
) -> None:
    _execute_jdbc(
        spark,
        runtime,
        f"""
        INSERT INTO {runtime.tables.run_oracle}
            (run_id, pipeline_name, run_mode, started_at, input_cutoff, status)
        VALUES (?, 'BON_VOYAGE', ?, ?, ?, 'RUNNING')
        """,
        [
            context.run_id,
            context.mode,
            context.started_at,
            context.input_cutoff,
        ],
    )


def finish_run(
    spark: SparkSession,
    runtime: RuntimeConfig,
    run_id: str,
    status: str,
    error_message: Optional[str] = None,
    input_cutoff: Optional[datetime] = None,
) -> None:
    _execute_jdbc(
        spark,
        runtime,
        f"""
        UPDATE {runtime.tables.run_oracle}
           SET status = ?, finished_at = SYSTIMESTAMP,
               input_cutoff = COALESCE(?, input_cutoff),
               error_message = ?
         WHERE run_id = ?
        """,
        [status, input_cutoff, (error_message or "")[:4000], run_id],
    )


def get_last_checkpoint(
    spark: SparkSession,
    runtime: RuntimeConfig,
    source_id: str,
    pipeline_name: str = "BON_VOYAGE_SILVER",
) -> Optional[datetime]:
    query = (
        f"(SELECT processed_until FROM {runtime.tables.checkpoint_oracle} "
        f"WHERE pipeline_name = '{pipeline_name}' "
        f"AND source_id = '{source_id.replace(chr(39), chr(39) * 2)}' "
        f"AND status = 'SUCCESS') checkpoint_query"
    )
    try:
        rows = (
            spark.read.format("jdbc")
            .option("url", runtime.jdbc_url)
            .option("dbtable", query)
            .options(**dict(runtime.jdbc_properties))
            .load()
            .limit(1)
            .collect()
        )
    except Exception as exc:
        if "ORA-00942" in str(exc):
            return None
        raise
    if not rows or rows[0][0] is None:
        return None
    return _parse_datetime(rows[0][0])


def update_checkpoint(
    spark: SparkSession,
    runtime: RuntimeConfig,
    source_id: str,
    processed_until: datetime,
    run_id: str,
    pipeline_name: str = "BON_VOYAGE_SILVER",
) -> None:
    _execute_jdbc(
        spark,
        runtime,
        f"""
        MERGE INTO {runtime.tables.checkpoint_oracle} t
        USING (
            SELECT ? pipeline_name, ? source_id, ? processed_until,
                   ? run_id FROM dual
        ) s
        ON (t.pipeline_name = s.pipeline_name AND t.source_id = s.source_id)
        WHEN MATCHED THEN UPDATE SET
            t.processed_until = s.processed_until,
            t.processed_at = SYSTIMESTAMP,
            t.run_id = s.run_id,
            t.status = 'SUCCESS'
        WHEN NOT MATCHED THEN INSERT
            (pipeline_name, source_id, processed_until, processed_at,
             run_id, status)
        VALUES
            (s.pipeline_name, s.source_id, s.processed_until, SYSTIMESTAMP,
             s.run_id, 'SUCCESS')
        """,
        [pipeline_name, source_id, processed_until, run_id],
    )


# ---------------------------------------------------------------------------
# Creation des tables Spark
# ---------------------------------------------------------------------------


def ensure_spark_tables(spark: SparkSession, runtime: RuntimeConfig) -> None:
    runtime.params.validate()
    spark.conf.set("spark.sql.session.timeZone", runtime.params.session_timezone)
    spark.conf.set("spark.sql.storeAssignmentPolicy", "ANSI")
    _create_iceberg_table(
        spark,
        runtime.tables.silver_events,
        SILVER_SCHEMA,
        ["day", "silver_bucket"],
    )
    if "content_hash" not in spark.table(runtime.tables.silver_events).columns:
        spark.sql(
            f"ALTER TABLE {runtime.tables.silver_events} "
            "ADD COLUMN content_hash STRING"
        )
    if (
        "support_bronze_row_ids"
        not in spark.table(runtime.tables.silver_events).columns
    ):
        spark.sql(
            f"ALTER TABLE {runtime.tables.silver_events} "
            "ADD COLUMN support_bronze_row_ids ARRAY<STRING>"
        )
    if (
        "identity_resolution_method"
        not in spark.table(runtime.tables.silver_events).columns
    ):
        spark.sql(
            f"ALTER TABLE {runtime.tables.silver_events} "
            "ADD COLUMN identity_resolution_method STRING"
        )
    _create_iceberg_table(
        spark,
        runtime.tables.impacted_entities,
        IMPACT_SCHEMA,
        ["run_id"],
    )
    if "impact_status" not in spark.table(runtime.tables.impacted_entities).columns:
        spark.sql(
            f"ALTER TABLE {runtime.tables.impacted_entities} "
            "ADD COLUMN impact_status STRING"
        )
    _create_iceberg_table(
        spark,
        runtime.tables.gold_resolved_internal,
        GOLD_SCHEMA,
        ["entity_bucket"],
    )
    _create_iceberg_table(
        spark,
        runtime.tables.gold_unresolved_identity_internal,
        GOLD_SCHEMA,
        ["segment_month", "entity_bucket"],
    )
    _create_iceberg_table(
        spark,
        runtime.tables.gold_temporal_gaps_internal,
        TEMPORAL_GAP_SCHEMA,
        ["gap_month", "entity_bucket"],
    )


# ---------------------------------------------------------------------------
# Configuration des sources
# ---------------------------------------------------------------------------


def load_source_configs(
    spark: SparkSession,
    runtime: RuntimeConfig,
    config_column: str = "CONFIG_JSON",
) -> list[dict[str, Any]]:
    rows = (
        spark.read.format("jdbc")
        .option("url", runtime.jdbc_url)
        .option("dbtable", runtime.tables.source_config_oracle)
        .options(**dict(runtime.jdbc_properties))
        .load()
        .select(F.col(config_column).cast("string").alias("config_json"))
        .collect()
    )
    configs: list[dict[str, Any]] = []
    for row in rows:
        decoded = json.loads(row["config_json"])
        candidates = decoded if isinstance(decoded, list) else [decoded]
        for config in candidates:
            if config.get("enabled", True):
                configs.append(_normalise_source_config(config, runtime.params))
    if not configs:
        raise ValueError("Aucune source active dans SOURCE_CONFIG")
    duplicated = [
        source_id
        for source_id in {cfg["source_id"] for cfg in configs}
        if sum(cfg["source_id"] == source_id for cfg in configs) > 1
    ]
    if duplicated:
        raise ValueError(f"source_id dupliques dans la configuration: {duplicated}")
    return configs


def _normalise_source_config(
    config: Mapping[str, Any],
    params: PipelineParameters,
) -> dict[str, Any]:
    cfg = dict(config)
    required = [
        "source_id",
        "logic_id",
        "source_table",
        "bronze_table",
        "columns",
    ]
    missing = [name for name in required if not cfg.get(name)]
    if missing:
        raise ValueError(f"Configuration source incomplete, manque: {missing}")
    cfg.setdefault("dependency_group", cfg["logic_id"])
    cfg.setdefault("event_type", "PRESENCE")
    cfg.setdefault("source_kind", "TABLE")
    cfg.setdefault("source_confidence", params.default_source_confidence)
    cfg.setdefault("identity_mode", "INDIRECT")
    cfg.setdefault("identifier_type", "UNKNOWN")
    cfg.setdefault("observation_granularity", "DAY")
    cfg.setdefault("extra_columns", [])
    cfg.setdefault("manual_score", None)
    cfg.setdefault("computed_score", None)
    cfg.setdefault("lateness_days", params.incremental_lookback_days)
    if len(cfg["extra_columns"]) > params.max_extra_json_columns:
        raise ValueError(
            f"[{cfg['source_id']}] trop de colonnes extra: "
            f"{len(cfg['extra_columns'])}"
        )
    columns = dict(cfg["columns"])
    if not columns.get("observation_ts"):
        raise ValueError(f"[{cfg['source_id']}] observation_ts est obligatoire")
    if not columns.get("raw_identifier") and not columns.get("person_id"):
        raise ValueError(
            f"[{cfg['source_id']}] raw_identifier ou person_id est obligatoire"
        )
    event_type = str(cfg["event_type"]).upper()
    if event_type in {"TRANSITIONS", "MOVEMENT", "MOVE"}:
        event_type = "TRANSITION"
    if event_type == "PRESENCE" and not columns.get("country_code"):
        raise ValueError(
            f"[{cfg['source_id']}] country_code est obligatoire pour PRESENCE"
        )
    if event_type == "TRANSITION" and (
        not columns.get("country_from") or not columns.get("country_to")
    ):
        raise ValueError(
            f"[{cfg['source_id']}] country_from et country_to sont "
            "obligatoires pour TRANSITION"
        )
    if event_type not in {"PRESENCE", "TRANSITION"}:
        raise ValueError(
            f"[{cfg['source_id']}] event_type invalide: {cfg['event_type']}"
        )
    cfg["event_type"] = event_type
    return cfg


def get_current_watermark(
    spark: SparkSession,
    runtime: RuntimeConfig,
    source_config: Mapping[str, Any],
) -> Optional[datetime]:
    """Retourne la borne source disponible, diminuee du retard de securite.

    Pour une source JDBC, ``watermark_query`` peut etre renseigne dans le JSON
    afin de pousser ``MAX`` au moteur source. Sinon Spark calcule le MAX de la
    colonne configuree.
    """
    cfg = dict(source_config)
    timestamp_column = cfg["columns"]["observation_ts"]
    if cfg.get("watermark_query"):
        source = (
            spark.read.format("jdbc")
            .option("url", cfg.get("jdbc_url", runtime.jdbc_url))
            .option("dbtable", f"({cfg['watermark_query']}) watermark_source")
            .options(**dict(runtime.jdbc_properties))
            .load()
        )
        value = source.select(F.max(F.col("watermark")).alias("watermark")).first()
    else:
        source = _read_unfiltered_source(spark, runtime, cfg)
        value = source.select(
            F.max(F.col(timestamp_column).cast("timestamp")).alias("watermark")
        ).first()
    if not value or value["watermark"] is None:
        return None
    watermark = _parse_datetime(value["watermark"])
    if str(cfg.get("observation_granularity", "DAY")).upper() == "DAY":
        watermark = datetime.combine(watermark.date(), datetime.min.time()) + timedelta(
            days=1
        )
    else:
        # read_source_slice utilise une borne haute exclusive.
        watermark += timedelta(microseconds=1)
    lateness = int(cfg.get("watermark_safety_days", 0))
    return watermark - timedelta(days=lateness)


# ---------------------------------------------------------------------------
# Bronze
# ---------------------------------------------------------------------------


def _read_unfiltered_source(
    spark: SparkSession,
    runtime: RuntimeConfig,
    config: Mapping[str, Any],
) -> DataFrame:
    kind = str(config.get("source_kind", "TABLE")).upper()
    if kind in {"TABLE", "ICEBERG", "HIVE"}:
        return spark.table(str(config["source_table"]))
    if kind in {"PARQUET", "ORC", "DELTA"}:
        return spark.read.format(kind.lower()).load(str(config["source_table"]))
    if kind == "JDBC":
        return (
            spark.read.format("jdbc")
            .option("url", str(config.get("jdbc_url", runtime.jdbc_url)))
            .option("dbtable", str(config["source_table"]))
            .options(**dict(runtime.jdbc_properties))
            .load()
        )
    raise ValueError(f"source_kind non supporte: {kind}")


def read_source_slice(
    spark: SparkSession,
    runtime: RuntimeConfig,
    config: Mapping[str, Any],
    from_ts: datetime,
    to_ts: datetime,
) -> DataFrame:
    source = _read_unfiltered_source(spark, runtime, config)
    timestamp_column = config["columns"]["observation_ts"]
    return source.where(
        (F.col(timestamp_column).cast("timestamp") >= F.lit(from_ts))
        & (F.col(timestamp_column).cast("timestamp") < F.lit(to_ts))
    )


def build_bronze_batch(
    source: DataFrame,
    config: Mapping[str, Any],
    context: RunContext,
) -> DataFrame:
    cfg = dict(config)
    columns = cfg["columns"]
    observation_ts = F.col(columns["observation_ts"]).cast("timestamp")
    primary_keys = [name for name in cfg.get("primary_key_columns", []) if name]
    content_hash = _stable_id(
        F.to_json(F.struct(*[F.col(name) for name in sorted(source.columns)]))
    )
    if primary_keys:
        business_key = _stable_id(
            F.lit(cfg["source_id"]),
            *[F.col(name) for name in primary_keys],
        )
    else:
        # Les colonnes sont triees pour rendre le hash stable si l'ordre du
        # schema source change. Une vraie cle source reste preferable.
        business_key = _stable_id(F.lit(cfg["source_id"]), content_hash)
    raw_key = _stable_id(business_key, content_hash)
    return (
        source
        .withColumn("_bronze_source_id", F.lit(cfg["source_id"]))
        .withColumn("_bronze_logic_id", F.lit(cfg["logic_id"]))
        .withColumn("_bronze_business_key", business_key)
        .withColumn("_bronze_content_hash", content_hash)
        .withColumn("_bronze_row_id", raw_key)
        .withColumn("_bronze_observation_ts", observation_ts)
        .withColumn("_bronze_day", F.to_date(observation_ts))
        .withColumn("_bronze_partition", F.date_format(observation_ts, "yyyy-MM-dd"))
        .withColumn("_bronze_run_id", F.lit(context.run_id))
        .withColumn("_bronze_ingested_at", F.current_timestamp())
        .dropDuplicates(["_bronze_row_id"])
    )


def write_bronze(
    spark: SparkSession,
    bronze_batch: DataFrame,
    bronze_table: str,
) -> None:
    _create_iceberg_table_like(
        spark,
        bronze_table,
        bronze_batch,
        ["_bronze_day"],
    )
    existing_columns = set(spark.table(bronze_table).columns)
    for field in bronze_batch.schema.fields:
        if field.name not in existing_columns:
            spark.sql(
                f"ALTER TABLE {bronze_table} ADD COLUMN `{field.name}` "
                f"{field.dataType.simpleString()}"
            )
    # Si une colonne a disparu du schema source, elle existe encore dans la
    # table Bronze historique. On l'aligne a NULL pour que le MERGE reste
    # compatible avec l'evolution additive du schema.
    target_schema = spark.table(bronze_table).schema
    bronze_to_write = bronze_batch
    for field in target_schema.fields:
        if field.name not in bronze_to_write.columns:
            bronze_to_write = bronze_to_write.withColumn(
                field.name, F.lit(None).cast(field.dataType)
            )
    _merge_iceberg(
        spark,
        bronze_table,
        bronze_to_write,
        ["_bronze_row_id"],
        update_matched=False,
    )


# ---------------------------------------------------------------------------
# Silver : standardisation et identite
# ---------------------------------------------------------------------------


def _mapped_column(
    dataframe: DataFrame,
    mapping: Mapping[str, Any],
    canonical_name: str,
    data_type: str,
) -> F.Column:
    source_name = mapping.get(canonical_name)
    if source_name and source_name in dataframe.columns:
        return F.col(str(source_name)).cast(data_type)
    return F.lit(None).cast(data_type)


def _normalised_country(column: F.Column) -> F.Column:
    return F.when(
        F.length(F.trim(column.cast("string"))) > 0,
        F.upper(F.trim(column.cast("string"))),
    ).otherwise(F.lit(None).cast("string"))


def _identity_mapping_for_batch(
    spark: SparkSession,
    runtime: RuntimeConfig,
    identifiers: DataFrame,
) -> DataFrame:
    mapping = spark.table(runtime.tables.identity_mapping)
    if "priority" in mapping.columns:
        mapping = mapping.where(F.col("priority") == 1)
    elif "rank" in mapping.columns:
        mapping = mapping.where(F.col("rank") == 1)

    id_column = "raw_identifier" if "raw_identifier" in mapping.columns else "identifier"
    type_column = "identifier_type"
    confidence_column = (
        "identity_confidence"
        if "identity_confidence" in mapping.columns
        else "score"
    )
    version_column = (
        "identity_graph_version"
        if "identity_graph_version" in mapping.columns
        else None
    )

    selected = mapping.select(
        F.col(id_column).cast("string").alias("raw_identifier"),
        F.col(type_column).cast("string").alias("identifier_type"),
        F.col("person_id").cast("string").alias("mapped_person_id"),
        F.col(confidence_column).cast("double").alias("mapped_identity_confidence"),
        (
            F.col(version_column).cast("string")
            if version_column
            else F.lit(None).cast("string")
        ).alias("mapped_identity_graph_version"),
    ).dropDuplicates(["raw_identifier", "identifier_type"])

    # Mapping tres volumineux, mais peu de correspondances : semi-join avec
    # les identifiants reellement presents, puis broadcast du petit resultat.
    relevant = selected.join(
        identifiers.select("raw_identifier", "identifier_type").distinct(),
        ["raw_identifier", "identifier_type"],
        "left_semi",
    )
    return relevant.hint("broadcast")


def standardizer(
    spark: SparkSession,
    runtime: RuntimeConfig,
    bronze_batch: DataFrame,
    config: Mapping[str, Any],
    context: RunContext,
) -> DataFrame:
    """Transforme une source Bronze en contrat Silver canonique."""
    cfg = dict(config)
    mapping = dict(cfg["columns"])
    params = runtime.params

    raw_identifier = _mapped_column(
        bronze_batch, mapping, "raw_identifier", "string"
    )
    direct_person_id = _mapped_column(
        bronze_batch, mapping, "person_id", "string"
    )
    identifier_type = F.lit(str(cfg.get("identifier_type", "UNKNOWN")))
    observation_ts = _mapped_column(
        bronze_batch, mapping, "observation_ts", "timestamp"
    )
    observation_date = F.to_date(observation_ts)

    event_type = str(cfg.get("event_type", "PRESENCE")).upper()
    if event_type in {"TRANSITIONS", "MOVEMENT", "MOVE"}:
        event_type = "TRANSITION"
    if event_type not in {"PRESENCE", "TRANSITION"}:
        raise ValueError(
            f"[{cfg['source_id']}] event_type invalide: {event_type}"
        )

    explicit_valid_from = _mapped_column(
        bronze_batch, mapping, "valid_from", "timestamp"
    )
    explicit_valid_to = _mapped_column(
        bronze_batch, mapping, "valid_to", "timestamp"
    )
    granularity = str(cfg.get("observation_granularity", "DAY")).upper()
    if granularity == "DAY":
        default_from = observation_date.cast("timestamp")
        default_to = F.date_add(observation_date, 1).cast("timestamp")
    else:
        default_from = observation_ts
        default_to = F.from_unixtime(
            F.unix_timestamp(observation_ts) + F.lit(params.point_event_seconds)
        ).cast("timestamp")

    base = bronze_batch.select(
        _mapped_column(bronze_batch, mapping, "event_id", "string").alias("event_id"),
        raw_identifier.alias("raw_identifier"),
        direct_person_id.alias("direct_person_id"),
        identifier_type.alias("identifier_type"),
        observation_ts.alias("observation_ts"),
        observation_date.alias("observation_date"),
        F.coalesce(explicit_valid_from, default_from).alias("valid_from"),
        F.coalesce(explicit_valid_to, default_to).alias("valid_to"),
        _normalised_country(
            _mapped_column(bronze_batch, mapping, "country_code", "string")
        ).alias("country_code"),
        _normalised_country(
            _mapped_column(bronze_batch, mapping, "country_from", "string")
        ).alias("country_from"),
        _normalised_country(
            _mapped_column(bronze_batch, mapping, "country_to", "string")
        ).alias("country_to"),
        F.coalesce(
            _mapped_column(bronze_batch, mapping, "transition_ts", "timestamp"),
            observation_ts if event_type == "TRANSITION" else F.lit(None).cast("timestamp"),
        ).alias("transition_ts"),
        F.coalesce(
            _mapped_column(bronze_batch, mapping, "evidence_count", "long"),
            F.lit(1).cast("long"),
        ).alias("evidence_count"),
        F.col("_bronze_business_key").alias("bronze_business_key"),
        F.col("_bronze_row_id").alias("bronze_row_id"),
        F.col("_bronze_partition").alias("bronze_partition"),
    )

    if cfg.get("extra_columns"):
        extra_fields = [
            F.col(name).cast("string").alias(name)
            for name in cfg["extra_columns"]
            if name in bronze_batch.columns
        ]
        extra_value = (
            F.to_json(F.struct(*extra_fields))
            if extra_fields
            else F.lit(None).cast("string")
        )
        extra_by_row = bronze_batch.select(
            F.col("_bronze_row_id").alias("bronze_row_id"),
            extra_value.alias("details_json"),
        )
        base = base.join(extra_by_row, "bronze_row_id", "left")
    else:
        base = base.withColumn("details_json", F.lit(None).cast("string"))

    identifiers = base.select("raw_identifier", "identifier_type").where(
        F.col("raw_identifier").isNotNull()
    )
    relevant_mapping = _identity_mapping_for_batch(spark, runtime, identifiers)

    enriched = base.join(
        relevant_mapping,
        ["raw_identifier", "identifier_type"],
        "left",
    )
    identity_mode = str(cfg.get("identity_mode", "INDIRECT")).upper()
    chosen_person = (
        F.col("direct_person_id")
        if identity_mode == "DIRECT"
        else F.coalesce(F.col("direct_person_id"), F.col("mapped_person_id"))
    )
    mapped_confidence_100 = F.when(
        F.col("mapped_identity_confidence") <= F.lit(1.0),
        F.col("mapped_identity_confidence") * F.lit(100.0),
    ).otherwise(F.col("mapped_identity_confidence"))
    identity_confidence = F.greatest(
        F.lit(0.0),
        F.least(
            F.lit(100.0),
            F.when(
                F.col("direct_person_id").isNotNull(),
                F.lit(params.direct_identity_confidence),
            )
            .when(
                F.col("mapped_person_id").isNotNull(),
                F.coalesce(
                    mapped_confidence_100,
                    F.lit(params.direct_identity_confidence),
                ),
            )
            .otherwise(F.lit(params.unresolved_identity_confidence)),
        ),
    )
    identity_resolution_method = (
        F.when(
            F.col("direct_person_id").isNotNull(),
            F.lit("DIRECT_PERSON_ID"),
        )
        .when(
            (F.lit(identity_mode) != F.lit("DIRECT"))
            & F.col("mapped_person_id").isNotNull(),
            F.lit("IDENTITY_MAPPING"),
        )
        .otherwise(F.lit("UNRESOLVED"))
    )

    configured_source_score = cfg.get("computed_score")
    if configured_source_score is None:
        configured_source_score = cfg.get("manual_score")
    if configured_source_score is None:
        configured_source_score = cfg.get(
            "source_confidence", params.default_source_confidence
        )

    standard = (
        enriched
        .withColumn("person_id", chosen_person.cast("string"))
        .withColumn("isresolved", F.col("person_id").isNotNull())
        .withColumn(
            "identity_resolution_method", identity_resolution_method
        )
        .withColumn(
            "entity_key",
            F.coalesce(F.col("person_id"), F.col("raw_identifier")),
        )
        .withColumn("identity_confidence", identity_confidence.cast("double"))
        .withColumn(
            "event_id",
            F.coalesce(
                F.col("event_id"),
                F.col("bronze_business_key"),
                F.col("bronze_row_id"),
            ),
        )
        .withColumn(
            "silver_event_id",
            _stable_id(
                F.lit(cfg["source_id"]),
                F.col("event_id"),
                F.lit(event_type),
            ),
        )
        .withColumn("source_id", F.lit(cfg["source_id"]))
        .withColumn("logic_id", F.lit(cfg["logic_id"]))
        .withColumn("dependency_group", F.lit(cfg["dependency_group"]))
        .withColumn("run_id", F.lit(context.run_id))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("bronze_table", F.lit(cfg["bronze_table"]))
        .withColumn("identity_graph_version", F.col("mapped_identity_graph_version"))
        .withColumn("event_type", F.lit(event_type))
        .withColumn(
            "source_confidence",
            F.greatest(
                F.lit(0.0),
                F.least(F.lit(1.0), F.lit(float(configured_source_score))),
            ),
        )
        .withColumn(
            "manual_score",
            F.lit(cfg.get("manual_score")).cast("double"),
        )
        .withColumn(
            "computed_score",
            F.lit(cfg.get("computed_score")).cast("double"),
        )
        .withColumn("inference_method", F.lit("DIRECT"))
        .withColumn("inferred_from_person_id", F.lit(None).cast("string"))
        .withColumn("support_event_ids", F.array(F.col("event_id")))
        .withColumn(
            "support_bronze_row_ids", F.array(F.col("bronze_row_id"))
        )
        .withColumn(
            "content_hash",
            _stable_id(
                F.col("event_id"),
                F.col("entity_key"),
                F.col("person_id"),
                F.col("identity_confidence"),
                F.col("identity_resolution_method"),
                F.col("event_type"),
                F.col("country_code"),
                F.col("country_from"),
                F.col("country_to"),
                F.col("observation_ts"),
                F.col("valid_from"),
                F.col("valid_to"),
                F.col("transition_ts"),
                F.col("source_confidence"),
                F.col("evidence_count"),
                F.col("inference_method"),
                F.col("details_json"),
            ),
        )
        .withColumn("day", F.col("observation_date"))
        .withColumn(
            "silver_bucket",
            _entity_bucket(F.col("entity_key"), params.silver_buckets),
        )
        .where(
            F.col("entity_key").isNotNull()
            & F.col("observation_ts").isNotNull()
            & F.col("valid_from").isNotNull()
            & F.col("valid_to").isNotNull()
            & (F.col("valid_to") > F.col("valid_from"))
        )
    )

    if event_type == "PRESENCE":
        standard = standard.where(F.col("country_code").isNotNull())
    else:
        standard = standard.where(
            F.col("transition_ts").isNotNull()
            & F.col("country_from").isNotNull()
            & F.col("country_to").isNotNull()
        )
    return _align_to_schema(standard, SILVER_SCHEMA)


def aggregate_silver_batch(
    silver_batch: DataFrame,
    runtime: RuntimeConfig,
    context: RunContext,
) -> DataFrame:
    """Reduit les PRESENCE au grain source-entite-jour-pays.

    Les TRANSITION ne sont jamais agregees car leur timestamp et leur ordre
    portent la sequence metier. Pour les presences, les IDs Bronze restent
    disponibles dans ``support_bronze_row_ids`` (liste bornee) et le nombre
    total reste dans ``evidence_count``.
    """
    params = runtime.params
    presences = silver_batch.where(F.col("event_type") == "PRESENCE")
    transitions = silver_batch.where(F.col("event_type") == "TRANSITION")

    group_columns = [
        "source_id",
        "logic_id",
        "dependency_group",
        "raw_identifier",
        "identifier_type",
        "person_id",
        "identity_confidence",
        "identity_graph_version",
        "isresolved",
        "identity_resolution_method",
        "entity_key",
        "country_code",
        "observation_date",
        "source_confidence",
        "manual_score",
        "computed_score",
        "inference_method",
        "inferred_from_person_id",
        "bronze_table",
        "bronze_partition",
        "day",
        "silver_bucket",
    ]
    aggregate_id = _stable_id(
        F.col("source_id"),
        F.col("logic_id"),
        F.coalesce(F.col("raw_identifier"), F.col("person_id")),
        F.col("identifier_type"),
        F.col("observation_date"),
        F.col("country_code"),
        F.col("inference_method"),
    )
    aggregated = (
        presences.groupBy(*group_columns)
        .agg(
            F.min("observation_ts").alias("observation_ts"),
            F.min("valid_from").alias("valid_from"),
            F.max("valid_to").alias("valid_to"),
            F.sum("evidence_count").cast("long").alias("evidence_count"),
            F.slice(
                F.sort_array(F.collect_set("event_id")),
                1,
                params.max_support_ids_per_silver_row,
            ).alias("support_event_ids"),
            F.slice(
                F.sort_array(F.collect_set("bronze_row_id")),
                1,
                params.max_support_ids_per_silver_row,
            ).alias("support_bronze_row_ids"),
            F.slice(
                F.sort_array(F.collect_set("details_json")),
                1,
                params.max_details_per_silver_row,
            ).alias("_details"),
            F.first("bronze_row_id", ignorenulls=True).alias("bronze_row_id"),
        )
        .withColumn("event_id", aggregate_id)
        .withColumn("silver_event_id", aggregate_id)
        .withColumn("event_type", F.lit("PRESENCE"))
        .withColumn("country_from", F.lit(None).cast("string"))
        .withColumn("country_to", F.lit(None).cast("string"))
        .withColumn("transition_ts", F.lit(None).cast("timestamp"))
        .withColumn("run_id", F.lit(context.run_id))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn(
            "details_json",
            F.to_json(
                F.struct(
                    F.lit(True).alias("silver_aggregated"),
                    F.col("evidence_count").alias("total_evidence_count"),
                    F.col("_details").alias("sample_details"),
                )
            ),
        )
        .withColumn(
            "content_hash",
            _stable_id(
                F.col("event_id"),
                F.col("entity_key"),
                F.col("person_id"),
                F.col("identity_confidence"),
                F.col("country_code"),
                F.col("observation_ts"),
                F.col("valid_from"),
                F.col("valid_to"),
                F.col("source_confidence"),
                F.col("evidence_count"),
                F.to_json(F.col("support_event_ids")),
                F.to_json(F.col("support_bronze_row_ids")),
            ),
        )
        .drop("_details")
    )
    return _align_to_schema(
        aggregated.unionByName(transitions, allowMissingColumns=True),
        SILVER_SCHEMA,
    )


def write_silver(
    spark: SparkSession,
    runtime: RuntimeConfig,
    silver_batch: DataFrame,
) -> None:
    aligned = _align_to_schema(silver_batch, SILVER_SCHEMA)
    bounds = aligned.agg(
        F.min("day").alias("minimum_day"),
        F.max("day").alias("maximum_day"),
        F.min("silver_bucket").alias("minimum_bucket"),
        F.max("silver_bucket").alias("maximum_bucket"),
    ).first()
    if not bounds or bounds["minimum_day"] is None:
        return
    existing_hashes = spark.table(runtime.tables.silver_events).where(
        (F.col("day") >= F.lit(bounds["minimum_day"]))
        & (F.col("day") <= F.lit(bounds["maximum_day"]))
        & (F.col("silver_bucket") >= F.lit(int(bounds["minimum_bucket"])))
        & (F.col("silver_bucket") <= F.lit(int(bounds["maximum_bucket"])))
    ).select(
        F.col("silver_event_id").alias("_existing_id"),
        F.col("content_hash").alias("_existing_hash"),
    )
    changed = (
        aligned.alias("s")
        .join(
            existing_hashes.alias("t"),
            F.col("s.silver_event_id") == F.col("t._existing_id"),
            "left",
        )
        .where(
            F.col("t._existing_id").isNull()
            | ~F.col("s.content_hash").eqNullSafe(F.col("t._existing_hash"))
        )
        .select("s.*")
        .dropDuplicates(["silver_event_id"])
        .persist(StorageLevel.DISK_ONLY)
    )
    try:
        if _is_empty(changed):
            return
        # Le journal d'impact est ecrit avant le MERGE Silver. Ainsi, une
        # coupure entre les deux operations ne peut pas rendre une evolution
        # Silver invisible au Gold lors de la reprise. Le run courant s'arrete
        # si le MERGE echoue ; le prochain run rejouera le delta avant Gold.
        register_pending_impacts(spark, runtime, changed)
        _merge_iceberg(
            spark,
            runtime.tables.silver_events,
            changed,
            ["silver_event_id"],
            update_matched=True,
            matched_condition="NOT (t.content_hash <=> s.content_hash)",
        )
    finally:
        changed.unpersist()


def relink_silver_identities(
    spark: SparkSession,
    runtime: RuntimeConfig,
    context: RunContext,
    changed_since: datetime,
) -> DataFrame:
    """Met a jour les anciens evenements devenus resolus.

    La fonction retourne les anciennes ``entity_key`` non resolues. Elles sont
    ajoutees aux scopes du Gold afin que leurs anciens segments soient
    supprimes, tandis que les lignes Silver mises a jour font apparaitre la
    nouvelle ``person_id`` parmi les impacts normaux.

    Le mapping doit exposer une colonne ``updated_at`` (nom configurable). En
    son absence la fonction n'invente pas un delta et se desactive proprement.
    """
    empty = spark.createDataFrame(
        [],
        "entity_key string, person_id string, isresolved boolean, "
        "impacted_from timestamp, impacted_to timestamp",
    )
    if not runtime.params.enable_identity_relink:
        return empty
    mapping = spark.table(runtime.tables.identity_mapping)
    updated_at_column = runtime.params.identity_mapping_updated_at_column
    if updated_at_column not in mapping.columns:
        log_event(
            "identity_relink_skipped",
            run_id=context.run_id,
            reason=f"missing_column:{updated_at_column}",
        )
        return empty

    if "priority" in mapping.columns:
        mapping = mapping.where(F.col("priority") == 1)
    elif "rank" in mapping.columns:
        mapping = mapping.where(F.col("rank") == 1)
    mapping = mapping.where(F.col(updated_at_column) > F.lit(changed_since))

    id_column = "raw_identifier" if "raw_identifier" in mapping.columns else "identifier"
    confidence_column = (
        "identity_confidence" if "identity_confidence" in mapping.columns else "score"
    )
    version_column = (
        "identity_graph_version"
        if "identity_graph_version" in mapping.columns
        else None
    )
    changes = (
        mapping.select(
            F.col(id_column).cast("string").alias("raw_identifier"),
            F.col("identifier_type").cast("string").alias("identifier_type"),
            F.col("person_id").cast("string").alias("new_person_id"),
            F.col(confidence_column)
            .cast("double")
            .alias("new_identity_confidence"),
            (
                F.col(version_column).cast("string")
                if version_column
                else F.lit(None).cast("string")
            ).alias("new_identity_graph_version"),
        )
        .withColumn(
            "new_identity_confidence_100",
            F.greatest(
                F.lit(0.0),
                F.least(
                    F.lit(100.0),
                    F.coalesce(
                        F.when(
                            F.col("new_identity_confidence") <= F.lit(1.0),
                            F.col("new_identity_confidence") * F.lit(100.0),
                        ).otherwise(F.col("new_identity_confidence")),
                        F.lit(runtime.params.direct_identity_confidence),
                    ),
                ),
            ),
        )
        .dropDuplicates(["raw_identifier", "identifier_type"])
    )
    if _is_empty(changes):
        return empty

    old_rows = (
        spark.table(runtime.tables.silver_events).alias("s")
        .where(
            (F.col("s.identity_resolution_method") == "IDENTITY_MAPPING")
            | (F.col("s.identity_resolution_method") == "UNRESOLVED")
            | (
                F.col("s.identity_resolution_method").isNull()
                & (
                    F.col("s.person_id").isNull()
                    | F.col("s.identity_graph_version").isNotNull()
                )
            )
        )
        .join(
            changes.hint("broadcast").alias("m"),
            (F.col("s.raw_identifier") == F.col("m.raw_identifier"))
            & (F.col("s.identifier_type") == F.col("m.identifier_type")),
            "inner",
        )
        .select(
            F.col("s.*"),
            F.col("s.person_id").alias("_old_person_id"),
            F.col("m.new_person_id"),
            F.col("m.new_identity_confidence"),
            F.col("m.new_identity_confidence_100"),
            F.col("m.new_identity_graph_version"),
        )
        .where(
            F.col("new_person_id").isNotNull()
            & (
                ~F.col("new_person_id").eqNullSafe(F.col("_old_person_id"))
                | ~F.col("new_identity_confidence_100").eqNullSafe(
                    F.col("identity_confidence")
                )
                | ~F.col("new_identity_graph_version").eqNullSafe(
                    F.col("identity_graph_version")
                )
            )
        )
        .persist(StorageLevel.DISK_ONLY)
    )
    if _is_empty(old_rows):
        old_rows.unpersist()
        return empty

    old_impacts = _base_impacts(old_rows.select(*SILVER_SCHEMA.fieldNames()), runtime).persist(
        StorageLevel.DISK_ONLY
    )
    old_impacts.count()  # fige l'ancienne entity_key avant le MERGE Silver

    updates = (
        old_rows
        .withColumn("person_id", F.col("new_person_id"))
        .withColumn("entity_key", F.col("new_person_id"))
        .withColumn("isresolved", F.lit(True))
        .withColumn(
            "identity_confidence", F.col("new_identity_confidence_100")
        )
        .withColumn(
            "identity_resolution_method", F.lit("IDENTITY_MAPPING")
        )
        .withColumn(
            "identity_graph_version", F.col("new_identity_graph_version")
        )
        .withColumn("run_id", F.lit(context.run_id))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn(
            "silver_bucket",
            _entity_bucket(F.col("new_person_id"), runtime.params.silver_buckets),
        )
        .withColumn(
            "content_hash",
            _stable_id(
                F.col("event_id"),
                F.col("new_person_id"),
                F.col("identity_confidence"),
                F.col("identity_resolution_method"),
                F.col("event_type"),
                F.col("country_code"),
                F.col("country_from"),
                F.col("country_to"),
                F.col("observation_ts"),
                F.col("valid_from"),
                F.col("valid_to"),
                F.col("transition_ts"),
                F.col("source_confidence"),
                F.col("evidence_count"),
                F.col("inference_method"),
                F.col("details_json"),
            ),
        )
    )
    write_silver(spark, runtime, _align_to_schema(updates, SILVER_SCHEMA))
    register_external_impacts(
        spark, runtime, old_impacts, context.run_id
    )
    old_rows.unpersist()
    return old_impacts


def ingest_source_window(
    spark: SparkSession,
    runtime: RuntimeConfig,
    config: Mapping[str, Any],
    context: RunContext,
    from_ts: datetime,
    to_ts: datetime,
) -> DataFrame:
    log_event(
        "source_window_started",
        run_id=context.run_id,
        source_id=config["source_id"],
        from_ts=from_ts,
        to_ts=to_ts,
    )
    source = read_source_slice(spark, runtime, config, from_ts, to_ts)
    bronze = build_bronze_batch(source, config, context).persist(
        StorageLevel.DISK_ONLY
    )
    try:
        write_bronze(spark, bronze, str(config["bronze_table"]))
        silver = aggregate_silver_batch(
            standardizer(spark, runtime, bronze, config, context),
            runtime,
            context,
        )
        write_silver(spark, runtime, silver)
        update_checkpoint(
            spark,
            runtime,
            str(config["source_id"]),
            to_ts,
            context.run_id,
        )
        log_event(
            "source_window_completed",
            run_id=context.run_id,
            source_id=config["source_id"],
            from_ts=from_ts,
            to_ts=to_ts,
        )
        return silver
    finally:
        bronze.unpersist()


# ---------------------------------------------------------------------------
# Silver : inference de presence par groupe
# ---------------------------------------------------------------------------


def _find_column_case_insensitive(
    dataframe: DataFrame,
    *candidates: str,
) -> str:
    by_lower = {name.lower(): name for name in dataframe.columns}
    for candidate in candidates:
        if candidate.lower() in by_lower:
            return by_lower[candidate.lower()]
    raise ValueError(
        f"Aucune colonne parmi {candidates}; colonnes presentes={dataframe.columns}"
    )


def build_bidirectional_group_edges(groups: DataFrame) -> DataFrame:
    """Transforme A->B en deux aretes A->B et B->A.

    Le couple reste inferable uniquement pendant la periode commune : de
    ``startday`` jusqu'au premier retour connu de l'un des deux voyageurs.
    """
    leader = _find_column_case_insensitive(groups, "leader")
    member = _find_column_case_insensitive(groups, "member")
    start_day = _find_column_case_insensitive(groups, "startday", "start_day")
    leader_return = _find_column_case_insensitive(
        groups, "leaderreturnday", "leader_return_day"
    )
    member_return = _find_column_case_insensitive(
        groups, "memberreturnday", "member_return_day"
    )

    clean = groups.select(
        F.col(leader).cast("string").alias("leader"),
        F.col(member).cast("string").alias("member"),
        F.to_date(F.col(start_day)).alias("start_day"),
        F.to_date(F.col(leader_return)).alias("leader_return_day"),
        F.to_date(F.col(member_return)).alias("member_return_day"),
    ).where(
        F.col("leader").isNotNull()
        & F.col("member").isNotNull()
        & (F.col("leader") != F.col("member"))
        & F.col("start_day").isNotNull()
    )

    forward = clean.select(
        F.col("leader").alias("source_person_id"),
        F.col("member").alias("target_person_id"),
        "start_day",
        F.col("leader_return_day").alias("source_return_day"),
        F.col("member_return_day").alias("target_return_day"),
    )
    reverse = clean.select(
        F.col("member").alias("source_person_id"),
        F.col("leader").alias("target_person_id"),
        "start_day",
        F.col("member_return_day").alias("source_return_day"),
        F.col("leader_return_day").alias("target_return_day"),
    )
    far_future = F.lit("2999-12-31").cast("date")
    return (
        forward.unionByName(reverse)
        .withColumn(
            "common_return_day",
            F.least(
                F.coalesce(F.col("source_return_day"), far_future),
                F.coalesce(F.col("target_return_day"), far_future),
            ),
        )
        .withColumn(
            "group_edge_id",
            _stable_id(
                F.col("source_person_id"),
                F.col("target_person_id"),
                F.col("start_day"),
                F.col("common_return_day"),
            ),
        )
        .dropDuplicates(["group_edge_id"])
    )


def infer_group_presences(
    spark: SparkSession,
    runtime: RuntimeConfig,
    context: RunContext,
    day_from: date,
    day_to: date,
) -> DataFrame:
    """Propage une presence directe vers les compagnons de voyage.

    Une presence dont ``inference_method`` vaut deja ``INFERRED_BY_GROUP`` ne
    peut jamais servir de source. Cette regle empeche A->B->C, les boucles et
    l'amplification artificielle. L'identifiant deterministe rend l'ecriture
    idempotente au prochain run.
    """
    if not _table_exists(spark, runtime.tables.travel_groups):
        return spark.createDataFrame([], SILVER_SCHEMA)

    edges = build_bidirectional_group_edges(
        spark.table(runtime.tables.travel_groups)
    )
    direct_presences = (
        spark.table(runtime.tables.silver_events)
        .where(
            (F.col("day") >= F.lit(day_from))
            & (F.col("day") <= F.lit(day_to))
            & F.col("person_id").isNotNull()
            & (F.col("event_type") == "PRESENCE")
            & (F.col("inference_method") != "INFERRED_BY_GROUP")
        )
    )

    inferred = (
        direct_presences.alias("p")
        .join(
            edges.alias("g"),
            (F.col("p.person_id") == F.col("g.source_person_id"))
            & (F.col("p.day") >= F.col("g.start_day"))
            & (F.col("p.day") <= F.col("g.common_return_day")),
            "inner",
        )
        .select(
            F.col("p.*"),
            F.col("g.target_person_id").alias("_target_person_id"),
            F.col("g.source_person_id").alias("_source_person_id"),
            F.col("g.group_edge_id").alias("_group_edge_id"),
            F.col("p.silver_event_id").alias("_source_silver_event_id"),
            F.col("p.source_id").alias("_source_source_id"),
            F.col("p.identity_confidence").alias(
                "_source_identity_confidence"
            ),
            F.col("p.source_confidence").alias("_source_source_confidence"),
            F.col("p.details_json").alias("_source_details_json"),
        )
        .withColumn("person_id", F.col("_target_person_id"))
        .withColumn("entity_key", F.col("_target_person_id"))
        .withColumn("raw_identifier", F.col("_target_person_id"))
        .withColumn("identifier_type", F.lit("PERSON_ID"))
        .withColumn("isresolved", F.lit(True))
        .withColumn(
            "identity_resolution_method", F.lit("GROUP_PERSON_ID")
        )
        .withColumn(
            "silver_event_id",
            _stable_id(
                F.lit("INFERRED_BY_GROUP"),
                F.col("_group_edge_id"),
                F.col("_source_silver_event_id"),
                F.col("_target_person_id"),
            ),
        )
        .withColumn("event_id", F.col("silver_event_id"))
        .withColumn(
            "source_id",
            F.concat(F.lit("GROUP:"), F.col("_source_source_id")),
        )
        .withColumn("logic_id", F.lit("INFERRED_BY_GROUP"))
        .withColumn(
            "dependency_group",
            F.concat(F.lit("GROUP:"), F.col("_group_edge_id")),
        )
        .withColumn("run_id", F.lit(context.run_id))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("bronze_table", F.lit(None).cast("string"))
        .withColumn("bronze_partition", F.lit(None).cast("string"))
        .withColumn("bronze_row_id", F.lit(None).cast("string"))
        .withColumn(
            "identity_confidence",
            F.least(
                F.lit(100.0),
                F.col("_source_identity_confidence")
                * F.lit(runtime.params.group_confidence_factor),
            ),
        )
        .withColumn(
            "source_confidence",
            F.col("_source_source_confidence"),
        )
        .withColumn("inference_method", F.lit("INFERRED_BY_GROUP"))
        .withColumn("inferred_from_person_id", F.col("_source_person_id"))
        .withColumn(
            "support_event_ids", F.array(F.col("_source_silver_event_id"))
        )
        .withColumn(
            "details_json",
            F.to_json(
                F.struct(
                    F.col("_group_edge_id").alias("group_edge_id"),
                    F.col("_source_person_id").alias("source_person_id"),
                    F.col("_source_silver_event_id").alias(
                        "source_silver_event_id"
                    ),
                    F.col("_source_details_json").alias("source_details_json"),
                )
            ),
        )
        .withColumn(
            "content_hash",
            _stable_id(
                F.col("event_id"),
                F.col("entity_key"),
                F.col("person_id"),
                F.col("country_code"),
                F.col("observation_ts"),
                F.col("valid_from"),
                F.col("valid_to"),
                F.col("source_confidence"),
                F.col("identity_confidence"),
                F.col("identity_resolution_method"),
                F.col("inference_method"),
                F.col("details_json"),
            ),
        )
        .withColumn(
            "silver_bucket",
            _entity_bucket(F.col("entity_key"), runtime.params.silver_buckets),
        )
        .drop(
            "_target_person_id",
            "_source_person_id",
            "_group_edge_id",
            "_source_silver_event_id",
            "_source_source_id",
            "_source_identity_confidence",
            "_source_source_confidence",
            "_source_details_json",
        )
        .dropDuplicates(["silver_event_id"])
    )
    result = _align_to_schema(inferred, SILVER_SCHEMA)
    write_silver(spark, runtime, result)
    return result


# ---------------------------------------------------------------------------
# Gold : preparation et scoring des preuves
# ---------------------------------------------------------------------------


def _normalise_identity_confidence(column: F.Column) -> F.Column:
    # Silver stocke 0..100. Cette fonction tolere aussi une ancienne valeur
    # 0..1 afin de ne pas multiplier accidentellement le poids par 100.
    return F.when(column <= F.lit(1.0), column).otherwise(column / F.lit(100.0))


def _identity_evidence_factor(
    isresolved: F.Column,
    identity_confidence: F.Column,
) -> F.Column:
    """L'incertitude d'identite ne supprime pas la geographie d'une cle brute.

    Pour une entite resolue, le score du lien vers ``person_id`` module la
    preuve. Pour une entite non resolue, la presence reste valable pour son
    ``entity_key`` brute : facteur geographique 1, ``identity_score`` final 0.
    """
    return F.when(
        isresolved,
        F.greatest(
            F.lit(0.0),
            F.least(
                F.lit(1.0),
                _normalise_identity_confidence(identity_confidence),
            ),
        ),
    ).otherwise(F.lit(1.0))


def _count_factor(params: PipelineParameters) -> F.Column:
    saturated = F.least(
        F.sqrt(F.greatest(F.col("evidence_count"), F.lit(1.0)))
        / F.sqrt(F.lit(float(params.volume_reference))),
        F.lit(1.0),
    )
    return F.lit(params.count_factor_floor) + F.lit(
        params.count_factor_amplitude
    ) * saturated


def build_transition_anchors(
    silver: DataFrame,
    runtime: RuntimeConfig,
) -> tuple[DataFrame, DataFrame]:
    """Produit des preuves FROM/TO et conserve les transitions detaillees.

    * FROM couvre le debut du jour jusqu'au timestamp de depart.
    * TO couvre l'arrivee jusqu'a la transition suivante, avec un plafond.
    * Si ``country_to`` de N == ``country_from`` de N+1, la preuve TO recoit
      ``transition_sequence_weight``. Sinon elle recoit ``transition_weight``.
    """
    params = runtime.params
    transitions = silver.where(F.col("event_type") == "TRANSITION")

    # Plusieurs sources peuvent decrire exactement la meme transition. Elles
    # restent des preuves distinctes pour le scoring et la tracabilite, mais ne
    # doivent pas se succeder artificiellement dans la chaine temporelle. La
    # recherche de la transition suivante se fait donc au grain
    # (entity_key, transition_ts), puis le resultat est rattache a chaque
    # preuve source. En cas de pays de depart contradictoires au timestamp
    # suivant, aucun bonus de chaine n'est accorde.
    timestamp_transitions = transitions.groupBy(
        "entity_key", "transition_ts"
    ).agg(
        F.sort_array(F.collect_set("country_from")).alias(
            "current_country_froms"
        )
    )
    timestamp_order = Window.partitionBy("entity_key").orderBy(
        "transition_ts"
    )
    transition_chain = (
        timestamp_transitions
        .withColumn(
            "next_transition_ts",
            F.lead("transition_ts").over(timestamp_order),
        )
        .withColumn(
            "next_country_froms",
            F.lead("current_country_froms").over(timestamp_order),
        )
        .drop("current_country_froms")
    )
    maximum_seconds = params.transition_anchor_max_days * 86400
    prepared = (
        transitions
        .join(
            transition_chain,
            ["entity_key", "transition_ts"],
            "left",
        )
        .withColumn(
            "sequence_supported",
            F.col("next_transition_ts").isNotNull()
            & (F.size(F.col("next_country_froms")) == F.lit(1))
            & (
                F.element_at(F.col("next_country_froms"), 1)
                == F.col("country_to")
            ),
        )
        .withColumn(
            "transition_evidence_weight",
            F.col("source_confidence")
            * _identity_evidence_factor(
                F.col("isresolved"), F.col("identity_confidence")
            )
            * F.when(
                F.col("sequence_supported"),
                F.lit(params.transition_sequence_weight),
            ).otherwise(F.lit(params.transition_weight)),
        )
    )

    common = [
        "entity_key",
        "person_id",
        "isresolved",
        "silver_event_id",
        "event_id",
        "source_id",
        "logic_id",
        "dependency_group",
        "identity_confidence",
        "source_confidence",
        "evidence_count",
        "inference_method",
        "details_json",
    ]
    from_evidence = prepared.select(
        *common,
        F.to_date("transition_ts").cast("timestamp").alias("evidence_from"),
        F.col("transition_ts").alias("evidence_to"),
        F.col("country_from").alias("country_code"),
        F.lit("TRANSITION_FROM").alias("evidence_kind"),
        F.lit("TRANSITION").alias("event_type"),
        (
            F.col("source_confidence")
            * _identity_evidence_factor(
                F.col("isresolved"), F.col("identity_confidence")
            )
            * F.lit(params.transition_weight)
        ).alias("base_contribution"),
        F.lit(False).alias("sequence_supported"),
    )

    capped_to = F.from_unixtime(
        F.unix_timestamp("transition_ts") + F.lit(maximum_seconds)
    ).cast("timestamp")
    to_evidence = prepared.select(
        *common,
        F.col("transition_ts").alias("evidence_from"),
        F.least(
            F.coalesce(F.col("next_transition_ts"), capped_to),
            capped_to,
        ).alias("evidence_to"),
        F.col("country_to").alias("country_code"),
        F.lit("TRANSITION_TO").alias("evidence_kind"),
        F.lit("TRANSITION").alias("event_type"),
        F.col("transition_evidence_weight").alias("base_contribution"),
        F.col("sequence_supported"),
    )

    # Pour le remplissage des gaps, une transition logique ne doit apparaitre
    # qu'une fois, meme si elle est confirmee par plusieurs sources. Les
    # preuves sources restent toutes presentes dans details_json. Comme dans
    # le scoring principal, les doublons d'un meme dependency_group ne
    # s'additionnent pas, tandis que les logiques independantes apportent un
    # bonus borne.
    logical_keys = [
        "entity_key",
        "transition_ts",
        "country_from",
        "country_to",
        "next_transition_ts",
        "sequence_supported",
    ]
    detailed_by_dependency = prepared.groupBy(
        *logical_keys, "dependency_group"
    ).agg(
        F.max("transition_evidence_weight").alias("dependency_weight"),
        F.collect_set("source_id").alias("source_ids"),
        F.collect_set("logic_id").alias("logic_ids"),
        F.collect_set("silver_event_id").alias("silver_event_ids"),
        F.collect_set("event_id").alias("event_ids"),
        F.collect_list("details_json").alias("source_details"),
        F.first("person_id", ignorenulls=True).alias("person_id"),
        F.max(F.col("isresolved").cast("int")).alias("isresolved_int"),
    )
    detailed = (
        detailed_by_dependency.groupBy(*logical_keys)
        .agg(
            F.sum("dependency_weight").alias("raw_transition_weight"),
            F.array_distinct(F.flatten(F.collect_list("source_ids"))).alias(
                "source_ids"
            ),
            F.array_distinct(F.flatten(F.collect_list("logic_ids"))).alias(
                "logic_ids"
            ),
            F.array_distinct(
                F.flatten(F.collect_list("silver_event_ids"))
            ).alias("silver_event_ids"),
            F.array_distinct(F.flatten(F.collect_list("event_ids"))).alias(
                "event_ids"
            ),
            F.flatten(F.collect_list("source_details")).alias(
                "source_details"
            ),
            F.first("person_id", ignorenulls=True).alias("person_id"),
            F.max("isresolved_int").alias("isresolved_int"),
        )
        .withColumn(
            "logical_transition_id",
            _stable_id(
                F.col("entity_key"),
                F.col("transition_ts"),
                F.col("country_from"),
                F.col("country_to"),
            ),
        )
        .withColumn(
            "logic_factor",
            F.least(
                F.lit(params.independent_logic_cap),
                F.lit(1.0)
                + F.lit(params.independent_logic_bonus)
                * F.greatest(
                    F.size(F.col("logic_ids")) - F.lit(1), F.lit(0)
                ),
            ),
        )
        .withColumn(
            "transition_evidence_weight",
            F.col("raw_transition_weight") * F.col("logic_factor"),
        )
        .withColumn(
            "details_json",
            F.to_json(
                F.struct(
                    "source_ids",
                    "logic_ids",
                    "silver_event_ids",
                    "event_ids",
                    "source_details",
                )
            ),
        )
        .select(
            "entity_key",
            "person_id",
            F.col("isresolved_int").cast("boolean").alias("isresolved"),
            F.col("logical_transition_id").alias("silver_event_id"),
            F.col("logical_transition_id").alias("event_id"),
            F.concat_ws("|", F.sort_array("source_ids")).alias("source_id"),
            F.concat_ws("|", F.sort_array("logic_ids")).alias("logic_id"),
            F.concat(
                F.lit("LOGICAL_TRANSITION:"), F.col("logical_transition_id")
            ).alias("dependency_group"),
            "transition_ts",
            "country_from",
            "country_to",
            "transition_evidence_weight",
            "sequence_supported",
            "details_json",
        )
    )
    evidence = from_evidence.unionByName(to_evidence).where(
        F.col("evidence_to") > F.col("evidence_from")
    )
    return evidence, detailed


def build_presence_evidence(
    silver: DataFrame,
    runtime: RuntimeConfig,
) -> DataFrame:
    params = runtime.params
    presences = silver.where(F.col("event_type") == "PRESENCE")
    contradiction_counts = presences.groupBy(
        "entity_key", "observation_date", "dependency_group"
    ).agg(
        F.countDistinct("country_code").alias("contradictory_country_count")
    )
    return (
        presences.join(
            contradiction_counts,
            ["entity_key", "observation_date", "dependency_group"],
            "left",
        )
        .withColumn(
            "contradiction_split",
            F.lit(1.0)
            / F.greatest(F.col("contradictory_country_count"), F.lit(1)),
        )
        .withColumn(
            "evidence_type_weight",
            F.when(
                F.col("inference_method") == "INFERRED_BY_GROUP",
                F.lit(params.inferred_group_weight),
            ).otherwise(F.lit(params.presence_weight)),
        )
        .withColumn(
            "base_contribution",
            F.col("source_confidence")
            * _identity_evidence_factor(
                F.col("isresolved"), F.col("identity_confidence")
            )
            * _count_factor(params)
            * F.col("contradiction_split")
            * F.col("evidence_type_weight"),
        )
        .select(
            "entity_key",
            "person_id",
            "isresolved",
            "silver_event_id",
            "event_id",
            "source_id",
            "logic_id",
            "dependency_group",
            "identity_confidence",
            "source_confidence",
            "evidence_count",
            "inference_method",
            "details_json",
            F.col("valid_from").alias("evidence_from"),
            F.col("valid_to").alias("evidence_to"),
            "country_code",
            F.lit("PRESENCE").alias("evidence_kind"),
            F.lit("PRESENCE").alias("event_type"),
            "base_contribution",
            F.lit(False).alias("sequence_supported"),
        )
        .where(
            F.col("country_code").isNotNull()
            & (F.col("evidence_to") > F.col("evidence_from"))
        )
    )


def build_scored_evidence(
    silver: DataFrame,
    runtime: RuntimeConfig,
) -> tuple[DataFrame, DataFrame]:
    presence = build_presence_evidence(silver, runtime)
    transition_evidence, transition_details = build_transition_anchors(
        silver, runtime
    )
    return presence.unionByName(transition_evidence), transition_details


def _explode_evidence_by_day(evidence: DataFrame) -> DataFrame:
    return (
        evidence
        .withColumn(
            "_last_evidence_day",
            F.to_date(
                F.from_unixtime(F.unix_timestamp("evidence_to") - F.lit(1))
            ),
        )
        .withColumn(
            "day",
            F.explode(
                F.sequence(
                    F.to_date("evidence_from"),
                    F.col("_last_evidence_day"),
                    F.expr("INTERVAL 1 DAY"),
                )
            ),
        )
        .withColumn(
            "day_evidence_from",
            F.greatest(F.col("evidence_from"), F.col("day").cast("timestamp")),
        )
        .withColumn(
            "day_evidence_to",
            F.least(
                F.col("evidence_to"),
                F.date_add(F.col("day"), 1).cast("timestamp"),
            ),
        )
        .where(F.col("day_evidence_to") > F.col("day_evidence_from"))
        .drop("_last_evidence_day")
    )


def score_atomic_ranges(
    evidence: DataFrame,
    runtime: RuntimeConfig,
) -> tuple[DataFrame, DataFrame]:
    """Note chaque plage atomique et separe resultat accepte / ambigu.

    L'acceptation n'utilise jamais seulement ``rank == 1``. Le top 1 doit
    depasser le seuil absolu ET la marge sur le top 2, sauf s'il est seul.
    """
    params = runtime.params
    by_day = _explode_evidence_by_day(evidence).persist(StorageLevel.DISK_ONLY)
    try:
        cuts = (
            by_day.select(
                "entity_key", "day", F.col("day_evidence_from").alias("cut")
            )
            .unionByName(
                by_day.select(
                    "entity_key", "day", F.col("day_evidence_to").alias("cut")
                )
            )
            .distinct()
        )
        cut_order = Window.partitionBy("entity_key", "day").orderBy("cut")
        atomic = (
            cuts
            .withColumn("range_to", F.lead("cut").over(cut_order))
            .withColumnRenamed("cut", "range_from")
            .where(
                F.col("range_to").isNotNull()
                & (F.col("range_to") > F.col("range_from"))
            )
        )

        active = (
            atomic.alias("r")
            .join(by_day.alias("e"), ["entity_key", "day"], "inner")
            .where(
                (F.col("e.day_evidence_from") < F.col("r.range_to"))
                & (F.col("e.day_evidence_to") > F.col("r.range_from"))
            )
        )

        support_struct = F.struct(
            F.col("silver_event_id").alias("silver_event_id"),
            F.col("event_id").alias("event_id"),
            F.col("source_id").alias("source_id"),
            F.col("logic_id").alias("logic_id"),
            F.col("dependency_group").alias("dependency_group"),
            F.col("event_type").alias("event_type"),
            F.col("evidence_kind").alias("evidence_kind"),
            F.col("inference_method").alias("inference_method"),
            F.col("base_contribution").cast("double").alias("contribution"),
            F.col("details_json").alias("details_json"),
        )

        # Une realite decrite deux fois dans le meme dependency_group ne compte
        # qu'une fois : on prend le meilleur support du groupe, sans perdre la
        # liste des preuves pour l'explication.
        dependency_scores = (
            active
            .groupBy(
                "entity_key",
                "day",
                "range_from",
                "range_to",
                "country_code",
                "dependency_group",
            )
            .agg(
                F.max("base_contribution").alias("dependency_score"),
                F.max(F.col("sequence_supported").cast("int")).alias(
                    "has_transition_sequence"
                ),
                F.max("identity_confidence").alias("identity_score"),
                F.sum("evidence_count").alias("evidence_count"),
                F.collect_set("source_id").alias("source_ids"),
                F.collect_set("logic_id").alias("logic_ids"),
                F.collect_set("event_type").alias("event_types"),
                F.collect_set("silver_event_id").alias("support_event_ids"),
                F.collect_list(support_struct).alias("supports"),
                F.first("person_id", ignorenulls=True).alias("person_id"),
                F.max(F.col("isresolved").cast("int")).alias("isresolved_int"),
            )
        )

        country_scores_base = (
            dependency_scores
            .groupBy(
                "entity_key",
                "day",
                "range_from",
                "range_to",
                "country_code",
            )
            .agg(
                F.sum("dependency_score").alias("raw_score"),
                F.max("has_transition_sequence").alias("has_transition_sequence"),
                F.max("identity_score").alias("identity_score"),
                F.sum("evidence_count").alias("evidence_count"),
                F.array_distinct(F.flatten(F.collect_list("source_ids"))).alias(
                    "source_ids"
                ),
                F.array_distinct(F.flatten(F.collect_list("logic_ids"))).alias(
                    "logic_ids"
                ),
                F.array_distinct(F.flatten(F.collect_list("event_types"))).alias(
                    "event_types"
                ),
                F.array_distinct(
                    F.flatten(F.collect_list("support_event_ids"))
                ).alias("support_event_ids"),
                F.flatten(F.collect_list("supports")).alias("supports"),
                F.first("person_id", ignorenulls=True).alias("person_id"),
                F.max("isresolved_int").alias("isresolved_int"),
                F.countDistinct("dependency_group").alias("dependency_count"),
                F.size(
                    F.array_distinct(F.flatten(F.collect_list("logic_ids")))
                ).alias("logic_count"),
            )
            .withColumn(
                "logic_factor",
                F.least(
                    F.lit(params.independent_logic_cap),
                    F.lit(1.0)
                    + F.lit(params.independent_logic_bonus)
                    * F.greatest(F.col("logic_count") - F.lit(1), F.lit(0)),
                ),
            )
        )

        country_days = country_scores_base.select(
            "entity_key", "day", "country_code"
        ).distinct()
        recurrence_order = Window.partitionBy(
            "entity_key", "country_code"
        ).orderBy("day")
        recurrence = (
            country_days
            .withColumn("previous_same_country_day", F.lag("day").over(recurrence_order))
            .withColumn("next_same_country_day", F.lead("day").over(recurrence_order))
            .withColumn(
                "is_recurrent_country",
                (
                    F.datediff("day", "previous_same_country_day")
                    <= F.lit(params.recurrence_window_days)
                )
                | (
                    F.datediff("next_same_country_day", "day")
                    <= F.lit(params.recurrence_window_days)
                ),
            )
            .select("entity_key", "day", "country_code", "is_recurrent_country")
        )
        country_scores = (
            country_scores_base
            .join(recurrence, ["entity_key", "day", "country_code"], "left")
            .withColumn(
                "coherence_factor",
                F.when(
                    F.coalesce(F.col("is_recurrent_country"), F.lit(False)),
                    F.lit(params.recurrent_evidence_bonus),
                ).otherwise(F.lit(params.isolated_evidence_factor)),
            )
            .withColumn(
                "candidate_score",
                F.col("raw_score")
                * F.col("logic_factor")
                * F.col("coherence_factor"),
            )
        )

        candidate_window = Window.partitionBy(
            "entity_key", "day", "range_from", "range_to"
        )
        rank_window = candidate_window.orderBy(
            F.col("candidate_score").desc(),
            F.col("has_transition_sequence").desc(),
            F.col("country_code").asc(),
        )
        ranked = (
            country_scores
            .withColumn("candidate_total_score", F.sum("candidate_score").over(candidate_window))
            .withColumn(
                "probability",
                F.when(
                    F.col("candidate_total_score") > 0,
                    F.col("candidate_score") / F.col("candidate_total_score"),
                ).otherwise(F.lit(0.0)),
            )
            .withColumn("candidate_count", F.count(F.lit(1)).over(candidate_window))
            .withColumn("candidate_rank", F.row_number().over(rank_window))
        )

        alternatives = (
            ranked
            .groupBy("entity_key", "day", "range_from", "range_to")
            .agg(
                F.sort_array(
                    F.collect_list(
                        F.struct(
                            "candidate_rank",
                            "country_code",
                            "candidate_score",
                            "probability",
                            "source_ids",
                            "logic_ids",
                            "event_types",
                            "support_event_ids",
                        )
                    )
                ).alias("candidate_options")
            )
        )

        top = (
            ranked.where(F.col("candidate_rank") == 1)
            .join(
                ranked.where(F.col("candidate_rank") == 2).select(
                    "entity_key",
                    "day",
                    "range_from",
                    "range_to",
                    F.col("probability").alias("runner_up_probability"),
                ),
                ["entity_key", "day", "range_from", "range_to"],
                "left",
            )
            .join(
                alternatives,
                ["entity_key", "day", "range_from", "range_to"],
                "left",
            )
            .fillna({"runner_up_probability": 0.0})
            .withColumn(
                "probability_margin",
                F.col("probability") - F.col("runner_up_probability"),
            )
            .withColumn(
                "is_accepted",
                (F.col("probability") >= F.lit(params.observed_min_share))
                & (
                    (F.col("candidate_count") == 1)
                    | (F.col("probability_margin") >= F.lit(params.observed_min_margin))
                    | (F.col("has_transition_sequence") == 1)
                ),
            )
            .withColumn(
                "range_id",
                _stable_id(
                    F.col("entity_key"),
                    F.col("range_from"),
                    F.col("range_to"),
                    F.col("country_code"),
                    F.lit("OBSERVED"),
                ),
            )
            .withColumn("isresolved", F.col("isresolved_int").cast("boolean"))
            .withColumn("adjusted_score", F.col("candidate_score"))
            .withColumn("candidate_share", F.col("probability"))
            .withColumn("base_resolution_method", F.lit("EVIDENCE_SCORING"))
            .withColumn("temporal_operation", F.lit("NONE"))
            .withColumn("is_temporally_inferred", F.lit(False))
            .withColumn("gap_id", F.lit(None).cast("string"))
            .withColumn("supports_json", F.to_json("supports"))
            .withColumn("alternatives_json", F.to_json("candidate_options"))
        )
        return top.where("is_accepted"), top.where("NOT is_accepted")
    finally:
        by_day.unpersist()


# ---------------------------------------------------------------------------
# Gold : trous temporels et candidats
# ---------------------------------------------------------------------------


def fill_temporal_gaps(
    resolved_ranges: DataFrame,
    ambiguous_ranges: DataFrame,
    transition_details: DataFrame,
    runtime: RuntimeConfig,
) -> tuple[DataFrame, DataFrame]:
    """Complete uniquement les trous demontres et retourne les autres a part.

    Regles fortes :

    * A ... A sans transition ni preuve ambiguë -> continuite A ;
    * A ... transitions formant une chaine A->...->B ... B -> ranges coupes
      aux timestamps des transitions ;
    * tout le reste reste dans ``unresolved_gaps``.

    Les transitions concretes restent dans le contrat du gap et ne sont plus
    perdues avant ``build_gap_candidates``.
    """
    required = {
        "entity_key",
        "range_id",
        "range_from",
        "range_to",
        "country_code",
        "adjusted_score",
        "candidate_share",
    }
    _require_columns(resolved_ranges, required, "resolved_ranges")

    order = Window.partitionBy("entity_key").orderBy(
        "range_from", "range_to", "range_id"
    )
    neighbours = (
        resolved_ranges
        .withColumn("_next_range_id", F.lead("range_id").over(order))
        .withColumn("_next_range_from", F.lead("range_from").over(order))
        .withColumn("_next_country", F.lead("country_code").over(order))
        .withColumn("_next_score", F.lead("adjusted_score").over(order))
        .withColumn("_next_share", F.lead("candidate_share").over(order))
        .withColumn("_next_identity", F.lead("identity_score").over(order))
        .withColumn("_next_person", F.lead("person_id").over(order))
        .withColumn("_next_isresolved", F.lead("isresolved").over(order))
    )
    gaps = (
        neighbours
        .where(F.col("_next_range_from") > F.col("range_to"))
        .select(
            "entity_key",
            F.coalesce(F.col("person_id"), F.col("_next_person")).alias("person_id"),
            (F.col("isresolved") | F.col("_next_isresolved")).alias("isresolved"),
            _stable_id(
                F.col("entity_key"), F.col("range_to"), F.col("_next_range_from")
            ).alias("gap_id"),
            F.col("range_to").alias("gap_from"),
            F.col("_next_range_from").alias("gap_to"),
            F.col("range_id").alias("range_id_before"),
            F.col("_next_range_id").alias("range_id_after"),
            F.col("country_code").alias("country_before"),
            F.col("_next_country").alias("country_after"),
            F.col("adjusted_score").cast("double").alias("score_before"),
            F.col("_next_score").cast("double").alias("score_after"),
            F.col("candidate_share").cast("double").alias("share_before"),
            F.col("_next_share").cast("double").alias("share_after"),
            F.greatest(F.col("identity_score"), F.col("_next_identity")).alias(
                "identity_score"
            ),
        )
        .withColumn(
            "gap_duration_seconds",
            F.unix_timestamp("gap_to") - F.unix_timestamp("gap_from"),
        )
        .withColumn(
            "gap_duration_days", F.col("gap_duration_seconds") / F.lit(86400.0)
        )
    )

    blocked = (
        gaps.alias("g")
        .join(
            ambiguous_ranges.select("entity_key", "range_from", "range_to").alias("a"),
            (F.col("g.entity_key") == F.col("a.entity_key"))
            & (F.col("a.range_from") < F.col("g.gap_to"))
            & (F.col("a.range_to") > F.col("g.gap_from")),
            "left",
        )
        .groupBy(F.col("g.gap_id").alias("gap_id"))
        .agg(F.max(F.col("a.range_from").isNotNull().cast("int")).alias("has_ambiguous_evidence"))
    )

    transition_struct = F.struct(
        F.col("t.transition_ts").alias("transition_ts"),
        F.col("t.country_from").alias("country_from"),
        F.col("t.country_to").alias("country_to"),
        F.col("t.transition_evidence_weight").cast("double").alias("evidence_weight"),
        F.col("t.silver_event_id").alias("silver_event_id"),
        F.col("t.source_id").alias("source_id"),
        F.col("t.logic_id").alias("logic_id"),
        F.col("t.sequence_supported").alias("sequence_supported"),
        F.col("t.details_json").alias("details_json"),
    )
    transition_stats = (
        gaps.alias("g")
        .join(
            transition_details.dropDuplicates(["silver_event_id"]).alias("t"),
            (F.col("g.entity_key") == F.col("t.entity_key"))
            & (F.col("t.transition_ts") >= F.col("g.gap_from"))
            & (F.col("t.transition_ts") <= F.col("g.gap_to")),
            "left",
        )
        .groupBy(F.col("g.gap_id").alias("gap_id"))
        .agg(
            F.countDistinct("t.silver_event_id").alias("transition_count"),
            F.sort_array(
                F.collect_list(
                    F.when(F.col("t.silver_event_id").isNotNull(), transition_struct)
                )
            ).alias("transition_evidence"),
        )
    )

    classified = (
        gaps.join(blocked, "gap_id", "left")
        .join(transition_stats, "gap_id", "left")
        .fillna({"has_ambiguous_evidence": 0, "transition_count": 0})
        .withColumn(
            "is_compatible_transition_chain",
            F.expr(
                """
                CASE
                  WHEN transition_count = 0 THEN false
                  WHEN element_at(transition_evidence, 1).country_from <> country_before THEN false
                  WHEN element_at(transition_evidence, -1).country_to <> country_after THEN false
                  WHEN size(transition_evidence) = 1 THEN true
                  ELSE aggregate(
                    sequence(2, size(transition_evidence)),
                    true,
                    (ok, i) -> ok AND
                      element_at(transition_evidence, i - 1).country_to =
                      element_at(transition_evidence, i).country_from
                  )
                END
                """
            ),
        )
    )

    same_country_condition = (
        (F.col("country_before") == F.col("country_after"))
        & (F.col("transition_count") == 0)
        & (F.col("has_ambiguous_evidence") == 0)
    )
    chain_condition = (
        F.col("is_compatible_transition_chain")
        & (F.col("has_ambiguous_evidence") == 0)
    )

    same_country = (
        classified.where(same_country_condition)
        .select(
            "entity_key",
            "person_id",
            "isresolved",
            _stable_id(F.col("gap_id"), F.lit("SAME_COUNTRY")).alias("range_id"),
            F.col("gap_from").alias("range_from"),
            F.col("gap_to").alias("range_to"),
            F.col("country_before").alias("country_code"),
            F.least("score_before", "score_after").alias("adjusted_score"),
            F.least("share_before", "share_after").alias("candidate_share"),
            F.lit(1).alias("candidate_count"),
            F.lit(1).alias("candidate_rank"),
            "identity_score",
            F.lit(0).cast("long").alias("evidence_count"),
            F.expr("cast(array() as array<string>)").alias("source_ids"),
            F.expr("cast(array() as array<string>)").alias("logic_ids"),
            F.expr("cast(array() as array<string>)").alias("event_types"),
            F.expr("cast(array() as array<string>)").alias("support_event_ids"),
            F.lit("[]").alias("supports_json"),
            F.lit("[]").alias("alternatives_json"),
            F.lit("TEMPORAL_CONTINUITY").alias("base_resolution_method"),
            F.lit("SAME_COUNTRY_FILL").alias("temporal_operation"),
            F.lit(True).alias("is_temporally_inferred"),
            "gap_id",
        )
    )

    chain_prepared = (
        classified.where(chain_condition)
        .withColumn(
            "_chain_boundaries",
            F.expr(
                "concat(array(gap_from), "
                "transform(transition_evidence, x -> x.transition_ts), array(gap_to))"
            ),
        )
        .withColumn(
            "_chain_countries",
            F.expr(
                "concat(array(country_before), "
                "transform(transition_evidence, x -> x.country_to))"
            ),
        )
        .selectExpr("*", "posexplode(_chain_countries) as (chain_pos, country_code)")
        .withColumn(
            "range_from",
            F.expr("element_at(_chain_boundaries, chain_pos + 1)"),
        )
        .withColumn(
            "range_to",
            F.expr("element_at(_chain_boundaries, chain_pos + 2)"),
        )
        .where(F.col("range_to") > F.col("range_from"))
    )
    chain_ranges = chain_prepared.select(
        "entity_key",
        "person_id",
        "isresolved",
        _stable_id(
            F.col("gap_id"), F.col("chain_pos"), F.col("country_code")
        ).alias("range_id"),
        "range_from",
        "range_to",
        "country_code",
        F.when(F.col("chain_pos") == 0, F.col("score_before"))
        .when(F.col("chain_pos") == F.col("transition_count"), F.col("score_after"))
        .otherwise(
            F.expr(
                "element_at(transition_evidence, chain_pos).evidence_weight"
            )
        )
        .alias("adjusted_score"),
        F.when(F.col("chain_pos") == 0, F.col("share_before"))
        .when(F.col("chain_pos") == F.col("transition_count"), F.col("share_after"))
        .otherwise(F.lit(1.0))
        .alias("candidate_share"),
        F.lit(1).alias("candidate_count"),
        F.lit(1).alias("candidate_rank"),
        "identity_score",
        F.lit(1).cast("long").alias("evidence_count"),
        F.expr("transform(transition_evidence, x -> x.source_id)").alias("source_ids"),
        F.expr("transform(transition_evidence, x -> x.logic_id)").alias("logic_ids"),
        F.array(F.lit("TRANSITION")).alias("event_types"),
        F.expr("transform(transition_evidence, x -> x.silver_event_id)").alias(
            "support_event_ids"
        ),
        F.to_json("transition_evidence").alias("supports_json"),
        F.lit("[]").alias("alternatives_json"),
        F.lit("TRANSITION_CHAIN").alias("base_resolution_method"),
        F.lit("TRANSITION_CHAIN_FILL").alias("temporal_operation"),
        F.lit(True).alias("is_temporally_inferred"),
        "gap_id",
    )

    originals = (
        resolved_ranges
        .withColumn("gap_id", F.lit(None).cast("string"))
        .withColumn(
            "temporal_operation",
            F.coalesce(F.col("temporal_operation"), F.lit("NONE")),
        )
    )
    filled_ranges = _union_all([originals, same_country, chain_ranges])

    regular_unresolved = (
        classified.where(~same_country_condition & ~chain_condition)
        .withColumn(
            "unresolved_reason",
            F.when(
                F.col("has_ambiguous_evidence") == 1,
                F.lit("AMBIGUOUS_EVIDENCE_INSIDE_GAP"),
            )
            .when(
                (F.col("country_before") == F.col("country_after"))
                & (F.col("transition_count") > 0),
                F.lit("TRANSITION_INSIDE_SAME_COUNTRY_GAP"),
            )
            .when(F.col("transition_count") == 0, F.lit("NO_TRANSITION"))
            .otherwise(F.lit("INCOMPATIBLE_OR_AMBIGUOUS_TRANSITION_CHAIN")),
        )
        .withColumn("precomputed_candidates_json", F.lit(None).cast("string"))
    )

    transition_array_type = transition_stats.schema["transition_evidence"].dataType
    ambiguous_as_gaps = ambiguous_ranges.select(
        "entity_key",
        "person_id",
        "isresolved",
        _stable_id(
            F.col("entity_key"),
            F.col("range_from"),
            F.col("range_to"),
            F.lit("AMBIGUOUS_OBSERVED"),
        ).alias("gap_id"),
        F.col("range_from").alias("gap_from"),
        F.col("range_to").alias("gap_to"),
        F.lit(None).cast("string").alias("range_id_before"),
        F.lit(None).cast("string").alias("range_id_after"),
        F.lit(None).cast("string").alias("country_before"),
        F.lit(None).cast("string").alias("country_after"),
        F.lit(None).cast("double").alias("score_before"),
        F.lit(None).cast("double").alias("score_after"),
        F.lit(None).cast("double").alias("share_before"),
        F.lit(None).cast("double").alias("share_after"),
        "identity_score",
        (
            F.unix_timestamp("range_to") - F.unix_timestamp("range_from")
        ).alias("gap_duration_seconds"),
        (
            (F.unix_timestamp("range_to") - F.unix_timestamp("range_from"))
            / F.lit(86400.0)
        ).alias("gap_duration_days"),
        F.lit(0).cast("int").alias("has_ambiguous_evidence"),
        F.lit(0).cast("long").alias("transition_count"),
        F.lit(None).cast(transition_array_type).alias("transition_evidence"),
        F.lit(False).alias("is_compatible_transition_chain"),
        F.lit("AMBIGUOUS_OBSERVED_RANGE").alias("unresolved_reason"),
        F.col("alternatives_json").alias("precomputed_candidates_json"),
    )
    unresolved_gaps = regular_unresolved.unionByName(
        ambiguous_as_gaps, allowMissingColumns=True
    )
    return filled_ranges, unresolved_gaps


def build_gap_candidates(
    unresolved_gaps: DataFrame,
    runtime: RuntimeConfig,
) -> DataFrame:
    """Construit et accepte eventuellement des hypotheses dans les vrais gaps.

    Les pays portes par les transitions internes sont ajoutes aux deux pays de
    bord. Un top 1 reste une hypothese tant que ``is_accepted`` vaut False.
    """
    params = runtime.params
    day_slices = (
        unresolved_gaps
        .where(
            (F.col("unresolved_reason") != "AMBIGUOUS_OBSERVED_RANGE")
            & (F.col("gap_duration_seconds") > 0)
            & (F.col("gap_duration_days") <= F.lit(params.max_inference_gap_days))
        )
        .withColumn(
            "_last_gap_day",
            F.to_date(F.from_unixtime(F.unix_timestamp("gap_to") - F.lit(1))),
        )
        .withColumn(
            "slice_day",
            F.explode(
                F.sequence(
                    F.to_date("gap_from"),
                    F.col("_last_gap_day"),
                    F.expr("INTERVAL 1 DAY"),
                )
            ),
        )
        .withColumn(
            "_day_range_from",
            F.greatest(F.col("gap_from"), F.col("slice_day").cast("timestamp")),
        )
        .withColumn(
            "_day_range_to",
            F.least(
                F.col("gap_to"), F.date_add(F.col("slice_day"), 1).cast("timestamp")
            ),
        )
        .withColumn(
            "_slice_cuts",
            F.expr(
                """
                array_distinct(concat(
                  array(_day_range_from, _day_range_to),
                  coalesce(
                    transform(
                      filter(
                        transition_evidence,
                        x -> x.transition_ts > _day_range_from
                          AND x.transition_ts < _day_range_to
                      ),
                      x -> x.transition_ts
                    ),
                    cast(array() as array<timestamp>)
                  )
                ))
                """
            ),
        )
        .withColumn("_cut", F.explode("_slice_cuts"))
    )
    cut_order = Window.partitionBy("entity_key", "gap_id", "slice_day").orderBy(
        "_cut"
    )
    eligible = (
        day_slices
        .withColumn("range_from", F.col("_cut"))
        .withColumn("range_to", F.lead("_cut").over(cut_order))
        .where(
            F.col("range_to").isNotNull()
            & (F.col("range_to") > F.col("range_from"))
        )
        .withColumn(
            "slice_mid_epoch",
            (F.unix_timestamp("range_from") + F.unix_timestamp("range_to"))
            / F.lit(2.0),
        )
    )
    common = [
        "entity_key",
        "person_id",
        "isresolved",
        "gap_id",
        "gap_from",
        "gap_to",
        "range_from",
        "range_to",
        "identity_score",
        "unresolved_reason",
    ]
    before = eligible.select(
        *common,
        F.col("country_before").alias("country_code"),
        (
            F.col("score_before")
            * F.exp(
                -(
                    (F.col("slice_mid_epoch") - F.unix_timestamp("gap_from"))
                    / F.lit(86400.0)
                )
                / F.lit(params.time_decay_days)
            )
        ).alias("candidate_score"),
        F.array(F.lit("BEFORE")).alias("supported_by"),
        F.expr("cast(array() as array<string>)").alias("source_ids"),
        F.expr("cast(array() as array<string>)").alias("logic_ids"),
        F.expr("cast(array() as array<string>)").alias("support_event_ids"),
    )
    after = eligible.select(
        *common,
        F.col("country_after").alias("country_code"),
        (
            F.col("score_after")
            * F.exp(
                -(
                    (F.unix_timestamp("gap_to") - F.col("slice_mid_epoch"))
                    / F.lit(86400.0)
                )
                / F.lit(params.time_decay_days)
            )
        ).alias("candidate_score"),
        F.array(F.lit("AFTER")).alias("supported_by"),
        F.expr("cast(array() as array<string>)").alias("source_ids"),
        F.expr("cast(array() as array<string>)").alias("logic_ids"),
        F.expr("cast(array() as array<string>)").alias("support_event_ids"),
    )

    transition = (
        eligible
        .withColumn("transition", F.explode_outer("transition_evidence"))
        .where(F.col("transition.silver_event_id").isNotNull())
        .select(
            *common,
            F.when(
                F.col("slice_mid_epoch") < F.unix_timestamp("transition.transition_ts"),
                F.col("transition.country_from"),
            ).otherwise(F.col("transition.country_to")).alias("country_code"),
            (
                F.col("transition.evidence_weight")
                * F.exp(
                    -F.abs(
                        F.col("slice_mid_epoch")
                        - F.unix_timestamp("transition.transition_ts")
                    )
                    / F.lit(86400.0 * params.time_decay_days)
                )
            ).alias("candidate_score"),
            F.array(F.lit("TRANSITION")).alias("supported_by"),
            F.array(F.col("transition.source_id")).alias("source_ids"),
            F.array(F.col("transition.logic_id")).alias("logic_ids"),
            F.array(F.col("transition.silver_event_id")).alias("support_event_ids"),
        )
    )

    raw = _union_all([before, after, transition]).where(
        F.col("country_code").isNotNull()
        & F.col("candidate_score").isNotNull()
        & (F.col("candidate_score") > 0)
    )
    candidates = (
        raw.groupBy(
            "entity_key",
            "person_id",
            "isresolved",
            "gap_id",
            "gap_from",
            "gap_to",
            "range_from",
            "range_to",
            "identity_score",
            "unresolved_reason",
            "country_code",
        )
        .agg(
            F.sum("candidate_score").alias("candidate_score"),
            F.array_distinct(F.flatten(F.collect_list("supported_by"))).alias(
                "supported_by"
            ),
            F.array_distinct(F.flatten(F.collect_list("source_ids"))).alias("source_ids"),
            F.array_distinct(F.flatten(F.collect_list("logic_ids"))).alias("logic_ids"),
            F.array_distinct(F.flatten(F.collect_list("support_event_ids"))).alias(
                "support_event_ids"
            ),
        )
    )
    group_window = Window.partitionBy(
        "entity_key", "gap_id", "range_from", "range_to"
    )
    rank_window = group_window.orderBy(
        F.col("candidate_score").desc(), F.col("country_code").asc()
    )
    ranked = (
        candidates
        .withColumn("candidate_total_score", F.sum("candidate_score").over(group_window))
        .withColumn(
            "candidate_share", F.col("candidate_score") / F.col("candidate_total_score")
        )
        .withColumn("candidate_count", F.count(F.lit(1)).over(group_window))
        .withColumn("candidate_rank", F.row_number().over(rank_window))
    )
    runner_up = ranked.where("candidate_rank = 2").select(
        "entity_key",
        "gap_id",
        "range_from",
        "range_to",
        F.col("candidate_share").alias("runner_up_share"),
    )
    return (
        ranked
        .join(
            runner_up,
            ["entity_key", "gap_id", "range_from", "range_to"],
            "left",
        )
        .fillna({"runner_up_share": 0.0})
        .withColumn(
            "candidate_margin", F.col("candidate_share") - F.col("runner_up_share")
        )
        .withColumn(
            "is_accepted",
            (F.col("candidate_rank") == 1)
            & (F.col("candidate_share") >= F.lit(params.gap_min_share))
            & (
                (F.col("candidate_count") == 1)
                | (F.col("candidate_margin") >= F.lit(params.gap_min_margin))
            ),
        )
        .withColumn(
            "range_id",
            _stable_id(
                F.col("gap_id"),
                F.col("range_from"),
                F.col("range_to"),
                F.col("country_code"),
            ),
        )
        .withColumn("adjusted_score", F.col("candidate_score"))
        .withColumn("probability", F.col("candidate_share"))
        .withColumn("evidence_count", F.lit(0).cast("long"))
        .withColumn("event_types", F.array(F.lit("TEMPORAL_INFERENCE")))
        .withColumn("base_resolution_method", F.lit("GAP_CANDIDATE"))
        .withColumn("temporal_operation", F.lit("TIME_DECAY"))
        .withColumn("is_temporally_inferred", F.lit(True))
        .withColumn("supports_json", F.to_json("supported_by"))
        .withColumn("alternatives_json", F.lit(None).cast("string"))
    )


# ---------------------------------------------------------------------------
# Gold : construction des location segments
# ---------------------------------------------------------------------------


def build_location_segments(
    filled_ranges: DataFrame,
    gap_candidates: DataFrame,
    runtime: RuntimeConfig,
    context: RunContext,
) -> dict[str, DataFrame]:
    """Construit les segments, sans promouvoir automatiquement le rank 1.

    Cette fonction reste l'etape metier qui transforme les ranges en sejours.
    Seuls les candidats dont ``is_accepted`` vaut True rejoignent la timeline.
    La fin maximale cumulee remplace le simple ``lag(range_to)`` afin de ne pas
    couper a tort un ilot lorsque des ranges du meme pays sont imbriques.
    """
    _require_columns(
        filled_ranges,
        ["entity_key", "range_id", "range_from", "range_to", "country_code"],
        "filled_ranges",
    )
    _require_columns(
        gap_candidates,
        [
            "entity_key",
            "range_id",
            "range_from",
            "range_to",
            "country_code",
            "candidate_rank",
            "candidate_share",
            "is_accepted",
        ],
        "gap_candidates",
    )

    gap_options = (
        gap_candidates
        .groupBy("entity_key", "gap_id", "range_from", "range_to")
        .agg(
            F.sort_array(
                F.collect_list(
                    F.struct(
                        "candidate_rank",
                        "country_code",
                        "candidate_score",
                        "candidate_share",
                        "candidate_margin",
                        "supported_by",
                        "source_ids",
                        "logic_ids",
                        "support_event_ids",
                        "is_accepted",
                    )
                )
            ).alias("_gap_options")
        )
        .withColumn("_gap_options_json", F.to_json("_gap_options"))
    )
    accepted_gap_ranges = (
        gap_candidates.where(F.col("is_accepted"))
        .join(
            gap_options.select(
                "entity_key", "gap_id", "range_from", "range_to", "_gap_options_json"
            ),
            ["entity_key", "gap_id", "range_from", "range_to"],
            "left",
        )
        .withColumn("alternatives_json", F.col("_gap_options_json"))
        .drop("_gap_options_json")
    )
    primary_timeline = filled_ranges.unionByName(
        accepted_gap_ranges, allowMissingColumns=True
    )

    prepared = (
        primary_timeline
        .withColumn(
            "_duration_seconds",
            F.unix_timestamp("range_to") - F.unix_timestamp("range_from"),
        )
        .withColumn(
            "_probability",
            F.coalesce(
                _column_or_null(primary_timeline, "probability", "double"),
                _column_or_null(primary_timeline, "candidate_share", "double"),
                F.lit(0.0),
            ),
        )
        .withColumn(
            "_identity_score",
            _column_or_value(primary_timeline, "identity_score", 0.0, "double"),
        )
        .withColumn(
            "_evidence_count",
            _column_or_value(primary_timeline, "evidence_count", 0, "long"),
        )
        .withColumn(
            "_source_ids", _array_or_empty(primary_timeline, "source_ids")
        )
        .withColumn(
            "_logic_ids", _array_or_empty(primary_timeline, "logic_ids")
        )
        .withColumn(
            "_event_types", _array_or_empty(primary_timeline, "event_types")
        )
        .withColumn(
            "_support_event_ids",
            _array_or_empty(primary_timeline, "support_event_ids"),
        )
        .withColumn(
            "_is_temporally_inferred",
            _column_or_value(
                primary_timeline, "is_temporally_inferred", False, "boolean"
            ),
        )
        .withColumn(
            "_candidate_count",
            _column_or_value(primary_timeline, "candidate_count", 1, "long"),
        )
        .withColumn(
            "_base_resolution_method",
            F.coalesce(
                _column_or_null(
                    primary_timeline, "base_resolution_method", "string"
                ),
                F.lit("UNKNOWN"),
            ),
        )
        .withColumn(
            "_temporal_operation",
            F.coalesce(
                _column_or_null(primary_timeline, "temporal_operation", "string"),
                F.lit("NONE"),
            ),
        )
        .withColumn(
            "_supports_json",
            _column_or_null(primary_timeline, "supports_json", "string"),
        )
        .withColumn(
            "_alternatives_json",
            _column_or_null(primary_timeline, "alternatives_json", "string"),
        )
        .withColumn(
            "_person_id", _column_or_null(primary_timeline, "person_id", "string")
        )
        .withColumn(
            "_isresolved",
            _column_or_value(primary_timeline, "isresolved", False, "boolean"),
        )
    )

    invalid_condition = (
        F.col("entity_key").isNull()
        | F.col("range_id").isNull()
        | F.col("range_from").isNull()
        | F.col("range_to").isNull()
        | F.col("country_code").isNull()
        | (F.col("range_to") <= F.col("range_from"))
    )
    invalid_ranges = prepared.where(invalid_condition).withColumn(
        "invalid_reason",
        F.when(F.col("entity_key").isNull(), F.lit("NULL_ENTITY_KEY"))
        .when(F.col("range_id").isNull(), F.lit("NULL_RANGE_ID"))
        .when(F.col("range_from").isNull(), F.lit("NULL_RANGE_FROM"))
        .when(F.col("range_to").isNull(), F.lit("NULL_RANGE_TO"))
        .when(F.col("country_code").isNull(), F.lit("NULL_COUNTRY_CODE"))
        .otherwise(F.lit("NON_POSITIVE_DURATION")),
    )
    valid = prepared.where(~invalid_condition)

    order = Window.partitionBy("entity_key").orderBy(
        "range_from", "range_to", "range_id"
    )
    prior_rows = order.rowsBetween(Window.unboundedPreceding, -1)
    ordered = (
        valid
        .withColumn("_previous_country", F.lag("country_code").over(order))
        .withColumn("_previous_range_to", F.lag("range_to").over(order))
        .withColumn("_previous_coverage_end", F.max("range_to").over(prior_rows))
    )
    overlap_anomalies = (
        ordered
        .where(
            F.col("_previous_coverage_end").isNotNull()
            & (F.col("range_from") < F.col("_previous_coverage_end"))
        )
        .withColumn(
            "overlap_type",
            F.when(
                F.col("country_code") == F.col("_previous_country"),
                F.lit("SAME_COUNTRY_OVERLAP"),
            ).otherwise(F.lit("DIFFERENT_COUNTRY_OVERLAP")),
        )
    )

    segmented = (
        ordered
        .withColumn(
            "_starts_new_segment",
            F.when(F.col("_previous_coverage_end").isNull(), F.lit(1))
            .when(F.col("country_code") != F.col("_previous_country"), F.lit(1))
            .when(F.col("range_from") > F.col("_previous_coverage_end"), F.lit(1))
            .otherwise(F.lit(0)),
        )
        .withColumn(
            "_segment_seq",
            F.sum("_starts_new_segment").over(
                order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .withColumn(
            "_range_details",
            F.struct(
                "range_from",
                "range_to",
                "range_id",
                "gap_id",
                "country_code",
                F.col("_probability").alias("probability"),
                F.col("_identity_score").alias("identity_score"),
                F.col("_base_resolution_method").alias("base_resolution_method"),
                F.col("_temporal_operation").alias("temporal_operation"),
                F.col("_is_temporally_inferred").alias("is_temporally_inferred"),
                F.col("_supports_json").alias("supports_json"),
                F.col("_alternatives_json").alias("alternatives_json"),
            ),
        )
    )

    aggregates = (
        segmented
        .groupBy("entity_key", "_segment_seq", "country_code")
        .agg(
            F.first("_person_id", ignorenulls=True).alias("person_id"),
            F.max(F.col("_isresolved").cast("int")).cast("boolean").alias("isresolved"),
            F.min("range_from").alias("segment_from"),
            F.max("range_to").alias("segment_to"),
            F.sum(F.col("_probability") * F.col("_duration_seconds")).alias(
                "_weighted_probability_sum"
            ),
            F.sum("_duration_seconds").alias("_weighted_probability_duration"),
            F.max("_identity_score").alias("identity_score"),
            F.sum("_evidence_count").cast("long").alias("evidence_count"),
            F.array_distinct(F.flatten(F.collect_list("_source_ids"))).alias("source_ids"),
            F.array_distinct(F.flatten(F.collect_list("_logic_ids"))).alias("logic_ids"),
            F.array_distinct(F.flatten(F.collect_list("_event_types"))).alias(
                "event_types"
            ),
            F.collect_list("range_id").alias("range_ids"),
            F.max(F.col("_is_temporally_inferred").cast("int")).cast("boolean").alias(
                "has_temporal_inference"
            ),
            F.max((F.col("_candidate_count") > 1).cast("int")).cast("boolean").alias(
                "has_unresolved_alternatives"
            ),
            F.sort_array(
                F.collect_set(
                    F.concat_ws(
                        ":", "_base_resolution_method", "_temporal_operation"
                    )
                )
            ).alias("resolution_methods"),
            F.sort_array(F.collect_list("_range_details")).alias("range_details"),
        )
        .withColumn(
            "probability",
            F.when(
                F.col("_weighted_probability_duration") > 0,
                F.col("_weighted_probability_sum")
                / F.col("_weighted_probability_duration"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "segment_id",
            _stable_id(
                F.col("entity_key"),
                F.col("country_code"),
                F.col("segment_from"),
                F.col("segment_to"),
            ),
        )
        .withColumn(
            "details_json",
            F.to_json(
                F.struct(
                    "range_details",
                    "source_ids",
                    "logic_ids",
                    "event_types",
                    "resolution_methods",
                )
            ),
        )
        .withColumn("probability_is_calibrated", F.lit(False))
        .withColumn("algorithm_version", F.lit(runtime.params.algorithm_version))
        .withColumn("inference_run_id", F.lit(context.run_id))
        .withColumn(
            "input_cutoff",
            F.lit(context.input_cutoff or datetime.utcnow()).cast("timestamp"),
        )
        .withColumn("segment_month", F.date_format("segment_from", "yyyy-MM"))
        .withColumn(
            "entity_bucket",
            F.when(
                F.col("isresolved"),
                _entity_bucket(F.col("entity_key"), runtime.params.resolved_gold_buckets),
            ).otherwise(
                _entity_bucket(
                    F.col("entity_key"), runtime.params.unresolved_gold_buckets
                )
            ),
        )
    )
    location_segments = _align_to_schema(aggregates, GOLD_SCHEMA)

    segment_keys = aggregates.select("entity_key", "_segment_seq", "segment_id")
    location_segment_range_link = segmented.join(
        segment_keys, ["entity_key", "_segment_seq"], "inner"
    ).select(
        "segment_id",
        "entity_key",
        "range_id",
        "gap_id",
        "range_from",
        "range_to",
        "country_code",
        F.col("_probability").alias("probability"),
        F.col("_supports_json").alias("supports_json"),
        F.col("_alternatives_json").alias("alternatives_json"),
    )

    location_segment_candidate_link = (
        gap_candidates.alias("a")
        .join(
            location_segment_range_link.where(F.col("gap_id").isNotNull()).alias("r"),
            (F.col("a.entity_key") == F.col("r.entity_key"))
            & (F.col("a.gap_id") == F.col("r.gap_id"))
            & (F.col("a.range_from") == F.col("r.range_from"))
            & (F.col("a.range_to") == F.col("r.range_to")),
            "left",
        )
        .select(
            F.col("r.segment_id").alias("segment_id"),
            F.col("a.entity_key").alias("entity_key"),
            F.col("a.gap_id").alias("gap_id"),
            F.col("a.range_id").alias("candidate_range_id"),
            F.col("a.country_code").alias("country_code"),
            F.col("a.candidate_score").alias("candidate_score"),
            F.col("a.candidate_share").alias("candidate_share"),
            F.col("a.candidate_rank").alias("candidate_rank"),
            F.col("a.is_accepted").alias("is_accepted"),
            F.col("a.supported_by").alias("supported_by"),
            F.col("a.support_event_ids").alias("support_event_ids"),
        )
    )
    return {
        "primary_timeline": primary_timeline,
        "location_segment": location_segments,
        "location_segment_range_link": location_segment_range_link,
        "location_segment_candidate_link": location_segment_candidate_link,
        "invalid_ranges": invalid_ranges,
        "overlap_anomalies": overlap_anomalies,
    }


def build_temporal_gap_output(
    unresolved_gaps: DataFrame,
    gap_candidates: DataFrame,
    runtime: RuntimeConfig,
    context: RunContext,
) -> DataFrame:
    slice_candidates = (
        gap_candidates
        .groupBy("entity_key", "gap_id", "range_from", "range_to")
        .agg(
            F.to_json(
                F.sort_array(
                    F.collect_list(
                        F.struct(
                            "range_from",
                            "range_to",
                            "country_code",
                            "candidate_score",
                            "candidate_share",
                            "candidate_rank",
                            "candidate_margin",
                            "supported_by",
                            "support_event_ids",
                            "is_accepted",
                        )
                    )
                )
            ).alias("slice_candidates_json"),
            F.max(F.col("is_accepted").cast("int")).alias(
                "slice_accepted"
            ),
        )
    )

    generated_gap_keys = slice_candidates.select(
        "entity_key", "gap_id"
    ).distinct()
    untouched_gaps = (
        unresolved_gaps.alias("g")
        .join(generated_gap_keys.alias("k"), ["entity_key", "gap_id"], "left_anti")
        .withColumn(
            "candidates_json", F.col("precomputed_candidates_json")
        )
    )

    # Un gap partiellement resolu est publie uniquement sur ses tranches qui
    # restent inconnues. Cela evite de marquer comme UNKNOWN une sous-periode
    # qui a effectivement franchi les seuils d'acceptation.
    unresolved_slices = (
        unresolved_gaps.alias("g")
        .join(slice_candidates.alias("c"), ["entity_key", "gap_id"], "inner")
        .where(F.col("c.slice_accepted") == 0)
        .select(
            "g.*",
            F.col("c.range_from").alias("_slice_from"),
            F.col("c.range_to").alias("_slice_to"),
            F.col("c.slice_candidates_json").alias(
                "_slice_candidates_json"
            ),
        )
        .withColumn("_parent_gap_id", F.col("gap_id"))
        .withColumn(
            "gap_id",
            _stable_id(
                F.col("_parent_gap_id"),
                F.col("_slice_from"),
                F.col("_slice_to"),
            ),
        )
        .withColumn("gap_from", F.col("_slice_from"))
        .withColumn("gap_to", F.col("_slice_to"))
        .withColumn("candidates_json", F.col("_slice_candidates_json"))
        .drop(
            "_slice_from",
            "_slice_to",
            "_slice_candidates_json",
            "_parent_gap_id",
        )
    )

    output = (
        untouched_gaps.unionByName(
            unresolved_slices, allowMissingColumns=True
        )
        .withColumn(
            "candidates_json",
            F.coalesce(
                F.col("precomputed_candidates_json"),
                F.col("candidates_json"),
            ),
        )
        .withColumn("transitions_json", F.to_json("transition_evidence"))
        .withColumn("algorithm_version", F.lit(runtime.params.algorithm_version))
        .withColumn("inference_run_id", F.lit(context.run_id))
        .withColumn(
            "input_cutoff",
            F.lit(context.input_cutoff or datetime.utcnow()).cast("timestamp"),
        )
        .withColumn("gap_month", F.date_format("gap_from", "yyyy-MM"))
        .withColumn(
            "entity_bucket",
            _entity_bucket(F.col("entity_key"), runtime.params.unresolved_gold_buckets),
        )
    )
    return _align_to_schema(output, TEMPORAL_GAP_SCHEMA)


# ---------------------------------------------------------------------------
# Incremental : entites impactees et segments voisins
# ---------------------------------------------------------------------------


def _all_internal_segments(
    spark: SparkSession,
    runtime: RuntimeConfig,
) -> DataFrame:
    return spark.table(runtime.tables.gold_resolved_internal).unionByName(
        spark.table(runtime.tables.gold_unresolved_identity_internal)
    )


def _base_impacts(
    silver: DataFrame,
    runtime: RuntimeConfig,
) -> DataFrame:
    max_anchor_seconds = runtime.params.transition_anchor_max_days * 86400
    impact_from = F.when(
        F.col("event_type") == "TRANSITION",
        F.to_date("transition_ts").cast("timestamp"),
    ).otherwise(F.col("valid_from"))
    impact_to = F.when(
        F.col("event_type") == "TRANSITION",
        F.from_unixtime(
            F.unix_timestamp("transition_ts") + F.lit(max_anchor_seconds)
        ).cast("timestamp"),
    ).otherwise(F.col("valid_to"))
    return (
        silver
        .withColumn("_impact_from", impact_from)
        .withColumn("_impact_to", impact_to)
        .groupBy("entity_key")
        .agg(
            F.first("person_id", ignorenulls=True).alias("person_id"),
            F.max(F.col("isresolved").cast("int")).cast("boolean").alias("isresolved"),
            F.min("_impact_from").alias("impacted_from"),
            F.max("_impact_to").alias("impacted_to"),
        )
        .where(
            F.col("entity_key").isNotNull()
            & F.col("impacted_from").isNotNull()
            & F.col("impacted_to").isNotNull()
        )
    )


def _merge_pending_impact_rows(
    spark: SparkSession,
    runtime: RuntimeConfig,
    rows: DataFrame,
) -> None:
    rows = _align_to_schema(rows, IMPACT_SCHEMA)
    if _is_empty(rows):
        return
    table = runtime.tables.impacted_entities
    rows = rows.select(*spark.table(table).columns)
    view = _temporary_view_name("pending_impacts")
    rows.createOrReplaceTempView(view)
    try:
        spark.sql(
            f"""
            MERGE INTO {table} t
            USING {view} s
            ON t.run_id = s.run_id AND t.entity_key = s.entity_key
            WHEN MATCHED THEN UPDATE SET
                t.person_id = COALESCE(s.person_id, t.person_id),
                t.isresolved = s.isresolved OR t.isresolved,
                t.impacted_from = LEAST(t.impacted_from, s.impacted_from),
                t.impacted_to = GREATEST(t.impacted_to, s.impacted_to),
                t.recompute_from = LEAST(t.recompute_from, s.recompute_from),
                t.recompute_to = GREATEST(t.recompute_to, s.recompute_to),
                t.work_bucket = s.work_bucket,
                t.impact_status = s.impact_status,
                t.created_at = LEAST(t.created_at, s.created_at)
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    finally:
        spark.catalog.dropTempView(view)


def register_pending_impacts(
    spark: SparkSession,
    runtime: RuntimeConfig,
    changed_silver: DataFrame,
) -> None:
    """Journalise le delta Silver au grain entite, avant le calcul Gold."""
    max_anchor_seconds = runtime.params.transition_anchor_max_days * 86400
    impact_from = F.when(
        F.col("event_type") == "TRANSITION",
        F.to_date("transition_ts").cast("timestamp"),
    ).otherwise(F.col("valid_from"))
    impact_to = F.when(
        F.col("event_type") == "TRANSITION",
        F.from_unixtime(
            F.unix_timestamp("transition_ts") + F.lit(max_anchor_seconds)
        ).cast("timestamp"),
    ).otherwise(F.col("valid_to"))
    raw = (
        changed_silver
        .withColumn("_impact_from", impact_from)
        .withColumn("_impact_to", impact_to)
        .groupBy("run_id", "entity_key")
        .agg(
            F.first("person_id", ignorenulls=True).alias("person_id"),
            F.max(F.col("isresolved").cast("int")).cast("boolean").alias("isresolved"),
            F.min("_impact_from").alias("impacted_from"),
            F.max("_impact_to").alias("impacted_to"),
        )
        .withColumn("recompute_from", F.col("impacted_from"))
        .withColumn("recompute_to", F.col("impacted_to"))
        .withColumn(
            "work_bucket",
            _entity_bucket(F.col("entity_key"), runtime.params.silver_buckets),
        )
        .withColumn("impact_status", F.lit("RAW"))
        .withColumn("created_at", F.current_timestamp())
    )
    _merge_pending_impact_rows(spark, runtime, raw)


def register_external_impacts(
    spark: SparkSession,
    runtime: RuntimeConfig,
    impacts: DataFrame,
    run_id: str,
) -> None:
    raw = (
        impacts
        .withColumn("run_id", F.lit(run_id))
        .withColumn("recompute_from", F.col("impacted_from"))
        .withColumn("recompute_to", F.col("impacted_to"))
        .withColumn(
            "work_bucket",
            _entity_bucket(F.col("entity_key"), runtime.params.silver_buckets),
        )
        .withColumn("impact_status", F.lit("RAW"))
        .withColumn("created_at", F.current_timestamp())
    )
    _merge_pending_impact_rows(spark, runtime, raw)


def mark_impacts_completed(
    spark: SparkSession,
    runtime: RuntimeConfig,
    scopes: DataFrame,
) -> None:
    """Acquitte les deltas entierement couverts par un scope publie."""
    if _is_empty(scopes):
        return
    view = _temporary_view_name("completed_impact_entities")
    (
        scopes.groupBy("entity_key")
        .agg(
            F.min("recompute_from").alias("recompute_from"),
            F.max("recompute_to").alias("recompute_to"),
        )
        .createOrReplaceTempView(view)
    )
    try:
        spark.sql(
            f"""
            MERGE INTO {runtime.tables.impacted_entities} t
            USING {view} s
            ON t.entity_key = s.entity_key
               AND COALESCE(t.impact_status, 'RAW') <> 'COMPLETED'
               AND t.impacted_from >= s.recompute_from
               AND t.impacted_to <= s.recompute_to
            WHEN MATCHED THEN UPDATE SET t.impact_status = 'COMPLETED'
            """
        )
    finally:
        spark.catalog.dropTempView(view)


def add_neighbour_segment_bounds(
    impacts: DataFrame,
    existing_segments: DataFrame,
    runtime: RuntimeConfig,
    run_id: str,
) -> DataFrame:
    """Aligne la fenetre de recalcul sur les anciens segments voisins.

    Le precedent est le segment dont ``segment_to`` est le plus proche avant
    ``impacted_from``. Le suivant est celui dont ``segment_from`` est le plus
    proche apres ``impacted_to``. Les segments qui chevauchent directement la
    zone impactee sont aussi inclus. Le recalcul remplace donc des segments
    complets et ne coupe jamais un ancien segment en son milieu.
    """
    params = runtime.params
    created_at_value = (
        F.col("created_at")
        if "created_at" in impacts.columns
        else F.current_timestamp()
    )
    joined = impacts.alias("i").join(
        existing_segments.select(
            "entity_key", "segment_from", "segment_to"
        ).alias("s"),
        "entity_key",
        "left",
    )
    previous_rank = Window.partitionBy("entity_key").orderBy(
        F.col("s.segment_to").desc(), F.col("s.segment_from").desc()
    )
    next_rank = Window.partitionBy("entity_key").orderBy(
        F.col("s.segment_from").asc(), F.col("s.segment_to").asc()
    )
    previous = (
        joined.where(F.col("s.segment_to") <= F.col("i.impacted_from"))
        .withColumn("_rn", F.row_number().over(previous_rank))
        .where(F.col("_rn") <= params.neighbour_segments_each_side)
        .select(
            "entity_key",
            F.col("s.segment_from").alias("context_from"),
            F.col("s.segment_to").alias("context_to"),
        )
    )
    following = (
        joined.where(F.col("s.segment_from") >= F.col("i.impacted_to"))
        .withColumn("_rn", F.row_number().over(next_rank))
        .where(F.col("_rn") <= params.neighbour_segments_each_side)
        .select(
            "entity_key",
            F.col("s.segment_from").alias("context_from"),
            F.col("s.segment_to").alias("context_to"),
        )
    )
    overlapping = joined.where(
        (F.col("s.segment_from") < F.col("i.impacted_to"))
        & (F.col("s.segment_to") > F.col("i.impacted_from"))
    ).select(
        "entity_key",
        F.col("s.segment_from").alias("context_from"),
        F.col("s.segment_to").alias("context_to"),
    )
    context_bounds = (
        previous.unionByName(following)
        .unionByName(overlapping)
        .groupBy("entity_key")
        .agg(
            F.min("context_from").alias("context_from"),
            F.max("context_to").alias("context_to"),
        )
    )
    lookback_seconds = params.incremental_lookback_days * 86400
    return (
        impacts.join(context_bounds, "entity_key", "left")
        .withColumn(
            "recompute_from",
            F.least(
                F.from_unixtime(
                    F.unix_timestamp("impacted_from") - F.lit(lookback_seconds)
                ).cast("timestamp"),
                F.coalesce(F.col("context_from"), F.col("impacted_from")),
            ),
        )
        .withColumn(
            "recompute_to",
            F.greatest(
                F.from_unixtime(
                    F.unix_timestamp("impacted_to") + F.lit(lookback_seconds)
                ).cast("timestamp"),
                F.coalesce(F.col("context_to"), F.col("impacted_to")),
            ),
        )
        .withColumn("run_id", F.lit(run_id))
        .withColumn(
            "work_bucket",
            _entity_bucket(F.col("entity_key"), params.silver_buckets),
        )
        .withColumn("impact_status", F.lit("SCOPED"))
        .withColumn("created_at", created_at_value)
        .select(*[field.name for field in IMPACT_SCHEMA.fields])
    )


def create_incremental_scopes(
    spark: SparkSession,
    runtime: RuntimeConfig,
    run_id: str,
    after_exclusive: datetime,
    until_inclusive: datetime,
    additional_impacts: Optional[DataFrame] = None,
) -> DataFrame:
    # Cette table est au grain entite/run, donc des ordres de grandeur plus
    # petite que Silver. Les impacts d'un run Gold echoue restent RAW/SCOPED
    # et sont automatiquement repris au prochain lancement.
    pending = spark.table(runtime.tables.impacted_entities).where(
        F.coalesce(F.col("impact_status"), F.lit("RAW")) != "COMPLETED"
    )
    impacts = pending.select(
        "entity_key",
        "person_id",
        "isresolved",
        "impacted_from",
        "impacted_to",
        "created_at",
    )
    if additional_impacts is not None:
        impacts = impacts.unionByName(
            additional_impacts.select(
                "entity_key",
                "person_id",
                "isresolved",
                "impacted_from",
                "impacted_to",
            ).withColumn("created_at", F.lit(until_inclusive).cast("timestamp"))
        )
    impacts = impacts.groupBy("entity_key").agg(
        F.first("person_id", ignorenulls=True).alias("person_id"),
        F.max(F.col("isresolved").cast("int")).cast("boolean").alias("isresolved"),
        F.min("impacted_from").alias("impacted_from"),
        F.max("impacted_to").alias("impacted_to"),
        F.min("created_at").alias("created_at"),
    )
    scopes = add_neighbour_segment_bounds(
        impacts, _all_internal_segments(spark, runtime), runtime, run_id
    )
    _merge_iceberg(
        spark,
        runtime.tables.impacted_entities,
        _align_to_schema(scopes, IMPACT_SCHEMA),
        ["run_id", "entity_key"],
        update_matched=True,
    )
    return scopes


def create_first_fill_scopes(
    spark: SparkSession,
    runtime: RuntimeConfig,
    run_id: str,
    from_ts: datetime,
    to_ts: datetime,
) -> DataFrame:
    silver = spark.table(runtime.tables.silver_events).where(
        (F.col("valid_from") < F.lit(to_ts))
        & (F.col("valid_to") > F.lit(from_ts))
    )
    impacts = _base_impacts(silver, runtime)
    scopes = add_neighbour_segment_bounds(
        impacts, _all_internal_segments(spark, runtime), runtime, run_id
    )
    _merge_iceberg(
        spark,
        runtime.tables.impacted_entities,
        _align_to_schema(scopes, IMPACT_SCHEMA),
        ["run_id", "entity_key"],
        update_matched=True,
    )
    return scopes


def read_silver_for_scopes(
    spark: SparkSession,
    runtime: RuntimeConfig,
    scopes: DataFrame,
) -> DataFrame:
    bounds = scopes.agg(
        F.min("recompute_from").alias("minimum"),
        F.max("recompute_to").alias("maximum"),
        F.min("work_bucket").alias("minimum_bucket"),
        F.max("work_bucket").alias("maximum_bucket"),
    ).first()
    if not bounds or bounds["minimum"] is None:
        return spark.createDataFrame([], SILVER_SCHEMA)
    transition_context = timedelta(days=runtime.params.transition_anchor_max_days)
    global_from = _parse_datetime(bounds["minimum"]) - transition_context
    global_to = _parse_datetime(bounds["maximum"]) + transition_context
    candidates = spark.table(runtime.tables.silver_events).where(
        (F.col("day") >= F.lit(global_from.date()))
        & (F.col("day") <= F.lit(global_to.date()))
        & (F.col("silver_bucket") >= F.lit(int(bounds["minimum_bucket"])))
        & (F.col("silver_bucket") <= F.lit(int(bounds["maximum_bucket"])))
    )
    return (
        candidates.alias("e")
        .join(
            scopes.alias("s"),
            (F.col("e.entity_key") == F.col("s.entity_key"))
            & (F.col("e.silver_bucket") == F.col("s.work_bucket")),
            "inner",
        )
        .where(
            (
                (F.col("e.event_type") == "PRESENCE")
                & (F.col("e.valid_from") < F.col("s.recompute_to"))
                & (F.col("e.valid_to") > F.col("s.recompute_from"))
            )
            | (
                (F.col("e.event_type") == "TRANSITION")
                & (
                    F.col("e.transition_ts")
                    >= F.from_unixtime(
                        F.unix_timestamp("s.recompute_from")
                        - F.lit(runtime.params.transition_anchor_max_days * 86400)
                    ).cast("timestamp")
                )
                & (
                    F.col("e.transition_ts")
                    <= F.from_unixtime(
                        F.unix_timestamp("s.recompute_to")
                        + F.lit(
                            runtime.params.transition_anchor_max_days * 86400
                        )
                    ).cast("timestamp")
                )
            )
        )
        .select("e.*")
        .dropDuplicates(["silver_event_id"])
    )


def _clip_evidence_to_scopes(
    evidence: DataFrame,
    scopes: DataFrame,
) -> DataFrame:
    return (
        evidence.alias("e")
        .join(
            scopes.select("entity_key", "recompute_from", "recompute_to").alias("s"),
            "entity_key",
            "inner",
        )
        .withColumn(
            "evidence_from",
            F.greatest(F.col("e.evidence_from"), F.col("s.recompute_from")),
        )
        .withColumn(
            "evidence_to",
            F.least(F.col("e.evidence_to"), F.col("s.recompute_to")),
        )
        .where(F.col("evidence_to") > F.col("evidence_from"))
        .select(
            *[
                F.col(name)
                for name in evidence.columns
                if name not in {"evidence_from", "evidence_to"}
            ],
            "evidence_from",
            "evidence_to",
        )
    )


def _expand_scopes_for_temporal_context(
    scopes: DataFrame,
    runtime: RuntimeConfig,
) -> DataFrame:
    """Ajoute le contexte requis sans elargir la zone finalement ecrite."""
    context_days = max(
        runtime.params.recurrence_window_days,
        runtime.params.transition_anchor_max_days,
    )
    context_seconds = context_days * 86400
    return (
        scopes
        .withColumn(
            "recompute_from",
            F.from_unixtime(
                F.unix_timestamp("recompute_from") - F.lit(context_seconds)
            ).cast("timestamp"),
        )
        .withColumn(
            "recompute_to",
            F.from_unixtime(
                F.unix_timestamp("recompute_to") + F.lit(context_seconds)
            ).cast("timestamp"),
        )
    )


def _clip_intervals_to_scopes(
    dataframe: DataFrame,
    scopes: DataFrame,
    from_column: str,
    to_column: str,
) -> DataFrame:
    """Recoupe des intervalles semi-ouverts sur le scope d'ecriture."""
    retained = [
        F.col(f"d.{name}")
        for name in dataframe.columns
        if name not in {from_column, to_column}
    ]
    return (
        dataframe.alias("d")
        .join(
            scopes.select(
                "entity_key", "recompute_from", "recompute_to"
            ).alias("s"),
            "entity_key",
            "inner",
        )
        .withColumn(
            "_clipped_from",
            F.greatest(
                F.col(f"d.{from_column}"), F.col("s.recompute_from")
            ),
        )
        .withColumn(
            "_clipped_to",
            F.least(F.col(f"d.{to_column}"), F.col("s.recompute_to")),
        )
        .where(F.col("_clipped_to") > F.col("_clipped_from"))
        .select(
            *retained,
            F.col("_clipped_from").alias(from_column),
            F.col("_clipped_to").alias(to_column),
        )
    )


def temporal_engine(
    silver: DataFrame,
    runtime: RuntimeConfig,
    context: RunContext,
    scopes: DataFrame,
) -> dict[str, DataFrame]:
    """Orchestre le raisonnement temporel sans effectuer d'ecriture."""
    context_scopes = _expand_scopes_for_temporal_context(scopes, runtime)
    evidence, transition_details = build_scored_evidence(silver, runtime)
    evidence = _clip_evidence_to_scopes(evidence, context_scopes)
    contextual_resolved, contextual_ambiguous = score_atomic_ranges(
        evidence, runtime
    )
    contextual_filled, contextual_gaps = fill_temporal_gaps(
        contextual_resolved,
        contextual_ambiguous,
        transition_details,
        runtime,
    )
    contextual_candidates = build_gap_candidates(contextual_gaps, runtime)

    # Le contexte sert au scoring et aux chaines, mais seules les bornes du
    # scope initial sont publiees/remplacees.
    resolved_ranges = _clip_intervals_to_scopes(
        contextual_resolved, scopes, "range_from", "range_to"
    )
    ambiguous_ranges = _clip_intervals_to_scopes(
        contextual_ambiguous, scopes, "range_from", "range_to"
    )
    filled_ranges = _clip_intervals_to_scopes(
        contextual_filled, scopes, "range_from", "range_to"
    )
    unresolved_gaps = _clip_intervals_to_scopes(
        contextual_gaps, scopes, "gap_from", "gap_to"
    )
    gap_candidates = _clip_intervals_to_scopes(
        contextual_candidates, scopes, "range_from", "range_to"
    )
    segments = build_location_segments(
        filled_ranges, gap_candidates, runtime, context
    )
    temporal_gaps = build_temporal_gap_output(
        unresolved_gaps, gap_candidates, runtime, context
    )
    segments["resolved_ranges"] = resolved_ranges
    segments["ambiguous_ranges"] = ambiguous_ranges
    segments["filled_ranges"] = filled_ranges
    segments["unresolved_gaps"] = unresolved_gaps
    segments["gap_candidates"] = gap_candidates
    segments["temporal_gaps"] = temporal_gaps
    return segments


def compute_gold_impacted(
    spark: SparkSession,
    runtime: RuntimeConfig,
    context: RunContext,
    scopes: DataFrame,
) -> dict[str, DataFrame]:
    """Recalcule l'historique local necessaire pour les entites impactees."""
    silver = read_silver_for_scopes(
        spark,
        runtime,
        _expand_scopes_for_temporal_context(scopes, runtime),
    )
    return temporal_engine(silver, runtime, context, scopes)


# ---------------------------------------------------------------------------
# Validation et ecriture du Gold interne
# ---------------------------------------------------------------------------


def validate_computed_gold(
    computed: Mapping[str, DataFrame],
    runtime: RuntimeConfig,
    run_id: str,
) -> None:
    invalid_count = computed["invalid_ranges"].limit(1001).count()
    if invalid_count:
        raise ValueError(
            f"run={run_id}: {invalid_count} range(s) invalide(s); Gold non ecrit"
        )

    conflicting_overlap_count = (
        computed["overlap_anomalies"]
        .where(F.col("overlap_type") == "DIFFERENT_COUNTRY_OVERLAP")
        .limit(1001)
        .count()
    )
    if conflicting_overlap_count:
        raise ValueError(
            f"run={run_id}: {conflicting_overlap_count} overlap(s) entre pays; "
            "Gold non ecrit"
        )

    segments = computed["location_segment"]
    invalid_probability = segments.where(
        F.col("probability").isNull()
        | (F.col("probability") < 0.0)
        | (F.col("probability") > 1.0)
    ).limit(1).count()
    if invalid_probability:
        raise ValueError(f"run={run_id}: probability hors de [0,1]")

    duplicate_segment = (
        segments.groupBy("segment_id")
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate_segment:
        raise ValueError(f"run={run_id}: segment_id duplique")

    accepted_below_threshold = (
        computed["gap_candidates"]
        .where(
            F.col("is_accepted")
            & (
                F.col("candidate_share")
                < F.lit(runtime.params.gap_min_share)
            )
        )
        .limit(1)
        .count()
    )
    if accepted_below_threshold:
        raise ValueError(f"run={run_id}: candidat accepte avec score invalide")


def write_computed_gold_impacted(
    spark: SparkSession,
    runtime: RuntimeConfig,
    scopes: DataFrame,
    computed: Mapping[str, DataFrame],
) -> None:
    """Remplace seulement les anciens resultats couverts par les scopes."""
    segments = _align_to_schema(computed["location_segment"], GOLD_SCHEMA).persist(
        StorageLevel.MEMORY_AND_DISK
    )
    gaps = _align_to_schema(
        computed["temporal_gaps"], TEMPORAL_GAP_SCHEMA
    ).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        # On supprime dans les deux tables : une entite peut changer de classe
        # resolue/non resolue apres une evolution du mapping identite.
        _delete_gold_scope(spark, runtime.tables.gold_resolved_internal, scopes)
        _delete_gold_scope(
            spark, runtime.tables.gold_unresolved_identity_internal, scopes
        )
        _delete_gap_scope(spark, runtime.tables.gold_temporal_gaps_internal, scopes)

        _merge_iceberg(
            spark,
            runtime.tables.gold_resolved_internal,
            segments.where(F.col("isresolved")),
            ["segment_id"],
            update_matched=True,
        )
        _merge_iceberg(
            spark,
            runtime.tables.gold_unresolved_identity_internal,
            segments.where(~F.col("isresolved")),
            ["segment_id"],
            update_matched=True,
        )
        _merge_iceberg(
            spark,
            runtime.tables.gold_temporal_gaps_internal,
            gaps,
            ["gap_id"],
            update_matched=True,
        )
    finally:
        segments.unpersist()
        gaps.unpersist()


# ---------------------------------------------------------------------------
# Publication Hive/ORC
# ---------------------------------------------------------------------------


def _create_hive_orc_table(
    spark: SparkSession,
    table: str,
    schema: T.StructType,
    partition_columns: Sequence[str],
) -> None:
    if _table_exists(spark, table):
        return
    _ensure_namespace_for(spark, table)
    (
        spark.createDataFrame([], schema)
        .write.format("orc")
        .mode("overwrite")
        .partitionBy(*partition_columns)
        .saveAsTable(table)
    )


def ensure_hive_tables(spark: SparkSession, runtime: RuntimeConfig) -> None:
    _create_hive_orc_table(
        spark,
        runtime.tables.gold_resolved_hive,
        GOLD_SCHEMA,
        ["entity_bucket"],
    )
    _create_hive_orc_table(
        spark,
        runtime.tables.gold_unresolved_identity_hive,
        GOLD_SCHEMA,
        ["segment_month", "entity_bucket"],
    )
    _create_hive_orc_table(
        spark,
        runtime.tables.gold_temporal_gaps_hive,
        TEMPORAL_GAP_SCHEMA,
        ["gap_month", "entity_bucket"],
    )


def _partition_predicate_sql(
    partition_columns: Sequence[str],
    row: Any,
) -> str:
    predicates: list[str] = []
    for column in partition_columns:
        value = row[column]
        if value is None:
            predicates.append(f"`{column}` IS NULL")
        elif isinstance(value, (int, float)):
            predicates.append(f"`{column}` = {value}")
        else:
            escaped = str(value).replace("'", "''")
            predicates.append(f"`{column}` = '{escaped}'")
    return ", ".join(predicates)


def _write_hive_rows(
    spark: SparkSession,
    table: str,
    rows: DataFrame,
    partition_columns: Sequence[str],
    mode: str,
    row_id_column: str = "segment_id",
) -> None:
    if _is_empty(rows):
        return
    write_shards = max(spark.sparkContext.defaultParallelism, 64)
    prepared = (
        rows
        .withColumn(
            "_write_shard",
            F.pmod(
                F.xxhash64("entity_key", row_id_column),
                F.lit(write_shards),
            ),
        )
        .repartition(
            write_shards,
            *[F.col(column) for column in partition_columns],
            F.col("_write_shard"),
        )
        .drop("_write_shard")
        .select(*spark.table(table).columns)
    )
    prepared.write.mode(mode).insertInto(table)


def publish_hive_full(
    spark: SparkSession,
    runtime: RuntimeConfig,
) -> None:
    """Reconstruit les deux copies Hive depuis le Gold Iceberg autoritatif."""
    ensure_hive_tables(spark, runtime)
    targets = [
        (
            runtime.tables.gold_resolved_internal,
            runtime.tables.gold_resolved_hive,
            ["entity_bucket"],
            "segment_id",
        ),
        (
            runtime.tables.gold_unresolved_identity_internal,
            runtime.tables.gold_unresolved_identity_hive,
            ["segment_month", "entity_bucket"],
            "segment_id",
        ),
        (
            runtime.tables.gold_temporal_gaps_internal,
            runtime.tables.gold_temporal_gaps_hive,
            ["gap_month", "entity_bucket"],
            "gap_id",
        ),
    ]
    for internal, target, partitions, row_id_column in targets:
        spark.sql(f"TRUNCATE TABLE {target}")
        _write_hive_rows(
            spark,
            target,
            spark.table(internal),
            partitions,
            mode="append",
            row_id_column=row_id_column,
        )


def publish_hive_incremental_target(
    spark: SparkSession,
    internal_table: str,
    target_table: str,
    partition_columns: Sequence[str],
    scopes: DataFrame,
    row_id_column: str = "segment_id",
) -> None:
    """Reecrit uniquement les partitions Hive touchees.

    Le Gold Iceberg est deja a jour. Pour chaque cle impactee, on retire sa
    version Hive et on reprend toutes ses lignes courantes depuis Iceberg. Les
    autres entites des partitions touchees sont conservees.
    """
    impacted_keys = scopes.select("entity_key").distinct()
    current = spark.table(target_table)
    internal = spark.table(internal_table)

    old_impacted = current.join(impacted_keys, "entity_key", "left_semi")
    new_impacted = internal.join(impacted_keys, "entity_key", "left_semi")
    touched = (
        old_impacted.select(*partition_columns)
        .unionByName(new_impacted.select(*partition_columns))
        .distinct()
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    try:
        if _is_empty(touched):
            return
        current_touched = current.join(
            F.broadcast(touched), list(partition_columns), "inner"
        )
        preserved = current_touched.join(
            impacted_keys, "entity_key", "left_anti"
        )
        replacement = preserved.unionByName(new_impacted).persist(
            StorageLevel.DISK_ONLY
        )
        try:
            nonempty_partitions = replacement.select(*partition_columns).distinct()
            empty_partitions = touched.join(
                nonempty_partitions, list(partition_columns), "left_anti"
            ).collect()

            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
            _write_hive_rows(
                spark,
                target_table,
                replacement,
                partition_columns,
                mode="overwrite",
                row_id_column=row_id_column,
            )

            # Une partition devenue totalement vide ne figure pas dans le DF
            # d'overwrite dynamique. On la supprime explicitement, cible par cible.
            for partition in empty_partitions:
                spec = _partition_predicate_sql(partition_columns, partition)
                spark.sql(
                    f"ALTER TABLE {target_table} DROP IF EXISTS PARTITION ({spec})"
                )
        finally:
            replacement.unpersist()
    finally:
        touched.unpersist()


def publish_hive_incremental(
    spark: SparkSession,
    runtime: RuntimeConfig,
    scopes: DataFrame,
) -> None:
    ensure_hive_tables(spark, runtime)
    publish_hive_incremental_target(
        spark,
        runtime.tables.gold_resolved_internal,
        runtime.tables.gold_resolved_hive,
        ["entity_bucket"],
        scopes,
    )
    publish_hive_incremental_target(
        spark,
        runtime.tables.gold_unresolved_identity_internal,
        runtime.tables.gold_unresolved_identity_hive,
        ["segment_month", "entity_bucket"],
        scopes,
    )
    publish_hive_incremental_target(
        spark,
        runtime.tables.gold_temporal_gaps_internal,
        runtime.tables.gold_temporal_gaps_hive,
        ["gap_month", "entity_bucket"],
        scopes,
        row_id_column="gap_id",
    )


# ---------------------------------------------------------------------------
# Orchestration du calcul Gold par lots
# ---------------------------------------------------------------------------


def process_gold_scope_batches(
    spark: SparkSession,
    runtime: RuntimeConfig,
    context: RunContext,
    scopes: DataFrame,
    bucket_batch_size: int,
) -> None:
    scopes = scopes.persist(StorageLevel.DISK_ONLY)
    try:
        for bucket_from in range(
            0, runtime.params.silver_buckets, bucket_batch_size
        ):
            bucket_to = min(
                bucket_from + bucket_batch_size,
                runtime.params.silver_buckets,
            )
            batch_scopes = scopes.where(
                (F.col("work_bucket") >= bucket_from)
                & (F.col("work_bucket") < bucket_to)
            ).persist(StorageLevel.DISK_ONLY)
            try:
                if _is_empty(batch_scopes):
                    continue
                log_event(
                    "gold_bucket_batch_started",
                    run_id=context.run_id,
                    bucket_from=bucket_from,
                    bucket_to_exclusive=bucket_to,
                )
                computed = compute_gold_impacted(
                    spark, runtime, context, batch_scopes
                )
                computed["location_segment"] = computed[
                    "location_segment"
                ].persist(StorageLevel.MEMORY_AND_DISK)
                computed["temporal_gaps"] = computed["temporal_gaps"].persist(
                    StorageLevel.DISK_ONLY
                )
                try:
                    validate_computed_gold(computed, runtime, context.run_id)
                    write_computed_gold_impacted(
                        spark, runtime, batch_scopes, computed
                    )
                finally:
                    computed["location_segment"].unpersist()
                    computed["temporal_gaps"].unpersist()
                log_event(
                    "gold_bucket_batch_completed",
                    run_id=context.run_id,
                    bucket_from=bucket_from,
                    bucket_to_exclusive=bucket_to,
                )
            finally:
                batch_scopes.unpersist()
    finally:
        scopes.unpersist()


def _ingest_first_fill_sources(
    spark: SparkSession,
    runtime: RuntimeConfig,
    context: RunContext,
    configs: Sequence[Mapping[str, Any]],
    from_ts: datetime,
    to_ts: datetime,
) -> None:
    for chunk_from, chunk_to in _date_chunks(
        from_ts,
        to_ts,
        runtime.params.first_fill_source_chunk_days,
    ):
        for config in configs:
            ingest_source_window(
                spark,
                runtime,
                config,
                context,
                chunk_from,
                chunk_to,
            )


def _ingest_incremental_sources(
    spark: SparkSession,
    runtime: RuntimeConfig,
    context: RunContext,
    configs: Sequence[Mapping[str, Any]],
    bootstrap_from: Optional[datetime],
) -> Optional[tuple[datetime, datetime]]:
    processed_windows: list[tuple[datetime, datetime]] = []
    for config in configs:
        source_id = str(config["source_id"])
        last = get_last_checkpoint(spark, runtime, source_id)
        if last is None:
            source_bootstrap = (
                _parse_datetime(config["initial_from"])
                if config.get("initial_from")
                else bootstrap_from
            )
            if source_bootstrap is None:
                raise ValueError(
                    f"Aucun checkpoint pour {source_id}. Fournir bootstrap_from "
                    "ou initial_from dans sa configuration, ou lancer "
                    "run_first_fill."
                )
            last = source_bootstrap
        lateness_days = int(
            config.get("lateness_days", runtime.params.incremental_lookback_days)
        )
        raw_read_from = last - timedelta(days=lateness_days)
        # Les presences Silver sont agregees par jour. Relire depuis minuit
        # evite qu'un agregat partiel ecrase l'agregat complet de ce jour.
        read_from = datetime.combine(raw_read_from.date(), datetime.min.time())
        watermark = get_current_watermark(spark, runtime, config)
        if watermark is None or watermark <= read_from:
            continue
        for chunk_from, chunk_to in _date_chunks(
            read_from,
            watermark,
            runtime.params.first_fill_source_chunk_days,
        ):
            ingest_source_window(
                spark,
                runtime,
                config,
                context,
                chunk_from,
                chunk_to,
            )
        processed_windows.append((read_from, watermark))
    if not processed_windows:
        return None
    return (
        min(window[0] for window in processed_windows),
        max(window[1] for window in processed_windows),
    )


def run_first_fill(
    spark: SparkSession,
    runtime: RuntimeConfig,
    day_from: str | date | datetime,
    day_to_inclusive: str | date | datetime,
) -> str:
    """Premier chargement idempotent, bornes calendaires inclusives."""
    from_ts = _parse_datetime(day_from)
    parsed_to = _parse_datetime(day_to_inclusive)
    to_ts = (
        parsed_to + timedelta(days=1)
        if parsed_to.time() == datetime.min.time()
        else parsed_to
    )
    if to_ts <= from_ts:
        raise ValueError("day_to doit etre posterieur ou egal a day_from")

    run_id = uuid.uuid4().hex
    context = RunContext(
        run_id=run_id,
        mode="FIRST_FILL",
        started_at=datetime.utcnow(),
        requested_from=from_ts,
        requested_to=to_ts,
    )
    ensure_oracle_control_tables(spark, runtime)
    ensure_spark_tables(spark, runtime)
    ensure_hive_tables(spark, runtime)
    start_run(spark, runtime, context)
    try:
        configs = load_source_configs(spark, runtime)
        _ingest_first_fill_sources(
            spark, runtime, context, configs, from_ts, to_ts
        )
        infer_group_presences(
            spark,
            runtime,
            context,
            from_ts.date(),
            (to_ts - timedelta(microseconds=1)).date(),
        )

        input_cutoff = datetime.utcnow()
        context = replace(context, input_cutoff=input_cutoff)
        scopes = create_first_fill_scopes(
            spark, runtime, run_id, from_ts, to_ts
        ).persist(StorageLevel.DISK_ONLY)
        try:
            if not _is_empty(scopes):
                process_gold_scope_batches(
                    spark,
                    runtime,
                    context,
                    scopes,
                    runtime.params.first_fill_bucket_batch_size,
                )
            # La publication complete est volontaire ici : Iceberg reste la
            # source autoritative et permet de reconstruire Hive.
            publish_hive_full(spark, runtime)
            mark_impacts_completed(spark, runtime, scopes)
        finally:
            scopes.unpersist()

        update_checkpoint(
            spark,
            runtime,
            "__GOLD__",
            input_cutoff,
            run_id,
            pipeline_name="BON_VOYAGE_GOLD",
        )
        finish_run(
            spark,
            runtime,
            run_id,
            "SUCCESS",
            input_cutoff=input_cutoff,
        )
        log_event("first_fill_completed", run_id=run_id)
        return run_id
    except Exception as exc:
        try:
            finish_run(spark, runtime, run_id, "FAILED", str(exc))
        finally:
            log_event("first_fill_failed", run_id=run_id, error=str(exc))
        raise


def run_incremental(
    spark: SparkSession,
    runtime: RuntimeConfig,
    bootstrap_from: Optional[str | date | datetime] = None,
) -> str:
    """Ingestion quotidienne + recalcul local + publication Hive."""
    bootstrap = _parse_datetime(bootstrap_from) if bootstrap_from else None
    previous_gold_cutoff = get_last_checkpoint(
        spark,
        runtime,
        "__GOLD__",
        pipeline_name="BON_VOYAGE_GOLD",
    )
    if previous_gold_cutoff is None:
        if bootstrap is None:
            raise ValueError(
                "Aucun checkpoint Gold. Lancer run_first_fill ou fournir "
                "bootstrap_from pour une reprise explicite."
            )
        previous_gold_cutoff = bootstrap

    run_id = uuid.uuid4().hex
    context = RunContext(
        run_id=run_id,
        mode="INCREMENTAL",
        started_at=datetime.utcnow(),
        requested_from=None,
        requested_to=None,
    )
    ensure_oracle_control_tables(spark, runtime)
    ensure_spark_tables(spark, runtime)
    ensure_hive_tables(spark, runtime)
    start_run(spark, runtime, context)
    try:
        configs = load_source_configs(spark, runtime)
        processed_window = _ingest_incremental_sources(
            spark, runtime, context, configs, bootstrap
        )
        old_identity_impacts = relink_silver_identities(
            spark,
            runtime,
            context,
            previous_gold_cutoff,
        )
        relink_bounds = old_identity_impacts.agg(
            F.min("impacted_from").alias("minimum"),
            F.max("impacted_to").alias("maximum"),
        ).first()
        inference_from = processed_window[0] if processed_window else None
        inference_to = processed_window[1] if processed_window else None
        if relink_bounds and relink_bounds["minimum"] is not None:
            relink_from = _parse_datetime(relink_bounds["minimum"])
            relink_to = _parse_datetime(relink_bounds["maximum"])
            inference_from = (
                min(inference_from, relink_from) if inference_from else relink_from
            )
            inference_to = max(inference_to, relink_to) if inference_to else relink_to
        if inference_from and inference_to:
            infer_group_presences(
                spark,
                runtime,
                context,
                inference_from.date(),
                inference_to.date(),
            )

        # Tout ce qui est ecrit avant ce cutoff appartient a ce calcul. Les
        # ecritures concurrentes posterieures attendront le prochain run.
        input_cutoff = datetime.utcnow()
        context = replace(context, input_cutoff=input_cutoff)
        scopes = create_incremental_scopes(
            spark,
            runtime,
            run_id,
            previous_gold_cutoff,
            input_cutoff,
            additional_impacts=old_identity_impacts,
        ).persist(StorageLevel.DISK_ONLY)
        try:
            if not _is_empty(scopes):
                process_gold_scope_batches(
                    spark,
                    runtime,
                    context,
                    scopes,
                    runtime.params.incremental_bucket_batch_size,
                )
                publish_hive_incremental(spark, runtime, scopes)
                mark_impacts_completed(spark, runtime, scopes)
        finally:
            scopes.unpersist()
            old_identity_impacts.unpersist()

        update_checkpoint(
            spark,
            runtime,
            "__GOLD__",
            input_cutoff,
            run_id,
            pipeline_name="BON_VOYAGE_GOLD",
        )
        finish_run(
            spark,
            runtime,
            run_id,
            "SUCCESS",
            input_cutoff=input_cutoff,
        )
        log_event("incremental_completed", run_id=run_id)
        return run_id
    except Exception as exc:
        try:
            finish_run(spark, runtime, run_id, "FAILED", str(exc))
        finally:
            log_event("incremental_failed", run_id=run_id, error=str(exc))
        raise


# ---------------------------------------------------------------------------
# Commandes d'exploitation
# ---------------------------------------------------------------------------


def build_runtime_from_environment() -> RuntimeConfig:
    jdbc_url = os.environ.get("BON_VOYAGE_JDBC_URL")
    if not jdbc_url:
        raise ValueError("Variable BON_VOYAGE_JDBC_URL absente")
    properties_json = os.environ.get("BON_VOYAGE_JDBC_PROPERTIES_JSON", "{}")
    table_overrides_json = os.environ.get("BON_VOYAGE_TABLES_JSON", "{}")
    parameter_overrides_json = os.environ.get("BON_VOYAGE_PARAMETERS_JSON", "{}")
    properties = json.loads(properties_json)
    tables = TableNames(**json.loads(table_overrides_json))
    params = PipelineParameters(**json.loads(parameter_overrides_json))
    params.validate()
    return RuntimeConfig(
        jdbc_url=jdbc_url,
        jdbc_properties=properties,
        tables=tables,
        params=params,
    )


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pipeline Bon Voyage complet")
    parser.add_argument(
        "mode",
        choices=["create-tables", "first-fill", "incremental", "publish-hive"],
    )
    parser.add_argument("--from", dest="day_from")
    parser.add_argument("--to", dest="day_to")
    parser.add_argument("--bootstrap-from", dest="bootstrap_from")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _build_argument_parser().parse_args(argv)
    spark = (
        SparkSession.builder.appName("bon-voyage-pipeline")
        .enableHiveSupport()
        .getOrCreate()
    )
    runtime = build_runtime_from_environment()

    if args.mode == "create-tables":
        ensure_oracle_control_tables(spark, runtime)
        ensure_spark_tables(spark, runtime)
        ensure_hive_tables(spark, runtime)
        log_event("tables_created")
        return 0
    if args.mode == "first-fill":
        if not args.day_from or not args.day_to:
            raise ValueError("first-fill exige --from et --to")
        run_first_fill(spark, runtime, args.day_from, args.day_to)
        return 0
    if args.mode == "incremental":
        run_incremental(spark, runtime, args.bootstrap_from)
        return 0
    if args.mode == "publish-hive":
        ensure_hive_tables(spark, runtime)
        publish_hive_full(spark, runtime)
        log_event("hive_publication_completed")
        return 0
    raise AssertionError(f"Mode non gere: {args.mode}")


if __name__ == "__main__":
    raise SystemExit(main())
