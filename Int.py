from sedona.spark import SedonaContext
from pyspark.sql import functions as F

config  = SedonaContext.builder().getOrCreate()
sedona  = SedonaContext.create(config)
sedona.conf.set("spark.sedona.join.gridtype", "quadtree")
sedona.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# --- Polygones (une fois) ---
polys = (sedona.table("B")
         .selectExpr("poly_id", "ST_GeomFromGeoJSON(gj) AS geom")
         .where("geom IS NOT NULL"))
polys.cache().count()

# --- Points ---
pts = (sedona.table("A")
       .withColumn("id", F.expr("uuid()"))          # si pas déjà de clé unique
       .selectExpr("id", "jour", "x", "y",
                   "ST_Point(CAST(x AS double), CAST(y AS double)) AS geom"))

pts.createOrReplaceTempView("a")
polys.createOrReplaceTempView("b")

# --- Inner join spatial (seul optimisé) ---
dedans = sedona.sql("""
  SELECT DISTINCT a.id
  FROM a, b
  WHERE ST_Intersects(b.geom, a.geom)
""")

# --- Anti-join sur clé ---
dehors = pts.join(dedans, ["id"], "left_anti").select("id", "jour", "x", "y")

dehors.write.mode("overwrite").partitionBy("jour").parquet("/chemin/dehors")
