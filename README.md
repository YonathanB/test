def detect_duplicate_plates(spark, config):
    """
    Detects potentially duplicated license plates by identifying physically impossible patterns
    and parallel paths.
    """
    window_spec = Window.partitionBy("lpr_number").orderBy("spott_date")
    
    # First, get all readings with their sequence
    base_df = (
        spark.table(config.TABLE_3)
        .withColumn("row_number", F.row_number().over(window_spec))
        # Self-join to get all pairs of readings for the same plate
        .alias("a")
        .join(
            spark.table(config.TABLE_3).alias("b"),
            (F.col("a.lpr_number") == F.col("b.lpr_number")) &
            (F.col("a.row_number") < F.col("b.row_number"))
        )
    )
    
    # Detect parallel paths
    parallel_paths = (
        base_df
        # Group consecutive readings (within some window)
        .withColumn(
            "time_diff_minutes",
            (F.unix_timestamp("b.spott_date") - F.unix_timestamp("a.spott_date")) / 60
        )
        # Find plates that appear in different sequences within a reasonable timeframe
        .filter(F.col("time_diff_minutes") < 60)  # Adjust window as needed
        # Look for different sensor progression
        .withColumn(
            "is_different_path",
            F.col("a.sensor_code") != F.col("b.sensor_code")
        )
        # Group to find consistent patterns
        .groupBy("a.lpr_number")
        .agg(
            F.count("*").alias("parallel_occurrences"),
            F.sum(F.when(F.col("is_different_path"), 1).otherwise(0)).alias("different_paths")
        )
        .filter(
            (F.col("parallel_occurrences") > 5) &  # Adjust threshold
            (F.col("different_paths") > 3)         # Adjust threshold
        )
    )
    
    # Detect physically impossible transitions
    impossible_transitions = (
        base_df
        # Calculate time and "speed" between readings
        .withColumn(
            "time_diff_minutes",
            (F.unix_timestamp("b.spott_date") - F.unix_timestamp("a.spott_date")) / 60
        )
        .filter(F.col("time_diff_minutes") < 30)  # Look at short time windows
        # Group consecutive readings
        .groupBy("a.lpr_number")
        .agg(
            # Count how many times this plate shows "impossible" patterns
            F.count(
                F.when(
                    (F.col("a.sensor_code") != F.col("b.sensor_code")) &
                    (F.col("time_diff_minutes") < 5),  # Very conservative time window
                    1
                )
            ).alias("impossible_transitions")
        )
        .filter(F.col("impossible_transitions") > 2)  # Adjust threshold
    )
    
    # Combine both detection methods
    suspicious_plates = (
        parallel_paths
        .join(impossible_transitions, "lpr_number", "full_outer")
        .withColumn(
            "duplicate_confidence",
            F.when(
                F.col("parallel_occurrences").isNotNull() &
                F.col("impossible_transitions").isNotNull(),
                "HIGH"
            ).when(
                F.col("parallel_occurrences").isNotNull() |
                F.col("impossible_transitions").isNotNull(),
                "MEDIUM"
            ).otherwise("LOW")
        )
    )
    
    # Mark original records
    return (
        spark.table(config.TABLE_3)
        .join(
            suspicious_plates,
            "lpr_number",
            "left"
        )
        .withColumn(
            "potential_duplicate",
            F.col("duplicate_confidence").isNotNull()
        )
        .withColumn(
            "duplicate_confidence",
            F.coalesce(F.col("duplicate_confidence"), F.lit("NONE"))
        )
    )
