from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnull, to_timestamp, sum as spark_sum, explode, input_file_name 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, BooleanType, ArrayType
from datetime import datetime, timedelta
import json
import sys

spark = SparkSession.builder \
    .appName("NBADataQuality") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minio_access_key")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minio_secret_key")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Define schema
schema = StructType([
    StructField("id", StringType(), False),
    StructField("scheduled", StringType(), False),
    StructField("status", StringType(), False),
    StructField("neutral_site", BooleanType(), True),
    StructField("home", StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("points", IntegerType(), True),
        StructField("statistics", StructType([
            StructField("field_goals_made", IntegerType(), True),
            StructField("field_goals_att", IntegerType(), True),
            StructField("three_points_made", IntegerType(), True),
            StructField("three_points_att", IntegerType(), True),
            StructField("free_throws_made", IntegerType(), True),
            StructField("free_throws_att", IntegerType(), True),
            StructField("offensive_rebounds", IntegerType(), True),
            StructField("defensive_rebounds", IntegerType(), True),
            StructField("assists", IntegerType(), True),
            StructField("steals", IntegerType(), True),
            StructField("blocks", IntegerType(), True),
            StructField("personal_fouls", IntegerType(), True),
            StructField("points", IntegerType(), True)
        ]), True),
        StructField("players", ArrayType(StructType([
            StructField("id", StringType(), False),
            StructField("full_name", StringType(), False),
            StructField("statistics", StructType([
                StructField("points", IntegerType(), True),
                StructField("field_goals_made", IntegerType(), True),
                StructField("field_goals_att", IntegerType(), True),
                StructField("three_points_made", IntegerType(), True),
                StructField("three_points_att", IntegerType(), True),
                StructField("free_throws_made", IntegerType(), True),
                StructField("free_throws_att", IntegerType(), True),
                StructField("offensive_rebounds", IntegerType(), True),
                StructField("defensive_rebounds", IntegerType(), True),
                StructField("assists", IntegerType(), True),
                StructField("steals", IntegerType(), True),
                StructField("blocks", IntegerType(), True),
                StructField("personal_fouls", IntegerType(), True)
            ]), True)
        ])), True)
    ]), False),
    StructField("away", StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("points", IntegerType(), True),
        StructField("statistics", StructType([
            StructField("field_goals_made", IntegerType(), True),
            StructField("field_goals_att", IntegerType(), True),
            StructField("three_points_made", IntegerType(), True),
            StructField("three_points_att", IntegerType(), True),
            StructField("free_throws_made", IntegerType(), True),
            StructField("free_throws_att", IntegerType(), True),
            StructField("offensive_rebounds", IntegerType(), True),
            StructField("defensive_rebounds", IntegerType(), True),
            StructField("assists", IntegerType(), True),
            StructField("steals", IntegerType(), True),
            StructField("blocks", IntegerType(), True),
            StructField("personal_fouls", IntegerType(), True),
            StructField("points", IntegerType(), True)
        ]), True),
        StructField("players", ArrayType(StructType([
            StructField("id", StringType(), False),
            StructField("full_name", StringType(), False),
            StructField("statistics", StructType([
                StructField("points", IntegerType(), True),
                StructField("field_goals_made", IntegerType(), True),
                StructField("field_goals_att", IntegerType(), True),
                StructField("three_points_made", IntegerType(), True),
                StructField("three_points_att", IntegerType(), True),
                StructField("free_throws_made", IntegerType(), True),
                StructField("free_throws_att", IntegerType(), True),
                StructField("offensive_rebounds", IntegerType(), True),
                StructField("defensive_rebounds", IntegerType(), True),
                StructField("assists", IntegerType(), True),
                StructField("steals", IntegerType(), True),
                StructField("blocks", IntegerType(), True),
                StructField("personal_fouls", IntegerType(), True)
            ]), True)
        ])), True)
    ]), False)
])


current_date = "2025-02-15"
input_path = f"s3a://demo-nba-games/games/{current_date}/raw_games"

df = spark.read.schema(schema).json(input_path).withColumn("source_file", input_file_name())


def check_completeness(df):
    completeness_check = df.filter(
        isnull("id") | isnull("scheduled") | isnull("home.id") | isnull("home.name") |
        isnull("away.id") | isnull("away.name")
    ).select("id", "source_file") # Select id and source_file
    return completeness_check

def check_consistency(df):
    home_players_df = df.select(
        col("id"),
        col("source_file"), # Include source_file
        col("home.points").alias("team_points"),
        explode("home.players").alias("player")
    ).select(
        col("id"),
        col("source_file"), # Include source_file
        col("team_points"),
        col("player.statistics.points").alias("player_points")
    )

    away_players_df = df.select(
        col("id"),
        col("source_file"), # Include source_file
        col("away.points").alias("team_points"),
        explode("away.players").alias("player")
    ).select(
        col("id"),
        col("source_file"), # Include source_file
        col("team_points"),
        col("player.statistics.points").alias("player_points")
    )

    consistency_check = df.filter(
        (col("home.points") < 0) | (col("away.points") < 0)
    ).select("id", "source_file").union( # Select id and source_file
        home_players_df.filter(col("player_points") < 0).select("id", "source_file") # Select id and source_file
    ).union(
        away_players_df.filter(col("player_points") < 0).select("id", "source_file") # Select id and source_file
    ).distinct()
    return consistency_check

def check_conformity(df):
    conformity_check = df.filter(
          ~to_timestamp(col("scheduled"), "yyyy-MM-dd'T'HH:mm:ssX").isNotNull()
    ).select("id", "source_file").distinct() # Select id and source_file
    return conformity_check

def check_accuracy(df):
    # Keep source_file throughout joins
    home_players_df = df.select(
        col("id"),
        col("source_file"), # Include source_file
        col("home.points").alias("hometeam_points"),
        explode("home.players").alias("player")
    ).select(
        col("id"),
        col("source_file"), # Include source_file
        col("hometeam_points"),
        col("player.statistics.points").alias("player_points")
    ).groupBy("id", "source_file", "hometeam_points").agg( # Group by source_file too
        spark_sum("player_points").alias("home_player_points")
    )

    away_players_df = df.select(
        col("id"),
        col("source_file"), # Include source_file
        col("away.points").alias("awayteam_points"),
        explode("away.players").alias("player")
    ).select(
        col("id"),
        col("source_file"), # Include source_file
        col("awayteam_points"),
        col("player.statistics.points").alias("player_points")
    ).groupBy("id", "source_file", "awayteam_points").agg( # Group by source_file too
        spark_sum("player_points").alias("away_player_points")
    )

    # Join on id and source_file
    accuracy_check = home_players_df.join(away_players_df, ["id", "source_file"]).filter(
        (col("hometeam_points") != col("home_player_points")) | # team_points here is home team points
        (col("awayteam_points") != col("away_player_points"))  # team_points here is away team points
    ).select("id", "source_file").distinct() # Select id and source_file
    return accuracy_check

def check_integrity(df):
    # This check needs careful handling of source_file when comparing players across teams
    # A simpler way is to identify invalid records and then pull their source_file
    # from the original DataFrame. Let's refine this check slightly.

    # Get IDs of records where a player ID is in both home and away rosters
    integrity_issue_ids = df.filter(
        col("home.players.id").isin(col("away.players.id"))
    ).select("id").distinct()

    # Join back to the original df to get the source_file for these invalid IDs
    integrity_check = df.join(integrity_issue_ids, "id").select("id", "source_file").distinct()

    return integrity_check

# Timeliness check - Uncomment if needed and adjust logic/output as above
# def check_timeliness(df):
#     current_time = datetime.now()
#     timeliness_check = df.filter(
#         to_timestamp(col("scheduled")) < current_time - timedelta(days=1)
#     ).select("id", "source_file").distinct() # Select id and source_file
#     return timeliness_check

# Execute quality checks
completeness_invalid = check_completeness(df)
consistency_invalid = check_consistency(df)
conformity_invalid = check_conformity(df)
accuracy_invalid = check_accuracy(df)
integrity_invalid = check_integrity(df)
# timeliness_invalid = check_timeliness(df) # Uncomment if used

# Collect invalid record IDs AND their source files
invalid_records_df = (
    completeness_invalid.union(
        consistency_invalid
    ).union(
        conformity_invalid
    ).union(
        accuracy_invalid
    ).union(
        integrity_invalid
    )
).distinct() 

# Identify the files that contain at least one invalid record
files_with_issues_df = invalid_records_df.select("source_file").distinct()



# Filter valid records (exclude invalid IDs regardless of source file, or by id and source_file pair)
# To filter out records that were found invalid, you can join the original df with invalid_records_df
valid_df = df.join(files_with_issues_df,  "source_file", "left_anti")
valid_df.show()

# Save valid records to MinIO

valid_df.write.mode("overwrite").parquet(f"s3a://demo-nba-games/accept_data/{current_date}/valid/")

# Clean up
spark.stop()