from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, explode, col
from modules.module_job import get_latest_partition_date
from datetime import date, datetime
import logging

def checking_espn(df, spark, tag):
    df = df.withColumn("standing", explode(col("standings")))
    df_filter = df.filter(col("standing.type") == "TOTAL") \
                        .select(explode(col("standing.table")).alias("table"))
    n_count = df_filter.select(col("table.team.shortName")).count()
    n_count.show()
    if tag == "pl" and n_count != 20:
        print("Premier league data missing team")
        return False
    elif tag == "la" and n_count != 20:
        print("La liga data missing team")
        return False
    elif tag == "fl" and n_count != 18:
        print("Ligue 1 data missing team")
        return False
    elif tag == "bun" and n_count != 18:
        print("Bundesliga data missing team")
        return False
    elif tag == "se" and n_count != 20:
        print("Serie A data missing team")
        return False
    else:
        print(f"DATA VALIDATION: There is no missing team in {tag}!")
        return True

def checking_capology(df, spark, tag):
    n_count = df.select(col("team")).distinct().count()
    n_count.show()
    if tag == "pl" and n_count != 20:
        print("Premier league data missing team")
        return False
    elif tag == "la" and n_count != 20:
        print("La liga data missing team")
        return False
    elif tag == "fl" and n_count != 18:
        print("Ligue 1 data missing team")
        return False
    elif tag == "bun" and n_count != 18:
        print("Bundesliga data missing team")
        return False
    elif tag == "se" and n_count != 20:
        print("Serie A data missing team")
        return False
    else:
        print(f"DATA VALIDATION: There is no missing team in {tag}!")

def checking_fbref(df, spark, tag, path):
    n_count = df.select(col("team_name")).distinct().count()
    col_count = len(df.columns)
    column_expectations = {
        "attacking": 8,
        "defending": 9,
        "overall": 8,
        "team": 8,
    }
    for keyword, expected_col_count in column_expectations.items():
        if keyword in path and col_count != expected_col_count:
            print(f"Not enough columns in path {path} (expected {expected_col_count}, got {col_count})")
            return False
    team_expectations = {
        "pl": 20,
        "la": 20,
        "fl": 18,
        "bun": 18,
        "se": 20,
    }

    expected_teams = team_expectations.get(tag)
    if expected_teams and n_count != expected_teams:
        print(f"{tag.upper()} data missing team (expected {expected_teams}, got {n_count})")
        return False

    print(f"DATA VALIDATION: There is no missing team in {tag}!")
    return True
    