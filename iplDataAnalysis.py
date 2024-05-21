from pyspark.sql.types import StructField, StructType, IntegerType, StringType, BooleanType, DateType, DecimalType
from pyspark.sql.functions import *
from pyspark.sql.window import Window

from pyspark.sql import SparkSession

ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])

player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])

team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])



if __name__ == '__main__':
    # create session
    spark = SparkSession.builder.appName("IPL Data Analysis").getOrCreate()

    ball_by_ball_df = spark.read.schema(ball_by_ball_schema).format("csv").option("header","true").load("Ball_By_Ball.csv")
    # print("ball_by_ball_df printed below")
    # ball_by_ball_df.show(5)

    match_df = spark.read.schema(match_schema).format("csv").option("header", "true").load("Match.csv")
    print("match_df printed below")
    match_df.show(5)

    player_df = spark.read.schema(player_schema).format("csv").option("header", "true").load("Player.csv")
    print("player_df printed below")
    player_df.show(5)

    player_match_df = spark.read.schema(player_match_schema).format("csv").option("header", "true").load("Player_match.csv")
    print("player_match_df printed below")
    player_match_df.show(5)

    team_df = spark.read.schema(team_schema).format("csv").option("header", "true").load("Team.csv")
    print("team_df printed below")
    team_df.show(5)

    # Filter to include only valid deliveries (excluding extras like wides and no balls for specific analyses)
    ball_by_ball_df = ball_by_ball_df.filter((col("wides") == 0) & (col("noballs")==0))

    # Aggregation: Calculate the total and average runs scored in each match and inning
    # total_and_avg_runs = ball_by_ball_df.groupBy("match_id", "innings_no").agg(
    #     sum("runs_scored").alias("total_runs"),
    #     avg("runs_scored").alias("average_runs")
    # )
    # total_and_avg_runs = ball_by_ball_df.groupBy("match_id", "innings_no").agg(sum("runs_scored").alias("Total_Runs"))
    # total_and_avg_runs = total_and_avg_runs.sort(("Total_Runs"))
    #
    # print("Aggregation: Calculate the total and average runs scored in each match and inning")
    # total_and_avg_runs.show(10)

    # Window Function: Calculate running total of runs in each match for each over
    windowSpec = Window.partitionBy("match_id", "innings_no").orderBy("over_id")

    ball_by_ball_df = ball_by_ball_df.withColumn(
        "running_total_runs",
        sum("runs_scored").over(windowSpec)
    )

    # Conditional Column: Flag for high impact balls (either a wicket or more than 6 runs including extras)
    ball_by_ball_df = ball_by_ball_df.withColumn(
        "high_impact",
        when((col("runs_scored") + col("extra_runs") > 6) | (col("bowler_wicket") == True), True).otherwise(False)
    )

    ball_by_ball_df.select("match_id", "innings_no", "over_id","running_total_runs", "high_impact").where(col("high_impact")=="true").distinct().show(100)
    # ball_by_ball_df.select("match_id", "innings_no", "over_id", "running_total_runs").where(col("match_id")==598028).distinct().show(40)

    # # Extracting year, month, and day from the match date for more detailed time-based analysis
    # match_df = match_df.withColumn("year", year("match_date"))
    # match_df = match_df.withColumn("month", month("match_date"))
    # match_df = match_df.withColumn("day", dayofmonth("match_date"))
    #
    # # High margin win: categorizing win margins into 'high', 'medium', and 'low'
    # match_df = match_df.withColumn(
    #     "win_margin_category",
    #     when(col("win_margin") >= 100, "High")
    #     .when((col("win_margin") >= 50) & (col("win_margin") < 100), "Medium")
    #     .otherwise("Low")
    # )
    #
    # # Analyze the impact of the toss: who wins the toss and the match
    # match_df = match_df.withColumn(
    #     "toss_match_winner",
    #     when(col("toss_winner") == col("match_winner"), "Yes").otherwise("No")
    # )
    #
    # # Show the enhanced match DataFrame
    # match_df.show(2)

    print("Display venue of team where it won most matches")

    # Calculate wins per team-venue combination
    win_counts = match_df.filter(col("match_winner") == col("team1")).distinct() \
        .groupBy("match_winner", "venue_name") \
        .agg(count("match_winner").alias("wins"))\
        .orderBy("match_winner")

    win_counts.show(500)

    # Select venue for the team with most wins
    windowSpecOder = Window.partitionBy("match_winner").orderBy("match_winner")

    max_wins_df = win_counts.withColumn("max_wins", max("wins").over(windowSpecOder))\
                    .filter(col("wins") == col("max_wins"))

    # Select winner and venue
    max_wins_df_result = max_wins_df.drop("wins")
    max_wins_df_result.show(50)

    # Select venue for the team with most wins
    venue_of_most_wins = max_wins_df.select("match_winner", "venue_name").distinct()

    # Display the venue(s)
    venue_of_most_wins.show(200)

