import os
import polars as pl

script_dir = os.path.dirname(os.path.abspath(__file__))
mmr_file = os.path.join(script_dir, "../data/mmr_history.parquet")

df_mmr = pl.read_parquet(mmr_file, columns=["player_score"])
df_mmr = df_mmr.filter(pl.col("player_score") >= 300)

df_mmr = df_mmr.with_columns([
    pl.when(pl.col("player_score") < 600).then(pl.lit("300–599"))
     .when(pl.col("player_score") < 900).then(pl.lit("600–899"))
     .when(pl.col("player_score") < 1200).then(pl.lit("900–1199"))
     .when(pl.col("player_score") < 1500).then(pl.lit("1200–1499"))
     .when(pl.col("player_score") < 1800).then(pl.lit("1500–1799"))
     .when(pl.col("player_score") < 2100).then(pl.lit("1800–2099"))
     .when(pl.col("player_score") < 2400).then(pl.lit("2100–2399"))
     .when(pl.col("player_score") < 2700).then(pl.lit("2400–2699"))
     .when(pl.col("player_score") < 3000).then(pl.lit("2700–2999"))
     .otherwise(pl.lit("3000+"))
     .alias("mmr_bracket")
])
distribution = df_mmr.group_by("mmr_bracket").agg(pl.len().alias("player_count"))
distribution = distribution.with_columns([
    pl.when(pl.col("mmr_bracket") == "300–599").then(1)
     .when(pl.col("mmr_bracket") == "600–899").then(2)
     .when(pl.col("mmr_bracket") == "900–1199").then(3)
     .when(pl.col("mmr_bracket") == "1200–1499").then(4)
     .when(pl.col("mmr_bracket") == "1500–1799").then(5)
     .when(pl.col("mmr_bracket") == "1800–2099").then(6)
     .when(pl.col("mmr_bracket") == "2100–2399").then(7)
     .when(pl.col("mmr_bracket") == "2400–2699").then(8)
     .when(pl.col("mmr_bracket") == "2700–2999").then(9)
     .when(pl.col("mmr_bracket") == "3000+").then(10)
     .alias("bracket_index")
])

total_players = distribution.select(pl.sum("player_count")).item()
distribution = distribution.with_columns([(pl.col("player_count") / total_players * 100).round(2).alias("percentage_of_total")])
distribution = distribution.sort("bracket_index").drop("bracket_index")

print("\nMMR Distribution:")
print(distribution)
